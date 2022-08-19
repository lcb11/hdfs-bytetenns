package com.bytetenns.datanode.namenode;

import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import com.bytetenns.datanode.file.FileInfo;
import com.bytetenns.datanode.netty.NettyPacket;
import com.bytetenns.datanode.enums.PacketType;
// import com.bytetenns.common.ha.BackupNodeManager;
import com.bytetenns.datanode.network.NetClient;
import com.bytetenns.datanode.network.RequestWrapper;
import com.bytetenns.datanode.utils.DefaultScheduler;
import com.bytetenns.datanode.conf.DataNodeConfig;
import com.bytetenns.datanode.replica.PeerDataNodes;
import com.bytetenns.datanode.replica.ReplicateManager;
import com.bytetenns.datanode.storage.StorageInfo;
import com.bytetenns.datanode.storage.StorageManager;
import com.bytetenns.model.backup.BackupNodeInfo;
import com.bytetenns.model.datanode.*;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.ScheduledFuture;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 *
 * @Author gongwei
 * @Description 负责跟NameNode进行通讯
 * @Date 2022/8/16
 * @Param
 * @return
 **/

@Slf4j
public class NameNodeClient {
  private final ReplicateManager replicateManager;
  private final DefaultScheduler defaultScheduler;
  private final BackupNodeManager backupNodeManager;
  private final StorageManager storageManager;
  private NetClient netClient;
  private final DataNodeConfig datanodeConfig;
  private ScheduledFuture<?> scheduledFuture;

  public NameNodeClient(StorageManager storageManager, DataNodeConfig datanodeConfig, DefaultScheduler defaultScheduler,
      PeerDataNodes peerDataNodes) {
    this.netClient = new NetClient("DataNode-NameNode-" + datanodeConfig.getNameNodeAddr(), defaultScheduler);
    this.datanodeConfig = datanodeConfig;
    this.storageManager = storageManager;
    this.defaultScheduler = defaultScheduler;
    this.backupNodeManager = new BackupNodeManager(defaultScheduler);
    this.replicateManager = new ReplicateManager(defaultScheduler, peerDataNodes, storageManager, this);
    peerDataNodes.setNameNodeClient(this);
  }

  /**
   * 启动NameNode连接的客户端
   */
  public void start() {
    this.netClient.addNettyPackageListener(this::onReceiveMessage);
    this.netClient.addConnectListener(connected -> {
      if (connected) {
        fetchBackupInfo();
        register();
      } else {
        if (scheduledFuture != null) {
          scheduledFuture.cancel(true);
          scheduledFuture = null;
        }
      }
    });
    this.netClient.addNetClientFailListener(() -> {
      log.info("DataNode检测到NameNode挂了，标记NameNode已宕机");
      backupNodeManager.markNameNodeDown();
    });
    this.netClient.connect(datanodeConfig.getNameNodeAddr(), datanodeConfig.getNameNodePort());
  }

  /**
   * DatNode节点注册
   */
  private void register() throws InterruptedException {
    StorageInfo storageInfo = storageManager.getStorageInfo();
    RegisterRequest request = RegisterRequest.newBuilder()
        .setHostname(datanodeConfig.getDataNodeTransportAddr())
        .setNioPort(datanodeConfig.getDataNodeTransportPort())
        .setHttpPort(datanodeConfig.getDataNodeHttpPort())
        .setStoredDataSize(storageInfo.getStorageSize())
        .setFreeSpace(storageInfo.getFreeSpace())
        .setNodeId(datanodeConfig.getDataNodeId())
        .build();
    NettyPacket nettyPacket = NettyPacket.buildPacket(request.toByteArray(),
        PacketType.DATA_NODE_REGISTER);
    log.info("DataNode发起注册请求: {}", request.getHostname());
    netClient.send(nettyPacket);
  }

  private void onReceiveMessage(RequestWrapper requestWrapper) throws Exception {
    PacketType packetType = PacketType.getEnum(requestWrapper.getRequest().getPacketType());
    switch (packetType) {
      case FETCH_BACKUP_NODE_INFO:
        handleFetchBackupNodeInfoResponse(requestWrapper);
        break;
      case DATA_NODE_REGISTER:
        handleDataNodeRegisterResponse(requestWrapper);
        break;
      case HEART_BRET:
        handleDataNodeHeartbeatResponse(requestWrapper);
        break;
      default:
        break;
    }
  }

  private void handleDataNodeHeartbeatResponse(RequestWrapper requestWrapper) throws Exception {
    if (requestWrapper.getRequest().isError()) {
      log.warn("心跳失败, 重新发起注册请求：[error={}]", requestWrapper.getRequest().getError());
      register();
      return;
    }
    HeartbeatResponse heartbeatResponse = HeartbeatResponse.parseFrom(requestWrapper.getRequest().getBody());
    handleCommand(heartbeatResponse);
  }

  private void handleDataNodeRegisterResponse(RequestWrapper requestWrapper) {
    ChannelHandlerContext ctx = requestWrapper.getCtx();
    if (scheduledFuture == null) {
      log.info("开启定时任务发送心跳，心跳间隔为: [interval={}ms]", datanodeConfig.getHeartbeatInterval());
      scheduledFuture = ctx.executor().scheduleAtFixedRate(new HeartbeatTask(ctx, datanodeConfig),
          0, datanodeConfig.getHeartbeatInterval(), TimeUnit.MILLISECONDS);
    }
    if (requestWrapper.getRequest().isSuccess()) {
      StorageInfo storageInfo = storageManager.getStorageInfo();
      log.info("注册成功，发送请求到NameNode进行全量上报存储信息。[size={}]", storageInfo.getFiles().size());
      reportStorageInfo(ctx, storageInfo);
    } else {
      log.info("DataNode重启，不需要全量上报存储信息。");
    }
  }

  private void handleCommand(HeartbeatResponse response) {
    if (response.getCommandsList().isEmpty()) {
      return;
    }
    List<ReplicaCommand> commands = response.getCommandsList();
    replicateManager.addReplicateTask(commands);
  }

  private void handleFetchBackupNodeInfoResponse(RequestWrapper requestWrapper) throws InvalidProtocolBufferException {
    if (requestWrapper.getRequest().getBody().length == 0) {
      log.warn("拉取BackupNode信息为空，设置NetClient为无限重试.");
      netClient.setRetryTime(-1);
      return;
    }
    netClient.setRetryTime(3);
    BackupNodeInfo backupNodeInfo = BackupNodeInfo.parseFrom(requestWrapper.getRequest().getBody());
    backupNodeManager.maybeEstablishConnect(backupNodeInfo, hostname -> {
      datanodeConfig.setNameNodeServers(hostname + ":" + datanodeConfig.getNameNodePort());
      netClient.shutdown();
      netClient = new NetClient("DataNode-NameNode-" + datanodeConfig.getNameNodeAddr(), defaultScheduler);
      log.info("检测到BackupNode升级为NameNode了，替换NameNode链接信息，并重新建立链接：[hostname={}, port={}]",
          datanodeConfig.getNameNodeAddr(), datanodeConfig.getNameNodePort());
      start();
    });
  }

  private void fetchBackupInfo() throws InterruptedException {
    NettyPacket nettyPacket = NettyPacket.buildPacket(new byte[0], PacketType.FETCH_BACKUP_NODE_INFO);
    netClient.send(nettyPacket);
  }

  /**
   * 上报文件副本信息
   *
   * @param fileName 文件名称
   * @param fileSize 文件大小
   */
  public void informReplicaReceived(String fileName, long fileSize) throws InterruptedException {
    InformReplicaReceivedRequest replicaReceivedRequest = InformReplicaReceivedRequest.newBuilder()
        .setFilename(fileName)
        .setHostname(datanodeConfig.getDataNodeTransportAddr())
        .setFileSize(fileSize)
        .build();
    NettyPacket nettyPacket = NettyPacket.buildPacket(replicaReceivedRequest.toByteArray(), PacketType.REPLICA_RECEIVE);
    netClient.send(nettyPacket);
  }

  /**
   * 上报文件副本信息
   *
   * @param fileName 文件名称
   * @param fileSize 文件大小
   */
  public void informReplicaRemoved(String fileName, long fileSize) throws InterruptedException {
    InformReplicaReceivedRequest replicaReceivedRequest = InformReplicaReceivedRequest.newBuilder()
        .setFilename(fileName)
        .setHostname(datanodeConfig.getDataNodeTransportAddr())
        .setFileSize(fileSize)
        .build();
    NettyPacket nettyPacket = NettyPacket.buildPacket(replicaReceivedRequest.toByteArray(), PacketType.REPLICA_REMOVE);
    netClient.send(nettyPacket);
  }

  private void reportStorageInfo(ChannelHandlerContext ctx, StorageInfo storageInfo) {
    List<FileInfo> files = storageInfo.getFiles();
    // 每次最多上传100个文件信息
    if (files.isEmpty()) {
      ReportCompleteStorageInfoRequest request = ReportCompleteStorageInfoRequest.newBuilder()
          .setHostname(datanodeConfig.getDataNodeTransportAddr())
          .setFinished(true)
          .build();
      NettyPacket nettyPacket = NettyPacket.buildPacket(request.toByteArray(), PacketType.REPORT_STORAGE_INFO);
      ctx.writeAndFlush(nettyPacket);
    } else {
      List<List<FileInfo>> partition = Lists.partition(files, 100);
      for (int i = 0; i < partition.size(); i++) {
        List<FileInfo> fileInfos = partition.get(i);
        List<FileMetaInfo> fileMetaInfos = fileInfos.stream()
            .map(e -> FileMetaInfo.newBuilder()
                .setFilename(e.getFileName())
                .setFileSize(e.getFileSize())
                .build())
            .collect(Collectors.toList());
        boolean isFinish = i == partition.size() - 1;
        ReportCompleteStorageInfoRequest.Builder builder = ReportCompleteStorageInfoRequest.newBuilder()
            .setHostname(datanodeConfig.getDataNodeTransportAddr())
            .addAllFileInfos(fileMetaInfos)
            .setFinished(isFinish);
        ReportCompleteStorageInfoRequest request = builder.build();
        NettyPacket nettyPacket = NettyPacket.buildPacket(request.toByteArray(), PacketType.REPORT_STORAGE_INFO);
        ctx.writeAndFlush(nettyPacket);
      }
    }
  }

  /**
   * 停止服务
   */
  public void shutdown() {
    if (netClient != null) {
      netClient.shutdown();
    }
  }
}
