package com.bytetenns.datanode.server;

import com.bytetenns.datanode.network.NetServer;
import com.bytetenns.datanode.utils.DefaultScheduler;
import com.bytetenns.datanode.conf.DataNodeConfig;
import com.bytetenns.datanode.replica.PeerDataNodes;
import lombok.extern.slf4j.Slf4j;
import com.google.protobuf.InvalidProtocolBufferException;
import com.bytetenns.datanode.netty.NettyPacket;
import com.bytetenns.datanode.enums.PacketType;
// import com.bytetenns.datanode.common.metrics.Prometheus;
import com.bytetenns.datanode.network.AbstractChannelHandler;
import com.bytetenns.datanode.network.RequestWrapper;
import com.bytetenns.datanode.network.file.DefaultFileSendTask;
import com.bytetenns.datanode.network.file.FilePacket;
import com.bytetenns.datanode.network.file.FileReceiveHandler;
import com.bytetenns.datanode.namenode.NameNodeClient;
import com.bytetenns.datanode.model.common.GetFileRequest;
import com.bytetenns.datanode.model.datanode.PeerNodeAwareRequest;
import com.bytetenns.datanode.storage.StorageManager;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.SocketChannel;

// import com.bytetenns.dfs.common.metrics.MetricsHandler;
import com.bytetenns.datanode.network.BaseChannelInitializer;
import com.bytetenns.datanode.server.HttpFileServerHandler;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.stream.ChunkedWriteHandler;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import java.util.Arrays;
import java.util.Collections;

/**
 * 用于上传、下载文件的Netty服务端
 *
 * @author gongwei
 */

@Slf4j
public class DataNodeServer {

  private DataNodeApis dataNodeApis;
  private StorageManager storageManager;
  private NetServer netServer;
  private DataNodeConfig dataNodeConfig;
  private PeerDataNodes peerDataNodes;

  public DataNodeServer(DataNodeConfig dataNodeConfig, DefaultScheduler defaultScheduler, StorageManager storageManager,
      PeerDataNodes peerDataNodes, DataNodeApis dataNodeApis) {
    this.dataNodeConfig = dataNodeConfig;
    this.peerDataNodes = peerDataNodes;
    this.storageManager = storageManager;
    this.dataNodeApis = dataNodeApis;
    this.netServer = new NetServer("DataNode-Server", defaultScheduler, dataNodeConfig.getDataNodeWorkerThreads());
  }

  /**
   * 启动
   */
  public void start() throws InterruptedException {
    // 用于接收PeerDataNode发过来的通知信息
    MultiPortChannelInitializer multiPortChannelInitializer = new MultiPortChannelInitializer(dataNodeConfig,
        storageManager);
    multiPortChannelInitializer.addHandlers(Collections.singletonList(dataNodeApis));
    this.netServer.setChannelInitializer(multiPortChannelInitializer);
    this.netServer.bind(Arrays.asList(dataNodeConfig.getDataNodeTransportPort(), dataNodeConfig.getDataNodeHttpPort()));
  }

  /**
   * 优雅停止
   */
  public void shutdown() {
    log.info("Shutdown DataNodeServer");
    this.netServer.shutdown();
    this.peerDataNodes.shutdown();
  }

  /**
   * DataNode节点的服务端接口
   *
   * @author Sun Dasheng
   */

  public class DataNodeApis extends AbstractChannelHandler {

    private DefaultScheduler defaultScheduler;
    private DefaultFileTransportCallback transportCallback;
    private DataNodeConfig dataNodeConfig;
    private FileReceiveHandler fileReceiveHandler;
    private PeerDataNodes peerDataNodes;

    public DataNodeApis(DataNodeConfig dataNodeConfig, DefaultFileTransportCallback transportCallback,
        DefaultScheduler defaultScheduler) {
      this.dataNodeConfig = dataNodeConfig;
      this.transportCallback = transportCallback;
      this.defaultScheduler = defaultScheduler;
      // 负责接收文件: 1、客户端上传文件； 2、当前DataNode从其他DataNode中同步文件副本
      this.fileReceiveHandler = new FileReceiveHandler(transportCallback);
    }

    public void setPeerDataNodes(PeerDataNodes peerDataNodes) {
      this.peerDataNodes = peerDataNodes;
    }

    public void setNameNodeClient(NameNodeClient nameNodeClient) {
      this.transportCallback.setNameNodeClient(nameNodeClient);
    }

    @Override
    protected Set<Integer> interestPackageTypes() {
      return new HashSet<>();
    }

    @Override
    protected boolean handlePackage(ChannelHandlerContext ctx, NettyPacket request) throws Exception {
      PacketType packetType = PacketType.getEnum(request.getPacketType());
      RequestWrapper requestWrapper = new RequestWrapper(ctx, request);
      switch (packetType) {
        case TRANSFER_FILE:
          handleFileTransferRequest(requestWrapper);
          break;
        case GET_FILE:
          handleGetFileRequest(requestWrapper);
          break;
        case DATA_NODE_PEER_AWARE:
          handleDataNodePeerAwareRequest(requestWrapper);
          break;
        default:
          break;
      }
      return true;
    }

    private void handleDataNodePeerAwareRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException {
      NettyPacket request = requestWrapper.getRequest();
      PeerNodeAwareRequest peerNodeAwareRequest = PeerNodeAwareRequest.parseFrom(request.getBody());
      peerDataNodes.addPeerNode(peerNodeAwareRequest.getPeerDataNode(), peerNodeAwareRequest.getDataNodeId(),
          (SocketChannel) requestWrapper.getCtx().channel(), dataNodeConfig.getDataNodeId());
    }

    /**
     * 处理客户端或PeerDataNode过来下载文件的请求
     */
    private void handleGetFileRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException {
      GetFileRequest request = GetFileRequest.parseFrom(requestWrapper.getRequest().getBody());
      try {
        String filename = request.getFilename();
        String path = transportCallback.getPath(filename);
        File file = new File(path);
        log.info("收到下载文件请求：{}", filename);
        Prometheus.incCounter("datanode_get_file_count", "DataNode收到的下载文件请求数量");
        Prometheus.hit("datanode_get_file_qps", "DataNode瞬时下载文件QPS");
        DefaultFileSendTask fileSendTask = new DefaultFileSendTask(file, filename,
            (SocketChannel) requestWrapper.getCtx().channel(), (total, current, progress,
                currentReadBytes) -> Prometheus.hit("datanode_disk_read_bytes", "DataNode瞬时读磁盘字节大小", currentReadBytes));
        fileSendTask.execute(false);
      } catch (Exception e) {
        log.error("文件下载失败：", e);
      }
    }

    /**
     * 处理文件传输,客户端上传文件
     */
    private void handleFileTransferRequest(RequestWrapper requestWrapper) {
      FilePacket filePacket = FilePacket.parseFrom(requestWrapper.getRequest().getBody());
      if (filePacket.getType() == FilePacket.HEAD) {
        Prometheus.incCounter("datanode_put_file_count", "DataNode收到的上传文件请求数量");
        Prometheus.hit("datanode_put_file_qps", "DataNode瞬时上传文件QPS");
      }
      fileReceiveHandler.handleRequest(filePacket);
    }
  }

  /**
   * 绑定多端口的渠道处理器
   *
   * @author Sun Dasheng
   */
  public class MultiPortChannelInitializer extends BaseChannelInitializer {

    private StorageManager storageManager;
    private DataNodeConfig dataNodeConfig;

    public MultiPortChannelInitializer(DataNodeConfig dataNodeConfig, StorageManager storageManager) {
      this.dataNodeConfig = dataNodeConfig;
      this.storageManager = storageManager;
    }

    @Override
    protected void initChannel(SocketChannel ch) {
      int localPort = ch.localAddress().getPort();
      if (localPort == dataNodeConfig.getDataNodeTransportPort()) {
        super.initChannel(ch);
      } else if (localPort == dataNodeConfig.getDataNodeHttpPort()) {
        ch.pipeline().addLast(new HttpServerCodec());
        ch.pipeline().addLast(new HttpObjectAggregator(65536));
        ch.pipeline().addLast(new ChunkedWriteHandler());
        ch.pipeline().addLast(new HttpFileServerHandler(storageManager));
      }
    }
  }

}
