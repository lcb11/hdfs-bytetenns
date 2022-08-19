package com.bytetenns.backupnode.client;

import com.bytetenns.backupnode.config.BackupNodeConfig;
import com.bytetenns.backupnode.filesystem.InMemoryNameSystem;
import com.bytetenns.backupnode.fsimage.EditsLogFetcher;
import com.bytetenns.backupnode.fsimage.FsImageCheckPointer;
import com.bytetenns.backupnode.ha.NodeRoleSwitcher;
import com.bytetenns.dfs.model.backup.*;
import com.bytetenns.common.enums.PacketType;
import com.bytetenns.common.exception.RequestTimeoutException;
import com.bytetenns.namenode.NameNodeConfig;
import com.bytetenns.common.netty.NettyPacket;
import com.bytetenns.common.network.NetClient;
import com.bytetenns.common.scheduler.DefaultScheduler;
import com.google.protobuf.InvalidProtocolBufferException;
import com.ruyuan.dfs.model.namenode.UserEntity;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @Author jiaoyuliang
 * @Description 负责和nameNode通讯的客户端
 * @Date 2022/8/17
 */
@Slf4j
public class NameNodeClient {

    //BN配置文件
    private final BackupNodeConfig backupnodeConfig;

    // 基于内存的文件系统
    private final InMemoryNameSystem nameSystem;

    // 调度器
    private final DefaultScheduler defaultScheduler;

    // netty通讯
    private final NetClient netClient;

    private volatile boolean shutdown = false;

    //有参构造
    public NameNodeClient(DefaultScheduler defaultScheduler, BackupNodeConfig backupnodeConfig, InMemoryNameSystem nameSystem) {
        this.netClient = new NetClient("BackupNode-NameNode-" + backupnodeConfig.getNameNodeHostname(), defaultScheduler, 3);
        this.nameSystem = nameSystem;
        this.defaultScheduler = defaultScheduler;
        this.backupnodeConfig = backupnodeConfig;
    }

    /**
     * 和NameNode建立链接
     */
    public void start() {
        this.netClient.addNetClientFailListener(() -> {  //添加连接失败监听器
            shutdown = true;
            log.info("BackupNode检测到NameNode挂了，进行升级为NameNode的步骤...");
            try {
                NodeRoleSwitcher.getInstance().maybeUpgradeToNameNode();
            } catch (Exception e) {
                log.error("NodeRoleSwitcher#maybeUpgradeToNameNode occurs error.", e);
            }
        });
        this.netClient.addConnectListener(connected -> {  //添加连接状态监听器
            if (connected) {
                reportBackupNodeInfo();  //主动往NameNode上报自己的信息
            }
        });
        this.netClient.addNettyPackageListener(requestWrapper -> {  //添加网络包监听器
            NettyPacket request = requestWrapper.getRequest();
            if (request.getPacketType() == PacketType.BACKUP_NODE_SLOT.getValue()) {
                log.info("收到NameNode下发的Slots信息.");
                BackupNodeSlots backupNodeSlots = BackupNodeSlots.parseFrom(request.getBody());
                NodeRoleSwitcher.getInstance().setSlots(backupNodeSlots.getSlotsMap());
            }
        });
        this.netClient.connect(backupnodeConfig.getNameNodeHostname(), backupnodeConfig.getNameNodePort());
        EditsLogFetcher editsLogFetcher = new EditsLogFetcher(backupnodeConfig, this, nameSystem);
        defaultScheduler.schedule("抓取editLog", editsLogFetcher,
                backupnodeConfig.getFetchEditLogInterval(), backupnodeConfig.getFetchEditLogInterval(), TimeUnit.MILLISECONDS);
        FsImageCheckPointer fsImageCheckpointer = new FsImageCheckPointer(this, nameSystem, backupnodeConfig);
        defaultScheduler.schedule("FSImage Checkpoint操作", fsImageCheckpointer,
                backupnodeConfig.getCheckpointInterval(), backupnodeConfig.getCheckpointInterval(), TimeUnit.MILLISECONDS);
    }

    /**
     * 关闭
     */
    public void shutdown() {
        this.netClient.shutdown();
    }

    /**
     * 主动往NameNode上报自己的信息
     */
    private void reportBackupNodeInfo() {
        // 因为这里有阻塞方法，回调中不能阻塞，
        // 否则会导致后面网络收发失败，所以需要新开线程处理
        defaultScheduler.scheduleOnce("往NameNode上报自己的信息", () -> {
            try {
                BackupNodeInfo backupNodeInfo = BackupNodeInfo.newBuilder()
                        .setHostname(backupnodeConfig.getBackupNodeHostname())
                        .setPort(backupnodeConfig.getBackupNodePort())
                        .build();
                NettyPacket req = NettyPacket.buildPacket(backupNodeInfo.toByteArray(), PacketType.REPORT_BACKUP_NODE_INFO);
                log.info("上报BackupNode连接信息：[hostname={}, port={}]", backupnodeConfig.getBackupNodeHostname(),
                        backupnodeConfig.getBackupNodePort());
                NettyPacket nettyPacket = netClient.sendSync(req);
                if (nettyPacket.getPacketType() == PacketType.DUPLICATE_BACKUP_NODE.getValue()) {
                    log.error("该NameNode已经存在一个BackupNode了，出现重复的BackupNode, 程序即将退出...");
                    System.exit(0);
                    return;
                }
                NameNodeConf nameNodeConf = NameNodeConf.parseFrom(nettyPacket.getBody());
                log.info("上报BackupNode连接信息，返回的NameNode配置信息: [values={}]", nameNodeConf.getValuesMap());
                NodeRoleSwitcher.getInstance().setNameNodeConfig(new NameNodeConfig(nameNodeConf));
            } catch (Exception e) {
                log.error("上报BackupNode信息发生错误：", e);
            }
        });
    }

    /**
     * 抓取editLog数据
     *
     * @param txId 当前的txId
     * @return editLog数据
     */
    public List<EditLog> fetchEditsLog(long txId) throws InvalidProtocolBufferException, InterruptedException, RequestTimeoutException {
        boolean hasSlots = NodeRoleSwitcher.getInstance().hasSlots();
        FetchEditsLogRequest request = FetchEditsLogRequest.newBuilder()
                .setTxId(txId)
                .setNeedSlots(!hasSlots)
                .build();
        NettyPacket req = NettyPacket.buildPacket(request.toByteArray(), PacketType.FETCH_EDIT_LOG);
        NettyPacket nettyPacket = netClient.sendSync(req);
        FetchEditsLogResponse response = FetchEditsLogResponse.parseFrom(nettyPacket.getBody());
        List<UserEntity> usersList = response.getUsersList();
        NodeRoleSwitcher.getInstance().replaceUser(usersList.stream()
                .map(User::parse)
                .collect(Collectors.toList()));
        return response.getEditLogsList();
    }

    public DefaultScheduler getDefaultScheduler() {
        return defaultScheduler;
    }

    public NetClient getNetClient() {
        return netClient;
    }

}
