package com.bytetenns.datanode;

import com.bytetenns.datanode.namenode.NameNodeClient;
import com.bytetenns.datanode.replica.PeerDataNodes;
import com.bytetenns.datanode.server.DataNodeServer;
import com.bytetenns.datanode.storage.StorageManager;
// import com.ruyuan.dfs.common.metrics.Prometheus;
import com.bytetenns.datanode.utils.DefaultScheduler;
import com.bytetenns.datanode.conf.DataNodeConfig;
import com.bytetenns.datanode.server.DefaultFileTransportCallback;
import com.bytetenns.datanode.server.DataNodeApis;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 * @Author byte tens
 * @Description dataNode 核心启动类
 * @Date 2022/8/12
 * @Param
 * @return
 **/
@Slf4j
public class DataNode {

    // 同级dataNode
    private PeerDataNodes peerDataNodes;
    // 存储信息管理
    private StorageManager storageManager;
    // 负责跟NameNode进行通讯
    private NameNodeClient nameNodeClient;
    // 同级节点通讯
    private DataNodeServer dataNodeServer;

    // 调度器
    private DefaultScheduler defaultScheduler;
    // 是否启动
    private AtomicBoolean started = new AtomicBoolean(false);

    public static void main(String[] args) {
        if (args == null || args.length == 0) {
            throw new IllegalArgumentException("配置文件不能为空");
        }
        // 1. 初始化配置文件
        DataNodeConfig dataNodeConfig = null;
        try {
            Path path = Paths.get(args[0]);
            try (InputStream inputStream = Files.newInputStream(path)) {
                dataNodeConfig = new DataNodeConfig();
            }
            log.info("DameNode启动配置文件: {}", path.toAbsolutePath());
        } catch (Exception e) {
            log.error("无法加载配置文件 : ", e);
            System.exit(1);
        }
        try {
            DataNode datanode = new DataNode(dataNodeConfig);
            Runtime.getRuntime().addShutdownHook(new Thread(datanode::shutdown));
            datanode.start();
        } catch (Exception e) {
            log.error("启动DataNode失败：", e);
            System.exit(1);
        }
    }

    public DataNode(DataNodeConfig dataNodeConfig) {
        this.defaultScheduler = new DefaultScheduler("DataNode-Scheduler-");
        this.storageManager = new StorageManager(dataNodeConfig);
        DefaultFileTransportCallback defaultFileTransportCallback = new DefaultFileTransportCallback(storageManager);
        DataNodeApis dataNodeApis = new DataNodeApis(dataNodeConfig, defaultFileTransportCallback, defaultScheduler);
        this.peerDataNodes = new PeerDataNodes(defaultScheduler, dataNodeConfig, dataNodeApis);
        this.nameNodeClient = new NameNodeClient(storageManager, dataNodeConfig, defaultScheduler, peerDataNodes);
        this.dataNodeServer = new DataNodeServer(dataNodeConfig, defaultScheduler, storageManager, peerDataNodes,
                dataNodeApis);
    }

    /**
     * 启动DataNode
     *
     * @throws InterruptedException 中断异常
     */
    private void start() throws InterruptedException {
        if (started.compareAndSet(false, true)) {
            this.nameNodeClient.start();
            this.dataNodeServer.start();
        }
    }

    /**
     * 优雅停止
     */
    public void shutdown() {
        if (started.compareAndSet(true, false)) {
            this.defaultScheduler.shutdown();
            this.nameNodeClient.shutdown();
            this.dataNodeServer.shutdown();
        }
    }
}
