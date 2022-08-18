package com.bytetenns.backupnode;

import com.bytetenns.backupnode.client.NameNodeClient;
import com.bytetenns.backupnode.config.BackupNodeConfig;
import com.bytetenns.backupnode.filesystem.InMemoryNameSystem;
import com.bytetenns.backupnode.server.BackupNodeServer;
import lombok.extern.slf4j.Slf4j;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Author jiaoyuliang
 * @Description 负责同步edits log线程|namenode 备份节点
 * @Date 2022/8/17
 */
@Slf4j
public class BackupNode {

    // 引入 文件管理
    private final InMemoryNameSystem nameSystem;

    // 作为NameNode的客户端
    private final NameNodeClient nameNodeClient;

    // 作为服务端
    private final BackupNodeServer backupNodeServer;

    // 解决多线程的原子性问题 具有排他性，当某个线程进入方法，执行其中的指令时，不会被其他线程打断
    private final AtomicBoolean started = new AtomicBoolean(false);

    public static void main(String[] args) {
        // 1 获取配置信息
        BackupNodeConfig backupNodeConfig = new BackupNodeConfig();

        // 2 启动或关闭程序
        try {
            // 2.1 初始化backupNode启动类
            BackupNode backupNode = new BackupNode(backupNodeConfig);
            // 2.2 shutdown关闭
            Runtime.getRuntime().addShutdownHook(new Thread(backupNode::shutdown));
            // 2.3 start启动
            backupNode.start();
        } catch (Exception e) {
            log.error("BackupNode启动失败：", e);
            System.exit(1); //启动失败
        }
    }

    /**
     * 初始化BackupNode  有参构造
     * @param backupNodeConfig
     */
    public BackupNode(BackupNodeConfig backupNodeConfig) {
        this.nameSystem = new InMemoryNameSystem(backupNodeConfig);
        this.nameNodeClient = new NameNodeClient(backupNodeConfig, nameSystem);
        this.backupNodeServer = new BackupNodeServer(backupNodeConfig);
    }

    /**
     * 启动BackupNode
     * @throws Exception
     */
    private void start() throws Exception {
        if (started.compareAndSet(false, true)) {
            this.nameSystem.recoveryNamespace();
        }
    }

    /**
     * 关闭BackupNode
     */
    public void shutdown() {

    }

}
