package com.bytetenns.backupnode;

import com.bytetenns.backupnode.client.NameNodeClient;
import com.bytetenns.backupnode.config.BackupNodeConfig;
import com.bytetenns.backupnode.filesystem.InMemoryNameSystem;
import com.bytetenns.backupnode.ha.NodeRoleSwitcher;
import com.bytetenns.backupnode.server.BackupNodeServer;
import com.bytetenns.common.scheduler.DefaultScheduler;
import lombok.extern.slf4j.Slf4j;
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

    // 调度器
    private final DefaultScheduler defaultScheduler;

    // 解决多线程的原子性问题 具有排他性，当某个线程进入方法，执行其中的指令时，不会被其他线程打断
    private final AtomicBoolean started = new AtomicBoolean(false);

    public static void main(String[] args) {
        // 1 获取配置信息
        BackupNodeConfig backupNodeConfig = new BackupNodeConfig();

        // 2 启动或关闭程序
        try {
            // 2.1 初始化backupNode启动类
            BackupNode backupNode = new BackupNode(backupNodeConfig);

            NodeRoleSwitcher.getInstance().setBackupNode(backupNode);  //BN升级为NN

            // 2.2 start启动
            backupNode.start();

            // 2.3 shutdown关闭
            Runtime.getRuntime().addShutdownHook(new Thread(backupNode::shutdown));

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
        this.defaultScheduler = new DefaultScheduler("BackupNode-Scheduler-");
        this.nameSystem = new InMemoryNameSystem(backupNodeConfig);
        this.nameNodeClient = new NameNodeClient(defaultScheduler,backupNodeConfig, nameSystem);
        this.backupNodeServer = new BackupNodeServer(defaultScheduler,backupNodeConfig);
    }

    /**
     * 启动BackupNode
     * @throws Exception
     */
    public void start() throws Exception {
        if (started.compareAndSet(false, true)) {
            this.nameSystem.recoveryNamespace();
            this.nameNodeClient.start();
            this.backupNodeServer.start();
        }
    }

    /**
     * 关闭BackupNode
     */
    public void shutdown() {
        if (started.compareAndSet(true, false)) {
            this.defaultScheduler.shutdown();
            this.nameNodeClient.shutdown();
            this.backupNodeServer.shutdown();
        }
    }

    // 获取当前文件处理 对象
    public InMemoryNameSystem getNameSystem() {
        return this.nameSystem;
    }

}
