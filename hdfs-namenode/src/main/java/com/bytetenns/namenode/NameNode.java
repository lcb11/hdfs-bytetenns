package com.bytetenns.namenode;

import com.bytetenns.common.scheduler.DefaultScheduler;
import com.bytetenns.namenode.datanode.DataNodeManager;
import com.bytetenns.namenode.fs.DiskNameSystem;
import com.bytetenns.namenode.server.NameNodeApis;
import com.bytetenns.namenode.server.NameNodeServer;
import com.bytetenns.namenode.shard.ShardingManager;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
  * @Author lcb
  * @Description NameNode核心启动类
  * @Date 2022/8/10
  * @Param
  * @return
  **/
@Slf4j
public class NameNode {
    private final NameNodeApis nameNodeApis;//网络请求接口处理器
    private final DefaultScheduler defaultScheduler;//调度器
    private final DataNodeManager dataNodeManager;//负责管理dataNode
    private final ShardingManager shardingManager;//负责元数据分片的组件
    private final AtomicBoolean started = new AtomicBoolean(false);//原子布尔类
    private final DiskNameSystem diskNameSystem;//负责管理文件系统元数据的组件 落地磁盘的实现
    private final NameNodeServer nameNodeServer;//NameNode对外提供服务接口

    public static void main(String[] args) {
        NameNodeConfig nameNodeConfig=new NameNodeConfig();
        try {
            NameNode namenode = new NameNode(nameNodeConfig);
            /*
             * @Author lcb
             * @Description addShutdownHook方法：Registers a new virtual-machine shutdown hook.
             *              getRuntime()方法：Returns the runtime object associated with the current Java application.
             * @Date 2022/8/2
             **/
            Runtime.getRuntime().addShutdownHook(new Thread(namenode::shutdown));
            namenode.start();//开启namenode线程
        } catch (Exception e) {
            log.error("启动NameNode失败：", e);
            System.exit(1);
        }
    }

    public NameNode(NameNodeConfig nameNodeConfig) {
        this.defaultScheduler = new DefaultScheduler("NameNode-Scheduler-");
        this.dataNodeManager = new DataNodeManager(nameNodeConfig, defaultScheduler);
        this.diskNameSystem = new DiskNameSystem(nameNodeConfig, defaultScheduler, dataNodeManager);
        this.shardingManager = new ShardingManager(nameNodeConfig);
        this.nameNodeApis = new NameNodeApis(diskNameSystem.getNameNodeConfig(), dataNodeManager,
                shardingManager, diskNameSystem, defaultScheduler);
        this.nameNodeServer = new NameNodeServer(defaultScheduler, diskNameSystem, nameNodeApis);
    }

    /**
     * 启动
     *
     * @throws Exception 中断异常
     */
    public void start() throws Exception {
        //compareAndSet()：以原子的方式设置指定的跟新值
        //CAS:自旋锁
        if (started.compareAndSet(false, true)) {
            //diskNameSystem：负责管理文件系统元数据的组件 落地磁盘的实现
            //recoveryNamespace()：基于本地文件恢复元数据空间
            this.diskNameSystem.recoveryNamespace();
            this.shardingManager.start();
            //this.tomcatServer.start();
            this.nameNodeServer.start();
        }
    }

    /**
     * 优雅停机
     */
    public void shutdown() {
        if (started.compareAndSet(true, false)) {
            this.defaultScheduler.shutdown();
            this.diskNameSystem.shutdown();
            this.nameNodeServer.shutdown();
        }
    }
}
