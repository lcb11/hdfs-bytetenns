package com.bytetenns.namenode.server;

import com.bytetenns.common.network.NetServer;
import com.bytetenns.common.scheduler.DefaultScheduler;
import com.bytetenns.namenode.fs.DiskNameSystem;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;

/**
 *
   * @Author byte tens
   * @Description  namenode对外提供服务
   * @Date 2022/8/12
   * @Param
   * @return
   **/
@Slf4j
public class NameNodeServer {
    private NameNodeApis nameNodeApis;
    private DiskNameSystem diskNameSystem;
    private NetServer netServer;

    public NameNodeServer(DefaultScheduler defaultScheduler, DiskNameSystem diskNameSystem, NameNodeApis nameNodeApis) {
        this.diskNameSystem = diskNameSystem;
        this.nameNodeApis = nameNodeApis;
        this.netServer = new NetServer("NameNode-Server", defaultScheduler);
    }

    /**
     * 启动一个Socket Server，监听指定的端口号
     */
    public void start() throws InterruptedException {
        this.netServer.addHandlers(Collections.singletonList(nameNodeApis));
        netServer.bind(diskNameSystem.getNameNodeConfig().getPort());
    }


    /**
     * 停止服务
     */
    public void shutdown() {
        log.info("Shutdown NameNodeServer.");
        netServer.shutdown();
    }
}
