package com.bytetenns.backupnode.server;

import com.bytetenns.backupnode.config.BackupNodeConfig;
import com.bytetenns.common.network.NetServer;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;

/**
 * @Author jiaoyuliang
 * @Description BackupNode的服务端
 * @Date 2022/8/17
 */
@Slf4j
public class BackupNodeServer {

    // BN配置文件
    private BackupNodeConfig backupNodeConfig;

    private NetServer netServer;

    public BackupNodeServer(BackupNodeConfig backupNodeConfig) {
        this.backupNodeConfig = backupNodeConfig;
    }

    /**
     * 启动并绑定端口
     * @throws InterruptedException 中断异常
     */
    public void start() throws InterruptedException {
        netServer.addHandlers(Collections.singletonList(new AwareConnectHandler()));
        netServer.bind(backupNodeConfig.getBackupNodePort());
    }

    public void shutdown() {
        this.netServer.shutdown();
    }

}
