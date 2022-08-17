package com.bytetenns.backupnode.server;

import com.bytetenns.backupnode.config.BackupNodeConfig;
import lombok.extern.slf4j.Slf4j;

/**
 * @Author jiaoyuliang
 * @Description BackupNode的服务端
 * @Date 2022/8/17
 */
@Slf4j
public class BackupNodeServer {

    // BN配置文件
    private BackupNodeConfig backupNodeConfig;

    public BackupNodeServer(BackupNodeConfig backupNodeConfig) {
        this.backupNodeConfig = backupNodeConfig;
    }

}
