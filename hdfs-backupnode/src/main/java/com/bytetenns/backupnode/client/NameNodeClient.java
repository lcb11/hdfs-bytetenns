package com.bytetenns.backupnode.client;

import com.bytetenns.backupnode.config.BackupNodeConfig;
import com.bytetenns.backupnode.filesystem.InMemoryNameSystem;
import lombok.extern.slf4j.Slf4j;

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

    //有参构造
    public NameNodeClient(BackupNodeConfig backupnodeConfig, InMemoryNameSystem nameSystem) {
        this.nameSystem = nameSystem;
        this.backupnodeConfig = backupnodeConfig;
    }

}
