package com.bytetenns.backupnode.filesystem;

import com.bytetenns.backupnode.config.BackupNodeConfig;
import lombok.extern.slf4j.Slf4j;

/**
 * @Author jiaoyuliang
 * @Description 基于内存的文件系统
 * @Date 2022/8/17
 */
@Slf4j
public class InMemoryNameSystem {

    // 引入BN配置文件
    private BackupNodeConfig backupNodeConfig;

    // 有参构造
    public InMemoryNameSystem(BackupNodeConfig backupNodeConfig) {
        this.backupNodeConfig = backupNodeConfig;
    }

}
