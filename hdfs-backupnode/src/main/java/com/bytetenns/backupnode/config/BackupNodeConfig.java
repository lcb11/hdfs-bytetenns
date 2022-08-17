package com.bytetenns.backupnode.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author: jiaoyuliang
 * @Description: backupnode配置文件
 * @Date: 2022/08/17
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class BackupNodeConfig {

    // 本地文件存放路径
    private String baseDir;

    // 每次拉取editslog的间隔
    private long fetchEditLogInterval;

    // 每次拉取editslog的数量
    private int fetchEditLogSize;

    // checkpoint操作的时间间隔, 默认60分钟
    private long checkpointInterval;

    // namenode地址
    private String nameNodeServer;

    // backupNode地址
    private String backupNodeServer;
}
