package com.bytetenns.backupnode.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.Properties;

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

    /**
     * 将properties格式的hashtable 转化 为NameNodeConfig中的属性
     * @param properties
     * @return
     */
    public static BackupNodeConfig parse(Properties properties) {
        String baseDir = (String) properties.get("base.dir");
        long fetchEditLogInterval = Integer.parseInt((String) properties.get("fetch.editslog.interval"));
        int fetchEditLogSize = Integer.parseInt((String) properties.get("fetch.editslog.size"));
        long checkpointInterval = Long.parseLong((String) properties.get("checkpoint.interval"));
        String nameNodeServer = (String) properties.get("namenode.server");
        String backupNodeServer = (String) properties.get("backupnode.server");
        return BackupNodeConfig.builder()
                .baseDir(baseDir)
                .fetchEditLogInterval(fetchEditLogInterval)
                .fetchEditLogSize(fetchEditLogSize)
                .checkpointInterval(checkpointInterval)
                .nameNodeServer(nameNodeServer)
                .backupNodeServer(backupNodeServer)
                .build();
    }

}
