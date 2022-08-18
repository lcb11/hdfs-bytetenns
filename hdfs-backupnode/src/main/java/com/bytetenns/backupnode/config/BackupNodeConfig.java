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
    private String baseDir = "/bytedance/dfs/backupnode";

    // 每次拉取editslog的间隔
    private long fetchEditLogInterval = 5000;

    // 每次拉取editslog的数量
    private int fetchEditLogSize = 10;

    // checkpoint操作的时间间隔, 默认60分钟
    private long checkpointInterval = 3600000;

    // namenode地址
    private String nameNodeServer = "localhost:2341";

    // backupNode地址
    private String backupNodeServer = "localhost:12341";

}
