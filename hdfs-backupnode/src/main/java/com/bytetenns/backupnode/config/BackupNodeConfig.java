package com.bytetenns.backupnode.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.File;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    private static final Pattern PATTERN = Pattern.compile("(\\S+):(\\S+)");
    public static final String FS_IMAGE_NAME = "fsimage-%s";

    // 本地文件存放路径
    private String baseDir = "/bytedance/dfs/backupnode";

    // 每次拉取editslog的间隔
    private long fetchEditLogInterval = 5000;

    // 每次拉取editslog的数量
    private int fetchEditLogSize = 10;

    // checkpoint操作的时间间隔, 默认60分钟
    private long checkpointInterval = 3600000;

    // namenode地址
    private String nameNodeServer = "localhost:2345";

    // backupNode地址
    private String backupNodeServer = "localhost:12341";
    public String getNameNodeHostname() {
        Matcher matcher = PATTERN.matcher(nameNodeServer);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return "";
    }

    // 获取服务地址
    public String getBackupNodeHostname() {
        Matcher matcher = PATTERN.matcher(backupNodeServer);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return "";
    }

    // 获取IP
    public int getBackupNodePort() {
        Matcher matcher = PATTERN.matcher(backupNodeServer);
        if (matcher.find()) {
            return Integer.parseInt(matcher.group(2));
        }
        return 0;
    }

    // 获取nameNode端口
    public int getNameNodePort() {
        Matcher matcher = PATTERN.matcher(nameNodeServer);
        if (matcher.find()) {
            return Integer.parseInt(matcher.group(2));
        }
        return 0;
    }

    public String getFsImageFile(String time) {
        return baseDir + File.separator + String.format(FS_IMAGE_NAME, time);
    }

}
