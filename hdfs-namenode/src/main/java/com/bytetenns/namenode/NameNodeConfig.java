package com.bytetenns.namenode;

import com.bytetenns.dfs.model.backup.NameNodeConf;
import com.bytetenns.common.enums.NameNodeLaunchMode;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 *
   * @Author lcb
   * @Description nameNode 配置信息
   * @Date 2022/8/11
   * @Param
   * @return
   **/
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NameNodeConfig {
    /**
     * 默认的文件目录
     */
    private final String DEFAULT_BASEDIR = "/bytetenns/hdfs/namenode";

    /**
     * 默认监听的端口号
     */
    private final int DEFAULT_PORT = 2345;
    /**
     * 默认EditLog Buffer刷磁盘的阈值
     */
    private final int DEFAULT_EDITLOG_FLUSH_THRESHOLD = 524288;//512*1024
    /**
     * 默认DataNode心跳超时的阈值
     */
    private final int DEFAULT_DATANODE_HEARTBEAT_TIMEOUT = 90000;//150*60
    /**
     * 默认副本数量
     */
    private final int DEFAULT_REPLICA_NUM = 2;
    /**
     * 默认检查DataNode是否心跳超时的时间间隔
     */
    private final int DEFAULT_DATANODE_ALIVE_CHECK_INTERVAL = 30000;//50*60


    private String baseDir;//默认的文件目录
    private int port;//默认监听的端口号   DEFAULT_PORT + ""
    private int editLogFlushThreshold;//默认EditLog Buffer刷磁盘的阈值
    private long dataNodeHeartbeatTimeout;//默认DataNode心跳超时的阈值
    private int replicaNum;//默认副本数量
    private long dataNodeAliveCheckInterval;//默认检查DataNode是否心跳超时的时间间隔
    private String nameNodePeerServers;//nameNodeConf.getValuesOrThrow("nameNodePeerServers");通过ptotobuf序列化所得的一个文件
    private int nameNodeId;
    private String nameNodeLaunchMode;//nameNode启动模块
    private int httpPort;
    private long nameNodeTrashCheckInterval;//垃圾检查间隔
    private long nameNodeTrashClearThreshold;//垃圾清理条件
    private int nameNodeApiCoreSize;//namenode处理线程池相关||核心线程数量
    private int nameNodeApiMaximumPoolSize;//线程池最大线程数量
    private int nameNodeApiQueueSize;//线程队列大小

    public NameNodeConfig(NameNodeConf nameNodeConf) {//NameNodeConf通过protobuf生成的一个序列化文件
        this.baseDir = nameNodeConf.getValuesOrDefault("baseDir", DEFAULT_BASEDIR);
        this.port = Integer.parseInt(nameNodeConf.getValuesOrDefault("port", DEFAULT_PORT + ""));
        this.editLogFlushThreshold = Integer.parseInt(nameNodeConf.getValuesOrDefault("editLogFlushThreshold",
                DEFAULT_EDITLOG_FLUSH_THRESHOLD + ""));
        this.dataNodeHeartbeatTimeout = Integer.parseInt(nameNodeConf.getValuesOrDefault("dataNodeHeartbeatTimeout",
                DEFAULT_DATANODE_HEARTBEAT_TIMEOUT + ""));
        this.replicaNum = Integer.parseInt(nameNodeConf.getValuesOrDefault("replicaNum",
                DEFAULT_REPLICA_NUM + ""));
        this.dataNodeAliveCheckInterval = Integer.parseInt(nameNodeConf.getValuesOrDefault("dataNodeAliveCheckInterval",
                DEFAULT_DATANODE_ALIVE_CHECK_INTERVAL + ""));
        this.nameNodePeerServers = nameNodeConf.getValuesOrThrow("nameNodePeerServers");
        this.nameNodeId = Integer.parseInt(nameNodeConf.getValuesOrThrow("nameNodeId"));
        this.nameNodeLaunchMode = nameNodeConf.getValuesOrThrow("nameNodeLaunchMode");
        this.httpPort = Integer.parseInt(nameNodeConf.getValuesOrThrow("httpPort"));
        this.nameNodeTrashCheckInterval = Integer.parseInt(nameNodeConf.getValuesOrThrow("nameNodeTrashCheckInterval"));
        this.nameNodeTrashClearThreshold = Integer.parseInt(nameNodeConf.getValuesOrThrow("nameNodeTrashClearThreshold"));
        this.nameNodeApiCoreSize = Integer.parseInt(nameNodeConf.getValuesOrThrow("nameNodeApiCoreSize"));
        this.nameNodeApiMaximumPoolSize = Integer.parseInt(nameNodeConf.getValuesOrThrow("nameNodeApiMaximumPoolSize"));
        this.nameNodeApiQueueSize = Integer.parseInt(nameNodeConf.getValuesOrThrow("nameNodeApiQueueSize"));
    }

    public Map<String, String> getConfig() {
        Map<String, String> ret = new HashMap<>(8);
        ret.put("baseDir", baseDir);
        ret.put("port", port + "");
        ret.put("editLogFlushThreshold", editLogFlushThreshold + "");
        ret.put("dataNodeHeartbeatTimeout", dataNodeHeartbeatTimeout + "");
        ret.put("replicaNum", replicaNum + "");
        ret.put("dataNodeAliveCheckInterval", dataNodeAliveCheckInterval + "");
        ret.put("nameNodePeerServers", nameNodePeerServers + "");
        ret.put("nameNodeId", nameNodeId + "");
        ret.put("nameNodeLaunchMode", nameNodeLaunchMode);
        ret.put("httpPort", httpPort + "");
        ret.put("nameNodeTrashCheckInterval", nameNodeTrashCheckInterval + "");
        ret.put("nameNodeTrashClearThreshold", nameNodeTrashClearThreshold + "");
        ret.put("nameNodeApiCoreSize", nameNodeApiCoreSize + "");
        ret.put("nameNodeApiMaximumPoolSize", nameNodeApiMaximumPoolSize + "");
        ret.put("nameNodeApiQueueSize", nameNodeApiQueueSize + "");
        return ret;
    }


    public static NameNodeConfig parse(Properties properties) {
        String baseDir = (String) properties.get("base.dir");
        int port = Integer.parseInt((String) properties.get("port"));
        int editLogFlushThreshold = Integer.parseInt((String) properties.get("editlogs.flush.threshold"));
        long dataNodeHeartbeatTimeout = Long.parseLong((String) properties.get("datanode.heartbeat.timeout"));
        int replicaNum = Integer.parseInt((String) properties.get("rebalance.num"));
        long dataNodeAliveCheckInterval = Integer.parseInt((String) properties.get("datanode.alive.check.interval"));
        String nameNodePeerServers = (String) properties.get("namenode.peer.servers");
        int nameNodeId = Integer.parseInt((String) properties.get("namenode.id"));
        String nameNodeLaunchMode = (String) properties.get("namenode.launch.mode");
        int httpPort = Integer.parseInt((String) properties.get("http.port"));
        long nameNodeTrashCheckInterval = Integer.parseInt((String) properties.get("namenode.trash.check.interval"));
        long nameNodeTrashClearThreshold = Integer.parseInt((String) properties.get("namenode.trash.clear.threshold"));
        int nameNodeApiCoreSize = Integer.parseInt((String) properties.get("namenode.api.coreSize"));
        int nameNodeApiMaximumPoolSize = Integer.parseInt((String) properties.get("namenode.api.maximumPoolSize"));
        int nameNodeApiQueueSize = Integer.parseInt((String) properties.get("namenode.api.queueSize"));
        return NameNodeConfig.builder()
                .baseDir(baseDir)
                .port(port)
                .editLogFlushThreshold(editLogFlushThreshold)
                .dataNodeHeartbeatTimeout(dataNodeHeartbeatTimeout)
                .replicaNum(replicaNum)
                .dataNodeAliveCheckInterval(dataNodeAliveCheckInterval)
                .nameNodePeerServers(nameNodePeerServers)
                .nameNodeId(nameNodeId)
                .nameNodeLaunchMode(nameNodeLaunchMode)
                .httpPort(httpPort)
                .nameNodeTrashCheckInterval(nameNodeTrashCheckInterval)
                .nameNodeTrashClearThreshold(nameNodeTrashClearThreshold)
                .nameNodeApiCoreSize(nameNodeApiCoreSize)
                .nameNodeApiMaximumPoolSize(nameNodeApiMaximumPoolSize)
                .nameNodeApiQueueSize(nameNodeApiQueueSize)
                .build();
    }

    public String getEditlogsFile(long start, long end) {
        return baseDir + File.separator + "editslog-" + start + "_" + end + ".log";
    }

    public String getFsimageFile(String time) {
        return baseDir + File.separator + "fsimage-" + time;
    }

    public String getSlotFile() {
        return baseDir + File.separator + "slots.meta";
    }

    public int numOfNode() {
        return StringUtils.isEmpty(nameNodePeerServers) ? 1 : nameNodePeerServers.split(",").length;
    }

    public String getAuthInfoFile() {
        return baseDir + File.separator + "auth.meta";
    }

    public NameNodeLaunchMode getMode() {
        return NameNodeLaunchMode.getEnum(nameNodeLaunchMode);
    }

    public long getClearStorageThreshold() {
        return nameNodeTrashClearThreshold;
    }
}
