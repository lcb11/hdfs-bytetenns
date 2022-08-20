package com.bytetenns.namenode.shard;

import com.bytetenns.common.enums.NameNodeLaunchMode;
import com.bytetenns.common.netty.Constants;
import com.bytetenns.common.utils.StringUtils;
import com.bytetenns.namenode.NameNodeConfig;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 *
   * @Author byte tens
   * @Description 负责元数据分片组件
   * @Date 2022/8/12
   * @Param
   * @return
   **/
@Slf4j
public class ShardingManager {
    private NameNodeConfig nameNodeConfig;

    public ShardingManager(NameNodeConfig nameNodeConfig) {
        this.nameNodeConfig = nameNodeConfig;
    }

    /**
     * 启动
     */
    public void start() throws Exception {
        log.info("NameNode模式模式：[mode={}]", nameNodeConfig.getMode());
        if (NameNodeLaunchMode.SINGLE.equals(nameNodeConfig.getMode())) {
            return;
        }
        log.info("NameNode当前节点为：[nodeId={}, baseDir={}]", nameNodeConfig.getNameNodeId(), nameNodeConfig.getBaseDir());
        if (nameNodeConfig.getNameNodePeerServers() == null || nameNodeConfig.getNameNodePeerServers().length() == 0
                || nameNodeConfig.getNameNodePeerServers().split(",").length == 1) {
            log.info("NameNode集群模式为单点模式, 自己就是Controller节点");

        }
    }

    /**
     * 根据文件名获取该文件属于哪个slot，返回该slot所在的nameNodeId
     *
     * @param filename 文件名
     * @return 节点ID
     */
    public int getNameNodeIdByFileName(String filename) {
        int slot = StringUtils.hash(filename, Constants.SLOTS_COUNT);
        //直接返回该文件在当前nameNode下的slot位
        return slot;
    }

}
