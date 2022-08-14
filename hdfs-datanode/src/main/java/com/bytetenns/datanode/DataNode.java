package com.bytetenns.datanode;

import com.bytetenns.datanode.namenode.NameNodeClient;
import com.bytetenns.datanode.replica.PeerDataNodes;
import com.bytetenns.datanode.server.DataNodeServer;
import com.bytetenns.datanode.storage.StorageManager;

/**
 *
   * @Author byte tens
   * @Description dataNode 核心启动类
   * @Date 2022/8/12
   * @Param
   * @return
   **/
public class DataNode {

    //同级dataNode
    private PeerDataNodes peerDataNodes;
    //存储信息管理
    private StorageManager storageManager;
    //负责跟NameNode进行通讯
    private NameNodeClient nameNodeClient;
    //同级节点通讯
    private DataNodeServer dataNodeServer;
}
