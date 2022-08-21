package com.bytetenns.namenode.datanode;

import com.bytetenns.dfs.model.common.DataNode;
import com.bytetenns.dfs.model.datanode.HeartbeatRequest;
import com.bytetenns.dfs.model.datanode.RegisterRequest;

import java.util.List;

/**
 *
 */
public class DataNodeManager {
    public boolean registerRequest(RegisterRequest registerRequest){
        return true;
    }
    public boolean heartbeat(String heartbeatRequest){
        return true;
    }
    public DataNodeInfo getDataNode(String hostname){
        return new DataNodeInfo();
    }

    public List<DataNodeInfo> allocateDataNodes(String userName, int replicaNum, String fileName) {

        return null;
    }
}
