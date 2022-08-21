package com.bytetenns.namenode.datanode;

import java.util.LinkedList;
import java.util.List;

/**
 * @author yw
 * @create 2022-08-20 21:37
 */
public class DataNodeInfo {
    public List<ReplicaTask> pollReplicaTask(int maxNum){
        return new LinkedList<ReplicaTask>();
    }

    public String getHostname() {
        return null;
    }

    public int getHttpPort() {
        return 0;
    }

    public int getNioPort() {
        return 0;
    }
}
