package com.bytetenns.namenode.datanode;

import com.bytetenns.namenode.rebalance.RemoveReplicaTask;
import com.bytetenns.namenode.rebalance.ReplicaTask;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 *
   * @Author lcb
   * @Description DataNode信息
   * @Date 2022/8/19
   * @Param
   * @return
   **/
@Data
@Slf4j
public class DataNodeInfo implements Comparable<DataNodeInfo>{
    //初始化状态
    public static final int STATUS_INIT = 1;
    //准备状态
    public static final int STATUS_READY = 2;

    //datanode Id
    private Integer nodeId;
    //datanode的名字
    private String hostname;
    //http请求端口号
    private int httpPort;
    //nio通道端口号
    private int nioPort;
    //下一次心跳时间
    private long latestHeartbeatTime;
    //当前datanode存储的大小
    private volatile long storedDataSize;
    //当前datanode还剩余多大
    private volatile long freeSpace;
    //状态
    private int status;
    //副本复制队列
    private ConcurrentLinkedQueue<ReplicaTask> replicaTasks = new ConcurrentLinkedQueue<>();
    //副本删除队列
    private ConcurrentLinkedQueue<RemoveReplicaTask> removeReplicaTasks = new ConcurrentLinkedQueue<>();

    public DataNodeInfo(String hostname, int nioPort, int httpPort, long latestHeartbeatTime) {
        this.hostname = hostname;
        this.nioPort = nioPort;
        this.httpPort = httpPort;
        this.latestHeartbeatTime = latestHeartbeatTime;
        //当dataNode初始化时，已存储大小初始化为0
        this.storedDataSize = 0L;
        //状态为初始化状态
        this.status = STATUS_INIT;
    }

    /**
     * 增加DataNode存储信息
     *
     * @param fileSize 文件大小
     */
    public void addStoredDataSize(long fileSize) {
        //采用同步锁，防止在扩大存储空间时，有其他操作对datanode进行操作
        synchronized (this) {
            this.storedDataSize += fileSize;
            this.freeSpace -= fileSize;
        }
    }

    /**
     * 添加副本复制任务
     *
     * @param task 任务
     */
    public void addReplicaTask(ReplicaTask task) {
        //将任务加入副本复制队列
        replicaTasks.add(task);
    }

    /**
     * 获取副本复制任务
     *
     * @return task任务
     */
    public List<ReplicaTask> pollReplicaTask(int maxNum) {
        List<ReplicaTask> result = new LinkedList<>();

        //遍历副本复制队列，将副本任务加入结果集中
        for (int i = 0; i < maxNum; i++) {
            ReplicaTask task = replicaTasks.poll();
            if (task == null) {
                break;
            }
            result.add(task);
        }
        return result;
    }

    /**
     * 获取副本删除任务
     *
     * @return task任务
      **/
    public List<RemoveReplicaTask> pollRemoveReplicaTask(int maxNum) {
        List<RemoveReplicaTask> result = new LinkedList<>();

        for (int i = 0; i < maxNum; i++) {
            RemoveReplicaTask task = removeReplicaTasks.poll();
            if (task == null) {
                break;
            }
            result.add(task);
        }
        return result;
    }

    public void addRemoveReplicaTask(RemoveReplicaTask task) {
        removeReplicaTasks.add(task);
    }


    @Override
    public int compareTo(DataNodeInfo o) {
        if (this.storedDataSize - o.getStoredDataSize() > 0) {
            return 1;
        } else if (this.storedDataSize - o.getStoredDataSize() < 0) {
            return -1;
        } else {
            return 0;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DataNodeInfo that = (DataNodeInfo) o;
        return nioPort == that.nioPort &&
                Objects.equals(hostname, that.hostname);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hostname, nioPort);
    }

    @Override
    public String toString() {
        return "DataNodeInfo{" +
                "hostname='" + hostname + '\'' +
                ", port=" + nioPort +
                '}';
    }
}
