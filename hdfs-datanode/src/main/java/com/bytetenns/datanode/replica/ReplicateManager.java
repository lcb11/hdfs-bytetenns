package com.bytetenns.datanode.replica;


import com.bytetenns.enums.CommandType;
import com.bytetenns.scheduler.DefaultScheduler;
import com.bytetenns.utils.FileUtil;
import com.bytetenns.datanode.namenode.NameNodeClient;
import com.bytetenns.datanode.storage.StorageManager;
import com.bytetenfs.dfs.model.datanode.ReplicaCommand;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

/**
 * 副本复制管理组件
 *
 * @author gongwei
 */
@Slf4j
public class ReplicateManager {

    private NameNodeClient nameNodeClient;
    private StorageManager storageManager;
    private PeerDataNodes peerDataNodes;
    private ConcurrentLinkedQueue<ReplicaCommand> replicaCommandQueue = new ConcurrentLinkedQueue<>();

    public ReplicateManager(DefaultScheduler defaultScheduler, PeerDataNodes peerDataNodes, StorageManager storageManager,
                            NameNodeClient nameNodeClient) {
        this.peerDataNodes = peerDataNodes;
        this.storageManager = storageManager;
        this.nameNodeClient = nameNodeClient;
        defaultScheduler.schedule("定时执行副本任务", new ReplicateWorker(),
                1000, 1000, TimeUnit.MILLISECONDS);
    }

    /**
     * 添加副本任务
     *
     * @param commands 命令
     */
    public void addReplicateTask(List<ReplicaCommand> commands) {
        replicaCommandQueue.addAll(commands);
    }

    /**
     * 副本复制线程
     */
    private class ReplicateWorker implements Runnable {
        @Override
        public void run() {
            try {
                ReplicaCommand command = replicaCommandQueue.poll();
                if (command == null) {
                    return;
                }
                if (CommandType.REPLICA_COPY.getValue() == command.getCommand()) {
                    log.info("收到副本复制任务：[hostname={}, filename={}]", command.getHostname(), command.getFilename());
                    peerDataNodes.getFileFromPeerDataNode(command.getHostname(), command.getPort(), command.getFilename());
                } else if (CommandType.REPLICA_REMOVE.getValue() == command.getCommand()) {
                    String absolutePathByFileName = storageManager.getAbsolutePathByFileName(command.getFilename());
                    File file = new File(absolutePathByFileName);
                    long length = 0;
                    if (file.exists()) {
                        length = file.length();
                    }
                    FileUtil.delete(absolutePathByFileName);
                    if (length > 0) {
                        nameNodeClient.informReplicaRemoved(command.getFilename(), length);
                    }
                    log.info("收到副本删除任务：[filename={}, path={}]", command.getFilename(), absolutePathByFileName);
                }
            } catch (Exception e) {
                log.info("ReplicateWorker处理副本任务异常:", e);
            }
        }
    }
}

