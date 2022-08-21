package com.bytetenns.datanode.server;

// import com.ruyuan.dfs.common.metrics.Prometheus;
import com.bytetenns.common.network.file.FileAttribute;
import com.bytetenns.datanode.namenode.NameNodeClient;
import com.bytetenns.common.network.file.FileTransportCallback;
import com.bytetenns.datanode.storage.StorageManager;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

/**
 * 默认的文件传输回调
 *
 * @author gongwei
 */
@Slf4j
public class DefaultFileTransportCallback implements FileTransportCallback {

    private NameNodeClient nameNodeClient;
    private StorageManager storageManager;

    public DefaultFileTransportCallback(StorageManager storageManager) {
        this.storageManager = storageManager;
    }

    public void setNameNodeClient(NameNodeClient nameNodeClient) {
        this.nameNodeClient = nameNodeClient;
    }

    /*
     * 获取文件路径
     */
    public String getPath(String filename) {
        String localFileName = storageManager.getAbsolutePathByFileName(filename);
        log.info("获取文件路径文件：[filename={}, location={}]", filename, localFileName);
        return localFileName;
    }

    /*
     * 文件传输完成后触发，记录收到一个副本
     */
    public void onCompleted(FileAttribute fileAttribute) throws InterruptedException, IOException {
        storageManager.recordReplicaReceive(fileAttribute.getFilename(), fileAttribute.getAbsolutePath(),
                fileAttribute.getSize());
        nameNodeClient.informReplicaReceived(fileAttribute.getFilename(), fileAttribute.getSize());
    }
    /*
     * 文件传输过程中触发，监控DataNode瞬时写磁盘大小
     */

    public void onProgress(String filename, long total, long current, float progress, int currentWriteBytes) {
        // Prometheus.hit("datanode_disk_write_bytes", "DataNode瞬时写磁盘大小",
        // currentWriteBytes);
    }
}
