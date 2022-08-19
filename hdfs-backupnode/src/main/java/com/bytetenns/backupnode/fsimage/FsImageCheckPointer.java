package com.bytetenns.backupnode.fsimage;

import com.bytetenns.backupnode.client.NameNodeClient;
import com.bytetenns.backupnode.config.BackupNodeConfig;
import com.bytetenns.backupnode.filesystem.InMemoryNameSystem;
import com.bytetenns.common.network.file.FileTransportClient;
import com.bytetenns.common.utils.FileUtil;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;

/**
 * FsImage 检查点
 *
 * <pre>
 *  将内存目录树 + txId 持久化在同一个FsImage文件中，将时间戳拼接在文件尾：
 *    - fsimage-1624846255954
 *    - fsimage-1624846265954
 *    - fsimage-1624846275954
 *
 *  在保存完成FsImage文件之后，BackupNode和NameNode都会做如下操作：
 *
 *  1. 扫描所有的FsImage文件，将文件按时间戳降序排序。
 *  2. 逐步校验FsImage文件，直到找到一个格式合法的FsImage文件。
 *      2.1 假设上面第3个FsImage文件不合法，保存到一半的时候BackupNode宕机了，或者传给NameNode的时候传了一半BackupNode宕机了，导致整个文件不完整
 *      2.2 首先判断第3个文件，校验得出第3个不合法，删除第三个文件。继续校验第2个文件，文件合法。把第一个文件删除。只保留第2个文件
 *
 *  3. NameNode基于第2步得到的FsImage文件，读取其中的TxId，然后删除比txId小的EditLogs文件
 *
 * </pre>
 */
@Slf4j
public class FsImageCheckPointer implements Runnable {

    private BackupNodeConfig backupNodeConfig;
    private InMemoryNameSystem nameSystem;
    private NameNodeClient namenodeClient;
    private FileTransportClient fileTransportClient;
    private long lastCheckpointTxId;
    private FsImageClearTask fsImageClearTask;

    public FsImageCheckPointer(NameNodeClient namenodeClient, InMemoryNameSystem nameSystem, BackupNodeConfig backupnodeConfig) {
        this.nameSystem = nameSystem;
        this.namenodeClient = namenodeClient;
        this.backupNodeConfig = backupnodeConfig;
        this.fileTransportClient = new FileTransportClient(namenodeClient.getNetClient(), false);
        this.lastCheckpointTxId = nameSystem.getMaxTxId();
        this.fsImageClearTask = new FsImageClearTask(nameSystem, backupnodeConfig.getBaseDir());
    }

    @Override
    public void run() {
        log.info("BackupNode启动checkpoint后台线程.");
        try {
            if (nameSystem.isRecovering()) {
                log.info("正在恢复元数据...");
                return;
            }
            if (nameSystem.getMaxTxId() == lastCheckpointTxId) {
                log.info("EditLog和上次没有变化，不进行checkpoint: [txId={}]", lastCheckpointTxId);
                return;
            }
            FsImage fsImage = nameSystem.getFsImage();
            lastCheckpointTxId = fsImage.getMaxTxId();
            String fsImageFile = backupNodeConfig.getFsImageFile(String.valueOf(System.currentTimeMillis()));

            log.info("开始执行checkpoint操作: [maxTxId={}]", fsImage.getMaxTxId());

            // 写入FsImage文件
            doCheckpoint(fsImage, fsImageFile);

            // 上传FsImage给NameNode
            uploadFsImage(fsImageFile);

            // 删除旧的FSImage
            namenodeClient.getDefaultScheduler().scheduleOnce("删除FSImage任务", fsImageClearTask, 0);
        } catch (Exception e) {
            log.error("FSImageCheckPointer error:", e);
        }
    }

    /**
     * 上传FsImage到NameNode
     */
    private void uploadFsImage(String path) {
        try {
            log.info("开始上传fsImage文件：[file={}]", path);
            fileTransportClient.sendFile(path);
            log.info("结束上传fsImage文件：[file={}]", path);
        } catch (Exception e) {
            log.info("上传FsImage异常：", e);
        }
    }

    /**
     * 写入fsImage文件
     */
    private void doCheckpoint(FsImage fsImage, String path) throws Exception {
        ByteBuffer buffer = ByteBuffer.wrap(fsImage.toByteArray());
        FileUtil.saveFile(path, true, buffer);
        log.info("保存FsImage文件：[file={}]", path);
    }
}
