package com.bytetenns.backupnode.fsimage;

import com.bytetenns.backupnode.client.NameNodeClient;
import com.bytetenns.backupnode.config.BackupNodeConfig;
import com.bytetenns.backupnode.filesystem.InMemoryNameSystem;
import com.bytetenns.dfs.model.backup.EditLog;
import com.bytetenns.enums.FsOpType;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * 抓取Editslog
 */
@Slf4j
public class EditsLogFetcher implements Runnable {

    /**
     * RPC通讯接口
     */
    private NameNodeClient nameNode;
    /**
     * 文件系统
     */
    private InMemoryNameSystem nameSystem;

    /**
     * BackupNode配置
     */
    private BackupNodeConfig backupnodeConfig;

    public EditsLogFetcher(BackupNodeConfig backupnodeConfig, NameNodeClient namenodeClient, InMemoryNameSystem nameSystem) {
        this.backupnodeConfig = backupnodeConfig;
        this.nameNode = namenodeClient;
        this.nameSystem = nameSystem;
    }

    /**
     * 启动抓取editslog的线程
     */
    @Override
    public void run() {
        try {
            if (nameSystem.isRecovering()) {
                log.info("正在恢复命名空间，等待...");
                Thread.sleep(1000);
                return;
            }
            List<EditLog> editLogs = nameNode.fetchEditsLog(nameSystem.getMaxTxId());
            if (editLogs.size() < backupnodeConfig.getFetchEditLogSize()) {
                return;
            }
            log.info("抓取到editLog: [max txId={}, size={}]", nameSystem.getMaxTxId(), editLogs.size());
            for (int i = 0; i < editLogs.size(); i++) {
                EditLog editLog = editLogs.get(i);
                if (editLog != null) {
                    int op = editLog.getOpType();
                    long txId = editLog.getTxId();
                    if (nameSystem.getMaxTxId() < txId) {
                        if (FsOpType.MKDIR.getValue() == op) {
                            nameSystem.mkdir(editLog.getPath(), editLog.getAttrMap());
                        } else if (FsOpType.CREATE.getValue() == op) {
                            nameSystem.createFile(editLog.getPath(), editLog.getAttrMap());
                        } else if (FsOpType.DELETE.getValue() == op) {
                            nameSystem.deleteFile(editLog.getPath());
                        }
                        nameSystem.setMaxTxId(txId);
                    }
                } else {
                    log.debug("EditLog is empty : {} ", editLogs.toString());
                }
            }
        } catch (Exception e) {
            log.error("抓取EditLog线程出现异常:", e);
        }
    }
}
