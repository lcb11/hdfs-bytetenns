package com.bytetenns.ha;


import lombok.extern.slf4j.Slf4j;

import java.util.Collections;

import com.bytetenns.dfs.model.backup.BackupNodeInfo;
import com.bytetenns.common.network.NetClient;
import com.bytetenns.common.scheduler.DefaultScheduler;

/**
 * 负责和BackupNode进行连接的管理器
 *
 * @author Sun Dasheng
 */
@Slf4j
public class BackupNodeManager {

    private DefaultScheduler defaultScheduler;
    private volatile NetClient netClient;
    private ReportNameNodeStatusHandler reportNameNodeStatusHandler;

    public BackupNodeManager(DefaultScheduler defaultScheduler) {
        this.defaultScheduler = defaultScheduler;
    }

    /**
     * 和BackupNode建立连接
     *
     * @param backupNodeInfo backupNode结果
     */
    public void maybeEstablishConnect(BackupNodeInfo backupNodeInfo, BackupUpGradeListener backupUpGradeListener) {
        if (netClient == null) {
            synchronized (this) {
                if (netClient == null) {
                    netClient = new NetClient("DataNode-BackupNod-" + backupNodeInfo.getHostname(), defaultScheduler, 3);
                    reportNameNodeStatusHandler = new ReportNameNodeStatusHandler();
                    netClient.addHandlers(Collections.singletonList(reportNameNodeStatusHandler));
                    netClient.connect(backupNodeInfo.getHostname(), backupNodeInfo.getPort());
                    log.info("收到NameNode返回的BackupNode信息，建立链接：[hostname={}, port={}]",
                            backupNodeInfo.getHostname(), backupNodeInfo.getPort());
                    netClient.addNetClientFailListener(() -> {
                        reset();
                        if (backupUpGradeListener != null) {
                            backupUpGradeListener.onBackupUpGrade(backupNodeInfo.getHostname());
                        }
                    });
                }
            }
        }
    }

    private void reset() {
        this.netClient.shutdown();
        this.netClient = null;
        this.reportNameNodeStatusHandler = null;
    }

    /**
     * 标识NameNode节点宕机了
     */
    public void markNameNodeDown() {
        if (reportNameNodeStatusHandler != null) {
            reportNameNodeStatusHandler.markNameNodeDown();
        }
    }
}
