package com.bytetenns.namenode.server;

import com.bytetenns.namenode.editlog.EditLogWrapper;
import com.bytetenns.namenode.editlog.EditslogInfo;
import com.bytetenns.namenode.fs.DiskNameSystem;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
  * @Author lcb
  * @Description 抓取EditLog请求处理器
  * @Date 2022/8/20
  * @Param
  * @return
  **/
@Slf4j
public class FetchEditLogBuffer {

    public static final int BACKUP_NODE_FETCH_SIZE = 10;
    private List<EditLogWrapper> bufferedEditLog = new ArrayList<>();
    private DiskNameSystem nameSystem;

    public FetchEditLogBuffer(DiskNameSystem nameSystem) {
        this.nameSystem = nameSystem;
    }

    /**
     * <pre>
     * 抓取Editlog
     * 1. 判断缓冲区的editslog数量没有达到阈值
     *      1.1 尝试从editslog日志文件中读取editslog
     *      1.2 如果editslog日志明显已经读取过了，直接将内存中最新的editslog读取到缓存中
     *      1.3 如果editslog日志文件没有读取过，只读取一部分editslog，直到达到BackupNode拉取editslog条数的阈值即可
     * 2. 判断缓冲区的editslog数量没有达到阈值
     *      2.1 如果没有达到阈值，直接返回空
     *      2.2 达到阈值，将数据返回给BackupNode
     * </pre>
     *
     * @return editLog结果
     */
    public List<EditLogWrapper> fetch(long txId) throws IOException {
        List<EditLogWrapper> result = new ArrayList<>();
        if (bufferedEditLog.size() <= BACKUP_NODE_FETCH_SIZE) {
            fetchEditLogAppendBuffer(txId);
        }
        if (bufferedEditLog.size() >= BACKUP_NODE_FETCH_SIZE) {
            Iterator<EditLogWrapper> iterator = bufferedEditLog.iterator();
            while (iterator.hasNext()) {
                EditLogWrapper next = iterator.next();
                result.add(next);
                iterator.remove();
            }
        }
        return result;
    }

    /**
     * 抓取editlog填充到缓存中
     */
    private void fetchEditLogAppendBuffer(long txId) throws IOException {
        List<EditslogInfo> sortedEditLogsFiles = nameSystem.getEditLog().getSortedEditLogFiles(txId);
        if (sortedEditLogsFiles.isEmpty()) {
            appendMemoryEditLogToBuffer(txId);
        } else {
            long bufferedTxId = 0L;
            for (EditslogInfo each : sortedEditLogsFiles) {
                bufferedTxId = each.getEnd();
                if (txId >= each.getEnd()) {
                    // 如果当前最大的txid已经入了缓存了，则检查下一个文件
                    continue;
                }
                List<EditLogWrapper> editsLogs = nameSystem.getEditLog().readEditLogFromFile(each.getName());
                // 整个文件保存到buffer中
                appendInternal(txId, editsLogs);
                // 如果当前文件缓存到editlog中已经满足抓取的需求，则跳出循环
                if (txId + BACKUP_NODE_FETCH_SIZE < each.getEnd()) {
                    break;
                }
            }
            // 表示读文件读不出更多的editlog了，此时需要从内存缓冲中读取
            if (bufferedTxId <= txId) {
                appendMemoryEditLogToBuffer(txId);
            }
        }
    }

    /**
     * 抓取内存editslog到缓存中
     */
    private void appendMemoryEditLogToBuffer(long minTxId) {
        List<EditLogWrapper> currentEditLog = nameSystem.getEditLog().getCurrentEditLog();
        if (currentEditLog != null) {
            appendInternal(minTxId, currentEditLog);
        }
    }

    /**
     * 将EditLog添加到缓存中
     *
     * @param minTxId     最小的TxId
     * @param editLogList EditLog列表
     */
    private void appendInternal(long minTxId, List<EditLogWrapper> editLogList) {
        for (EditLogWrapper editLog : editLogList) {
            long txId = editLog.getTxId();
            if (txId > minTxId) {
                bufferedEditLog.add(editLog);
            }
        }
    }
}
