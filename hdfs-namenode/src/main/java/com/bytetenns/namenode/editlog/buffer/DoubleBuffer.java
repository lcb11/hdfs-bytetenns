package com.bytetenns.namenode.editlog.buffer;

import com.bytetenns.namenode.NameNodeConfig;
import com.bytetenns.namenode.editlog.EditLogWrapper;
import com.bytetenns.namenode.editlog.EditslogInfo;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;

/**
  * @Author lcb
  * @Description 双缓冲机制
  * @Date 2022/8/19
  * @Param
  * @return
  **/
@Slf4j
public class DoubleBuffer {

    private NameNodeConfig nameNodeConfig;
    private EditLogBuffer currentBuffer;//专门用来承载线程写入edits log
    private EditLogBuffer syncBuffer;//将数据同步到磁盘

    public DoubleBuffer(NameNodeConfig nameNodeConfig) {
        this.nameNodeConfig = nameNodeConfig;
        this.currentBuffer = new EditLogBuffer(nameNodeConfig);
        this.syncBuffer = new EditLogBuffer(nameNodeConfig);
    }

    /**
     * 写入一条editlog
     */
    public void write(EditLogWrapper editLog) throws IOException {
        currentBuffer.write(editLog);
    }

    /**
     * 交换两块缓冲区
     */
    public void setReadyToSync() {
        EditLogBuffer temp = currentBuffer;
        currentBuffer = syncBuffer;
        syncBuffer = temp;
    }

    /**
     * 把缓冲区的editlog数据刷新到磁盘
     */
    public EditslogInfo flush() throws IOException {
        EditslogInfo editslogInfo = syncBuffer.flush();
        if (editslogInfo != null) {
            syncBuffer.clear();
        }
        return editslogInfo;
    }

    /**
     * 是否可以刷新磁盘
     *
     * @return 是否可以刷磁盘
     */
    public boolean shouldForceSync() {
        return currentBuffer.size() >= nameNodeConfig.getEditLogFlushThreshold();
    }

    public List<EditLogWrapper> getCurrentEditLog() {
        return currentBuffer.getCurrentEditLog();
    }
}
