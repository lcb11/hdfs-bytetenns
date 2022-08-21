package com.bytetenns.namenode.editlog.buffer;


import com.bytetenns.common.utils.FileUtil;
import com.bytetenns.namenode.NameNodeConfig;
import com.bytetenns.namenode.editlog.EditLogWrapper;
import com.bytetenns.namenode.editlog.EditslogInfo;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
  * @Author lcb
  * @Description EditLog缓冲区
  * @Date 2022/8/19
  * @Param
  * @return
  **/

@Slf4j
public class EditLogBuffer {

    private final NameNodeConfig nameNodeConfig;
    private ByteArrayOutputStream buffer;//字节数组IO流
    private volatile long startTxid = -1L;
    private volatile long endTxid = 0L;

    public EditLogBuffer(NameNodeConfig nameNodeConfig) {
        this.nameNodeConfig = nameNodeConfig;
        this.buffer = new ByteArrayOutputStream((nameNodeConfig.getEditLogFlushThreshold() * 2));
    }

    /**
     * 写入一条数据到缓冲区
     *
     * @param editLog editlog
     * @throws IOException IO异常
     */
    public void write(EditLogWrapper editLog) throws IOException {
        if (startTxid == -1) {
            startTxid = editLog.getTxId();
        }
        endTxid = editLog.getTxId();
        //将editLog转化为byte再写入bytebuffer中
        buffer.write(editLog.toByteArray());
    }

    /**
     * 获取当前缓冲区的EditLog
     *
     * @return 当前缓冲区的EditLog
     */
    public List<EditLogWrapper> getCurrentEditLog() {
        byte[] bytes = buffer.toByteArray();
        if (bytes.length == 0) {
            return new ArrayList<>();
        }
        return EditLogWrapper.parseFrom(bytes);
    }

    /**
     * 清除缓冲区
     */
    public void clear() {
        buffer.reset();
        startTxid = -1;
        endTxid = -1;
    }

    /**
     * 返回当前缓冲区大小
     */
    public Integer size() {
        return buffer.size();
    }

    /**
     * 刷磁盘
     */
    public EditslogInfo flush() throws IOException {
        if (buffer.size() <= 0) {
            return null;
        }
        byte[] data = buffer.toByteArray();
        ByteBuffer dataBuffer = ByteBuffer.wrap(data);
        String path = nameNodeConfig.getEditlogsFile(startTxid, endTxid);
        log.info("保存editslog文件：[file={}]", path);
        FileUtil.saveFile(path, false, dataBuffer);
        return new EditslogInfo(startTxid, endTxid, path);
    }
}