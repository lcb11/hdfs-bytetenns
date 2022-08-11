package com.bytetenns.namenode.editlog.buffer;

import com.bytetenns.namenode.NameNodeConfig;

import java.io.ByteArrayOutputStream;

/**
  * @Author lcb
  * @Description 一块缓存
  * @Date 2022/8/10
  * @Param
  * @return
  **/

public class EditLogBuffer {

    private ByteArrayOutputStream buffer;

    private long startTxid=-1L;

    private long endTxid=0L;

    //初始化buffer容量为25K
    public EditLogBuffer() {
        this.buffer=new ByteArrayOutputStream(NameNodeConfig.EDIT_LOG_BUFFER_LIMIT);
    }

    //获取当前缓冲区的字节数量
    public int getSize(){
        return buffer.size();
    }

    //todo 获取当前缓存区的edit log


    //todo 清除缓冲区

    //todo 讲edit log刷新到磁盘

    //todo 写入一条数据到缓存区

}
