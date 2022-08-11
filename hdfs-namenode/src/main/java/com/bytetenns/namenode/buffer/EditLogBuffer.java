package com.bytetenns.namenode.buffer;

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
        this.buffer=new ByteArrayOutputStream(25*1024);
    }

    //获取当前缓冲区的字节数量
    public int getSize(){
        return buffer.size();
    }
}
