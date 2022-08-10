package com.bytetenns.namenode.buffer;

import java.io.ByteArrayOutputStream;

/**
  * @Author lcb
  * @Description 一块缓存
  * @Date 2022/8/10
  * @Param
  * @return
  **/

public class Buffer {

    private ByteArrayOutputStream buffer;

    private long startTxid=-1L;

    private long endTxid=0L;

    //初始化buffer容量为25K
    public Buffer() {
        this.buffer=new ByteArrayOutputStream(25*1024);
    }
}
