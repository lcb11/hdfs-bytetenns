package com.bytetenns.namenode.editlog;

import com.bytetenns.namenode.buffer.DoubleBuffer;

/**
  * @Author lcb
  * @Description 负责管理EditLog的组件，表示EditLogs
  * @Date 2022/8/11
  * @Param
  * @return
  **/
public class FsEditLog {

    //edit log的Id
    private volatile long txidSeq=0;
    //双缓冲
    private DoubleBuffer doubleBuffer;
    //当前是否在刷磁盘
    private volatile boolean isSyncRunning=false;
    //当前刷新磁盘的最大txid
    private volatile long syncTxid=0;
    //是否正在调度一次刷盘操作
    private volatile Boolean isSchedulingSync=false;
    //每条线程保存的txid
    private ThreadLocal<Long> localTxid=new ThreadLocal<>();

}