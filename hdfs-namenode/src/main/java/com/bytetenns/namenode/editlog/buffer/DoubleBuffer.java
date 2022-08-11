package com.bytetenns.namenode.editlog.buffer;


import com.bytetenns.namenode.NameNodeConfig;

/**
  * @Author lcb
  * @Description 写edit log的双缓冲区
  * @Date 2022/8/10
  * @Param
  * @return
  **/
public class DoubleBuffer {

    //用来承载线程写入edit log的buffer
    private EditLogBuffer currentBuffer;

    //用来同步到磁盘上去的buffer
    private EditLogBuffer syncBuffer;

    public DoubleBuffer() {
        this.currentBuffer = new EditLogBuffer();
        this.syncBuffer = new EditLogBuffer();
    }

    //判断当前缓存区是否满了->是否可以刷新磁盘
    public boolean shouldForceSync(){

        return currentBuffer.getSize()>= NameNodeConfig.EDIT_LOG_BUFFER_LIMIT;
    }

    //交换两块缓冲区
    public void setReadyToSync(){
        EditLogBuffer temp=currentBuffer;
        currentBuffer=syncBuffer;
        syncBuffer=temp;
    }

    //todo 将缓存区的edit log刷新到磁盘

    //todo 向缓存区中写入数据

    //todo 获取当前缓存区的数据

    //todo 清除缓存区的数据


}
