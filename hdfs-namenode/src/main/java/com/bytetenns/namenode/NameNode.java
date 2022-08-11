package com.bytetenns.namenode;

import com.bytetenns.namenode.datanode.DataNodeManager;

/**
  * @Author lcb
  * @Description NameNode核心启动类
  * @Date 2022/8/10
  * @Param
  * @return
  **/
public class NameNode {

    private DataNodeManager dataNodeManager;

    public NameNode() {
        this.dataNodeManager=new DataNodeManager();

        initialize();
    }

    //初始化nameNode
    public void initialize(){
        //回访edit log文件


    }
}
