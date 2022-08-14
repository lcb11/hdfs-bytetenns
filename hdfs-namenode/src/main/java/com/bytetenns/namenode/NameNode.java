package com.bytetenns.namenode;

import com.bytetenns.namenode.datanode.DataNodeManager;
import com.bytetenns.namenode.fs.DiskNameSystem;
import com.bytetenns.namenode.server.NameNodeServer;
import com.bytetenns.namenode.shard.ShardingManager;

/**
  * @Author lcb
  * @Description NameNode核心启动类
  * @Date 2022/8/10
  * @Param
  * @return
  **/
public class NameNode {

    //datanode管理组件
    private DataNodeManager dataNodeManager;
    //负责管理元数据组件
    private DiskNameSystem dikNameSystem;
    //元数据分片组件
    private ShardingManager shardingManager;
    //对外提供服务
    private NameNodeServer nameNodeServer;

    public NameNode() {
        this.dataNodeManager=new DataNodeManager();
        this.dikNameSystem=new DiskNameSystem();
        this.nameNodeServer=new NameNodeServer();

        initialize();
    }

    //初始化nameNode
    public void initialize(){
        //回访edit log文件


    }
}
