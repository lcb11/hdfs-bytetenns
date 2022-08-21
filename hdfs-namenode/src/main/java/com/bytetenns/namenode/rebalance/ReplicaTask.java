package com.bytetenns.namenode.rebalance;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
  * @Author lcb
  * @Description 副本任务
  * @Date 2022/8/19
  * @Param
  * @return
  **/
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ReplicaTask {

    //文件名字
    private String filename;
    //datanode名字
    private String hostname;
    //通信端口号
    private int port;

}
