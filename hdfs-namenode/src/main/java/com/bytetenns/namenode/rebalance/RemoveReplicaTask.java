package com.bytetenns.namenode.rebalance;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
  * @Author lcb
  * @Description 移除副本任务
  * @Date 2022/8/19
  * @Param
  * @return
  **/
@Data
@NoArgsConstructor
@AllArgsConstructor
public class RemoveReplicaTask {

    //datanode名字
    private String hostname;
    //文件名字
    private String fileName;
}

