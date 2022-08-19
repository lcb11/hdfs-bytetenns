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

    private String filename;
    private String hostname;
    private int port;

}
