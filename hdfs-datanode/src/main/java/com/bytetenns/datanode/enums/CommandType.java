package com.bytetenns.datanode.enums;


import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 命令类型
 *
 * @author Sun Dasheng
 */
@Getter
@AllArgsConstructor
public enum CommandType {

    /**
     * 复制副本任务
     */
    REPLICA_COPY(1),
    REPLICA_REMOVE(2),
    ;

    private int value;

}
