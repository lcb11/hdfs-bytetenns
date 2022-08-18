package com.bytetenns.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 文件系统操作类型
 */
@Getter
@AllArgsConstructor
public enum FsOpType {

    /**
     * 创建文件夹
     */
    MKDIR(1),
    /**
     * 创建文件
     */
    CREATE(2),

    /**
     * 删除文件或者文件夹
     */
    DELETE(3),
    ;


    private int value;
}
