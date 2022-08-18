package com.bytetenns.netty;

import com.google.common.collect.Sets;

import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 常量
 */
public class Constants {

    /**
     * Netty传输最大字节数
     */
    public static final int MAX_BYTES = 10 * 1024 * 1024;

    /**
     * 分块传输，每一块的大小
     */
    public static final int CHUNKED_SIZE = (int) (MAX_BYTES * 0.5F);

    /**
     * NameNode的在线状态
     */
    public static final int NAMENODE_STATUS_UP = 1;

    /**
     * NameNode的宕机
     */
    public static final int NAMENODE_STATUS_DOWN = 0;

    /**
     * slot槽位的总数量
     */
    public static final int SLOTS_COUNT = 16384;

    /**
     * 文件属性之删除时间
     */
    public static final String ATTR_FILE_DEL_TIME = "DEL_TIME";

    /**
     * 文件属性之副本数量
     */
    public static final String ATTR_REPLICA_NUM = "REPLICA_NUM";

    /**
     * 文件属性之文件大小
     */
    public static final String ATTR_FILE_SIZE = "FILE_SIZE";

    /**
     * 应用请求包计数器
     */
    public static AtomicLong REQUEST_COUNTER = new AtomicLong(1);

    /**
     * 文件垃圾箱目录
     */
    public static final String TRASH_DIR = ".Trash";

    /**
     * 文件副本最大数量
     */
    public static final int MAX_REPLICA_NUM = 5;

    /**
     * 保留的属性名称
     */
    public static final Set<String> KEYS_ATTR_SET = Sets.newHashSet(ATTR_FILE_DEL_TIME,
            ATTR_REPLICA_NUM, ATTR_FILE_SIZE);
}
