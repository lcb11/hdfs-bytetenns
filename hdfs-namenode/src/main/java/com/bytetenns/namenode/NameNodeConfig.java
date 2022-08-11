package com.bytetenns.namenode;

/**
 *
   * @Author lcb
   * @Description nameNode 配置信息
   * @Date 2022/8/11
   * @Param
   * @return
   **/
public class NameNodeConfig {

    //文件副本数量
    public static Integer FILE_REPLICATION=2;
    //缓存EditLogBuffer大小
    public static Integer EDIT_LOG_BUFFER_LIMIT=25*1024;
    //editLog 文件持久化到磁盘的位置
    public static String EDIT_LOG_FILE_PATH="D:\\bytetenns\\editlogs\\";
    //fsImage文件持久化到磁盘的位置
    public static String FSIMAGE_FILE_PATH="D:\\bytetenns\\editlogs\\";
    //rpc通信端口
    public static int RCP_PORT=80010;

}
