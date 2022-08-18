package com.bytetenns.network.file;

import java.io.IOException;

/**
 * 接收到文件的回调
 */
public interface FileTransportCallback {

    /**
     * 获取文件在本机的保存地址
     *
     * @param filename 文件路径
     * @return 文件保存在本机的绝对路径
     */
    String getPath(String filename);


    /**
     * 文件传输进度监听器
     *
     * @param filename          文件名称
     * @param total             总大小
     * @param current           当前大小
     * @param progress          进度 0-100，保留1位小数
     * @param currentWriteBytes 当次回调写文件的字节数
     */
    default void onProgress(String filename, long total, long current, float progress, int currentWriteBytes) {
        // Default NO-OP
    }

    /**
     * 文件传输完成
     *
     * @param fileAttribute 文件属性
     */
    default void onCompleted(FileAttribute fileAttribute) throws InterruptedException, IOException {
        // Default NO-OP
    }

}
