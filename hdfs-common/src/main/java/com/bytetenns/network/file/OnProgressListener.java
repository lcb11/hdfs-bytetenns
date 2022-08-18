package com.bytetenns.network.file;

/**
 * 文件下载进度监听器
 */
public interface OnProgressListener {

    /**
     * 下载进度
     *
     * @param progress 进度，0-100，保留1位小数
     */
    void onProgress(long total, long current, float progress, int currentReadBytes);

    /**
     * 完成下载
     */
    default void onCompleted() {

    }
}
