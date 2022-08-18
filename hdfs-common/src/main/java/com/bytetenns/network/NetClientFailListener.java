package com.bytetenns.network;

/**
 * 网络客户端连接失败监听器
 */
public interface NetClientFailListener {

    /**
     * 连接失败
     */
    void onConnectFail();

}
