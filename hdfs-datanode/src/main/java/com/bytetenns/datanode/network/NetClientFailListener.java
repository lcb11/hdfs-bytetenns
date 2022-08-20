package com.bytetenns.datanode.network;

/**
 * 网络客户端连接失败监听器
 *
 * @author Sun Dasheng
 */
public interface NetClientFailListener {

    /**
     * 连接失败
     */
    void onConnectFail();

}
