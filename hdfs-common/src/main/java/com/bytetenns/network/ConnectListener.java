package com.bytetenns.network;

/**
 * 网络连接状态监听器
 */
public interface ConnectListener {

    /**
     * 网络状态监听
     * <pre>
     * 注意：
     *      为了保证消息的有序性，这里调用是在EventLoopGroup的线程。
     *      如果在后续处理过程中有什么需要阻塞的场景，需要开启别的线程处理
     *      否则会阻塞后续的网络包接收和处理
     * </pre>
     *
     * @param connected 是否连接
     */
    void onConnectStatusChanged(boolean connected) throws Exception;
}
