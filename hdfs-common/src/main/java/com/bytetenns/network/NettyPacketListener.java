package com.bytetenns.network;

/**
 * 网络包响应监听器
 */
public interface NettyPacketListener {

    /**
     * 收到消息
     * <pre>
     * 注意：
     *      为了保证消息的有序性，这里调用是在EventLoopGroup的线程。
     *      如果在后续处理过程中有什么需要阻塞的场景，需要开启别的线程处理
     *      否则会阻塞后续的网络包接收和处理
     * </pre>
     *
     * @param requestWrapper 消息
     * @throws Exception 异常
     */
    void onMessage(RequestWrapper requestWrapper) throws Exception;

}
