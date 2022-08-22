package com.bytetenns.common.network;

import com.bytetenns.common.enums.PacketType;
import com.bytetenns.common.exception.RequestTimeoutException;
import com.bytetenns.common.netty.Constants;
import com.bytetenns.common.netty.NettyPacket;
import com.bytetenns.common.scheduler.DefaultScheduler;
import io.netty.channel.socket.SocketChannel;
import lombok.extern.slf4j.Slf4j;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * 同步请求支持
 */
@Slf4j
public class SyncRequestSupport {

    private Map<String, RequestPromise> promiseMap = new ConcurrentHashMap<>();
    private SocketChannel socketChannel;
    private String name;

    public SyncRequestSupport(String name, DefaultScheduler defaultScheduler, long requestTimeout) {
        this.name = name;
        // 每一秒钟检查一下正在进行的同步发送任务，并进行标记
        defaultScheduler.schedule("定时检测超时", () -> checkRequestTimeout(requestTimeout),
                0, 1000, TimeUnit.MILLISECONDS);
    }

    public void setSocketChannel(SocketChannel socketChannel) {
        this.socketChannel = socketChannel;
    }

    /**
     * 同步发送请求
     *
     * @param request 请求
     * @return 响应
     */
    public NettyPacket sendRequest(NettyPacket request) throws RequestTimeoutException {
        setSequence(request);
        RequestPromise promise = new RequestPromise(request);
        promiseMap.put(request.getSequence(), promise);
        socketChannel.writeAndFlush(request);
        if (log.isDebugEnabled()) {
            log.debug("发送请求并同步等待结果：[request={}, sequence={}]",
                    PacketType.getEnum(request.getPacketType()).getDescription(), request.getSequence());
        }
        return promise.getResult();
    }

    /**
     * 收到响应
     *
     * @param response 响应
     * @return 是否处理消息
     */
    public boolean onResponse(NettyPacket response) {
        String sequence = response.getSequence();
        if (sequence != null) {
            // 响应不支持chunk或者响应题为0，则表示包已经接受完毕
            boolean isChunkFinish = !response.isSupportChunked() || response.getBody().length == 0;
            RequestPromise wrapper;
            if (isChunkFinish) {
                wrapper = promiseMap.remove(sequence);
            } else {
                wrapper = promiseMap.get(sequence);
            }
            if (wrapper != null) {
                wrapper.setResult(response);
                return true;
            }
        }
        return false;
    }

    /**
     * 设置请求的序列号
     *
     * @param nettyPacket 请求
     */
    private void setSequence(NettyPacket nettyPacket) {
        if (socketChannel == null || !socketChannel.isActive()) {
            throw new IllegalStateException("Socket channel is disconnect.");
        }
        nettyPacket.setSequence(name + "-" + Constants.REQUEST_COUNTER.getAndIncrement());
    }

    /**
     * 定时检测请求是否超时，避免请求被hang死
     *
     * @param requestTimeout 请求超时时间
     */
    private void checkRequestTimeout(long requestTimeout) {
        synchronized (this) {
            for (Map.Entry<String, RequestPromise> entry : promiseMap.entrySet()) {
                RequestPromise value = entry.getValue();
                if (value.isTimeout(requestTimeout)) {
                    value.markTimeout();
                }
            }
        }
    }

}
