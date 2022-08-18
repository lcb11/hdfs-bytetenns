package com.bytetenns.network;

import com.bytetenns.exception.RequestTimeoutException;
import com.bytetenns.netty.Constants;
import com.bytetenns.netty.NettyPacket;
import com.bytetenns.scheduler.DefaultScheduler;
import com.bytetenns.utils.NetUtils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.SocketChannel;
import lombok.extern.slf4j.Slf4j;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 默认的消息处理器
 */
@Slf4j
public class DefaultChannelHandler extends AbstractChannelHandler {

    private String name;
    private volatile SocketChannel socketChannel;
    private volatile boolean hasOtherHandlers = false;
    private List<NettyPacketListener> nettyPacketListeners = new CopyOnWriteArrayList<>();
    private List<ConnectListener> connectListeners = new CopyOnWriteArrayList<>();
    private SyncRequestSupport syncRequestSupport;

    public DefaultChannelHandler(String name, DefaultScheduler defaultScheduler, long requestTimeout) {
        this.name = name;
        this.syncRequestSupport = new SyncRequestSupport(name, defaultScheduler, requestTimeout);
    }

    public void setHasOtherHandlers(boolean hasOtherHandlers) {
        this.hasOtherHandlers = hasOtherHandlers;
    }

    /**
     * 是否建立了连接
     * @return 是否连接
     */
    public boolean isConnected() {
        return socketChannel != null;
    }

    /**
     * 获取SocketChannel
     * @return SocketChannel
     */
    public SocketChannel socketChannel() {
        return socketChannel;
    }


    /**
     * 发送消息，异步转同步获取响应
     * @param nettyPacket 网络包
     * @return 响应
     * @throws IllegalStateException 网络异常
     */
    public NettyPacket sendSync(NettyPacket nettyPacket) throws InterruptedException, RequestTimeoutException {
        return syncRequestSupport.sendRequest(nettyPacket);
    }


    private void setSequence(NettyPacket nettyPacket) {
        if (socketChannel == null) {
            throw new IllegalStateException("Socket channel is disconnect.");
        }
        nettyPacket.setSequence(name + "-" + Constants.REQUEST_COUNTER.getAndIncrement());
    }

    /**
     * 发送消息，不需要同步获取响应
     * <p>
     * 可以通过 {@link #addNettyPackageListener(NettyPacketListener)} 方法获取返回的数据包
     *
     * @param nettyPacket 网络包
     * @throws IllegalStateException 网络异常
     */
    public void send(NettyPacket nettyPacket) throws InterruptedException {
        setSequence(nettyPacket);
        socketChannel.writeAndFlush(nettyPacket);
    }


    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        socketChannel = (SocketChannel) ctx.channel();
        syncRequestSupport.setSocketChannel(socketChannel);
        invokeConnectListener(true);
        log.debug("Socket channel is connected. {}", NetUtils.getChannelId(ctx.channel()));
        ctx.fireChannelActive();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        socketChannel = null;
        syncRequestSupport.setSocketChannel(socketChannel);
        invokeConnectListener(false);
        log.debug("Socket channel is disconnected！{}", NetUtils.getChannelId(ctx.channel()));
        ctx.fireChannelInactive();
    }

    @Override
    protected boolean handlePackage(ChannelHandlerContext ctx, NettyPacket request) {
        synchronized (this) {
            boolean ret = syncRequestSupport.onResponse(request);
            RequestWrapper requestWrapper = new RequestWrapper(ctx, request);
            invokeListeners(requestWrapper);
            return !hasOtherHandlers || ret;
        }
    }

    /**
     * 回调消息监听器
     * @param requestWrapper 网络包
     */
    private void invokeListeners(RequestWrapper requestWrapper) {
        for (NettyPacketListener listener : nettyPacketListeners) {
            try {
                listener.onMessage(requestWrapper);
            } catch (Exception e) {
                log.error("Exception occur on invoke listener :", e);
            }
        }
    }

    /**
     * 回调连接监听器
     * @param isConnected 是否连接上
     */
    private void invokeConnectListener(boolean isConnected) {
        for (ConnectListener listener : connectListeners) {
            try {
                listener.onConnectStatusChanged(isConnected);
            } catch (Exception e) {
                log.error("Exception occur on invoke listener :", e);
            }
        }
    }

    @Override
    protected Set<Integer> interestPackageTypes() {
        return Collections.emptySet();
    }

    /**
     * 添加消息监听器
     * @param listener 监听器
     */
    public void addNettyPackageListener(NettyPacketListener listener) {
        nettyPacketListeners.add(listener);
    }


    public void addConnectListener(ConnectListener listener) {
        connectListeners.add(listener);
    }

    public void clearConnectListener() {
        connectListeners.clear();
    }

    public void clearNettyPackageListener() {
        nettyPacketListeners.clear();
    }

}
