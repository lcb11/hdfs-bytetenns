package com.bytetenns.common.network;

import com.bytetenns.common.exception.RequestTimeoutException;
import com.bytetenns.common.netty.NettyPacket;
import com.bytetenns.common.scheduler.DefaultScheduler;
import com.bytetenns.common.scheduler.NamedThreadFactory;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.ResourceLeakDetector;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 负责和NameNode通讯的组件
 * <pre>
 * 1. 负责和服务端维持链接
 * 2. 提供同步、异步收发消息功能
 *
 * 可以通过指定 {@link #retryTime} 来实现连接不上之后，可以重试多少次。
 *
 * 比如设置重试3次，累计重试3次都不成功后，则会不继续重试，
 * 会回调 {@link #addNetClientFailListener(NetClientFailListener)} 方法设置的监听器
 *
 * 如果需要监听连接状态变化，可以使用 {@link #addConnectListener(ConnectListener)}
 *
 * 注意：如果设置了重试，{@link ConnectListener#onConnectStatusChanged(boolean)} 方法可能会被多次重复调用
 *
 *
 * 可以通过 {@link #send(NettyPacket)} 和 {@link #sendSync(NettyPacket)} 进行同步或异步的网络包发送
 *
 * 同样可以通过设置 {@link #addNettyPackageListener(NettyPacketListener)} 来异步监听底层的网络包
 *
 * </pre>
 */
@Slf4j
public class NetClient {

    private BaseChannelInitializer baseChannelInitializer;
    private String name;
    private DefaultScheduler defaultScheduler;
    private EventLoopGroup connectThreadGroup;
    private DefaultChannelHandler defaultChannelHandler;
    private int retryTime;
    private List<NetClientFailListener> netClientFailListeners = new ArrayList<>();
    private AtomicBoolean started = new AtomicBoolean(true);

    public NetClient(String name, DefaultScheduler defaultScheduler) {
        this(name, defaultScheduler, -1, 3000);
    }

    public NetClient(String name, DefaultScheduler defaultScheduler, int retryTime) {
        this(name, defaultScheduler, retryTime, 3000);
    }

    public NetClient(String name, DefaultScheduler defaultScheduler, int retryTime, long requestTimeout) {
        this.name = name;
        this.retryTime = retryTime;
        this.defaultScheduler = defaultScheduler;
        this.connectThreadGroup = new NioEventLoopGroup(1,
                new NamedThreadFactory("NetClient-Event-", false));
        this.defaultChannelHandler = new DefaultChannelHandler(name, defaultScheduler, requestTimeout);
        this.defaultChannelHandler.addConnectListener(connected -> {
            if (connected) {
                synchronized (NetClient.this) {
                    NetClient.this.notifyAll();
                }
            }
        });
        this.baseChannelInitializer = new BaseChannelInitializer();
        this.baseChannelInitializer.addHandler(defaultChannelHandler);
    }

    public SocketChannel socketChannel() {
        return defaultChannelHandler.socketChannel();
    }


    /**
     * 同步等待确保连接已经建立。
     * 如果连接断开了，会阻塞直到连接重新建立
     */
    public void ensureConnected() throws InterruptedException {
        ensureConnected(-1);
    }

    /**
     * 同步等待确保连接已经建立。
     * 如果连接断开了，会阻塞直到连接重新建立
     *
     * @param timeout 等待超时时间
     */
    public void ensureConnected(int timeout) throws InterruptedException {
        int remainTimeout = timeout;
        synchronized (this) {
            while (!isConnected()) {
                if (!started.get()) {
                    throw new InterruptedException("无法连接上服务器：" + name);
                }
                if (timeout > 0) {
                    if (remainTimeout <= 0) {
                        throw new InterruptedException("无法连接上服务器：" + name);
                    }
                    wait(10);
                    remainTimeout -= 10;
                } else {
                    wait(10);
                }
            }
        }
    }

    /**
     * 添加自定义的handler
     */
    public void addHandlers(List<AbstractChannelHandler> handlers) {
        if (CollectionUtils.isEmpty(handlers)) {
            return;
        }
        defaultChannelHandler.setHasOtherHandlers(true);
        baseChannelInitializer.addHandlers(handlers);
    }

    /**
     * 启动连接
     * @param hostname 主机名
     * @param port     端口
     */
    public void connect(String hostname, int port) {
        connect(hostname, port, 1, 0);
    }

    /**
     * 启动连接，调用sheduleOnce方法，会立即执行
     * @param hostname 主机名
     * @param port     端口
     */
    private void connect(String hostname, int port, final int connectTimes, int delay) {
        defaultScheduler.scheduleOnce("连接服务端", () -> {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(connectThreadGroup)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .channel(NioSocketChannel.class)
                    .handler(baseChannelInitializer);
            ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.ADVANCED);
            try {
                ChannelFuture channelFuture = bootstrap.connect(hostname, port).sync();
                channelFuture.channel().closeFuture().addListener((ChannelFutureListener) f -> f.channel().close());
                channelFuture.channel().closeFuture().sync();
            } catch (InterruptedException e) {
                log.info("发起连接后同步等待连接被打断");
            } catch (Exception e) {
                log.error("发起连接过程中出现异常：[ex={}, started={}, name={}]", e.getMessage(), started.get(), name);
            } finally {
                int curConnectTimes = connectTimes + 1;
                maybeRetry(hostname, port, curConnectTimes);
            }
        }, delay);
    }

    /**
     * 尝试重新发起连接
     * @param hostname     主机名
     * @param port         端口号
     * @param connectTimes 当前重试次数
     */
    private void maybeRetry(String hostname, int port, int connectTimes) {
        if (started.get()) {
            boolean retry = retryTime < 0 || connectTimes <= retryTime;
            if (retry) {
                log.error("重新发起连接：[started={}, name={}]", started.get(), name);
                connect(hostname, port, connectTimes, 3000);
            } else {
                shutdown();
                log.info("重试次数超出阈值，不再进行重试：[retryTime={}]", retryTime);
                for (NetClientFailListener listener : new ArrayList<>(netClientFailListeners)) {
                    try {
                        listener.onConnectFail();
                    } catch (Exception e) {
                        log.error("Exception occur on invoke listener :", e);
                    }
                }
            }
        }
    }

    /**
     * 发送请求, 同步获取结果
     * @param nettyPacket 请求
     * @return 结果
     */
    public NettyPacket sendSync(NettyPacket nettyPacket) throws InterruptedException, RequestTimeoutException {
        ensureConnected();
        return defaultChannelHandler.sendSync(nettyPacket);
    }

    /**
     * 发送请求
     * 异步，通过Listener获取结果
     * @param nettyPacket 请求
     */
    public void send(NettyPacket nettyPacket) throws InterruptedException {
        ensureConnected();
        defaultChannelHandler.send(nettyPacket);
    }

    /**
     * 是否连接上
     * @return 是否已建立了链接
     */
    public boolean isConnected() {
        return defaultChannelHandler.isConnected();
    }

    /**
     * 关闭服务，关闭连接、释放资源
     */
    public void shutdown() {
        if(log.isDebugEnabled()) {
            log.debug("Shutdown NetClient : [name={}]", name);
        }
        started.set(false);
        if (connectThreadGroup != null) {
            connectThreadGroup.shutdownGracefully();
        }
        defaultChannelHandler.clearConnectListener();
        defaultChannelHandler.clearNettyPackageListener();
    }

    /**
     * 添加连接状态监听器
     * @param listener 连接监听器
     */
    public void addConnectListener(ConnectListener listener) {
        defaultChannelHandler.addConnectListener(listener);
    }

    /**
     * 添加网络包监听器
     * @param listener 监听器
     */
    public void addNettyPackageListener(NettyPacketListener listener) {
        defaultChannelHandler.addNettyPackageListener(listener);
    }

    /**
     * 添加连接失败监听器
     * @param listener 连接失败
     */
    public void addNetClientFailListener(NetClientFailListener listener) {
        netClientFailListeners.add(listener);
    }

    public void setRetryTime(int retryTime) {
        this.retryTime = retryTime;
    }
}

