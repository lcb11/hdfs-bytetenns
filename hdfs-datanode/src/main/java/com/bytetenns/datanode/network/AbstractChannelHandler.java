package com.bytetenns.datanode.network;

import com.bytetenns.datanode.netty.NettyPacket;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.MDC;

import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executor;

/**
 * 默认消息处理器
 *
 * @author Sun Dasheng
 */
@Slf4j
@ChannelHandler.Sharable
public abstract class AbstractChannelHandler extends ChannelInboundHandlerAdapter {

    private Set<Integer> interestPackageTypes;

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("AbstractChannelHandler#exceptionCaught：", cause);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        Executor executor = getExecutor();
        if (executor != null) {
            executor.execute(() -> channelReadInternal(ctx, msg));
        } else {
            channelReadInternal(ctx, msg);
        }
    }

    private void channelReadInternal(ChannelHandlerContext ctx, Object msg) {
        try {
            String loggerId = DigestUtils.md5Hex("" + System.nanoTime() + new Random().nextInt());
            MDC.put("logger_id", loggerId);
            NettyPacket nettyPacket = (NettyPacket) msg;
            boolean consumedMsg = false;
            if (getPackageTypes().isEmpty() || getPackageTypes().contains(nettyPacket.getPacketType())) {
                try {
                    consumedMsg = handlePackage(ctx, nettyPacket);
                } catch (Exception e) {
                    log.info("处理请求发生异常：", e);
                }
            }
            if (!consumedMsg) {
                ctx.fireChannelRead(msg);
            }
        } finally {
            MDC.remove("logger_id");
        }
    }

    private Set<Integer> getPackageTypes() {
        if (interestPackageTypes == null) {
            interestPackageTypes = interestPackageTypes();
        }
        return interestPackageTypes;
    }

    /**
     * 获取执行器，如果返回执行器，则表示请求逻辑在执行器中执行
     *
     * @return 执行器
     */
    protected Executor getExecutor() {
        return null;
    }

    /**
     * 处理网络包
     *
     * @param ctx          上下文
     * @param nettyPacket 网络包
     * @return 是否消费了该消息
     * @throws Exception 序列化异常
     */
    protected abstract boolean handlePackage(ChannelHandlerContext ctx, NettyPacket nettyPacket) throws Exception;


    /**
     * 感兴趣的消息类型集合
     *
     * @return 感兴趣的消息类型集合, 返回空集合表示所有信息都感兴趣
     */
    protected abstract Set<Integer> interestPackageTypes();
}
