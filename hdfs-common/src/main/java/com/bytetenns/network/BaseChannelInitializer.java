package com.bytetenns.network;

import com.bytetenns.netty.Constants;
import com.bytetenns.netty.NettyPacketDecoder;
import com.bytetenns.netty.NettyPacketEncoder;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldPrepender;
import org.apache.commons.collections4.CollectionUtils;
import java.util.LinkedList;
import java.util.List;

/**
 * 基础的消息处理器
 *
 * @author Sun Dasheng
 */
public class BaseChannelInitializer extends ChannelInitializer<SocketChannel> {

    private final List<AbstractChannelHandler> handlers = new LinkedList<>();

    @Override
    protected void initChannel(SocketChannel ch) {
        // 3个字节表示长度，最长支持16MB
        // OutboundHandler是按照Pipeline的加载顺序，逆序执行，所以LengthFieldPrepender需要在前面
        ch.pipeline().addLast(
                new NettyPacketDecoder(Constants.MAX_BYTES),
                new LengthFieldPrepender(3),
                new NettyPacketEncoder()
        );
        for (AbstractChannelHandler handler : handlers) {
            ch.pipeline().addLast(handler);
        }
    }


    /**
     * 添加自定义的handler
     */
    public void addHandler(AbstractChannelHandler handler) {
        this.handlers.add(handler);
    }

    /**
     * 添加自定义的handler
     * @param handlers 处理器
     */
    public void addHandlers(List<AbstractChannelHandler> handlers) {
        if (CollectionUtils.isEmpty(handlers)) {
            return;
        }
        this.handlers.addAll(handlers);
    }

}
