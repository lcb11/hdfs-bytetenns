package com.bytetenns.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import lombok.extern.slf4j.Slf4j;

/**
 * 网络包编码
 */
@Slf4j
public class NettyPacketEncoder extends MessageToByteEncoder<NettyPacket> {

    @Override
    protected void encode(ChannelHandlerContext ctx, NettyPacket msg, ByteBuf out) throws Exception {
        msg.write(out);
    }

}
