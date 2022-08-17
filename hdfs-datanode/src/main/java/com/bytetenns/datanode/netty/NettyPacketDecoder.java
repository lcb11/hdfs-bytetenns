package com.bytetenns.datanode.netty;


import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.ReferenceCountUtil;

/**
 * 网络包编码
 *
 * @author Sun Dasheng
 */
public class NettyPacketDecoder extends LengthFieldBasedFrameDecoder {


    public NettyPacketDecoder(int maxFrameLength) {
        super(maxFrameLength, 0,
                3, 0, 3);
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf buffer) throws Exception {
        ByteBuf byteBuf = (ByteBuf) super.decode(ctx, buffer);
        if (byteBuf != null) {
            try {
                return NettyPacket.parsePacket(byteBuf);
            } finally {
                ReferenceCountUtil.release(byteBuf);
            }
        }
        return null;
    }
}
