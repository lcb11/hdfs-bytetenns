package com.bytetenns.datanode.namenode;

import com.bytetenns.common.netty.NettyPacket;
import com.bytetenns.common.enums.PacketType;
import com.bytetenns.datanode.conf.DataNodeConfig;
import com.bytetenns.dfs.model.datanode.HeartbeatRequest;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * @author gongwei
 * @description 向namenode发送心跳
 * @date 2022/8/16
 * @param
 * @return
 **/

@Slf4j
public class HeartbeatTask implements Runnable {
    private DataNodeConfig datanodeConfig;
    private ChannelHandlerContext ctx;

    public HeartbeatTask(ChannelHandlerContext ctx, DataNodeConfig datanodeConfig) {
        this.ctx = ctx; // 获取上下文共享变量
        this.datanodeConfig = datanodeConfig; // 配置datenode
    }

    @Override
    public void run() {
        HeartbeatRequest request = HeartbeatRequest.newBuilder()
                .setHostname(datanodeConfig.getDataNodeTransportAddr())
                .build();
        // 发送心跳请求
        NettyPacket nettyPacket = NettyPacket.buildPacket(request.toByteArray(), PacketType.HEART_BRET);
        ctx.writeAndFlush(nettyPacket);
    }
}
