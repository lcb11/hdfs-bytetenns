package com.bytetenns.backupnode.server;

import com.bytetenns.backupnode.ha.NodeRoleSwitcher;
import com.bytetenns.netty.NettyPacket;
import com.bytetenns.network.AbstractChannelHandler;
import com.bytetenns.utils.ByteUtil;
import com.bytetenns.utils.NetUtils;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.Set;

/**
 * 自动感知客户端和DataNode连接的处理器
 */
@Slf4j
public class AwareConnectHandler extends AbstractChannelHandler {

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        if (log.isDebugEnabled()) {
            log.debug("BackupNode收到一个连接：[channel={}]", NetUtils.getChannelId(ctx.channel()));
        }
        NodeRoleSwitcher.getInstance().addConnect(ctx.channel());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        if (log.isDebugEnabled()) {
            log.debug("BackupNode移除一个连接：[channel={}]", NetUtils.getChannelId(ctx.channel()));
        }
        NodeRoleSwitcher.getInstance().removeConnect(ctx.channel());
    }

    @Override
    protected boolean handlePackage(ChannelHandlerContext ctx, NettyPacket nettyPacket) {
        // 0表示宕机 1-表示在线
        int status = ByteUtil.getInt(nettyPacket.getBody(), 0);
        NodeRoleSwitcher.getInstance().markNameNodeStatus(status);
        return true;
    }

    @Override
    protected Set<Integer> interestPackageTypes() {
        return Collections.emptySet();
    }
}
