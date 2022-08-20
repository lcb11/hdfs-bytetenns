package com.bytetenns.namenode.server;

import com.bytetenns.common.utils.NetUtils;
import com.bytetenns.dfs.model.backup.BackupNodeInfo;
import io.netty.channel.Channel;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
  * @Author lcb
  * @Description 用于处理BackupNode的信息同步到客户端和DataNode的处理器
  * @Date 2022/8/20
  * @Param
  * @return
  **/
@Getter
@AllArgsConstructor
public class BackupNodeInfoHolder {
    private BackupNodeInfo backupNodeInfo;
    private Channel channel;


    public boolean isActive() {
        return channel.isActive();
    }

    public boolean match(Channel channel) {
        return NetUtils.getChannelId(channel).equals(NetUtils.getChannelId(this.channel));
    }
}