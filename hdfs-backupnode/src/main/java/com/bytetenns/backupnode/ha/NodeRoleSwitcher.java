package com.bytetenns.backupnode.ha;

import com.alibaba.fastjson.JSONObject;
import com.bytetenns.backupnode.BackupNode;
import com.bytetenns.backupnode.fsimage.FsImage;
import com.bytetenns.common.enums.NameNodeLaunchMode;
import com.bytetenns.common.enums.PacketType;
import com.bytetenns.common.netty.Constants;
import com.bytetenns.common.netty.NettyPacket;
import com.bytetenns.common.utils.FileUtil;
import com.bytetenns.common.utils.NetUtils;
import com.bytetenns.dfs.model.namenode.NameNodeSlots;
import com.bytetenns.namenode.NameNode;
import com.bytetenns.namenode.NameNodeConfig;
import io.netty.channel.Channel;
import io.netty.channel.socket.SocketChannel;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 负责控制BackupNode节点升级为NameNode节点的组件
 *
 * @author Sun Dasheng
 */
@Slf4j
public class NodeRoleSwitcher {

    public static volatile NodeRoleSwitcher INSTANCE = null;

    private BackupNode backupNode;
    private final Map<String, SocketChannel> channelMap = new ConcurrentHashMap<>();
    private final AtomicBoolean upgrading = new AtomicBoolean(false);
    private final AtomicBoolean judgeNameNodeDown = new AtomicBoolean(false);

    /**
     * 自己作为BackupNode有一票
     */
    private final AtomicInteger downStatusCounter = new AtomicInteger(1);
    private final AtomicInteger reportCount = new AtomicInteger(0);
    private NameNodeConfig nameNodeConfig;
    private Map<Integer, Integer> slotsMap;
    private List<User> userList = new ArrayList<>();

    private NodeRoleSwitcher() {
    }

    public static NodeRoleSwitcher getInstance() {
        if (INSTANCE == null) {
            synchronized (NodeRoleSwitcher.class) {
                if (INSTANCE == null) {
                    INSTANCE = new NodeRoleSwitcher();
                }
            }
        }
        return INSTANCE;
    }

    public void replaceUser(List<User> users) {
        if (!users.isEmpty()) {
            userList.clear();
            userList.addAll(users);
        }
    }

    /**
     * 设置BackupNode实例
     *
     * @param backupNode BackupNode实例
     */
    public void setBackupNode(BackupNode backupNode) {
        this.backupNode = backupNode;
    }

    /**
     * 添加一个连接
     *
     * @param channel 连接
     */
    public void addConnect(Channel channel) {
        if (upgrading.get()) {
            log.info("正在进行BackupNode升级为NameNode的操作，忽略新增链接：[channel={}]", NetUtils.getChannelId(channel));
            return;
        }
        channelMap.put(NetUtils.getChannelId(channel), (SocketChannel) channel);
    }

    /**
     * 移除一个连接
     *
     * @param channel 连接
     */
    public void removeConnect(Channel channel) {
        channelMap.remove(NetUtils.getChannelId(channel));
    }

    /**
     * 尝试判断是否需要升级为NameNode
     */
    public void maybeUpgradeToNameNode() throws InterruptedException {
        if (!upgrading.compareAndSet(false, true)) {
            return;
        }
        if (!channelMap.isEmpty()) {
            for (SocketChannel channel : channelMap.values()) {
                if (channel.isActive()) {
                    NettyPacket nettyPacket = NettyPacket.buildPacket(new byte[0], PacketType.GET_NAME_NODE_STATUS);
                    channel.writeAndFlush(nettyPacket);
                }
            }
            log.info("向所有的客户端和DataNode发送请求询问NameNode状态...");
            synchronized (this) {
                // 这里就卡住了，等到超过quorum个数量的时候，会被唤醒
                wait();
            }
        } else {
            judgeNameNodeDown.set(true);
        }
        if (judgeNameNodeDown.get()) {
            upgrade();
        } else {
            upgrading.set(false);
            // 发起下一轮投票
            maybeUpgradeToNameNode();
        }
    }

    /**
     * 升级为NameNode节点
     */
    private void upgrade() {
        if (nameNodeConfig == null) {
            log.error("升级为NameNode失败，没有足够的配置信息. 程序即将退出 !!");
            System.exit(0);
        }
        if (slotsMap == null && nameNodeConfig.getMode().equals(NameNodeLaunchMode.CLUSTER)) {
            log.error("升级为NameNode失败，没有足够的配置信息. 程序即将退出 !!");
            System.exit(0);
        }
        FsImage fsImage = backupNode.getNameSystem().getFsImage();
        try {
            // 保存FSImage
            String nameNodeFsImage = nameNodeConfig.getFsimageFile(String.valueOf(System.currentTimeMillis()));
            ByteBuffer buffer = ByteBuffer.wrap(fsImage.toByteArray());
            FileUtil.saveFile(nameNodeFsImage, true, buffer);

            // 保存Slots信息
            String slotFile = nameNodeConfig.getSlotFile();
            NameNodeSlots.Builder slotsBuilder = NameNodeSlots.newBuilder();
            if (slotsMap != null) {
                slotsBuilder.putAllNewSlots(this.slotsMap);
            }
            NameNodeSlots slots = slotsBuilder.build();
            buffer = ByteBuffer.wrap(slots.toByteArray());
            FileUtil.saveFile(slotFile, true, buffer);

            // 保存用户信息
            String data = JSONObject.toJSONString(userList);
            FileUtil.saveFile(nameNodeConfig.getAuthInfoFile(), true, ByteBuffer.wrap(data.getBytes()));


            log.info("基于BackupNode最新的内存目录树保存为NameNode的FsImage文件：[file={}]", nameNodeFsImage);
            NameNode nameNode = new NameNode(nameNodeConfig);
            Runtime.getRuntime().addShutdownHook(new Thread(nameNode::shutdown));
            this.backupNode.shutdown();
            log.info("BackupNode关闭，启动NameNode程序.");
            nameNode.start();
        } catch (Exception e) {
            log.error("BackupNode升级为NameNode：", e);
        }
    }

    /**
     * 标识NameNode状态
     *
     * @param status 状态
     */
    public void markNameNodeStatus(int status) {
        log.debug("收到一个机器上报的NameNode状态：[status={}]", status);
        if (judgeNameNodeDown.get()) {
            log.info("BackupNode已经在升级为NameNode过程中了，忽略后面的NameNode状态上报：[status={}]", status);
            return;
        }
        reportCount.getAndIncrement();
        int downCount = downStatusCounter.get();
        if (Constants.NAMENODE_STATUS_DOWN == status) {
            downCount = downStatusCounter.incrementAndGet();
        }
        int quorum = (channelMap.size() + 1) / 2 + 1;
        if (downCount >= quorum) {
            if (judgeNameNodeDown.compareAndSet(false, true)) {
                log.info("超过一半机器认为NameNode宕机了，BackupNode开始升级：[downCount={},quorum={}]", downCount, quorum);
                synchronized (this) {
                    notifyAll();
                }
            }
        } else {
            // 如果所有的节点都上报了，但是统计结果是NameNode没有Down，此时发起第二轮状态上报
            if (reportCount.get() == channelMap.size()) {
                reportCount.set(0);
                downStatusCounter.set(0);
                log.info("此轮统计无法确定NameNode宕机，开始发起下一轮统计...");
                synchronized (this) {
                    notifyAll();
                }
            }
        }
    }

    /**
     * 设置NameNode的配置信息
     *
     * @param nameNodeConfig NameNode的配置信息
     */
    public void setNameNodeConfig(NameNodeConfig nameNodeConfig) {
        this.nameNodeConfig = nameNodeConfig;
    }

    public void setSlots(Map<Integer, Integer> slotsMap) {
        this.slotsMap = slotsMap;
    }

    public boolean hasSlots() {
        return this.slotsMap != null;
    }

    public boolean isUpgradeFromBackup() {
        return judgeNameNodeDown.get();
    }
}
