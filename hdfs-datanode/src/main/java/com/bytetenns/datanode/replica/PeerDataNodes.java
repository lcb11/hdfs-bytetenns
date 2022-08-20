package com.bytetenns.datanode.replica;

import com.bytetenns.datanode.netty.NettyPacket;
import com.bytetenns.datanode.enums.PacketType;
import com.bytetenns.datanode.network.NetClient;
import com.bytetenns.datanode.utils.DefaultScheduler;
import com.bytetenns.datanode.conf.DataNodeConfig;
import com.bytetenns.datanode.namenode.NameNodeClient;
import com.bytetenns.datanode.server.DataNodeServer;
import com.bytetenns.datanode.server.DataNodeApis;
import com.bytetenns.datanode.model.common.GetFileRequest;
import com.bytetenns.datanode.model.datanode.PeerNodeAwareRequest;
import io.netty.channel.socket.SocketChannel;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * DataNode相互感知的组件
 *
 * <pre>
 *     在需要复制副本的时候，假设datanode01需要从datanode02复制副本
 *
 *     则会从datanode01发起一个和datanode02的网络连接：  datanode01 -> datanode02
 *
 *     1. datanode01向datanode02发起连接，并保存这个链接
 *     2. datanode02收到下发文件的请求，同时将datanod01的连接保存在 dataNodeChannelMap 中
 *     3. 当datanode02需要从datanode01复制副本的时候，复用之前建立的链接
 *
 * </pre>
 *
 * @author gongwei
 */
@Slf4j
public class PeerDataNodes {

    private DataNodeApis dataNodeApis;
    private DataNodeConfig dataNodeConfig;
    private DefaultScheduler defaultScheduler;
    private Map<String, PeerDataNode> dataNodeChannelMap = new ConcurrentHashMap<>();

    public PeerDataNodes(DefaultScheduler defaultScheduler, DataNodeConfig dataNodeConfig, DataNodeApis dataNodeApis) {
        this.defaultScheduler = defaultScheduler;
        this.dataNodeConfig = dataNodeConfig;
        this.dataNodeApis = dataNodeApis;
        this.dataNodeApis.setPeerDataNodes(this);
    }

    public void setNameNodeClient(NameNodeClient nameNodeClient) {
        this.dataNodeApis.setNameNodeClient(nameNodeClient);
    }

    /**
     * <pre>
     * 从目标DataNode获取文件
     *
     *  发送一个GET_FILE的网络包, 会被Peer DataNode收到, 从而把文件发送过来
     *
     *  这里有两种可能：
     *  1. datanode01和datanode02没有建立连接：
     *
     *      1.1 如果当前实例是datanode01, 会主动发起一个连接：datanode01 -> datanode02
     *      1.2 如果当前实例是datanode02, 会主动发起一个连接：datanode02 -> datanode01
     *
     *  2. 如果datanode02和datanode02已经建立连接，则会从连接池中获取到对应的连接，发送一个GET_FILE请求，从而获取文件
     *
     * </pre>
     *
     * @param hostname 主机名
     * @param port     端口号
     * @param filename 文件名
     */
    public void getFileFromPeerDataNode(String hostname, int port, String filename) throws InterruptedException {
        GetFileRequest request = GetFileRequest.newBuilder()
                .setFilename(filename)
                .build();
        NettyPacket nettyPacket = NettyPacket.buildPacket(request.toByteArray(), PacketType.GET_FILE);
        PeerDataNode peerDataNode = maybeConnectPeerDataNode(hostname, port);
        peerDataNode.send(nettyPacket);
        log.info("PeerDataNode发送GET_FILE请求，请求下载文件：[hostname={}, port={}, filename={}]", hostname, port, filename);
    }


    private PeerDataNode maybeConnectPeerDataNode(String hostname, int port) throws InterruptedException {
        synchronized (this) {
            String peerDataNode = hostname + ":" + port;
            PeerDataNode peer = dataNodeChannelMap.get(peerDataNode);
            if (peer == null) {
                NetClient netClient = new NetClient("DataNode-PeerNode-" + hostname, defaultScheduler);
                netClient.addHandlers(Collections.singletonList(dataNodeApis));
                netClient.addConnectListener(connected -> sendPeerNodeAwareRequest(netClient));
                netClient.connect(hostname, port);
                netClient.ensureConnected();
                peer = new PeerDataNodeClient(netClient, dataNodeConfig.getDataNodeId());
                dataNodeChannelMap.put(peerDataNode, peer);
                log.info("新建PeerDataNode的链接：{}", peerDataNode);
            } else {
                log.info("从保存的连接中获取PeerDataNode的链接：{}", peerDataNode);
            }
            return peer;
        }
    }

    private void sendPeerNodeAwareRequest(NetClient netClient) throws InterruptedException {
        PeerNodeAwareRequest request = PeerNodeAwareRequest.newBuilder()
                .setPeerDataNode(dataNodeConfig.getDataNodeTransportServer())
                .setDataNodeId(dataNodeConfig.getDataNodeId())
                .build();
        NettyPacket nettyPacket = NettyPacket.buildPacket(request.toByteArray(),
                PacketType.DATA_NODE_PEER_AWARE);
        log.info("连接上其他PeerDataNode了, 发送一个通知网络包：{}", request.getPeerDataNode());
        netClient.send(nettyPacket);
    }

    /**
     * DataNodeServer识别出是DataNode Peer后将连接保存起来
     *
     * <pre>
     *
     * 这里可能出现一种情况, 两个DataNode节点同时发起连接
     *   datanode01 -> datanode02
     *   datanode02 -> datanode01
     *
     * 所以每个DataNode定义一个ID，遇到这种情况的时候，DataNode ID更小的断开连接
     *
     * 考虑这样的场景：
     *   datanode01 : id = 1
     *   datanode02 : id = 2
     *
     * 两个DataNode同时发起连接：
     *
     *  1.  datanode01往datanode02发起连接，带上自己的id：
     *           datanode01的连接集合： [datanodeId=2]
     *
     *  2.  datanode02往datanode01发起连接，带上自己的id:
     *          datanode02的连接集合： [datanodeId=1]
     *
     *  3.  datanode02收到datanode01的链接，发现新连接的dataNodeId=1, 维护的连接集合中已存在该连接：
     *       3.1 如果自身的id比新的连接要大，则关闭新的连接
     *       3.2 如果自身的id比新的连接要小，则关闭原本自身作为客户端的链接
     *       结果是保留链接 : [datanode02 -> datanode01]
     *
     *  4.  datanode01收到datanode02的链接，发现新连接的dataNodeId=2, 维护的连接集合中已存在该连接：
     *       4.1 如果自身的id比新的连接要大，则关闭新的连接
     *       4.2 如果自身的id比新的连接要小，则关闭原本自身作为客户端的链接
     *        结果是保留链接 : [datanode02 -> datanode01]
     *
     * </pre>
     *
     * @param peerDataNode Peer Node
     * @param channel      连接
     * @param dataNodeId   DataNode ID
     */
    public void addPeerNode(String peerDataNode, int dataNodeId, SocketChannel channel, int selfDataNodeId) {
        synchronized (this) {
            PeerDataNode oldPeer = dataNodeChannelMap.get(peerDataNode);
            PeerDataNode newPeer = new PeerDataNodeServer(channel, dataNodeId);
            if (oldPeer == null) {
                log.info("收到PeerDataNode的通知网络包, 保存连接以便下一次使用");
                dataNodeChannelMap.put(peerDataNode, newPeer);
            } else {
                if (oldPeer instanceof PeerDataNodeServer && newPeer.getDataNodeId() == oldPeer.getDataNodeId()) {
                    // 此种情况为断线重连, 需要更新channel
                    PeerDataNodeServer peerDataNodeServer = (PeerDataNodeServer) oldPeer;
                    peerDataNodeServer.setSocketChannel(channel);
                    log.info("PeerDataNode断线重连，更新channel: {}", oldPeer.getDataNodeId());
                } else {
                    // 此种情况为本身作为客户端以及发起连接，但是作为服务端同样收到了链接请求
                    // 首先判断是不是旧的连接已经不可用，如果已经不可用的话，直接替换新的
                    if (!oldPeer.isConnected()) {
                        oldPeer.close();
                        dataNodeChannelMap.put(peerDataNode, newPeer);
                        log.info("旧的连接已经失效，则关闭旧的连接, 并替换链接: {}", oldPeer.getDataNodeId());
                        return;
                    }
                    if (selfDataNodeId > dataNodeId) {
                        newPeer.close();
                        log.info("新的连接dataNodeId比较小，关闭新的连接: {}", newPeer.getDataNodeId());
                    } else {
                        oldPeer.close();
                        dataNodeChannelMap.put(peerDataNode, newPeer);
                        log.info("新的连接dataNodeId比较大，则关闭旧的连接, 并替换链接: {}", oldPeer.getDataNodeId());
                    }
                }
            }
        }
    }

    /**
     * 优雅停止
     */
    public void shutdown() {
        log.info("Shutdown PeerDataNodes");
        for (PeerDataNode peerDataNode : dataNodeChannelMap.values()) {
            peerDataNode.close();
        }
    }

    /**
     * 表示一个DataNode节点的连接
     */
    private interface PeerDataNode {
        /**
         * 往 Peer DataNode 发送网络包, 如果连接断开了，会同步等待连接重新建立
         *
         * @param nettyPacket 网络包
         * @throws InterruptedException 中断异常
         */
        void send(NettyPacket nettyPacket) throws InterruptedException;

        /**
         * 关闭连接
         */
        void close();

        /**
         * 获取DataNodeId
         *
         * @return DataNode ID
         */
        int getDataNodeId();

        /**
         * 是否连接上
         *
         * @return 是否连接上
         */
        boolean isConnected();

    }

    private static abstract class AbstractPeerDataNode implements PeerDataNode {
        private int dataNodeId;

        public AbstractPeerDataNode(int dataNodeId) {
            this.dataNodeId = dataNodeId;
        }

        @Override
        public int getDataNodeId() {
            return dataNodeId;
        }
    }


    /**
     * 表示和PeerDataNode的连接，当前DataNode作为客户端
     */
    private static class PeerDataNodeClient extends AbstractPeerDataNode {
        private NetClient netClient;


        public PeerDataNodeClient(NetClient netClient, int dataNodeId) {
            super(dataNodeId);
            this.netClient = netClient;
        }

        @Override
        public void send(NettyPacket nettyPacket) throws InterruptedException {
            netClient.send(nettyPacket);
        }

        @Override
        public void close() {
            netClient.shutdown();
        }

        @Override
        public boolean isConnected() {
            return netClient.isConnected();
        }
    }

    /**
     * 表示和PeerDataNode的连接，当前DataNode作为服务端
     */
    private static class PeerDataNodeServer extends AbstractPeerDataNode {
        private volatile SocketChannel socketChannel;

        public PeerDataNodeServer(SocketChannel socketChannel, int dataNodeId) {
            super(dataNodeId);
            this.socketChannel = socketChannel;
        }

        public void setSocketChannel(SocketChannel socketChannel) {
            synchronized (this) {
                this.socketChannel = socketChannel;
                notifyAll();
            }
        }

        @Override
        public void send(NettyPacket nettyPacket) throws InterruptedException {
            synchronized (this) {
                // 如果这里断开连接了，会一直等待直到客户端会重新建立连接
                while (!socketChannel.isActive()) {
                    try {
                        wait(10);
                    } catch (InterruptedException e) {
                        log.error("PeerDataNodeServer#send has Interrupted !!");
                    }
                }
            }
            socketChannel.writeAndFlush(nettyPacket);
        }

        @Override
        public void close() {
            socketChannel.close();
        }

        @Override
        public boolean isConnected() {
            return socketChannel != null && socketChannel.isActive();
        }
    }
}