package com.bytetenns.datanode.server;

import com.google.protobuf.InvalidProtocolBufferException;
import com.bytetenns.netty.NettyPacket;
import com.bytetenns.enums.PacketType;
// import com.bytetenns.datanode.metrics.Prometheus;
import com.bytetenns.network.AbstractChannelHandler;
import com.bytetenns.network.RequestWrapper;
import com.bytetenns.network.file.DefaultFileSendTask;
import com.bytetenns.network.file.FilePacket;
import com.bytetenns.network.file.FileReceiveHandler;
import com.bytetenns.scheduler.DefaultScheduler;
import com.bytetenns.datanode.conf.DataNodeConfig;
import com.bytetenns.datanode.namenode.NameNodeClient;
import com.bytetenns.datanode.replica.PeerDataNodes;
import com.bytetenns.dfs.model.GetFileRequest;
import com.bytetenns.dfs.model.datanode.PeerNodeAwareRequest;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.SocketChannel;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

/**
 * DataNode节点的服务端接口
 *
 * @author gongwei
 */
@Slf4j
public class DataNodeApis extends AbstractChannelHandler {

    private DefaultScheduler defaultScheduler;
    private DefaultFileTransportCallback transportCallback;
    private DataNodeConfig dataNodeConfig;
    private FileReceiveHandler fileReceiveHandler;
    private PeerDataNodes peerDataNodes;

    public DataNodeApis(DataNodeConfig dataNodeConfig, DefaultFileTransportCallback transportCallback,
            DefaultScheduler defaultScheduler) {
        this.dataNodeConfig = dataNodeConfig;
        this.transportCallback = transportCallback;
        this.defaultScheduler = defaultScheduler;
        // 负责接收文件: 1、客户端上传文件； 2、当前DataNode从其他DataNode中同步文件副本
        this.fileReceiveHandler = new FileReceiveHandler(transportCallback);
    }

    public void setPeerDataNodes(PeerDataNodes peerDataNodes) {
        this.peerDataNodes = peerDataNodes;
    }

    public void setNameNodeClient(NameNodeClient nameNodeClient) {
        this.transportCallback.setNameNodeClient(nameNodeClient);
    }

    /*
     * 感兴趣的消息类型集合, 返回空集合表示所有信息都感兴趣
     */
    @Override
    protected Set<Integer> interestPackageTypes() {
        return new HashSet<>();
    }

    /*
     * 处理网络包，根据不同的请求类型进行处理
     */
    @Override
    protected boolean handlePackage(ChannelHandlerContext ctx, NettyPacket request) throws Exception {
        PacketType packetType = PacketType.getEnum(request.getPacketType());
        RequestWrapper requestWrapper = new RequestWrapper(ctx, request);
        switch (packetType) {
            // 上传文件请求
            case TRANSFER_FILE:
                handleFileTransferRequest(requestWrapper);
                break;
            // Client或PeerDataNode下载文件的请求
            case GET_FILE:
                handleGetFileRequest(requestWrapper);
                break;
            // PeerDataNode感知请求
            case DATA_NODE_PEER_AWARE:
                handleDataNodePeerAwareRequest(requestWrapper);
                break;
            default:
                break;
        }
        return true;
    }

    /**
     * 处理PeerDataNode感知的请求
     */
    private void handleDataNodePeerAwareRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException {
        NettyPacket request = requestWrapper.getRequest();
        // 解析request，获得PeerDataNode的相关信息
        PeerNodeAwareRequest peerNodeAwareRequest = PeerNodeAwareRequest.parseFrom(request.getBody());
        // 添加PeerDataNode
        peerDataNodes.addPeerNode(peerNodeAwareRequest.getPeerDataNode(), peerNodeAwareRequest.getDataNodeId(),
                (SocketChannel) requestWrapper.getCtx().channel(), dataNodeConfig.getDataNodeId());
    }

    /**
     * 处理客户端或PeerDataNode过来下载文件的请求
     */
    private void handleGetFileRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException {
        // 解析下载文件请求
        GetFileRequest request = GetFileRequest.parseFrom(requestWrapper.getRequest().getBody());
        try {
            // 找到对应的文件
            String filename = request.getFilename();
            String path = transportCallback.getPath(filename);
            File file = new File(path);
            log.info("收到下载文件请求：{}", filename);
            // Prometheus.incCounter("datanode_get_file_count", "DataNode收到的下载文件请求数量");
            // Prometheus.hit("datanode_get_file_qps", "DataNode瞬时下载文件QPS");
            // 创建FileSendTask
            DefaultFileSendTask fileSendTask = new DefaultFileSendTask(file, filename,
                    (SocketChannel) requestWrapper.getCtx().channel(),
                    (total, current, progress, currentReadBytes) -> 
                    // Prometheus.hit("datanode_disk_read_bytes", "DataNode瞬时读磁盘字节大小", currentReadBytes)
                    log.info("DataNode正在读磁盘...")
                    );
            fileSendTask.execute(false);
        } catch (Exception e) {
            log.error("文件下载失败：", e);
        }
    }

    /**
     * 处理文件传输,客户端上传文件
     */
    private void handleFileTransferRequest(RequestWrapper requestWrapper) {
        // 解析上传文件请求
        FilePacket filePacket = FilePacket.parseFrom(requestWrapper.getRequest().getBody());
        // if (filePacket.getType() == FilePacket.HEAD) {
        //     Prometheus.incCounter("datanode_put_file_count", "DataNode收到的上传文件请求数量");
        //     Prometheus.hit("datanode_put_file_qps", "DataNode瞬时上传文件QPS");
        // }
        fileReceiveHandler.handleRequest(filePacket);
    }
}
