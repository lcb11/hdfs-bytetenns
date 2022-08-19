package com.bytetenns.datanode.server;

import com.google.protobuf.InvalidProtocolBufferException;
import com.bytetenns.datanode.netty.NettyPacket;
import com.bytetenns.datanode.enums.PacketType;
// import com.bytetenns.datanode.metrics.Prometheus;
import com.bytetenns.datanode.network.AbstractChannelHandler;
import com.bytetenns.datanode.network.RequestWrapper;
import com.bytetenns.datanode.network.file.DefaultFileSendTask;
import com.bytetenns.datanode.network.file.FilePacket;
import com.bytetenns.datanode.network.file.FileReceiveHandler;
import com.bytetenns.datanode.utils.DefaultScheduler;
import com.bytetenns.datanode.conf.DataNodeConfig;
import com.bytetenns.datanode.namenode.NameNodeClient;
import com.bytetenns.datanode.replica.PeerDataNodes;
import com.ruyuan.dfs.model.common.GetFileRequest;
import com.ruyuan.dfs.model.datanode.PeerNodeAwareRequest;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.SocketChannel;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

/**
 * DataNode节点的服务端接口
 *
 * @author Sun Dasheng
 */
@Slf4j
public class DataNodeApis extends AbstractChannelHandler {

    private DefaultScheduler defaultScheduler;
    private DefaultFileTransportCallback transportCallback;
    private DataNodeConfig dataNodeConfig;
    private FileReceiveHandler fileReceiveHandler;
    private PeerDataNodes peerDataNodes;

    public DataNodeApis(DataNodeConfig dataNodeConfig, DefaultFileTransportCallback transportCallback, DefaultScheduler defaultScheduler) {
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


    @Override
    protected Set<Integer> interestPackageTypes() {
        return new HashSet<>();
    }

    @Override
    protected boolean handlePackage(ChannelHandlerContext ctx, NettyPacket request) throws Exception {
        PacketType packetType = PacketType.getEnum(request.getPacketType());
        RequestWrapper requestWrapper = new RequestWrapper(ctx, request);
        switch (packetType) {
            case TRANSFER_FILE:
                handleFileTransferRequest(requestWrapper);
                break;
            case GET_FILE:
                handleGetFileRequest(requestWrapper);
                break;
            case DATA_NODE_PEER_AWARE:
                handleDataNodePeerAwareRequest(requestWrapper);
                break;
            default:
                break;
        }
        return true;
    }

    private void handleDataNodePeerAwareRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException {
        NettyPacket request = requestWrapper.getRequest();
        PeerNodeAwareRequest peerNodeAwareRequest = PeerNodeAwareRequest.parseFrom(request.getBody());
        peerDataNodes.addPeerNode(peerNodeAwareRequest.getPeerDataNode(), peerNodeAwareRequest.getDataNodeId(),
                (SocketChannel) requestWrapper.getCtx().channel(), dataNodeConfig.getDataNodeId());
    }

    /**
     * 处理客户端或PeerDataNode过来下载文件的请求
     */
    private void handleGetFileRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException {
        GetFileRequest request = GetFileRequest.parseFrom(requestWrapper.getRequest().getBody());
        try {
            String filename = request.getFilename();
            String path = transportCallback.getPath(filename);
            File file = new File(path);
            log.info("收到下载文件请求：{}", filename);
            Prometheus.incCounter("datanode_get_file_count", "DataNode收到的下载文件请求数量");
            Prometheus.hit("datanode_get_file_qps", "DataNode瞬时下载文件QPS");
            DefaultFileSendTask fileSendTask = new DefaultFileSendTask(file, filename,
                    (SocketChannel) requestWrapper.getCtx().channel(), (total, current, progress, currentReadBytes)
                    -> Prometheus.hit("datanode_disk_read_bytes", "DataNode瞬时读磁盘字节大小", currentReadBytes));
            fileSendTask.execute(false);
        } catch (Exception e) {
            log.error("文件下载失败：", e);
        }
    }

    /**
     * 处理文件传输,客户端上传文件
     */
    private void handleFileTransferRequest(RequestWrapper requestWrapper) {
        FilePacket filePacket = FilePacket.parseFrom(requestWrapper.getRequest().getBody());
        if (filePacket.getType() == FilePacket.HEAD) {
            Prometheus.incCounter("datanode_put_file_count", "DataNode收到的上传文件请求数量");
            Prometheus.hit("datanode_put_file_qps", "DataNode瞬时上传文件QPS");
        }
        fileReceiveHandler.handleRequest(filePacket);
    }
}
