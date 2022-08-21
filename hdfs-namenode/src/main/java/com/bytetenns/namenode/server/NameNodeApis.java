package com.bytetenns.namenode.server;

import com.bytetenns.dfs.model.client.CreateFileRequest;
import com.bytetenns.dfs.model.client.CreateFileResponse;
import com.bytetenns.dfs.model.client.MkdirRequest;
import com.bytetenns.dfs.model.common.DataNode;
import com.bytetenns.dfs.model.datanode.HeartbeatRequest;
import com.bytetenns.dfs.model.datanode.RegisterRequest;
import com.bytetenns.dfs.model.datanode.ReplicaCommand;
import com.bytetenns.enums.NameNodeLaunchMode;
import com.bytetenns.enums.PacketType;
import com.bytetenns.exception.NameNodeException;
import com.bytetenns.metrics.Prometheus;
import com.bytetenns.namenode.datanode.DataNodeInfo;
import com.bytetenns.namenode.datanode.DataNodeManager;
import com.bytetenns.namenode.datanode.ReplicaTask;
import com.bytetenns.namenode.fs.DiskNameSystem;
import com.bytetenns.namenode.fs.Node;
import com.bytetenns.namenode.shard.ShardingManager;
import com.bytetenns.netty.NettyPacket;
import com.bytetenns.network.AbstractChannelHandler;
import com.bytetenns.network.RequestWrapper;
import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;


/**
 * @author yw
 * Name网络请求接口服务器
 * @create 2022-08-20 15:00
 */
@Slf4j
public class NameNodeApis extends AbstractChannelHandler {
    private final ThreadPoolExecutor executor;
    private int nodeId;
    private final DataNodeManager dataNodeManager;
    private final  ShardingManager shardingManager;
    private final NameNodeLaunchMode mode;//
    private final DiskNameSystem diskNameSystem;

    public NameNodeApis(ThreadPoolExecutor executor, int nodeId, DataNodeManager dataNodeManager, ShardingManager shardingManager, NameNodeLaunchMode mode, DiskNameSystem diskNameSystem) {
        this.executor = executor;
        this.nodeId = nodeId;
        this.dataNodeManager = dataNodeManager;
        this.shardingManager = shardingManager;
        this.mode = mode;
        this.diskNameSystem = diskNameSystem;
    }

    @Override
    protected Set<Integer> interestPackageTypes() {
        return new HashSet<>();
    }

    //获取执行器
    @Override
    protected Executor getExecutor() {
        return executor;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
    }

    @Override
    protected boolean handlePackage(ChannelHandlerContext ctx, NettyPacket request) throws Exception {
        //判断请求包的类型
        PacketType packetType = PacketType.getEnum(request.getPacketType());
        //{
        // delete..}
        //网络请求,为什么要这样封装
        RequestWrapper requestWrapper = new RequestWrapper(ctx, request, nodeId, bodyLength -> {

        });
        switch (packetType) {
            case DATA_NODE_REGISTER://"1. DataNode往NameNode注册请求"
                handleDataNodeRegisterRequest(requestWrapper);
                break;
            case HEART_BRET: // "2.DataNode往NameNode发送的心跳请求"
                handleDataNodeHeartbeatRequest(requestWrapper);
                break;
            case MKDIR:// "3.客户端往NameNode发送Mkdir请求"
                handMkdirRequest(requestWrapper);
                break;
            case CREATE_FILE_CONFIRM: // "4.客户端往DataNode上传完文件之后，再发请求往NameNode确认"),
                break;
            case FETCH_EDIT_LOG://(5,"BackupNode往NameNode发送抓取EditLog请求"),
                handleFetchEitLogRequest(requestWrapper);
                break;
            case TRANSFER_FILE://(6, "文件传输的二进制包类型")
                handleFileTransferRequest(requestWrapper);
                break;
            case REPORT_STORAGE_INFO://(7, "DataNode往NameNode上报存储信息请求"),
                handleDataNodeReportStorageInfoRequest(requestWrapper);
                break;
            case CREATE_FILE://(8, "客户端端往NameNode发送创建文件请求"),
                handleCreateFileRequest(requestWrapper);
                break;
            case GET_DATA_NODE_FOR_FILE://(9, "客户端往NameNode发送获取文件所在DataNode的请求"),
                handleGetDataNodeForFileRequest(requestWrapper);
                break;
            case REPLICA_RECEIVE://(10, "DataNode往NameNode上报收到一个文件的请求"),
                handleReplicaReceiveRequest(requestWrapper);
                break;
            case GET_FILE://(11, "客户端从DataNode下载文件，或DataNode之间相互同步副本请求"),
                handleRemoveFileRequest(requestWrapper);
                break;
            // case DATA_NODE_PEER_AWARE://(12, "DataNode之间建立连接后发起的相互感知的请求"),
            case REPORT_BACKUP_NODE_INFO://(13, "BackupNode往NameNode上报自身信息"),
                handleReportBackupNodeInfoRequest(requestWrapper);
                break;
            case FETCH_BACKUP_NODE_INFO://(14, "客户端或者DataName往NameNode上发起请求获取BackupNode信息"),
                handleFetchBackupNodeInfoRequest(requestWrapper);
                break;
            case AUTHENTICATE://(15, "客户端发起的认证请求"),
                handleAuthenticateRequest(requestWrapper);
                break;
            case GET_NAME_NODE_STATUS://(17, "BackupNode感知到NameNode挂了，往客户端和DataNode确认NameNode的状态"),
            case DUPLICATE_BACKUP_NODE://(18, "一个NameNode只支持一个BackupNode, 重复的BackupNode异常信息类型"),
            case NAME_NODE_CONTROLLER_VOTE://(19, "NameNode投票选举的票据"),
                //case NAME_NODE_PEER_AWARE: //(20, "NameNode相互之间发起连接时的感知请求"),
                //case NAME_NODE_SLOT_BROADCAST: //(21, "Controller选举成功后，往所有NameNode广播Slots信息"),
                //case RE_BALANCE_SLOTS://(22, "新节点加入集群, 申请重新分配Slots"),
                //   controllerManager.onRebalanceSlots(requestWrapper);
                //    break;
                //case FETCH_SLOT_METADATA://(23, "新节点从旧节点中拉取文件目录树元数据"),
                //    controllerManager.writeMetadataToPeer(requestWrapper);
                //   break;
                //case FETCH_SLOT_METADATA_RESPONSE://(24, "旧节点往新节点发送文件目录树元数据"),
                //   controllerManager.onFetchMetadata(requestWrapper);
                //   break;
                //case FETCH_SLOT_METADATA_COMPLETED://(25, "新节点完成了文件目录树的拉取，通知Controller"),
                //   controllerManager.onLocalControllerFetchSlotMetadataCompleted(requestWrapper);
                //  break;
                //case FETCH_SLOT_METADATA_COMPLETED_BROADCAST://(26, "Controller广播给所有NameNode，新节点完成了元数据拉取回放"),
                //case REMOVE_METADATA_COMPLETED://(27, "旧节点删除不属于自身Slot的内存目录树之后，往Controller上报"),
                //    case BACKUP_NODE_SLOT://(28, "NameNode往BackupNode下发属于Slots分配信息"),
            case CLIENT_LOGOUT://(29, "客户端断开连接，清除认证消息"),
                log.debug("收到客户端退出登录的请求");
                // case USER_CHANGE_EVENT://(30, "用户信息增删查改事件"),
                //   userManager.onUserChangeEvent(request);
                // break;
            case REMOVE_FILE://(31, "客户端删除文件"),
                handleRemoveFileRequest(requestWrapper);
                break;
            case READ_ATTR://(32, "客户端读取文件属性"),
                handleReadAttrRequest(requestWrapper);
                break;
            // case FETCH_USER_INFO://(33, "NameNode往PeerNameNode获取用户信息"),

            //case LIST_FILES://(34, "NameNode往PeerNameNode获取文件列表"),
            case TRASH_RESUME://(35, "文件在垃圾箱放回原处的请求"),
                handleTrashResumeRequest(requestWrapper);
                break;
            case NEW_PEER_NODE_INFO://(36, "NameNode的Controller节点在新节点上线后发送的信息"),
                handleNewPeerDataNodeInfoRequest(requestWrapper);
                break;
            case REPLICA_REMOVE://(37, "DataNode往NameNode上报移除一个文件的请求"),
                handleReplicaRemoveRequest(requestWrapper);
                break;
            case FETCH_NAME_NODE_INFO://(38, "获取NameNode基本信息"),
                handleFetchNameNodeInfoRequest(requestWrapper);
                break;
            case NAME_NODE_REMOVE_FILE://(39, "NameNode节点删除文件"),
                handleNameNodeRemoveFileRequest(requestWrapper);
                break;
            case FETCH_DATA_NODE_BY_FILENAME://(40, "根据文件名获取DataNode机器名"),
                handleFetchDataNodeByFilenameRequest(requestWrapper);
                break;
            case ADD_REPLICA_NUM://(41, "新增文件副本数量"),
                handleAddReplicaNumRequest(requestWrapper);
                break;
            case CLIENT_LIST_FILES://(42, "客户端获取文件列表"),
                handleClientListFilesRequest(requestWrapper);
                break;
            case CLIENT_READ_NAME_NODE_INFO://(43, "客户端获取NameNode基本信息"),
                handleClientReadNameNodeInfoReqeust(requestWrapper);
                break;
            case CLIENT_READ_DATA_NODE_INFO://(44, "客户端获取DataNode基本信息"),
                handleClientReadDataNodeInfoRequest(requestWrapper);
                break;
            case CLIENT_READ_STORAGE_INFO://(45, "客户端获取文件存储信息"),
                handleClientReadStorageInfoRequest(requestWrapper);
                break;
            case CLIENT_PRE_CALCULATE://(46, "客户端导出文件/文件夹前计算文件数量的请求"),
                handleClientPreCalculateRequest(requestWrapper);
                break;
            case CLIENT_GET_ALL_FILENAME://(47, "客户端获取文件/文件夹包含的所有文件全路径"),
                handleClientGetAllFilenameRequest(requestWrapper);
                break;
            default:
                break;
        }
        return false;
    }

    private void handleAuthenticateRequest(RequestWrapper requestWrapper) {

    }

    private void handleFetchBackupNodeInfoRequest(RequestWrapper requestWrapper) {

    }

    private void handleClientGetAllFilenameRequest(RequestWrapper requestWrapper) {

    }

    private void handleClientPreCalculateRequest(RequestWrapper requestWrapper) {

    }

    private void handleClientReadStorageInfoRequest(RequestWrapper requestWrapper) {

    }

    private void handleClientReadDataNodeInfoRequest(RequestWrapper requestWrapper) {

    }

    private void handleClientReadNameNodeInfoReqeust(RequestWrapper requestWrapper) {

    }

    private void handleClientListFilesRequest(RequestWrapper requestWrapper) {

    }

    private void handleAddReplicaNumRequest(RequestWrapper requestWrapper) {


    }

    private void handleFetchDataNodeByFilenameRequest(RequestWrapper requestWrapper) {

    }

    private void handleNameNodeRemoveFileRequest(RequestWrapper requestWrapper) {

    }

    private void handleFetchNameNodeInfoRequest(RequestWrapper requestWrapper) {

    }

    private void handleReplicaRemoveRequest(RequestWrapper requestWrapper) {


    }

    private void handleNewPeerDataNodeInfoRequest(RequestWrapper requestWrapper) {

    }

    private void handleTrashResumeRequest(RequestWrapper requestWrapper) {

    }

    private void handleReadAttrRequest(RequestWrapper requestWrapper) {

    }

    private void handleReportBackupNodeInfoRequest(RequestWrapper requestWrapper) {

    }

    private void handleRemoveFileRequest(RequestWrapper requestWrapper) {

    }

    private void handleReplicaReceiveRequest(RequestWrapper requestWrapper) {

    }

    private void handleGetDataNodeForFileRequest(RequestWrapper requestWrapper) {

    }

    //(8, "客户端端往NameNode发送创建文件请求"),
    private void handleCreateFileRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException, NameNodeException {
       //获取请求包
        NettyPacket request = requestWrapper.getRequest();
        //序列化
        CreateFileRequest createFileRequest = CreateFileRequest.parseFrom(request.getBody());
        //获取文件名
        String fileName=File.separator+request.getUserName()+createFileRequest.getFilename();
        //获取节点DataID
        int nodeId = getNodeId(fileName);
        if (this.nodeId == nodeId) {//filename对应的noteId与接口服务器对应的noteID相等
            //装载序列化数据
            Map<String, String> attrMap = new HashMap<>(createFileRequest.getAttrMap());
            //副本数
            String replicaNumStr = attrMap.get("REPLICA_NUM");
            //文件大小
            attrMap.put("FILE_SIZE", String.valueOf(createFileRequest.getFileSize()));
            int replicaNum;//获取副本数量
            if (replicaNumStr != null) {
                replicaNum = Integer.parseInt(replicaNumStr);
                // 副本最少不能少于配置的数量
                replicaNum = Math.max(replicaNum, diskNameSystem.getNameNodeConfig().getReplicaNum());
                // 最大不能大于最大的数量
                replicaNum = Math.min(replicaNum, 5);
            } else {
                replicaNum = diskNameSystem.getNameNodeConfig().getReplicaNum();
                attrMap.put("REPLICA_NUM", String.valueOf(replicaNum));
            }
            //根据文件名获取现有节点
            Node node = diskNameSystem.listFiles(fileName);
            if (node != null) {
                throw new NameNodeException("文件已存在：" + createFileRequest.getFilename());
            }
           //获取文件和副本的布置DataNode节点
            List<DataNodeInfo> dataNodeList = dataNodeManager.allocateDataNodes(request.getUserName(), replicaNum, fileName);
            //nameNode收到和发送文件数量监控
            Prometheus.incCounter("namenode_put_file_count", "NameNode收到的上传文件请求数量");
            //上传和下载文件的速率
            Prometheus.hit("namenode_put_file_qps", "NameNode瞬时上传文件QPS");
           //逐一操作DataNode,创建连接
            List<DataNode> dataNodes = dataNodeList.stream()
                    .map(e -> DataNode.newBuilder().setHostname(e.getHostname())
                            .setNioPort(e.getNioPort())
                            .setHttpPort(e.getHttpPort())
                            .build())
                    .collect(Collectors.toList());
            //创建文件
            diskNameSystem.createFile(fileName, attrMap);
            List<String> collect = dataNodeList.stream().map(DataNodeInfo::getHostname).collect(Collectors.toList());
            log.info("创建文件：[filename={}, datanodes={}]", fileName, String.join(",", collect));
            CreateFileResponse response = CreateFileResponse.newBuilder()
                    .addAllDataNodes(dataNodes)
                    .setRealFileName(fileName)
                    .build();
            requestWrapper.sendResponse(response);
        } else {
            //forwardRequestToOtherNameNode(nodeId, requestWrapper);
        }
    }

    // DataNode往NameNode上报存储信息请求
    private void handleDataNodeReportStorageInfoRequest(RequestWrapper requestWrapper) {
    }

    //(6, "文件传输的二进制包类型")
    private void handleFileTransferRequest(RequestWrapper requestWrapper) {
    }

    private void handleFetchEitLogRequest(RequestWrapper requestWrapper) {

    }

    // "客户端往NameNode发送Mkdir请求"
    private void handMkdirRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException {
        //默认通过用户认证

        //获取请求的Netty包
        NettyPacket request = requestWrapper.getRequest();
        //序列化
        MkdirRequest mkdirRequest = MkdirRequest.parseFrom(request.getBody());
        //获取文件名
        String realFilename = File.separator + request.getUserName() + mkdirRequest.getPath();
        //根据文件名获取nodeId
        int nodeId = getNodeId(realFilename);

        if (this.nodeId == nodeId) {
            this.diskNameSystem.mkdir(realFilename, mkdirRequest.getAttrMap());
            requestWrapper.sendResponse();
        } else {
            //forwardRequestToOtherNameNode(nodeId, requestWrapper);
        }
    }

    //DataNode往NameNode发送请求
    private void handleDataNodeHeartbeatRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException, NameNodeException {
        //1.heart
        HeartbeatRequest heartbeatRequest = HeartbeatRequest.parseFrom(requestWrapper.getRequest().getBody());
        //发送心跳返回结果
        boolean heartbeat = dataNodeManager.heartbeat(heartbeatRequest.getHostname());
        if (!heartbeat) {
            throw new NameNodeException("心跳失败，DataNode不存在：" + heartbeatRequest.getHostname());
        }
        //通过请求回去dataNode信息
        DataNodeInfo dataNode = dataNodeManager.getDataNode(heartbeatRequest.getHostname());
        //获取dataNode的副本复制任务
        List<ReplicaTask> replicaTask = dataNode.pollReplicaTask(100);
        //建立副本命令的链表容器
        LinkedList<ReplicaCommand> replicaCommands = new LinkedList<>();
        if(!replicaTask.isEmpty()){
            replicaTask.stream().map(e->ReplicaCommand.newBuilder());
            //.setFilename()  未完成
                //    .setHostname()
        }

    }
    //DataNde发送注册请求

    private void handleDataNodeRegisterRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException, NameNodeException {
        //序列化
        RegisterRequest registerRequest = RegisterRequest.parseFrom(requestWrapper.getRequest().getBody());
        //注册并返回结果，是否注册
        boolean result = dataNodeManager.registerRequest(registerRequest);
        if (!result) {
            throw new NameNodeException("注册失败，DataNode节点已经存在！");
        }
        // {
        // delete :broadcast}

    }
    //通过文件名获取节点dataid
    public int getNodeId(String filename) {
        if (NameNodeLaunchMode.CLUSTER.equals(mode)) {
            return shardingManager.getNameNodeIdByFileName(filename);
        }
        return this.nodeId;
    }


}
//




