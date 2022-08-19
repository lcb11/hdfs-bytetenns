package com.bytetenns.namenode.server;

import com.bytetenns.common.enums.NameNodeLaunchMode;
import com.bytetenns.common.enums.PacketType;
import com.bytetenns.common.network.AbstractChannelHandler;
import com.bytetenns.common.network.RequestWrapper;
import com.bytetenns.common.scheduler.DefaultScheduler;
import com.bytetenns.namenode.NameNodeConfig;
import com.bytetenns.namenode.datanode.DataNodeManager;
import com.bytetenns.namenode.fs.DiskNameSystem;
import com.bytetenns.namenode.shard.ShardingManager;
import com.google.protobuf.InvalidProtocolBufferException;
import com.ruyuan.dfs.common.Constants;
import com.ruyuan.dfs.common.FileInfo;
import com.ruyuan.dfs.common.NettyPacket;
import com.ruyuan.dfs.common.enums.CommandType;
import com.ruyuan.dfs.common.enums.NameNodeLaunchMode;
import com.ruyuan.dfs.common.enums.NodeType;
import com.ruyuan.dfs.common.enums.PacketType;
import com.ruyuan.dfs.common.exception.NameNodeException;
import com.ruyuan.dfs.common.exception.RequestTimeoutException;
import com.ruyuan.dfs.common.metrics.Prometheus;
import com.ruyuan.dfs.common.network.AbstractChannelHandler;
import com.ruyuan.dfs.common.network.RequestWrapper;
import com.ruyuan.dfs.common.network.file.FilePacket;
import com.ruyuan.dfs.common.network.file.FileReceiveHandler;
import com.ruyuan.dfs.common.utils.DefaultScheduler;
import com.ruyuan.dfs.common.utils.NetUtils;
import com.ruyuan.dfs.common.utils.PrettyCodes;
import com.ruyuan.dfs.model.backup.*;
import com.ruyuan.dfs.model.client.*;
import com.ruyuan.dfs.model.common.DataNode;
import com.ruyuan.dfs.model.datanode.*;
import com.ruyuan.dfs.model.namenode.*;
import com.ruyuan.dfs.namenode.config.NameNodeConfig;
import com.ruyuan.dfs.namenode.datanode.DataNodeInfo;
import com.ruyuan.dfs.namenode.datanode.DataNodeManager;
import com.ruyuan.dfs.namenode.editslog.EditLogWrapper;
import com.ruyuan.dfs.namenode.fs.CalculateResult;
import com.ruyuan.dfs.namenode.fs.DiskNameSystem;
import com.ruyuan.dfs.namenode.fs.Node;
import com.ruyuan.dfs.namenode.rebalance.RemoveReplicaTask;
import com.ruyuan.dfs.namenode.rebalance.ReplicaTask;
import com.ruyuan.dfs.namenode.server.tomcat.domain.User;
import com.ruyuan.dfs.namenode.shard.ShardingManager;
import com.ruyuan.dfs.namenode.shard.controller.ControllerManager;
import com.ruyuan.dfs.namenode.shard.peer.PeerNameNode;
import com.ruyuan.dfs.namenode.shard.peer.PeerNameNodes;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.SocketChannel;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * NameNode网络请求接口处理器
 *
 * @author Sun Dasheng
 */
@Slf4j
public class NameNodeApis extends AbstractChannelHandler {

    private final DefaultScheduler defaultScheduler;
    private final DiskNameSystem diskNameSystem;
    private final ShardingManager shardingManager;
    private final NameNodeConfig nameNodeConfig;
    private final DataNodeManager dataNodeManager;
    private final ThreadPoolExecutor executor;
    protected int nodeId;
    private final NameNodeLaunchMode mode;
    private final FetchEditLogBuffer fetchEditLogBuffer;
    private final AtomicBoolean slotsChanged = new AtomicBoolean(false);
    private Map<Integer, Integer> slots;
    private final FileReceiveHandler fileReceiveHandler;
    private BackupNodeInfoHolder backupNodeInfoHolder;

    public NameNodeApis(NameNodeConfig nameNodeConfig, DataNodeManager dataNodeManager, PeerNameNodes peerNameNodes,
                        ShardingManager shardingManager, DiskNameSystem diskNameSystem, DefaultScheduler defaultScheduler,
                        UserManager userManager, ControllerManager controllerManager) {
        this.dataNodeManager = dataNodeManager;
        this.peerNameNodes = peerNameNodes;
        this.nameNodeConfig = nameNodeConfig;
        this.shardingManager = shardingManager;
        this.diskNameSystem = diskNameSystem;
        this.nodeId = nameNodeConfig.getNameNodeId();
        this.mode = nameNodeConfig.getMode();
        this.userManager = userManager;
        this.peerNameNodes.setNameNodeApis(this);
        this.controllerManager = controllerManager;
        this.defaultScheduler = defaultScheduler;
        this.fetchEditLogBuffer = new FetchEditLogBuffer(diskNameSystem);
        this.shardingManager.addOnSlotAllocateCompletedListener(slots -> {
            if (slotsChanged.compareAndSet(false, true)) {
                this.slots = slots;
            }
        });
        FsImageFileTransportCallback fsImageFileTransportCallback = new FsImageFileTransportCallback(nameNodeConfig,
                defaultScheduler, diskNameSystem);
        this.fileReceiveHandler = new FileReceiveHandler(fsImageFileTransportCallback);
        this.executor = new ThreadPoolExecutor(nameNodeConfig.getNameNodeApiCoreSize(), nameNodeConfig.getNameNodeApiMaximumPoolSize(),
                60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(nameNodeConfig.getNameNodeApiQueueSize()));
    }

    public BackupNodeInfoHolder getBackupNodeInfoHolder() {
        return backupNodeInfoHolder;
    }

    @Override
    protected Set<Integer> interestPackageTypes() {
        return new HashSet<>();
    }

    @Override
    protected Executor getExecutor() {
        return executor;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        String token = userManager.getTokenByChannel(ctx.channel());
        String username = userManager.logout(ctx.channel());
        if (username != null && token != null) {
            NettyPacket nettyPacket = NettyPacket.buildPacket(new byte[0], PacketType.CLIENT_LOGOUT);
            nettyPacket.setUsername(username);
            nettyPacket.setUserToken(token);
            broadcast(nettyPacket);
        }
        if (backupNodeInfoHolder != null && backupNodeInfoHolder.match(ctx.channel())) {
            backupNodeInfoHolder = null;
        }
        ctx.fireChannelInactive();
    }

    @Override
    protected boolean handlePackage(ChannelHandlerContext ctx, NettyPacket request) {
        boolean consumedMsg = peerNameNodes.onMessage(request);
        if (consumedMsg) {
            return true;
        }
        if (request.isError()) {
            // 在请求转发的情况下，如果目标NameNode节点发生未知异常，然后返回的结果是异常的，
            // 而源NameNode正常处理流程会出现空指针异常，从而出现死循环。
            log.warn("收到一个异常请求或响应, 丢弃不进行处理：[request={}]", request.getHeader());
            return true;
        }
        PacketType packetType = PacketType.getEnum(request.getPacketType());
        Prometheus.incCounter("namenode_net_package_inbound_count", "NameNode收到的请求数量");
        Prometheus.incCounter("namenode_net_package_inbound_bytes", "NameNode收到的请求大小", request.getBody().length);
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        RequestWrapper requestWrapper = new RequestWrapper(ctx, request, nodeId, bodyLength -> {
            Prometheus.incCounter("namenode_net_package_outbound_count", "NameNode返回的响应数量");
            Prometheus.incCounter("namenode_net_package_outbound_bytes", "NameNode返回的响应大小", bodyLength);
            stopWatch.stop();
            long time = stopWatch.getTime();
            Prometheus.gauge("namenode_request_times", "NameNode处理请求耗时", "requestType", packetType.getDescription(), time);
        });
        try {
            switch (packetType) {
                case DATA_NODE_REGISTER:
                    handleDataNodeRegisterRequest(requestWrapper);
                    break;
                case HEART_BRET:
                    handleDataNodeHeartbeatRequest(requestWrapper);
                    break;
                case MKDIR:
                    handleMkdirRequest(requestWrapper);
                    break;
                case FETCH_EDIT_LOG:
                    handleFetchEditLogRequest(requestWrapper);
                    break;
                case TRANSFER_FILE:
                    handleFileTransferRequest(requestWrapper);
                    break;
                case REPORT_STORAGE_INFO:
                    handleDataNodeReportStorageInfoRequest(requestWrapper);
                    break;
                case CREATE_FILE:
                    handleCreateFileRequest(requestWrapper);
                    break;
                case GET_DATA_NODE_FOR_FILE:
                    handleGetDataNodeForFileRequest(requestWrapper);
                    break;
                case REPLICA_RECEIVE:
                    handleReplicaReceiveRequest(requestWrapper);
                    break;
                case REMOVE_FILE:
                    handleRemoveFileRequest(requestWrapper);
                    break;
                case READ_ATTR:
                    handleReadAttrRequest(requestWrapper);
                    break;
                case AUTHENTICATE:
                    handleAuthenticateRequest(requestWrapper);
                    break;
                case REPORT_BACKUP_NODE_INFO:
                    handleReportBackupNodeInfoRequest(requestWrapper);
                    break;
                case FETCH_BACKUP_NODE_INFO:
                    handleFetchBackupNodeInfoRequest(requestWrapper);
                    break;
                case CREATE_FILE_CONFIRM:
                    handleCreateFileConfirmRequest(requestWrapper);
                    break;
                case NAME_NODE_PEER_AWARE:
                    handleNameNodePeerAwareRequest(requestWrapper);
                    break;
                case NAME_NODE_CONTROLLER_VOTE:
                    controllerManager.onReceiveControllerVote(requestWrapper);
                    break;
                case NAME_NODE_SLOT_BROADCAST:
                    controllerManager.onReceiveSlots(requestWrapper);
                    break;
                case RE_BALANCE_SLOTS:
                    controllerManager.onRebalanceSlots(requestWrapper);
                    break;
                case FETCH_SLOT_METADATA:
                    controllerManager.writeMetadataToPeer(requestWrapper);
                    break;
                case FETCH_SLOT_METADATA_RESPONSE:
                    controllerManager.onFetchMetadata(requestWrapper);
                    break;
                case FETCH_SLOT_METADATA_COMPLETED:
                    controllerManager.onLocalControllerFetchSlotMetadataCompleted(requestWrapper);
                    break;
                case FETCH_SLOT_METADATA_COMPLETED_BROADCAST:
                    controllerManager.onRemoteControllerFetchSlotMetadataCompleted(requestWrapper);
                    break;
                case REMOVE_METADATA_COMPLETED:
                    controllerManager.onRemoveMetadataCompleted(requestWrapper);
                    break;
                case CLIENT_LOGOUT:
                    log.debug("收到其他集群的客户端退出登录请求: [username={}]", request.getUserName());
                    userManager.logout(request.getUserName(), request.getUserToken());
                    break;
                case USER_CHANGE_EVENT:
                    userManager.onUserChangeEvent(request);
                    break;
                case FETCH_USER_INFO:
                    handleFetchUserInfoRequest(requestWrapper);
                    break;
                case LIST_FILES:
                    handleListFilesRequest(requestWrapper);
                    break;
                case TRASH_RESUME:
                    handleTrashResumeRequest(requestWrapper);
                    break;
                case NEW_PEER_NODE_INFO:
                    handleNewPeerDataNodeInfoRequest(requestWrapper);
                    break;
                case REPLICA_REMOVE:
                    handleReplicaRemoveRequest(requestWrapper);
                    break;
                case FETCH_NAME_NODE_INFO:
                    handleFetchNameNodeInfoRequest(requestWrapper);
                    break;
                case NAME_NODE_REMOVE_FILE:
                    handleNameNodeRemoveFileRequest(requestWrapper);
                    break;
                case FETCH_DATA_NODE_BY_FILENAME:
                    handleFetchDataNodeByFilenameRequest(requestWrapper);
                    break;
                case ADD_REPLICA_NUM:
                    handleAddReplicaNumRequest(requestWrapper);
                    break;
                case CLIENT_LIST_FILES:
                    handleClientListFilesRequest(requestWrapper);
                    break;
                case CLIENT_READ_NAME_NODE_INFO:
                    handleClientReadNameNodeInfoReqeust(requestWrapper);
                    break;
                case CLIENT_READ_DATA_NODE_INFO:
                    handleClientReadDataNodeInfoRequest(requestWrapper);
                    break;
                case CLIENT_READ_STORAGE_INFO:
                    handleClientReadStorageInfoRequest(requestWrapper);
                    break;
                case CLIENT_PRE_CALCULATE:
                    handleClientPreCalculateRequest(requestWrapper);
                    break;
                case CLIENT_GET_ALL_FILENAME:
                    handleClientGetAllFilenameRequest(requestWrapper);
                    break;
                default:
                    break;
            }
        } catch (NameNodeException e) {
            log.error("发生业务异常：", e);
            sendErrorResponse(requestWrapper, e.getMessage());
        } catch (RequestTimeoutException e) {
            log.info("转发超时了：", e);
            sendErrorResponse(requestWrapper, e.getMessage());
        } catch (Exception e) {
            log.error("NameNode处理消息发生异常：", e);
            sendErrorResponse(requestWrapper, "未知异常：nodeId=" + nodeId);
        }
        return true;
    }

    /**
     * 客户端获取文件/文件夹包含的所有文件全路径
     */
    private void handleClientGetAllFilenameRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException, NameNodeException {
        GetAllFilenameRequest request = GetAllFilenameRequest.parseFrom(requestWrapper.getRequest().getBody());
        String realFilename = File.separator + requestWrapper.getRequest().getUserName() + request.getPath();
        List<String> resultFiles = diskNameSystem.findAllFiles(realFilename);

        List<NettyPacket> nettyPackets = broadcastSync(requestWrapper);
        for (NettyPacket nettyPacket : nettyPackets) {
            GetAllFilenameResponse response = GetAllFilenameResponse.parseFrom(nettyPacket.getBody());
            resultFiles.addAll(response.getFilenameList());
        }
        List<String> result = resultFiles.stream()
                .map(e -> request.getPath() + e)
                .collect(Collectors.toList());

        // 这里如果文件很多，底层会通过chunked机制传输
        GetAllFilenameResponse response = GetAllFilenameResponse.newBuilder()
                .addAllFilename(result)
                .build();
        requestWrapper.sendResponse(response);
    }

    /**
     * 客户端导出文件/文件夹前计算文件数量的请求
     */
    private void handleClientPreCalculateRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException {
        PreCalculateRequest request = PreCalculateRequest.parseFrom(requestWrapper.getRequest().getBody());
        String realFilename = File.separator + requestWrapper.getRequest().getUserName() + request.getPath();
        // 计算本节点的结果
        CalculateResult result = diskNameSystem.calculate(realFilename);

        // 同步获取其他所有节点的结果
        List<NettyPacket> nettyPackets = broadcastSync(requestWrapper);
        for (NettyPacket nettyPacket : nettyPackets) {
            PreCalculateResponse response = PreCalculateResponse.parseFrom(nettyPacket.getBody());
            result.addFileCount(response.getFileCount());
            result.addTotalSize(response.getTotalSize());
        }
        PreCalculateResponse response = PreCalculateResponse.newBuilder()
                .setTotalSize(result.getTotalSize())
                .setFileCount(result.getFileCount())
                .build();
        requestWrapper.sendResponse(response);
    }

    /**
     * 客户端获取文件存储信息
     */
    private void handleClientReadStorageInfoRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException,
            NameNodeException, RequestTimeoutException, InterruptedException {
        ReadStorageInfoRequest request = ReadStorageInfoRequest.parseFrom(requestWrapper.getRequest().getBody());
        String realFilename = File.separator + requestWrapper.getRequest().getUserName() + request.getFilename();
        List<DataNodeInfo> dataNodeByFileName = dataNodeManager.getDataNodeByFileName(realFilename);
        List<String> dataNodeHosts = dataNodeByFileName.stream()
                .map(DataNodeInfo::getHostname)
                .collect(Collectors.toList());
        int nodeId = getNodeId(realFilename);
        if (this.nodeId == nodeId) {
            Node node = diskNameSystem.listFiles(realFilename);
            if (node == null || !node.isFile()) {
                throw new NameNodeException("文件路径错误：" + request.getFilename());
            }
            int replica = Integer.parseInt(node.getAttr().get(Constants.ATTR_REPLICA_NUM));
            ReadStorageInfoResponse response = ReadStorageInfoResponse.newBuilder()
                    .setDatanodes(String.join(",", dataNodeHosts))
                    .setReplica(replica)
                    .build();
            requestWrapper.sendResponse(response);
        } else {
            forwardRequestToOtherNameNode(nodeId, requestWrapper);
        }
    }

    /**
     * 客户端获取DataNode基本信息
     */
    private void handleClientReadDataNodeInfoRequest(RequestWrapper requestWrapper) {
        List<DataNodeInfo> dataNodeInfoList = dataNodeManager.getDataNodeInfoList();
        ClientDataNodeInfo.Builder builder = ClientDataNodeInfo.newBuilder();
        for (DataNodeInfo dataNodeInfo : dataNodeInfoList) {
            ClientDataNode clientDataNode = ClientDataNode.newBuilder()
                    .setHostname(dataNodeInfo.getHostname())
                    .setNodeId(dataNodeInfo.getNodeId())
                    .setStatus(dataNodeInfo.getStatus())
                    .setStoredDataSize(dataNodeInfo.getStoredDataSize())
                    .setFreeSpace(dataNodeInfo.getFreeSpace())
                    .build();
            builder.addClientDataNodes(clientDataNode);
        }
        ClientDataNodeInfo response = builder.build();
        requestWrapper.sendResponse(response);
    }

    /**
     * 客户端获取NameNode基本信息
     */
    private void handleClientReadNameNodeInfoReqeust(RequestWrapper requestWrapper) {
        ClientNameNodeInfo.Builder builder = ClientNameNodeInfo.newBuilder();
        Map<String, String> config = nameNodeConfig.getConfig();
        builder.putAllConfig(config);
        Map<Integer, Integer> slotNodeMap = shardingManager.getSlotNodeMap();
        builder.putAllSlots(slotNodeMap);
        if (backupNodeInfoHolder != null) {
            BackupNodeInfo backupNodeInfo = backupNodeInfoHolder.getBackupNodeInfo();
            builder.setBackup(backupNodeInfo.getHostname() + ":" + backupNodeInfo.getPort());
        }
        ClientNameNodeInfo response = builder.build();
        requestWrapper.sendResponse(response);
    }

    /**
     * 处理客户端读取文件列表请求
     */
    private void handleClientListFilesRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException {
        ListFileRequest listFileRequest = ListFileRequest.parseFrom(requestWrapper.getRequest().getBody());
        String basePath = File.separator + requestWrapper.getRequest().getUserName() + listFileRequest.getPath();
        Node ret = listNode(basePath);
        if (ret == null) {
            ret = new Node(listFileRequest.getPath(), NodeType.DIRECTORY.getValue());
        }
        INode response = Node.toINode(ret);
        requestWrapper.sendResponse(response);
    }

    public Node listNode(String basePath) throws InvalidProtocolBufferException {
        Node node = diskNameSystem.listFiles(basePath, 1);
        return maybeFetchFromOtherNode(basePath, node);
    }

    /**
     * 从别的节点中获取用户文件列表
     */
    private Node maybeFetchFromOtherNode(String basePath, Node node) throws InvalidProtocolBufferException {
        Node ret = node;
        if (NameNodeLaunchMode.CLUSTER.equals(nameNodeConfig.getMode())) {
            ListFileRequest listFileRequest = ListFileRequest.newBuilder()
                    .setPath(basePath)
                    .build();
            NettyPacket request = NettyPacket.buildPacket(listFileRequest.toByteArray(), PacketType.LIST_FILES);
            List<NettyPacket> responses = peerNameNodes.broadcastSync(request);
            for (NettyPacket response : responses) {
                INode iNode = INode.parseFrom(response.getBody());
                iNode.getPath();
                if (iNode.getPath().length() != 0) {
                    Node merge = merge(ret, iNode);
                    if (ret == null) {
                        ret = merge;
                    }
                }
            }
        }
        return ret;
    }

    /**
     * 合并两个节点
     */
    private Node merge(Node node, INode iNode) {
        if (node == null) {
            return Node.parseINode(iNode);
        }
        Node otherNode = Node.parseINode(iNode);
        node.getChildren().putAll(otherNode.getChildren());
        return node;
    }

    /**
     * 处理修改文件副本数量请求
     */
    private void handleAddReplicaNumRequest(RequestWrapper requestWrapper) throws Exception {
        String userName = requestWrapper.getRequest().getUserName();
        AddReplicaNumRequest request = AddReplicaNumRequest.parseFrom(requestWrapper.getRequest().getBody());
        String fullPath = File.separator + userName + request.getFilename();
        List<DataNodeInfo> dataNodeByFileName = dataNodeManager.getDataNodeByFileName(fullPath);
        int addReplicaNum = request.getReplicaNum() - dataNodeByFileName.size();
        if (addReplicaNum < 0) {
            throw new NameNodeException("修改失败，不能动态减少副本数量");
        } else if (addReplicaNum == 0) {
            requestWrapper.sendResponse();
        } else {
            dataNodeManager.addReplicaNum(userName, addReplicaNum, fullPath);
            requestWrapper.sendResponse();
        }
    }

    /**
     * 根据文件名获取DataNode机器名
     */
    private void handleFetchDataNodeByFilenameRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException {
        FetchDataNodeByFilenameRequest request = FetchDataNodeByFilenameRequest.parseFrom(requestWrapper.getRequest().getBody());
        String realFilename = File.separator + requestWrapper.getRequest().getUserName() + request.getFilename();
        List<DataNodeInfo> dataNodeByFileName = dataNodeManager.getDataNodeByFileName(realFilename);
        FetchDataNodeByFilenameResponse.Builder builder = FetchDataNodeByFilenameResponse.newBuilder();
        for (DataNodeInfo dataNodeInfo : dataNodeByFileName) {
            DataNode datanode = DataNode.newBuilder()
                    .setHostname(dataNodeInfo.getHostname())
                    .setHttpPort(dataNodeInfo.getHttpPort())
                    .setNioPort(dataNodeInfo.getNioPort())
                    .build();
            builder.addDatanodes(datanode);
        }
        requestWrapper.sendResponse(builder.build());
    }

    /**
     * 处理别的NameNode节点发送过来的删除文件或目录的请求
     */
    private void handleNameNodeRemoveFileRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException,
            NameNodeException {
        RemoveFileOrDirRequest request = RemoveFileOrDirRequest.parseFrom(requestWrapper.getRequest().getBody());
        int count = removeFileOrDirInternal(request.getFilesList(), requestWrapper.getRequest().getUserName());
        RemoveFileOrDirResponse response = RemoveFileOrDirResponse.newBuilder()
                .setFileCount(count)
                .build();
        requestWrapper.sendResponse(response);
    }


    public int removeFileOrDirInternal(List<String> paths, String username) throws NameNodeException {
        List<String> filenames = new ArrayList<>(PrettyCodes.trimMapSize());
        for (String path : paths) {
            String baseFile = File.separator + username + path;
            List<String> allFiles = diskNameSystem.findAllFiles(baseFile);
            for (String file : allFiles) {
                filenames.add(path + file);
            }
        }
        log.info("删除文件列表为: [path={}]", filenames);
        int count = filenames.size();
        for (String filename : filenames) {
            removeFileInternal(filename, username);
        }
        return count;
    }

    /**
     * 处理获取NameNode基本信息的请求
     */
    private void handleFetchNameNodeInfoRequest(RequestWrapper requestWrapper) {
        NameNodeInfo.Builder builder = NameNodeInfo.newBuilder();
        builder.setHostname(NetUtils.getHostName());
        builder.setNioPort(nameNodeConfig.getPort());
        builder.setHttpPort(nameNodeConfig.getHttpPort());
        builder.setNodeId(nameNodeConfig.getNameNodeId());
        String backupNodeInfo = "";
        if (backupNodeInfoHolder != null) {
            BackupNodeInfo backupNode = backupNodeInfoHolder.getBackupNodeInfo();
            backupNodeInfo = backupNode.getHostname() + ":" + backupNode.getPort();
        }
        builder.setBackupNodeInfo(backupNodeInfo);
        NameNodeInfo response = builder.build();
        requestWrapper.sendResponse(response);
    }

    /**
     * 处理DataNode删除文件的上报信息，需要释放空间
     */
    private void handleReplicaRemoveRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException {
        InformReplicaReceivedRequest request = InformReplicaReceivedRequest.parseFrom(requestWrapper.getRequest().getBody());
        boolean broadcast = broadcast(requestWrapper.getRequest());
        log.info("收到DataNod上报的存储移除的信息：[hostname={}, filename={}]", request.getHostname(), request.getFilename());
        DataNodeInfo dataNode = dataNodeManager.getDataNode(request.getHostname());
        // 所有节点都需要维护DataNode节点存储信息。
        dataNode.addStoredDataSize(-request.getFileSize());
        if (!broadcast) {
            requestWrapper.sendResponse();
        }
    }

    /**
     * 处理同步过来的DataNode信息请求
     */
    private void handleNewPeerDataNodeInfoRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException {
        NettyPacket request = requestWrapper.getRequest();
        NewPeerNodeInfo newPeerDataNodeInfo = NewPeerNodeInfo.parseFrom(request.getBody());
        List<RegisterRequest> requestsList = newPeerDataNodeInfo.getRequestsList();
        for (RegisterRequest registerRequest : requestsList) {
            dataNodeManager.register(registerRequest);
            DataNodeInfo dataNode = dataNodeManager.getDataNode(registerRequest.getHostname());
            dataNode.setStatus(DataNodeInfo.STATUS_READY);
        }
        List<UserEntity> usersList = newPeerDataNodeInfo.getUsersList();
        for (UserEntity userEntity : usersList) {
            User user = new User(userEntity.getUsername(), userEntity.getSecret(),
                    userEntity.getCreateTime(), new User.StorageInfo());
            userManager.addOrUpdateUser(user);
        }
    }


    private void handleTrashResumeRequest(RequestWrapper requestWrapper) throws NameNodeException, InvalidProtocolBufferException {
        NettyPacket request = requestWrapper.getRequest();
        TrashResumeRequest trashResumeRequest = TrashResumeRequest.parseFrom(request.getBody());
        int count = trashResumeInternal(trashResumeRequest.getFilesList(), requestWrapper.getRequest().getUserName());
        TrashResumeResponse response = TrashResumeResponse.newBuilder()
                .setFileCount(count)
                .build();
        requestWrapper.sendResponse(response);
    }

    public int trashResumeInternal(List<String> paths, String username) throws NameNodeException {
        List<String> filenames = new ArrayList<>(PrettyCodes.trimMapSize());
        for (String path : paths) {
            String baseFile = File.separator + username + File.separator + Constants.TRASH_DIR + path;
            List<String> allFiles = diskNameSystem.findAllFiles(baseFile);
            for (String file : allFiles) {
                filenames.add(path + file);
            }
        }
        log.info("恢复文件列表为: [path={}]", filenames);
        int count = 0;
        for (String filename : filenames) {
            String trashFilename = File.separator + username + File.separator +
                    Constants.TRASH_DIR + filename;
            Node node = diskNameSystem.listFiles(trashFilename, 1);
            if (node == null) {
                throw new NameNodeException("文件不存在");
            }
            if (NodeType.FILE.getValue() != node.getType()) {
                throw new NameNodeException("无法恢复文件夹");
            }
            diskNameSystem.deleteFile(trashFilename);
            String destFilename = File.separator + username + filename;
            Map<String, String> currentAttr = node.getAttr();
            currentAttr.remove(Constants.ATTR_FILE_DEL_TIME);
            boolean ret = diskNameSystem.createFile(destFilename, currentAttr);
            if (ret) {
                count++;
                log.debug("恢复文件：[src={}, target={}]", destFilename, destFilename);
            } else {
                log.warn("恢复文件失败，文件已存在：" + trashFilename);
            }
        }
        return count;
    }


    /**
     * 处理获取用户文件列表的请求
     */
    private void handleListFilesRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException {
        ListFileRequest listFileRequest = ListFileRequest.parseFrom(requestWrapper.getRequest().getBody());
        Node node = diskNameSystem.listFiles(listFileRequest.getPath(), 1);
        INode response;
        if (node != null) {
            response = Node.toINode(node);
        } else {
            response = INode.newBuilder().build();
        }
        requestWrapper.sendResponse(response);
    }

    /**
     * 处理抓取用户信息的请求
     */
    private void handleFetchUserInfoRequest(RequestWrapper requestWrapper) {
        List<UserEntity> collect = userManager.getAllUser().stream()
                .map(User::toEntity)
                .collect(Collectors.toList());
        UserList response = UserList.newBuilder()
                .addAllUserEntities(collect)
                .build();
        requestWrapper.sendResponse(response);
    }

    /**
     * 处理节点发起连接后立即发送的NameNode集群信息请求
     */
    private void handleNameNodePeerAwareRequest(RequestWrapper requestWrapper) throws Exception {
        NettyPacket request = requestWrapper.getRequest();
        ChannelHandlerContext ctx = requestWrapper.getCtx();
        NameNodeAwareRequest nameNodeAwareRequest = NameNodeAwareRequest.parseFrom(request.getBody());
        if (nameNodeAwareRequest.getIsClient()) {
            /*
             * 只有作为服务端的时候，才会保存新增的链接。
             *
             * 假设有一个连接 namenode03 -> namenode01
             *
             * 1、当namenode03建立好连接之后，会主动发送一个NAME_NODE_PEER_AWARE请求。
             * 2、当namenode01收到之后，会保存连接，并发送一个NAME_NODE_PEER_AWARE请求请求给namenode03
             * 3、当namenode03收到namenode01发送的NAME_NODE_PEER_AWARE请求时，就不再需要保存连接了。
             *
             */
            PeerNameNode peerNameNode = peerNameNodes.addPeerNode(nameNodeAwareRequest.getNameNodeId(), (SocketChannel) ctx.channel(),
                    nameNodeAwareRequest.getServer(), nameNodeConfig.getNameNodeId(), defaultScheduler);
            if (peerNameNode != null) {
                // 作为服务端收到连接请求也同时发送自身信息给别的节点
                controllerManager.reportSelfInfoToPeer(peerNameNode, false);
            }
        }
        controllerManager.onAwarePeerNameNode(nameNodeAwareRequest);
    }

    /**
     * 客户端上传文件后，向NameNode确认上传完成
     */
    private void handleCreateFileConfirmRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException,
            RequestTimeoutException, InterruptedException, NameNodeException {
        if (isNoAuth(requestWrapper)) {
            return;
        }
        NettyPacket request = requestWrapper.getRequest();
        CreateFileRequest createFileRequest = CreateFileRequest.parseFrom(request.getBody());
        String realFilename = File.separator + request.getUserName() + createFileRequest.getFilename();
        int nodeId = getNodeId(realFilename);
        if (this.nodeId == nodeId) {
            if (request.getAck() != 0) {
                dataNodeManager.waitFileReceive(realFilename, 3000);
            }
            userManager.addStorageInfo(request.getUserName(), createFileRequest.getFileSize());
            CreateFileResponse response = CreateFileResponse.newBuilder()
                    .build();
            requestWrapper.sendResponse(response);
        } else {
            forwardRequestToOtherNameNode(nodeId, requestWrapper);
        }
    }

    /**
     * 处理抓取BackupNode信息请求
     */
    private void handleFetchBackupNodeInfoRequest(RequestWrapper requestWrapper) {
        // DataNode或者Client过来抓取BackupNode的信息
        boolean backupNodeExist = backupNodeInfoHolder != null;
        log.debug("收到抓取BackupNode的信息，返回结果：[hostname={}, port={}]",
                backupNodeExist ? backupNodeInfoHolder.getBackupNodeInfo().getHostname() : null,
                backupNodeExist ? backupNodeInfoHolder.getBackupNodeInfo().getPort() : null);
        NettyPacket response = NettyPacket.buildPacket(backupNodeExist ? backupNodeInfoHolder.getBackupNodeInfo().toByteArray() : new byte[0],
                PacketType.FETCH_BACKUP_NODE_INFO);
        requestWrapper.sendResponse(response, requestWrapper.getRequestSequence());
    }

    /**
     * 处理BackupNode上报信息请求
     */
    private void handleReportBackupNodeInfoRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException {
        // BackupNode上报信息
        BackupNodeInfo backupNodeInfo = BackupNodeInfo.parseFrom(requestWrapper.getRequest().getBody());
        if (backupNodeInfoHolder != null && backupNodeInfoHolder.isActive()) {
            log.info("收到BackupNode上报的信息，但是发现了2个BackupNode，拒绝请求：[hostname={}, port={}]",
                    backupNodeInfo.getHostname(), backupNodeInfo.getPort());
            NettyPacket resp = NettyPacket.buildPacket(new byte[0], PacketType.DUPLICATE_BACKUP_NODE);
            requestWrapper.sendResponse(resp, null);
            return;
        }
        this.backupNodeInfoHolder = new BackupNodeInfoHolder(backupNodeInfo, requestWrapper.getCtx().channel());
        log.info("收到BackupNode上报的信息：[hostname={}, port={}]", backupNodeInfo.getHostname(), backupNodeInfo.getPort());
        NameNodeConf nameNodeConf = NameNodeConf.newBuilder()
                .putAllValues(nameNodeConfig.getConfig())
                .build();
        requestWrapper.sendResponse(nameNodeConf);
    }

    /**
     * 处理认证请求
     */
    private void handleAuthenticateRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException, NameNodeException {
        AuthenticateInfoRequest request = AuthenticateInfoRequest.parseFrom(requestWrapper.getRequest().getBody());
        boolean isBroadcastRequest = requestWrapper.getRequest().getBroadcast();
        if (!isBroadcastRequest) {
            boolean authenticate = userManager.login(requestWrapper.getCtx().channel(), request.getAuthenticateInfo());
            if (!authenticate) {
                throw new NameNodeException("认证失败：" + request.getAuthenticateInfo());
            }
            log.info("收到认证请求：[authenticateInfo={}]", request.getAuthenticateInfo());
            // 获取用户认证成功后的Token信息，将这个Token信息发送给其他节点
            String token = userManager.getTokenByChannel(requestWrapper.getCtx().channel());
            requestWrapper.getRequest().setUserToken(token);
            // 为了保证一致性，只有所有节点都认证通过了，才可以返回响应给客户端
            broadcastSync(requestWrapper);
            // 第一个收到认证的节点，写回响应
            AuthenticateInfoResponse response = AuthenticateInfoResponse.newBuilder()
                    .setToken(token)
                    .build();
            requestWrapper.sendResponse(response);
        } else {
            // 代码走到这里表示客户端认证请求是别的节点广播过来的，只需要保存Token就行了
            String userToken = requestWrapper.getRequest().getUserToken();
            String username = request.getAuthenticateInfo().split(",")[0];
            userManager.setToken(username, userToken);
            requestWrapper.sendResponse();
            log.info("收到别的节点广播过来的认证信息：[username={}, token={}]", username, userToken);
        }
    }

    /**
     * 判断请求是否是认证通过
     *
     * @param requestWrapper 请求
     * @return 是否认证同坐
     */
    private boolean isNoAuth(RequestWrapper requestWrapper) throws NameNodeException {
        NettyPacket request = requestWrapper.getRequest();
        String userToken = request.getUserToken();
        if (userToken == null) {
            log.warn("收到一个未认证的请求, 已被拦截: [channel={}, packetType={}]",
                    NetUtils.getChannelId(requestWrapper.getCtx().channel()), request.getPacketType());
            throw new NameNodeException("未通过认证的请求");
        }
        String userName = request.getUserName();
        if (!userManager.isUserToken(userName, userToken)) {
            log.warn("收到一个未认证的请求, 已被拦截: [channel={}, packetType={}]",
                    NetUtils.getChannelId(requestWrapper.getCtx().channel()), request.getPacketType());
            throw new NameNodeException("未通过认证的请求");
        } else {
            if (log.isDebugEnabled()) {
                log.debug("收到一个认证通过的请求：[username={}, token={}, package={}]", userName, userToken,
                        PacketType.getEnum(request.getPacketType()).getDescription());
            }
            return false;
        }
    }

    /**
     * 处理读取文件属性请求
     */
    private void handleReadAttrRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException,
            NameNodeException, RequestTimeoutException, InterruptedException {
        if (isNoAuth(requestWrapper)) {
            return;
        }
        ReadAttrRequest readAttrRequest = ReadAttrRequest.parseFrom(requestWrapper.getRequest().getBody());
        String userName = requestWrapper.getRequest().getUserName();
        String realFilename = File.separator + userName + readAttrRequest.getFilename();
        int nodeId = getNodeId(realFilename);
        if (this.nodeId == nodeId) {
            Map<String, String> attr = diskNameSystem.getAttr(realFilename);
            if (attr == null) {
                throw new NameNodeException("文件不存在：" + readAttrRequest.getFilename());
            }
            ReadAttrResponse response = ReadAttrResponse.newBuilder()
                    .putAllAttr(attr)
                    .build();
            requestWrapper.sendResponse(response);
        } else {
            forwardRequestToOtherNameNode(nodeId, requestWrapper);
        }
    }

    /**
     * 处理DataNode注册消息
     */
    private void handleDataNodeRegisterRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException, NameNodeException {
        RegisterRequest registerRequest = RegisterRequest.parseFrom(requestWrapper.getRequest().getBody());
        boolean broadcast = broadcast(requestWrapper.getRequest());
        boolean result = dataNodeManager.register(registerRequest);
        if (!result) {
            throw new NameNodeException("注册失败，DataNode节点已存在");
        }
        if (!broadcast) {
            requestWrapper.sendResponse();
        }
    }

    /**
     * 处理DataNode心跳请求
     *
     * @param requestWrapper 请求
     */
    private void handleDataNodeHeartbeatRequest(RequestWrapper requestWrapper) throws
            InvalidProtocolBufferException, NameNodeException {
        HeartbeatRequest heartbeatRequest = HeartbeatRequest.parseFrom(requestWrapper.getRequest().getBody());

        Boolean heartbeat = dataNodeManager.heartbeat(heartbeatRequest.getHostname());
        if (!heartbeat) {
            throw new NameNodeException("心跳失败，DataNode不存在：" + heartbeatRequest.getHostname());
        }
        DataNodeInfo dataNode = dataNodeManager.getDataNode(heartbeatRequest.getHostname());
        List<ReplicaTask> replicaTask = dataNode.pollReplicaTask(100);
        List<ReplicaCommand> replicaCommands = new LinkedList<>();
        if (!replicaTask.isEmpty()) {
            List<ReplicaCommand> commands = replicaTask.stream()
                    .map(e -> ReplicaCommand.newBuilder()
                            .setFilename(e.getFilename())
                            .setHostname(e.getHostname())
                            .setPort(e.getPort())
                            .setCommand(CommandType.REPLICA_COPY.getValue())
                            .build())
                    .collect(Collectors.toList());
            replicaCommands.addAll(commands);
        }
        List<RemoveReplicaTask> removeReplicaTasks = dataNode.pollRemoveReplicaTask(100);
        if (!removeReplicaTasks.isEmpty()) {
            List<ReplicaCommand> commands = removeReplicaTasks.stream()
                    .map(e -> ReplicaCommand.newBuilder()
                            .setFilename(e.getFileName())
                            .setHostname(e.getHostname())
                            .setCommand(CommandType.REPLICA_REMOVE.getValue())
                            .build())
                    .collect(Collectors.toList());
            replicaCommands.addAll(commands);
        }
        // 同步请求其他所有NameNode节点获取他们的响应
        List<NettyPacket> nettyPackets = broadcastSync(requestWrapper);
        // 将其他NameNode节点的命令添加到给客户端的响应中
        for (NettyPacket nettyPacket : nettyPackets) {
            HeartbeatResponse heartbeatResponse = HeartbeatResponse.parseFrom(nettyPacket.getBody());
            replicaCommands.addAll(heartbeatResponse.getCommandsList());
        }

        HeartbeatResponse response = HeartbeatResponse.newBuilder()
                .addAllCommands(replicaCommands)
                .build();
        requestWrapper.sendResponse(response);
    }

    /**
     * 处理创建文件夹请求
     *
     * @param requestWrapper 创建文件夹请求
     */
    private void handleMkdirRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException,
            RequestTimeoutException, InterruptedException, NameNodeException {
        if (isNoAuth(requestWrapper)) {
            return;
        }
        NettyPacket request = requestWrapper.getRequest();
        MkdirRequest mkdirRequest = MkdirRequest.parseFrom(request.getBody());
        String realFilename = File.separator + request.getUserName() + mkdirRequest.getPath();
        int nodeId = getNodeId(realFilename);
        if (this.nodeId == nodeId) {
            this.diskNameSystem.mkdir(realFilename, mkdirRequest.getAttrMap());
            requestWrapper.sendResponse();
        } else {
            forwardRequestToOtherNameNode(nodeId, requestWrapper);
        }
    }

    /**
     * 处理BackupNode拉取EditLog
     */
    private void handleFetchEditLogRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException {
        FetchEditsLogRequest fetchEditsLogRequest = FetchEditsLogRequest.parseFrom(requestWrapper.getRequest().getBody());
        long txId = fetchEditsLogRequest.getTxId();
        List<EditLogWrapper> result = new ArrayList<>();
        try {
            result = fetchEditLogBuffer.fetch(txId);
        } catch (IOException e) {
            log.error("读取EditLog失败：", e);
        }
        FetchEditsLogResponse response = FetchEditsLogResponse.newBuilder()
                .addAllEditLogs(result.stream()
                        .map(EditLogWrapper::getEditLog)
                        .collect(Collectors.toList()))
                .addAllUsers(userManager.getAllUser().stream()
                        .map(User::toEntity)
                        .collect(Collectors.toList()))
                .build();
        requestWrapper.sendResponse(response);
        if (NameNodeLaunchMode.SINGLE.equals(mode)) {
            return;
        }
        if (fetchEditsLogRequest.getNeedSlots() || slotsChanged.get()) {
            if (slotsChanged.compareAndSet(true, false)) {
                log.info("检测到Slots槽位分配发生了变化，下发给BackupNode最新的槽位信息.");
                if (slots != null) {
                    BackupNodeSlots backupNodeSlots = BackupNodeSlots.newBuilder()
                            .putAllSlots(slots)
                            .build();
                    NettyPacket nettyPacket = NettyPacket.buildPacket(backupNodeSlots.toByteArray(),
                            PacketType.BACKUP_NODE_SLOT);
                    requestWrapper.sendResponse(nettyPacket, null);
                }
            }
        }
    }

    /**
     * 处理文件传输，这里的场景主要是BackupNode上传FsImage文件
     */
    private void handleFileTransferRequest(RequestWrapper requestWrapper) {
        FilePacket filePacket = FilePacket.parseFrom(requestWrapper.getRequest().getBody());
        fileReceiveHandler.handleRequest(filePacket);
    }

    /**
     * 处理DataNode上报存储信息
     */
    private void handleDataNodeReportStorageInfoRequest(RequestWrapper requestWrapper) throws
            InvalidProtocolBufferException {
        ReportCompleteStorageInfoRequest request =
                ReportCompleteStorageInfoRequest.parseFrom(requestWrapper.getRequest().getBody());
        broadcast(requestWrapper.getRequest());
        log.info("全量上报存储信息：[hostname={}, files={}]", request.getHostname(), request.getFileInfosCount());
        for (FileMetaInfo file : request.getFileInfosList()) {
            if (getNodeId(file.getFilename()) == this.nodeId) {
                // 只有属于自己Slot的文件信息才进行保存
                // TODO 考虑和内存目录树进行对比，看看是否存在内存目录树不存在的文件，下发命令让DataNode清除
                FileInfo fileInfo = new FileInfo();
                fileInfo.setFileName(file.getFilename());
                fileInfo.setFileSize(file.getFileSize());
                fileInfo.setHostname(request.getHostname());
                dataNodeManager.addReplica(fileInfo);
            }
        }
        if (request.getFinished()) {
            dataNodeManager.setDataNodeReady(request.getHostname());
            log.info("全量上报存储信息完成：[hostname={}]", request.getHostname());
        }
    }

    /**
     * 处理客户端创建文件请求
     */
    private void handleCreateFileRequest(RequestWrapper requestWrapper) throws Exception {
        if (isNoAuth(requestWrapper)) {
            return;
        }
        NettyPacket request = requestWrapper.getRequest();
        CreateFileRequest createFileRequest = CreateFileRequest.parseFrom(request.getBody());
        String realFilename = File.separator + request.getUserName() + createFileRequest.getFilename();
        int nodeId = getNodeId(realFilename);
        if (this.nodeId == nodeId) {
            Map<String, String> attrMap = new HashMap<>(createFileRequest.getAttrMap());
            String replicaNumStr = attrMap.get(Constants.ATTR_REPLICA_NUM);
            attrMap.put(Constants.ATTR_FILE_SIZE, String.valueOf(createFileRequest.getFileSize()));
            int replicaNum;
            if (replicaNumStr != null) {
                replicaNum = Integer.parseInt(replicaNumStr);
                // 最少不能少于配置的数量
                replicaNum = Math.max(replicaNum, diskNameSystem.getNameNodeConfig().getReplicaNum());
                // 最大不能大于最大的数量
                replicaNum = Math.min(replicaNum, Constants.MAX_REPLICA_NUM);
            } else {
                replicaNum = diskNameSystem.getNameNodeConfig().getReplicaNum();
                attrMap.put(Constants.ATTR_REPLICA_NUM, String.valueOf(replicaNum));
            }
            Node node = diskNameSystem.listFiles(realFilename);
            if (node != null) {
                throw new NameNodeException("文件已存在：" + createFileRequest.getFilename());
            }
            List<DataNodeInfo> dataNodeList = dataNodeManager.allocateDataNodes(request.getUserName(), replicaNum, realFilename);
            Prometheus.incCounter("namenode_put_file_count", "NameNode收到的上传文件请求数量");
            Prometheus.hit("namenode_put_file_qps", "NameNode瞬时上传文件QPS");
            List<DataNode> dataNodes = dataNodeList.stream()
                    .map(e -> DataNode.newBuilder().setHostname(e.getHostname())
                            .setNioPort(e.getNioPort())
                            .setHttpPort(e.getHttpPort())
                            .build())
                    .collect(Collectors.toList());
            diskNameSystem.createFile(realFilename, attrMap);
            List<String> collect = dataNodeList.stream().map(DataNodeInfo::getHostname).collect(Collectors.toList());
            log.info("创建文件：[filename={}, datanodes={}]", realFilename, String.join(",", collect));
            CreateFileResponse response = CreateFileResponse.newBuilder()
                    .addAllDataNodes(dataNodes)
                    .setRealFileName(realFilename)
                    .build();
            requestWrapper.sendResponse(response);
        } else {
            forwardRequestToOtherNameNode(nodeId, requestWrapper);
        }
    }

    /**
     * 获取文件所在的DataNode节点
     */
    private void handleGetDataNodeForFileRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException,
            NameNodeException, RequestTimeoutException, InterruptedException {
        if (isNoAuth(requestWrapper)) {
            return;
        }
        GetDataNodeForFileRequest getDataNodeForFileRequest = GetDataNodeForFileRequest.parseFrom(requestWrapper.getRequest().getBody());
        String userName = requestWrapper.getRequest().getUserName();
        String realFilename = File.separator + userName + getDataNodeForFileRequest.getFilename();
        int nodeId = getNodeId(realFilename);
        if (this.nodeId == nodeId) {
            DataNodeInfo dataNodeInfo = dataNodeManager.chooseReadableDataNodeByFileName(realFilename);
            if (dataNodeInfo != null) {
                Prometheus.incCounter("namenode_get_file_count", "NameNode收到的下载文件请求数量");
                Prometheus.hit("namenode_get_file_qps", "NameNode收到的下载文件请求数量");
                DataNode dataNode = DataNode.newBuilder()
                        .setHostname(dataNodeInfo.getHostname())
                        .setNioPort(dataNodeInfo.getNioPort())
                        .setHttpPort(dataNodeInfo.getHttpPort())
                        .build();
                GetDataNodeForFileResponse response = GetDataNodeForFileResponse.newBuilder()
                        .setDataNode(dataNode)
                        .setRealFileName(realFilename)
                        .build();
                requestWrapper.sendResponse(response);
                return;
            }
            throw new NameNodeException("文件不存在：" + getDataNodeForFileRequest.getFilename());
        } else {
            forwardRequestToOtherNameNode(nodeId, requestWrapper);
        }
    }

    /**
     * 处理收到副本请求
     */
    private void handleReplicaReceiveRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException {
        InformReplicaReceivedRequest request = InformReplicaReceivedRequest.parseFrom(requestWrapper.getRequest().getBody());
        boolean broadcast = broadcast(requestWrapper.getRequest());
        log.info("收到增量上报的存储信息：[hostname={}, filename={}]", request.getHostname(), request.getFilename());
        FileInfo fileInfo = new FileInfo(request.getHostname(), request.getFilename(), request.getFileSize());
        int nodeId = getNodeId(request.getFilename());
        // 只有所属slot的NameNode节点会保存副本的相关信息，但是所有节点都需要维护DataNode节点存储信息。
        if (this.nodeId == nodeId) {
            dataNodeManager.addReplica(fileInfo);
        }
        DataNodeInfo dataNode = dataNodeManager.getDataNode(request.getHostname());
        dataNode.addStoredDataSize(request.getFileSize());
        if (!broadcast) {
            requestWrapper.sendResponse();
        }
    }


    /**
     * 处理删除文件请求
     */
    private void handleRemoveFileRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException,
            NameNodeException, RequestTimeoutException, InterruptedException {
        if (isNoAuth(requestWrapper)) {
            return;
        }
        RemoveFileRequest removeFileRequest = RemoveFileRequest.parseFrom(requestWrapper.getRequest().getBody());
        String userName = requestWrapper.getRequest().getUserName();
        String realFilename = File.separator + userName + removeFileRequest.getFilename();
        int nodeId = getNodeId(realFilename);
        if (this.nodeId == nodeId) {
            removeFileInternal(removeFileRequest.getFilename(), userName);
            requestWrapper.sendResponse();
        } else {
            forwardRequestToOtherNameNode(nodeId, requestWrapper);
        }
    }

    /**
     * 执行删除文件逻辑
     *
     * @param filename 删除文件
     * @param userName 用户名
     */
    public void removeFileInternal(String filename, String userName) throws NameNodeException {
        String realFilename = File.separator + userName + filename;
        Node node = diskNameSystem.listFiles(realFilename);
        if (node == null) {
            throw new NameNodeException("文件不存在：" + filename);
        }
        Map<String, String> attr = new HashMap<>(PrettyCodes.trimMapSize());
        attr.put(Constants.ATTR_FILE_DEL_TIME, String.valueOf(System.currentTimeMillis()));
        if (node.getChildren().isEmpty()) {
            diskNameSystem.deleteFile(realFilename);
            String destFilename = File.separator + userName + File.separator + Constants.TRASH_DIR + filename;
            Map<String, String> currentAttr = node.getAttr();
            currentAttr.putAll(attr);
            diskNameSystem.createFile(destFilename, currentAttr);
            log.debug("删除文件，并移动到垃圾箱：[src={}, target={}]", realFilename, destFilename);
        } else {
            throw new NameNodeException("文件夹不为空，无法删除：" + filename);
        }
    }

    /**
     * 广播请求给所有的NameNode节点
     *
     * @param request 请求
     * @return 该请求是否是别的NameNode广播过来的
     */
    private boolean broadcast(NettyPacket request) {
        boolean isBroadcastRequest = request.getBroadcast();
        if (!isBroadcastRequest) {
            // 请求是客户端发过来的，并不是别的NameNode广播过来的
            request.setBroadcast(true);
            List<Integer> broadcast = peerNameNodes.broadcast(request);
            if (!broadcast.isEmpty()) {
                log.debug("广播请求给所有的NameNode: [sequence={}, broadcast={}, packetType={}]",
                        request.getSequence(), broadcast, PacketType.getEnum(request.getPacketType()).getDescription());
            }
        }
        return isBroadcastRequest;
    }

    /**
     * 广播请求给所有的NameNode节点, 同时获取所有节点的响应
     *
     * @param requestWrapper 请求
     * @return 请求结果
     */
    private List<NettyPacket> broadcastSync(RequestWrapper requestWrapper) {
        NettyPacket request = requestWrapper.getRequest();
        boolean isBroadcastRequest = request.getBroadcast();
        if (!isBroadcastRequest) {
            // 请求是客户端发过来的，并不是别的NameNode广播过来的
            request.setBroadcast(true);
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();
            List<NettyPacket> nettyPackets = new ArrayList<>(peerNameNodes.broadcastSync(request));
            stopWatch.stop();
            if (!nettyPackets.isEmpty()) {
                log.debug("同步发送请求给所有的NameNode，并获取到了响应: [sequence={}, broadcast={}, packetType={}, cost={} s]",
                        request.getSequence(), peerNameNodes.getAllNodeId(),
                        PacketType.getEnum(request.getPacketType()).getDescription(),
                        stopWatch.getTime() / 1000.0D);
            }
            return nettyPackets;
        }
        return new ArrayList<>();
    }

    /**
     * 返回异常响应信息
     */
    private void sendErrorResponse(RequestWrapper requestWrapper, String msg) {
        NettyPacket nettyResponse = NettyPacket.buildPacket(new byte[0],
                PacketType.getEnum(requestWrapper.getRequest().getPacketType()));
        nettyResponse.setError(msg);
        requestWrapper.sendResponse(nettyResponse, requestWrapper.getRequestSequence());
    }

    /**
     * 根据文件名获取处理该请求的节点ID
     *
     * @param filename 文件名
     * @return 节点ID
     */
    public int getNodeId(String filename) {
        if (NameNodeLaunchMode.CLUSTER.equals(mode)) {
            return shardingManager.getNameNodeIdByFileName(filename);
        }
        return this.nodeId;
    }

    /**
     * 转发请求到其他NameNode
     */
    private void forwardRequestToOtherNameNode(int nodeId, RequestWrapper requestWrapper) throws
            InterruptedException, RequestTimeoutException {
        NettyPacket request = requestWrapper.getRequest();
        log.debug("转发请求到别的NameNode: [targetNodeId={}, sequence={}, packetType={}]", nodeId,
                request.getSequence(), PacketType.getEnum(request.getPacketType()).getDescription());
        String sequence = request.getSequence();
        NettyPacket response = peerNameNodes.sendSync(nodeId, request);
        requestWrapper.sendResponse(response, sequence);
    }
}
