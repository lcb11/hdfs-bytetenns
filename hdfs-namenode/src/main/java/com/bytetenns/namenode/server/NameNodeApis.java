package com.bytetenns.namenode.server;

import com.bytetenns.common.FileInfo;
import com.bytetenns.common.enums.CommandType;
import com.bytetenns.common.enums.NameNodeLaunchMode;
import com.bytetenns.common.enums.NodeType;
import com.bytetenns.common.enums.PacketType;
import com.bytetenns.common.exception.NameNodeException;
import com.bytetenns.common.exception.RequestTimeoutException;
import com.bytetenns.common.metrics.Prometheus;
import com.bytetenns.common.netty.Constants;
import com.bytetenns.common.netty.NettyPacket;
import com.bytetenns.common.network.AbstractChannelHandler;
import com.bytetenns.common.network.RequestWrapper;
import com.bytetenns.common.network.file.FilePacket;
import com.bytetenns.common.network.file.FileReceiveHandler;
import com.bytetenns.common.scheduler.DefaultScheduler;
import com.bytetenns.common.utils.NetUtils;
import com.bytetenns.common.utils.PrettyCodes;
import com.bytetenns.dfs.model.backup.*;
import com.bytetenns.dfs.model.client.*;
import com.bytetenns.dfs.model.common.DataNode;
import com.bytetenns.dfs.model.datanode.*;
import com.bytetenns.dfs.model.namenode.*;
import com.bytetenns.namenode.NameNodeConfig;
import com.bytetenns.namenode.datanode.DataNodeInfo;
import com.bytetenns.namenode.datanode.DataNodeManager;
import com.bytetenns.namenode.editlog.EditLogWrapper;
import com.bytetenns.namenode.fs.CalculateResult;
import com.bytetenns.namenode.fs.DiskNameSystem;
import com.bytetenns.namenode.fs.Node;
import com.bytetenns.namenode.rebalance.RemoveReplicaTask;
import com.bytetenns.namenode.rebalance.ReplicaTask;
import com.bytetenns.namenode.shard.ShardingManager;
import com.google.protobuf.InvalidProtocolBufferException;
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
 * NameNode???????????????????????????
 *
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

    public NameNodeApis(NameNodeConfig nameNodeConfig, DataNodeManager dataNodeManager,
                        ShardingManager shardingManager, DiskNameSystem diskNameSystem,
                        DefaultScheduler defaultScheduler) {
        this.dataNodeManager = dataNodeManager;
        this.nameNodeConfig = nameNodeConfig;
        this.shardingManager = shardingManager;
        this.diskNameSystem = diskNameSystem;
        this.nodeId = nameNodeConfig.getNameNodeId();
        this.mode = nameNodeConfig.getMode();
        this.defaultScheduler = defaultScheduler;
        this.fetchEditLogBuffer = new FetchEditLogBuffer(diskNameSystem);
//        this.shardingManager.addOnSlotAllocateCompletedListener(slots -> {
//            if (slotsChanged.compareAndSet(false, true)) {
//                this.slots = slots;
//            }
//        });
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
//        String token = userManager.getTokenByChannel(ctx.channel());
//        String username = userManager.logout(ctx.channel());
//        if (true) {
//            NettyPacket nettyPacket = NettyPacket.buildPacket(new byte[0], PacketType.CLIENT_LOGOUT);
//            nettyPacket.setUsername(username);
//            nettyPacket.setUserToken(token);
//            broadcast(nettyPacket);
//        }
        if (backupNodeInfoHolder != null && backupNodeInfoHolder.match(ctx.channel())) {
            backupNodeInfoHolder = null;
        }
        ctx.fireChannelInactive();
    }

    @Override
    protected boolean handlePackage(ChannelHandlerContext ctx, NettyPacket request) {
//        boolean consumedMsg = peerNameNodes.onMessage(request);
//        if (consumedMsg) {
//            return true;
//        }
        if (request.isError()) {
            // ??????????????????????????????????????????NameNode???????????????????????????????????????????????????????????????
            // ??????NameNode?????????????????????????????????????????????????????????????????????
            log.warn("?????????????????????????????????, ????????????????????????[request={}]", request.getHeader());
            return true;
        }
        PacketType packetType = PacketType.getEnum(request.getPacketType());
        Prometheus.incCounter("namenode_net_package_inbound_count", "NameNode?????????????????????");
        Prometheus.incCounter("namenode_net_package_inbound_bytes", "NameNode?????????????????????", request.getBody().length);
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        RequestWrapper requestWrapper = new RequestWrapper(ctx, request, nodeId, bodyLength -> {
            Prometheus.incCounter("namenode_net_package_outbound_count", "NameNode?????????????????????");
            Prometheus.incCounter("namenode_net_package_outbound_bytes", "NameNode?????????????????????", bodyLength);
            stopWatch.stop();
            long time = stopWatch.getTime();
            Prometheus.gauge("namenode_request_times", "NameNode??????????????????", "requestType", packetType.getDescription(), time);
        });
        try {
            switch (packetType) {
                case DATA_NODE_REGISTER://1.datanode????????????
                    handleDataNodeRegisterRequest(requestWrapper);
                    break;
                case HEART_BRET://2.??????datanode??????
                    handleDataNodeHeartbeatRequest(requestWrapper);
                    break;
                case MKDIR://3.????????????,???namenode????????????
                    handleMkdirRequest(requestWrapper);
                    break;
                case FETCH_EDIT_LOG://??????editlog?????????
                    handleFetchEditLogRequest(requestWrapper);
                    break;
                case TRANSFER_FILE://????????????????????????
                    handleFileTransferRequest(requestWrapper);
                    break;
                case REPORT_STORAGE_INFO://dataNode??????????????????
                    handleDataNodeReportStorageInfoRequest(requestWrapper);
                    break;
                case CREATE_FILE://????????????
                    handleCreateFileRequest(requestWrapper);
                    break;
                case GET_DATA_NODE_FOR_FILE://??????file?????????datanode??????
                    handleGetDataNodeForFileRequest(requestWrapper);
                    break;
                case REPLICA_RECEIVE://?????????????????????
                    handleReplicaReceiveRequest(requestWrapper);
                    break;
                case REMOVE_FILE://????????????
                    handleRemoveFileRequest(requestWrapper);
                    break;
                case READ_ATTR://?????????
                    handleReadAttrRequest(requestWrapper);
                    break;
                case AUTHENTICATE://???????????????
                    handleAuthenticateRequest(requestWrapper);
                    break;
                case REPORT_BACKUP_NODE_INFO://?????????backup???????????????
                    handleReportBackupNodeInfoRequest(requestWrapper);
                    break;
                case FETCH_BACKUP_NODE_INFO://??????backup??????
                    handleFetchBackupNodeInfoRequest(requestWrapper);
                    break;
                case CREATE_FILE_CONFIRM://??????????????????
                    handleCreateFileConfirmRequest(requestWrapper);
                    break;
//                case NAME_NODE_PEER_AWARE:
//                    handleNameNodePeerAwareRequest(requestWrapper);
//                    break;
//                case NAME_NODE_CONTROLLER_VOTE:
//                    controllerManager.onReceiveControllerVote(requestWrapper);
//                    break;
//                case NAME_NODE_SLOT_BROADCAST:
//                    controllerManager.onReceiveSlots(requestWrapper);
//                    break;
//                case RE_BALANCE_SLOTS:
//                    controllerManager.onRebalanceSlots(requestWrapper);
//                    break;
//                case FETCH_SLOT_METADATA:
//                    controllerManager.writeMetadataToPeer(requestWrapper);
//                    break;
//                case FETCH_SLOT_METADATA_RESPONSE:
//                    controllerManager.onFetchMetadata(requestWrapper);
//                    break;
//                case FETCH_SLOT_METADATA_COMPLETED:
//                    controllerManager.onLocalControllerFetchSlotMetadataCompleted(requestWrapper);
//                    break;
//                case FETCH_SLOT_METADATA_COMPLETED_BROADCAST:
//                    controllerManager.onRemoteControllerFetchSlotMetadataCompleted(requestWrapper);
//                    break;
//                case REMOVE_METADATA_COMPLETED:
//                    controllerManager.onRemoveMetadataCompleted(requestWrapper);
//                    break;
//                case CLIENT_LOGOUT:
//                    log.debug("????????????????????????????????????????????????: [username={}]", request.getUserName());
//                    userManager.logout(request.getUserName(), request.getUserToken());
//                    break;
//                case USER_CHANGE_EVENT:
//                    userManager.onUserChangeEvent(request);
//                    break;
//                case FETCH_USER_INFO:
//                    handleFetchUserInfoRequest(requestWrapper);
//                    break;
                case LIST_FILES:
                    handleListFilesRequest(requestWrapper);
                    break;
                case TRASH_RESUME://???????????????????????????????????????
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
            log.error("?????????????????????", e);
            sendErrorResponse(requestWrapper, e.getMessage());
        } catch (RequestTimeoutException e) {
            log.info("??????????????????", e);
            sendErrorResponse(requestWrapper, e.getMessage());
        } catch (Exception e) {
            log.error("NameNode???????????????????????????", e);
            sendErrorResponse(requestWrapper, "???????????????nodeId=" + nodeId);
        }
        return true;
    }

    /**
     * ?????????????????????/???????????????????????????????????????
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

        // ??????????????????????????????????????????chunked????????????
        GetAllFilenameResponse response = GetAllFilenameResponse.newBuilder()
                .addAllFilename(result)
                .build();
        requestWrapper.sendResponse(response);
    }

    /**
     * ?????????????????????/???????????????????????????????????????
     */
    private void handleClientPreCalculateRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException {
        PreCalculateRequest request = PreCalculateRequest.parseFrom(requestWrapper.getRequest().getBody());
        String realFilename = File.separator + requestWrapper.getRequest().getUserName() + request.getPath();
        // ????????????????????????
        CalculateResult result = diskNameSystem.calculate(realFilename);

        // ???????????????????????????????????????
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
     * ?????????????????????????????????
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
                throw new NameNodeException("?????????????????????" + request.getFilename());
            }
            int replica = Integer.parseInt(node.getAttr().get(Constants.ATTR_REPLICA_NUM));
            ReadStorageInfoResponse response = ReadStorageInfoResponse.newBuilder()
                    .setDatanodes(String.join(",", dataNodeHosts))
                    .setReplica(replica)
                    .build();
            requestWrapper.sendResponse(response);
        }
//        else {
//            forwardRequestToOtherNameNode(nodeId, requestWrapper);
//        }
    }

    /**
     * ???????????????DataNode????????????
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
     * ???????????????NameNode????????????
     */
    private void handleClientReadNameNodeInfoReqeust(RequestWrapper requestWrapper) {
        ClientNameNodeInfo.Builder builder = ClientNameNodeInfo.newBuilder();
        Map<String, String> config = nameNodeConfig.getConfig();
        builder.putAllConfig(config);
//        Map<Integer, Integer> slotNodeMap = shardingManager.getSlotNodeMap();
//        builder.putAllSlots(slotNodeMap);
        if (backupNodeInfoHolder != null) {
            BackupNodeInfo backupNodeInfo = backupNodeInfoHolder.getBackupNodeInfo();
            builder.setBackup(backupNodeInfo.getHostname() + ":" + backupNodeInfo.getPort());
        }
        ClientNameNodeInfo response = builder.build();
        requestWrapper.sendResponse(response);
    }

    /**
     * ???????????????????????????????????????
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
//        return maybeFetchFromOtherNode(basePath, node);
        return node;
    }


    /**
     * ??????????????????????????????????????????
     */
//    private Node maybeFetchFromOtherNode(String basePath, Node node) throws InvalidProtocolBufferException {
//        Node ret = node;
//        if (NameNodeLaunchMode.CLUSTER.equals(nameNodeConfig.getMode())) {
//            ListFileRequest listFileRequest = ListFileRequest.newBuilder()
//                    .setPath(basePath)
//                    .build();
//            NettyPacket request = NettyPacket.buildPacket(listFileRequest.toByteArray(), PacketType.LIST_FILES);
//            List<NettyPacket> responses = peerNameNodes.broadcastSync(request);
//            for (NettyPacket response : responses) {
//                INode iNode = INode.parseFrom(response.getBody());
//                iNode.getPath();
//                if (iNode.getPath().length() != 0) {
//                    Node merge = merge(ret, iNode);
//                    if (ret == null) {
//                        ret = merge;
//                    }
//                }
//            }
//        }
//        return ret;
//    }

    /**
     * ??????????????????
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
     * ????????????????????????????????????
     */
    private void handleAddReplicaNumRequest(RequestWrapper requestWrapper) throws Exception {
        String userName = requestWrapper.getRequest().getUserName();
        AddReplicaNumRequest request = AddReplicaNumRequest.parseFrom(requestWrapper.getRequest().getBody());
        String fullPath = File.separator + userName + request.getFilename();
        List<DataNodeInfo> dataNodeByFileName = dataNodeManager.getDataNodeByFileName(fullPath);
        int addReplicaNum = request.getReplicaNum() - dataNodeByFileName.size();
        if (addReplicaNum < 0) {
            throw new NameNodeException("?????????????????????????????????????????????");
        } else if (addReplicaNum == 0) {
            requestWrapper.sendResponse();
        } else {
            throw new NameNodeException("?????????????????????????????????");
//            dataNodeManager.addReplicaNum(userName, addReplicaNum, fullPath);
//            requestWrapper.sendResponse();
        }
    }

    /**
     * ?????????????????????DataNode?????????
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
     * ????????????NameNode???????????????????????????????????????????????????
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
        log.info("?????????????????????: [path={}]", filenames);
        int count = filenames.size();
        for (String filename : filenames) {
            removeFileInternal(filename, username);
        }
        return count;
    }

    /**
     * ????????????NameNode?????????????????????
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
     * ??????DataNode????????????????????????????????????????????????
     */
    private void handleReplicaRemoveRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException {
        InformReplicaReceivedRequest request = InformReplicaReceivedRequest.parseFrom(requestWrapper.getRequest().getBody());
//        boolean broadcast = broadcast(requestWrapper.getRequest());
        log.info("??????DataNod?????????????????????????????????[hostname={}, filename={}]", request.getHostname(), request.getFilename());
        DataNodeInfo dataNode = dataNodeManager.getDataNode(request.getHostname());
        // ???????????????????????????DataNode?????????????????????
        dataNode.addStoredDataSize(-request.getFileSize());
//        if (!broadcast) {
            requestWrapper.sendResponse();
//        }
    }

    /**
     * ?????????????????????DataNode????????????
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
//        for (UserEntity userEntity : usersList) {
//            User user = new User(userEntity.getUsername(), userEntity.getSecret(),
//                    userEntity.getCreateTime(), new User.StorageInfo());
//            userManager.addOrUpdateUser(user);
//        }
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
        log.info("?????????????????????: [path={}]", filenames);
        int count = 0;
        for (String filename : filenames) {
            String trashFilename = File.separator + username + File.separator +
                    Constants.TRASH_DIR + filename;
            Node node = diskNameSystem.listFiles(trashFilename, 1);
            if (node == null) {
                throw new NameNodeException("???????????????");
            }
            if (NodeType.FILE.getValue() != node.getType()) {
                throw new NameNodeException("?????????????????????");
            }
            diskNameSystem.deleteFile(trashFilename);
            String destFilename = File.separator + username + filename;
            Map<String, String> currentAttr = node.getAttr();
            currentAttr.remove(Constants.ATTR_FILE_DEL_TIME);
            boolean ret = diskNameSystem.createFile(destFilename, currentAttr);
            if (ret) {
                count++;
                log.debug("???????????????[src={}, target={}]", destFilename, destFilename);
            } else {
                log.warn("???????????????????????????????????????" + trashFilename);
            }
        }
        return count;
    }


    /**
     * ???????????????????????????????????????
     */
    private void handleListFilesRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException {
        ListFileRequest listFileRequest = ListFileRequest.parseFrom(requestWrapper.getRequest().getBody());
        //
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
     * ?????????????????????????????????
     */
//    private void handleFetchUserInfoRequest(RequestWrapper requestWrapper) {
//        List<UserEntity> collect = userManager.getAllUser().stream()
//                .map(User::toEntity)
//                .collect(Collectors.toList());
//        UserList response = UserList.newBuilder()
//                .addAllUserEntities(collect)
//                .build();
//        requestWrapper.sendResponse(response);
//    }

    /**
     * ??????????????????????????????????????????NameNode??????????????????
     */
//    private void handleNameNodePeerAwareRequest(RequestWrapper requestWrapper) throws Exception {
//        NettyPacket request = requestWrapper.getRequest();
//        ChannelHandlerContext ctx = requestWrapper.getCtx();
//        NameNodeAwareRequest nameNodeAwareRequest = NameNodeAwareRequest.parseFrom(request.getBody());
//        if (nameNodeAwareRequest.getIsClient()) {
//            /*
//             * ???????????????????????????????????????????????????????????????
//             *
//             * ????????????????????? namenode03 -> namenode01
//             *
//             * 1??????namenode03?????????????????????????????????????????????NAME_NODE_PEER_AWARE?????????
//             * 2??????namenode01????????????????????????????????????????????????NAME_NODE_PEER_AWARE???????????????namenode03
//             * 3??????namenode03??????namenode01?????????NAME_NODE_PEER_AWARE?????????????????????????????????????????????
//             *
//             */
//            PeerNameNode peerNameNode = peerNameNodes.addPeerNode(nameNodeAwareRequest.getNameNodeId(), (SocketChannel) ctx.channel(),
//                    nameNodeAwareRequest.getServer(), nameNodeConfig.getNameNodeId(), defaultScheduler);
//            if (peerNameNode != null) {
//                // ???????????????????????????????????????????????????????????????????????????
//                controllerManager.reportSelfInfoToPeer(peerNameNode, false);
//            }
//        }
//        controllerManager.onAwarePeerNameNode(nameNodeAwareRequest);
//    }

    /**
     * ??????????????????????????????NameNode??????????????????
     */
    private void handleCreateFileConfirmRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException,
            RequestTimeoutException, InterruptedException, NameNodeException {
//        if (isNoAuth(requestWrapper)) {
//            return;
//        }
        NettyPacket request = requestWrapper.getRequest();
        CreateFileRequest createFileRequest = CreateFileRequest.parseFrom(request.getBody());
        String realFilename = File.separator + request.getUserName() + createFileRequest.getFilename();
        int nodeId = getNodeId(realFilename);
//        if (this.nodeId == nodeId) {
            if (request.getAck() != 0) {
                dataNodeManager.waitFileReceive(realFilename, 3000);
            }
            //???????????????????????????
//            userManager.addStorageInfo(request.getUserName(), createFileRequest.getFileSize());
            CreateFileResponse response = CreateFileResponse.newBuilder()
                    .build();
            requestWrapper.sendResponse(response);
//        } else {
//            forwardRequestToOtherNameNode(nodeId, requestWrapper);
//        }
    }

    /**
     * ????????????BackupNode????????????
     */
    private void handleFetchBackupNodeInfoRequest(RequestWrapper requestWrapper) {
        // DataNode??????Client????????????BackupNode?????????
        boolean backupNodeExist = backupNodeInfoHolder != null;
        log.debug("????????????BackupNode???????????????????????????[hostname={}, port={}]",
                backupNodeExist ? backupNodeInfoHolder.getBackupNodeInfo().getHostname() : null,
                backupNodeExist ? backupNodeInfoHolder.getBackupNodeInfo().getPort() : null);

        NettyPacket response = NettyPacket.buildPacket(backupNodeExist ? backupNodeInfoHolder.getBackupNodeInfo().toByteArray() : new byte[0],
                PacketType.FETCH_BACKUP_NODE_INFO);
        requestWrapper.sendResponse(response, requestWrapper.getRequestSequence());
    }

    /**
     * ??????BackupNode??????????????????
     */
    private void handleReportBackupNodeInfoRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException {
        // BackupNode????????????
        BackupNodeInfo backupNodeInfo = BackupNodeInfo.parseFrom(requestWrapper.getRequest().getBody());

//        if (backupNodeInfoHolder != null && backupNodeInfoHolder.isActive()) {
//            log.info("??????BackupNode?????????????????????????????????2???BackupNode??????????????????[hostname={}, port={}]",
//                    backupNodeInfo.getHostname(), backupNodeInfo.getPort());
//            NettyPacket resp = NettyPacket.buildPacket(new byte[0], PacketType.DUPLICATE_BACKUP_NODE);
//            requestWrapper.sendResponse(resp, null);
//            return;
//        }
        this.backupNodeInfoHolder = new BackupNodeInfoHolder(backupNodeInfo, requestWrapper.getCtx().channel());
        log.info("??????BackupNode??????????????????[hostname={}, port={}]", backupNodeInfo.getHostname(), backupNodeInfo.getPort());
        NameNodeConf nameNodeConf = NameNodeConf.newBuilder()
                .putAllValues(nameNodeConfig.getConfig())
                .build();
        requestWrapper.sendResponse(nameNodeConf);//???????????????backup???????????????
    }

    /**
     * ??????????????????
     */
    private void handleAuthenticateRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException, NameNodeException {
        AuthenticateInfoRequest request = AuthenticateInfoRequest.parseFrom(requestWrapper.getRequest().getBody());
        boolean isBroadcastRequest = requestWrapper.getRequest().getBroadcast();
//        if (!isBroadcastRequest) {
//            boolean authenticate = userManager.login(requestWrapper.getCtx().channel(), request.getAuthenticateInfo());
//            if (!authenticate) {
//                throw new NameNodeException("???????????????" + request.getAuthenticateInfo());
//            }
            log.info("?????????????????????[authenticateInfo={}]", request.getAuthenticateInfo());
            // ??????????????????????????????Token??????????????????Token???????????????????????????
            String token = "Successful";
//                    userManager.getTokenByChannel(requestWrapper.getCtx().channel());
            requestWrapper.getRequest().setUserToken(token);
            // ????????????????????????????????????????????????????????????????????????????????????????????????
            broadcastSync(requestWrapper);
            // ?????????????????????????????????????????????
            AuthenticateInfoResponse response = AuthenticateInfoResponse.newBuilder()
                    .setToken(token)
                    .build();
            requestWrapper.sendResponse(response);

//        userManager.setToken(username, userToken);
//            requestWrapper.sendResponse();
//            log.info("????????????????????????????????????????????????[username={}, token={}]", username, userToken);
//        }
    }

    /**
     * ?????????????????????????????????
     *
     * @param requestWrapper ??????
     * @return ??????????????????
     */
    private boolean isNoAuth(RequestWrapper requestWrapper) throws NameNodeException {
        NettyPacket request = requestWrapper.getRequest();
        String userToken = request.getUserToken();
        if (userToken == null) {
            log.warn("??????????????????????????????, ????????????: [channel={}, packetType={}]",
                    NetUtils.getChannelId(requestWrapper.getCtx().channel()), request.getPacketType());
            throw new NameNodeException("????????????????????????");
        }
        String userName = request.getUserName();
//        if (!userManager.isUserToken(userName, userToken)) {
//            log.warn("??????????????????????????????, ????????????: [channel={}, packetType={}]",
//                    NetUtils.getChannelId(requestWrapper.getCtx().channel()), request.getPacketType());
//            throw new NameNodeException("????????????????????????");
//        } else {

            if (log.isDebugEnabled()) {
                log.debug("????????????????????????????????????[username={}, token={}, package={}]", userName, userToken,
                        PacketType.getEnum(request.getPacketType()).getDescription());
            }
            return false;

    }

    /**
     * ??????????????????????????????
     */
    private void handleReadAttrRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException,
            NameNodeException, RequestTimeoutException, InterruptedException {
//        if (isNoAuth(requestWrapper)) {
//            return;
//        }
        //
        ReadAttrRequest readAttrRequest = ReadAttrRequest.parseFrom(requestWrapper.getRequest().getBody());

        String userName = requestWrapper.getRequest().getUserName();
      //???????????????
        String realFilename = File.separator + userName + readAttrRequest.getFilename();

//        int nodeId = getNodeId(realFilename);
//        if (this.nodeId == nodeId) {
            Map<String, String> attr = diskNameSystem.getAttr(realFilename);//?????????????????????????????????
            if (attr == null) {
                throw new NameNodeException("??????????????????" + readAttrRequest.getFilename());
            }
            ReadAttrResponse response = ReadAttrResponse.newBuilder()
                    .putAllAttr(attr)
                    .build();
            requestWrapper.sendResponse(response);  //??????????????????
//        }
//            else {
//            forwardRequestToOtherNameNode(nodeId, requestWrapper);
//        }
    }

    /**
     * ??????DataNode????????????
     */

    private void handleDataNodeRegisterRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException, NameNodeException {
        //?????????
        RegisterRequest registerRequest = RegisterRequest.parseFrom(requestWrapper.getRequest().getBody());
//        boolean broadcast = broadcast(requestWrapper.getRequest());
        //?????????????????????
        boolean result = dataNodeManager.register(registerRequest);
        if (!result) {
            throw new NameNodeException("???????????????DataNode???????????????");
        }
//        if (!broadcast) {
//            requestWrapper.sendResponse();
//        }
    }

    /**
     * ??????DataNode????????????
     *
     * @param requestWrapper ??????
     */
    private void handleDataNodeHeartbeatRequest(RequestWrapper requestWrapper) throws
            InvalidProtocolBufferException, NameNodeException {
       //?????????
        HeartbeatRequest heartbeatRequest = HeartbeatRequest.parseFrom(requestWrapper.getRequest().getBody());
       //???????????????????????????
        Boolean heartbeat = dataNodeManager.heartbeat(heartbeatRequest.getHostname());
        if (!heartbeat) {
            throw new NameNodeException("???????????????DataNode????????????" + heartbeatRequest.getHostname());
        }
        //??????datanode??????
        DataNodeInfo dataNode = dataNodeManager.getDataNode(heartbeatRequest.getHostname());
        //datanode?????????????????????
        List<ReplicaTask> replicaTask = dataNode.pollReplicaTask(100);
        //????????????
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
        //??????????????????
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
        // ????????????????????????NameNode???????????????????????????
//        List<NettyPacket> nettyPackets = broadcastSync(requestWrapper);
        // ?????????NameNode????????????????????????????????????????????????
//        for (NettyPacket nettyPacket : nettyPackets) {
//            HeartbeatResponse heartbeatResponse = HeartbeatResponse.parseFrom(nettyPacket.getBody());
//            replicaCommands.addAll(heartbeatResponse.getCommandsList());
//        }
         //????????????
        HeartbeatResponse response = HeartbeatResponse.newBuilder()
                .addAllCommands(replicaCommands)
                .build();
        requestWrapper.sendResponse(response);
    }

    /**
     * ???????????????????????????
     *
     * @param requestWrapper ?????????????????????
     */
    private void handleMkdirRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException,
            RequestTimeoutException, InterruptedException, NameNodeException {
//      if (isNoAuth(requestWrapper)) {
//            return;
//        }
        //
        NettyPacket request = requestWrapper.getRequest();
        MkdirRequest mkdirRequest = MkdirRequest.parseFrom(request.getBody());
        //?????????????????????????????????
        String realFilename = File.separator + request.getUserName() + mkdirRequest.getPath();
       //???????????????????????????nodeID???namenode???
        int nodeId = getNodeId(realFilename);
        if (this.nodeId == nodeId) {
            this.diskNameSystem.mkdir(realFilename, mkdirRequest.getAttrMap());
            requestWrapper.sendResponse();
        }
    }

    /**
     * ??????BackupNode??????EditLog
     */
    private void handleFetchEditLogRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException {
       //?????????
        FetchEditsLogRequest fetchEditsLogRequest = FetchEditsLogRequest.parseFrom(requestWrapper.getRequest().getBody());

        long txId = fetchEditsLogRequest.getTxId();
        List<EditLogWrapper> result = new ArrayList<>();
        try {
            result = fetchEditLogBuffer.fetch(txId);//??????id??????editlog???????????????
        } catch (IOException e) {
            log.error("??????EditLog?????????", e);
        }
        //??????????????????
        FetchEditsLogResponse response = FetchEditsLogResponse.newBuilder()
                .addAllEditLogs(result.stream()
                        .map(EditLogWrapper::getEditLog)
                        .collect(Collectors.toList()))
//                .addAllUsers(userManager.getAllUser().stream()
//                        .map(User::toEntity)
//                        .collect(Collectors.toList()))
                .build();
        //????????????
        requestWrapper.sendResponse(response);
        if (NameNodeLaunchMode.SINGLE.equals(mode)) {
            return;
        }
        //????????????
        if (fetchEditsLogRequest.getNeedSlots() || slotsChanged.get()) {
            if (slotsChanged.compareAndSet(true, false)) {
                log.info("?????????Slots???????????????????????????????????????BackupNode?????????????????????.");
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
     * ?????????????????????????????????????????????BackupNode??????FsImage??????
     */
    private void handleFileTransferRequest(RequestWrapper requestWrapper) {
        FilePacket filePacket = FilePacket.parseFrom(requestWrapper.getRequest().getBody());
        fileReceiveHandler.handleRequest(filePacket);
    }

    /**
     * ??????DataNode??????????????????
     */
    private void handleDataNodeReportStorageInfoRequest(RequestWrapper requestWrapper) throws
            InvalidProtocolBufferException {
        //?????????
        ReportCompleteStorageInfoRequest request =
                ReportCompleteStorageInfoRequest.parseFrom(requestWrapper.getRequest().getBody());
//        broadcast(requestWrapper.getRequest());
        log.info("???????????????????????????[hostname={}, files={}]", request.getHostname(), request.getFileInfosCount());
        //???????????????????????????
        for (FileMetaInfo file : request.getFileInfosList()) {
            if (getNodeId(file.getFilename()) == this.nodeId) {
                // ??????????????????Slot??????????????????????????????
                // TODO ????????????????????????????????????????????????????????????????????????????????????????????????????????????DataNode??????
                FileInfo fileInfo = new FileInfo();
                fileInfo.setFileName(file.getFilename());
                fileInfo.setFileSize(file.getFileSize());
                fileInfo.setHostname(request.getHostname());
                dataNodeManager.addReplica(fileInfo); //?????????????????????????????????????????????
            }
        }
        if (request.getFinished()) {//????????????????????????
            dataNodeManager.setDataNodeReady(request.getHostname());
            log.info("?????????????????????????????????[hostname={}]", request.getHostname());
        }
    }

    /**
     * ?????????????????????????????????
     */
    private void handleCreateFileRequest(RequestWrapper requestWrapper) throws Exception {
//        if (isNoAuth(requestWrapper)) {
//            return;
//        }
        NettyPacket request = requestWrapper.getRequest();
        CreateFileRequest createFileRequest = CreateFileRequest.parseFrom(request.getBody());
        //???????????????
        String realFilename = File.separator + request.getUserName() + createFileRequest.getFilename();
        //?????????????????????nodeId
        int nodeId = getNodeId(realFilename);
        if (this.nodeId == nodeId) {//????????????namenode???
            Map<String, String> attrMap = new HashMap<>(createFileRequest.getAttrMap()); //??????????????????
            String replicaNumStr = attrMap.get(Constants.ATTR_REPLICA_NUM);//?????????????????????
            attrMap.put(Constants.ATTR_FILE_SIZE, String.valueOf(createFileRequest.getFileSize()));//????????????
            int replicaNum;
            if (replicaNumStr != null) {//??????????????????????????????
                replicaNum = Integer.parseInt(replicaNumStr);
                // ?????????????????????????????????
                replicaNum = Math.max(replicaNum, diskNameSystem.getNameNodeConfig().getReplicaNum());
                // ?????????????????????????????????
                replicaNum = Math.min(replicaNum, Constants.MAX_REPLICA_NUM);
            } else {
                replicaNum = diskNameSystem.getNameNodeConfig().getReplicaNum();
                attrMap.put(Constants.ATTR_REPLICA_NUM, String.valueOf(replicaNum));
            }
            Node node = diskNameSystem.listFiles(realFilename);//?????????????????????
            if (node != null) {
                throw new NameNodeException("??????????????????" + createFileRequest.getFilename());
            }
            //??????????????????????????????
            //?????????????????????????????? ??????????????????dataNodes??????

            //List<DataNodeInfo> dataNodeList = dataNodeManager.allocateDataNodes(request.getUserName(), replicaNum, realFilename);
            List<DataNodeInfo> dataNodeList = new LinkedList<>();
            DataNodeInfo dataNodeInfo = new DataNodeInfo("localhost",5671,8001,123L);
            dataNodeList.add(dataNodeInfo);

            Prometheus.incCounter("namenode_put_file_count", "NameNode?????????????????????????????????");
            Prometheus.hit("namenode_put_file_qps", "NameNode??????????????????QPS");
            //file??????dataNode????????????
            List<DataNode> dataNodes = dataNodeList.stream()
                    .map(e -> DataNode.newBuilder().setHostname(e.getHostname())
                            .setNioPort(e.getNioPort())
                            .setHttpPort(e.getHttpPort())
                            .build())
                    .collect(Collectors.toList());

            diskNameSystem.createFile(realFilename, attrMap);

            List<String> collect = dataNodeList.stream().map(DataNodeInfo::getHostname).collect(Collectors.toList());
            log.info("???????????????[filename={}, datanodes={}]", realFilename, String.join(",", collect));
            //??????????????????
            CreateFileResponse response = CreateFileResponse.newBuilder()
                    .addAllDataNodes(dataNodes)
                    .setRealFileName(realFilename)
                    .build();
            requestWrapper.sendResponse(response);
        }
//        else {
//            forwardRequestToOtherNameNode(nodeId, requestWrapper);
//        }
    }

    /**
     * ?????????????????????DataNode??????
     */
    private void handleGetDataNodeForFileRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException,
            NameNodeException, RequestTimeoutException, InterruptedException {
//        if (isNoAuth(requestWrapper)) {
//            return;
//        }
        //?????????
        GetDataNodeForFileRequest getDataNodeForFileRequest = GetDataNodeForFileRequest.parseFrom(requestWrapper.getRequest().getBody());
        String userName = requestWrapper.getRequest().getUserName();
        //??????????????????????????????
        String realFilename = File.separator + userName + getDataNodeForFileRequest.getFilename();
        //???????????????namenodeid
        int nodeId = getNodeId(realFilename);

        if (this.nodeId == nodeId) {
            //????????????????????????????????????DataNode?????????????????????DataNode??????????????????DataNode?????????????????????
            DataNodeInfo dataNodeInfo = dataNodeManager.chooseReadableDataNodeByFileName(realFilename);
            if (dataNodeInfo != null) {
                Prometheus.incCounter("namenode_get_file_count", "NameNode?????????????????????????????????");
                Prometheus.hit("namenode_get_file_qps", "NameNode?????????????????????????????????");

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
            throw new NameNodeException("??????????????????" + getDataNodeForFileRequest.getFilename());
        }
//        else {
//            forwardRequestToOtherNameNode(nodeId, requestWrapper);
//        }
    }

    /**
     * ????????????????????????
     */
    private void handleReplicaReceiveRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException {
        //?????????
        InformReplicaReceivedRequest request = InformReplicaReceivedRequest.parseFrom(requestWrapper.getRequest().getBody());
//        boolean broadcast = broadcast(requestWrapper.getRequest());
        log.info("????????????????????????????????????[hostname={}, filename={}]", request.getHostname(), request.getFilename());
       //??????????????????????????????
        FileInfo fileInfo = new FileInfo(request.getHostname(), request.getFilename(), request.getFileSize());

        int nodeId = getNodeId(request.getFilename());
        // ????????????slot???NameNode????????????????????????????????????????????????????????????????????????DataNode?????????????????????
        if (this.nodeId == nodeId) {
            dataNodeManager.addReplica(fileInfo);//??????????????????????????????
        }
        DataNodeInfo dataNode = dataNodeManager.getDataNode(request.getHostname());
        dataNode.addStoredDataSize(request.getFileSize());//?????????????????????
        if (true) {
            requestWrapper.sendResponse();
        }
    }


    /**
     * ????????????????????????
     */
    private void handleRemoveFileRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException,
            NameNodeException, RequestTimeoutException, InterruptedException {
//        if (isNoAuth(requestWrapper)) {
//            return;
//        }
        //???????????????
        RemoveFileRequest removeFileRequest = RemoveFileRequest.parseFrom(requestWrapper.getRequest().getBody());
        String userName = requestWrapper.getRequest().getUserName();
        //???????????????
        String realFilename = File.separator + userName + removeFileRequest.getFilename();

//        int nodeId = getNodeId(realFilename);
//        if (this.nodeId == nodeId) {
            removeFileInternal(removeFileRequest.getFilename(), userName);//????????????
            requestWrapper.sendResponse();//????????????

    }

    /**
     * ????????????????????????
     *
     * @param filename ????????????
     * @param userName ?????????
     */
    public void removeFileInternal(String filename, String userName) throws NameNodeException {
        String realFilename = File.separator + userName + filename;
        Node node = diskNameSystem.listFiles(realFilename);
        if (node == null) {
            throw new NameNodeException("??????????????????" + filename);
        }
        Map<String, String> attr = new HashMap<>(PrettyCodes.trimMapSize());
        attr.put(Constants.ATTR_FILE_DEL_TIME, String.valueOf(System.currentTimeMillis()));
        if (node.getChildren().isEmpty()) {
            diskNameSystem.deleteFile(realFilename);
            String destFilename = File.separator + userName + File.separator + Constants.TRASH_DIR + filename;
            Map<String, String> currentAttr = node.getAttr();
            currentAttr.putAll(attr);
            diskNameSystem.createFile(destFilename, currentAttr);
            log.debug("???????????????????????????????????????[src={}, target={}]", realFilename, destFilename);
        } else {
            throw new NameNodeException("????????????????????????????????????" + filename);
        }
    }

    /**
     * ????????????????????????NameNode??????
     *
     * @param request ??????
     * @return ????????????????????????NameNode???????????????
     */
//    private boolean broadcast(NettyPacket request) {
//        boolean isBroadcastRequest = request.getBroadcast();
//        if (!isBroadcastRequest) {
//            // ????????????????????????????????????????????????NameNode???????????????
//            request.setBroadcast(true);
//            List<Integer> broadcast = peerNameNodes.broadcast(request);
//            if (!broadcast.isEmpty()) {
//                log.debug("????????????????????????NameNode: [sequence={}, broadcast={}, packetType={}]",
//                        request.getSequence(), broadcast, PacketType.getEnum(request.getPacketType()).getDescription());
//            }
//        }
//        return isBroadcastRequest;
//    }

    /**
     * ????????????????????????NameNode??????, ?????????????????????????????????
     *
     * @param requestWrapper ??????
     * @return ????????????
//     */
    private List<NettyPacket> broadcastSync(RequestWrapper requestWrapper) {
        NettyPacket request = requestWrapper.getRequest();
        boolean isBroadcastRequest = request.getBroadcast();
        if (!isBroadcastRequest) {
            // ????????????????????????????????????????????????NameNode???????????????
            request.setBroadcast(true);
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();
//            List<NettyPacket> nettyPackets = new ArrayList<>(peerNameNodes.broadcastSync(request));
            stopWatch.stop();
//            if (!nettyPackets.isEmpty()) {
//                log.debug("??????????????????????????????NameNode????????????????????????: [sequence={}, broadcast={}, packetType={}, cost={} s]",
//                        request.getSequence(), peerNameNodes.getAllNodeId(),
//                        PacketType.getEnum(request.getPacketType()).getDescription(),
//                        stopWatch.getTime() / 1000.0D);
//            }
//            return nettyPackets;
        }
        return new ArrayList<>();
    }

    /**
     * ????????????????????????
     */
    private void sendErrorResponse(RequestWrapper requestWrapper, String msg) {
        NettyPacket nettyResponse = NettyPacket.buildPacket(new byte[0],
                PacketType.getEnum(requestWrapper.getRequest().getPacketType()));
        nettyResponse.setError(msg);
        requestWrapper.sendResponse(nettyResponse, requestWrapper.getRequestSequence());
    }

    /**
     * ?????????????????????????????????????????????ID
     *
     * @param filename ?????????
     * @return ??????ID
     */
    public int getNodeId(String filename) {
        if (NameNodeLaunchMode.CLUSTER.equals(mode)) {
            return shardingManager.getNameNodeIdByFileName(filename);
        }
        return this.nodeId;
    }

    /**
     * ?????????????????????NameNode
     */
//    private void forwardRequestToOtherNameNode(int nodeId, RequestWrapper requestWrapper) throws
//            InterruptedException, RequestTimeoutException {
//        NettyPacket request = requestWrapper.getRequest();
//        log.debug("?????????????????????NameNode: [targetNodeId={}, sequence={}, packetType={}]", nodeId,
//                request.getSequence(), PacketType.getEnum(request.getPacketType()).getDescription());
//        String sequence = request.getSequence();
//        NettyPacket response = peerNameNodes.sendSync(nodeId, request);
//        requestWrapper.sendResponse(response, sequence);
//    }
}
