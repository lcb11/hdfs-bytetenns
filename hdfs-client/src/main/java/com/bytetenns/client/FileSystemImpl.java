package com.ruyuan.dfs.client;

import com.google.protobuf.InvalidProtocolBufferException;
import com.ruyuan.dfs.client.config.FsClientConfig;
import com.ruyuan.dfs.client.exception.DfsClientException;
import com.ruyuan.dfs.client.tools.CommandLineListener;
import com.ruyuan.dfs.client.tools.OnMultiFileProgressListener;
import com.ruyuan.dfs.common.Constants;
import com.ruyuan.dfs.common.NettyPacket;
import com.ruyuan.dfs.common.enums.PacketType;
import com.ruyuan.dfs.common.exception.RequestTimeoutException;
import com.ruyuan.dfs.common.ha.BackupNodeManager;
import com.ruyuan.dfs.common.network.NetClient;
import com.ruyuan.dfs.common.network.RequestWrapper;
import com.ruyuan.dfs.common.network.file.FileTransportClient;
import com.ruyuan.dfs.common.network.file.OnProgressListener;
import com.ruyuan.dfs.common.utils.DefaultScheduler;
import com.ruyuan.dfs.common.utils.PrettyCodes;
import com.ruyuan.dfs.common.utils.StringUtils;
import com.ruyuan.dfs.model.backup.BackupNodeInfo;
import com.ruyuan.dfs.model.backup.INode;
import com.ruyuan.dfs.model.client.*;
import com.ruyuan.dfs.model.common.DataNode;
import com.ruyuan.dfs.model.namenode.ClientDataNodeInfo;
import com.ruyuan.dfs.model.namenode.ClientNameNodeInfo;
import com.ruyuan.dfs.model.namenode.ListFileRequest;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * 文件系统客户端的实现类
 *
 * @author Sun Dasheng
 */
@Slf4j
public class FileSystemImpl implements FileSystem {

    private static final int AUTH_INIT = 0;
    private static final int AUTH_SUCCESS = 1;
    private static final int AUTH_FAIL = 2;
    private FsClientConfig fsClientConfig;
    private NetClient netClient;
    private DefaultScheduler defaultScheduler;
    private volatile int authStatus = AUTH_INIT;
    private BackupNodeManager backupNodeManager;
    private CommandLineListener commandLineListener;


    public FileSystemImpl(FsClientConfig fsClientConfig) {
        this.defaultScheduler = new DefaultScheduler("FSClient-Scheduler-");
        int connectRetryTime = fsClientConfig.getConnectRetryTime() > 0 ? fsClientConfig.getConnectRetryTime() : -1;
        this.netClient = new NetClient("FSClient-NameNode-" + fsClientConfig.getServer(),
                defaultScheduler, connectRetryTime);
        this.fsClientConfig = fsClientConfig;
        this.backupNodeManager = new BackupNodeManager(defaultScheduler);
    }

    /**
     * 设置命令行监听器
     *
     * @param commandLineListener 监听器
     */
    public void setCommandLineListener(CommandLineListener commandLineListener) {
        this.commandLineListener = commandLineListener;
    }

    /**
     * 启动
     */
    public void start() throws Exception {
        this.netClient.addNettyPackageListener(this::onReceiveMessage);
        // 添加监听器，在断线重连的时候，自动发起认证
        this.netClient.addConnectListener(connected -> {
            if (connected) {
                authenticate();
                fetchBackupInfo();
            }
        });
        this.netClient.addNetClientFailListener(() -> {
            log.info("dfs-client检测到NameNode挂了，标记NameNode已经宕机");
            backupNodeManager.markNameNodeDown();
            if (commandLineListener != null) {
                commandLineListener.onConnectFailed();
            }
        });
        this.netClient.connect(fsClientConfig.getServer(), fsClientConfig.getPort());
        this.netClient.ensureConnected();
        log.info("和NameNode建立连接成功");
    }

    /**
     * 收到消息
     */
    private void onReceiveMessage(RequestWrapper requestWrapper) throws InvalidProtocolBufferException {
        PacketType packetType = PacketType.getEnum(requestWrapper.getRequest().getPacketType());
        if (packetType == PacketType.FETCH_BACKUP_NODE_INFO) {
            handleFetchBackupNodeInfoResponse(requestWrapper);
        }
    }

    private void handleFetchBackupNodeInfoResponse(RequestWrapper requestWrapper) throws InvalidProtocolBufferException {
        if (requestWrapper.getRequest().getBody().length == 0) {
            log.warn("拉取BackupNode信息为空，设置NetClient为无限重试.");
            netClient.setRetryTime(-1);
            return;
        }
        netClient.setRetryTime(3);
        BackupNodeInfo backupNodeInfo = BackupNodeInfo.parseFrom(requestWrapper.getRequest().getBody());
        backupNodeManager.maybeEstablishConnect(backupNodeInfo, hostname -> {
            fsClientConfig.setServer(hostname);
            netClient.shutdown();
            log.info("检测到BackupNode升级为NameNode了，替换NameNode链接信息，并重新建立链接：[hostname={}, port={}]",
                    fsClientConfig.getServer(), fsClientConfig.getPort());
            netClient = new NetClient("FSClient-NameNode-" + fsClientConfig.getServer(), defaultScheduler);
            try {
                start();
            } catch (Exception e) {
                log.error("连接失败：", e);
            }
        });
    }

    private void fetchBackupInfo() throws InterruptedException {
        NettyPacket nettyPacket = NettyPacket.buildPacket(new byte[0], PacketType.FETCH_BACKUP_NODE_INFO);
        netClient.send(nettyPacket);
    }

    /**
     * 发起认证请求
     * 因为这里需要发送请求并同步等待结果，onConnected方法不能阻塞，
     * 否则会导致后面网络收发失败，所以需要新开线程处理
     */
    private void authenticate() {
        defaultScheduler.scheduleOnce("发起认证", () -> {
            try {
                String authInfo = fsClientConfig.getUsername() + "," + fsClientConfig.getSecret();
                AuthenticateInfoRequest req = AuthenticateInfoRequest.newBuilder()
                        .setAuthenticateInfo(authInfo)
                        .build();
                NettyPacket nettyPacket = NettyPacket.buildPacket(req.toByteArray(), PacketType.AUTHENTICATE);
                NettyPacket resp = safeSendSync(nettyPacket);
                AuthenticateInfoResponse authenticateInfoResponse = AuthenticateInfoResponse.parseFrom(resp.getBody());
                authStatus = AUTH_SUCCESS;
                fsClientConfig.setUserToken(authenticateInfoResponse.getToken());
                notifyAuthenticate();
                log.info("发起认证成功：[username={}, token={}]", fsClientConfig.getUsername(), authenticateInfoResponse.getToken());
                if (commandLineListener != null) {
                    commandLineListener.onAuthResult(true);
                }
            } catch (Exception e) {
                log.error("发起认证失败：", e);
                authStatus = AUTH_FAIL;
                close();
                if (commandLineListener != null) {
                    commandLineListener.onAuthResult(false);
                }
            }
        });
    }

    /**
     * 创建目录
     */
    @Override
    public void mkdir(String path) throws Exception {
        mkdir(path, new HashMap<>(PrettyCodes.trimMapSize()));
    }

    @Override
    public void mkdir(String path, Map<String, String> attr) throws Exception {
        validate(path);
        MkdirRequest request = MkdirRequest.newBuilder()
                .setPath(path)
                .putAllAttr(attr)
                .build();
        NettyPacket nettyPacket = NettyPacket.buildPacket(request.toByteArray(), PacketType.MKDIR);
        safeSendSync(nettyPacket);
        log.info("创建文件夹成功：[filename={}]", path);
    }

    @Override
    public void put(String filename, File file) throws Exception {
        put(filename, file, -1, new HashMap<>(PrettyCodes.trimMapSize()));
    }

    @Override
    public void put(String filename, File file, int replicaNum) throws Exception {
        put(filename, file, replicaNum, new HashMap<>(PrettyCodes.trimMapSize()));
    }

    @Override
    public void put(String filename, File file, int numOfReplica, Map<String, String> attr) throws Exception {
        put(filename, file, numOfReplica, attr, null);
    }

    /**
     * <pre>
     *      1. 在NameNode中创建一个文件，接着NameNode返回一个DataNode机器列表
     *      2. 上传对应的文件到DataNode
     * </pre>
     */
    @Override
    public void put(String filename, File file, int replicaNum, Map<String, String> attr, OnProgressListener listener) throws Exception {
        validate(filename);
        if (replicaNum > Constants.MAX_REPLICA_NUM) {
            throw new DfsClientException("不合法的副本数量：" + replicaNum);
        }
        for (String key : Constants.KEYS_ATTR_SET) {
            if (attr.containsKey(key)) {
                log.warn("文件属性包含关键属性：[key={}]", key);
            }
        }
        if (replicaNum > 0) {
            attr.put(Constants.ATTR_REPLICA_NUM, String.valueOf(replicaNum));
        }
        CreateFileRequest request = CreateFileRequest.newBuilder()
                .setFilename(filename)
                .setFileSize(file.length())
                .putAllAttr(attr)
                .build();
        NettyPacket nettyPacket = NettyPacket.buildPacket(request.toByteArray(), PacketType.CREATE_FILE);
        NettyPacket resp = safeSendSync(nettyPacket);
        CreateFileResponse response = CreateFileResponse.parseFrom(resp.getBody());
        OnMultiFileProgressListener onMultiFileProgressListener =
                new OnMultiFileProgressListener(listener, response.getDataNodesList().size());
        for (int i = 0; i < response.getDataNodesList().size(); i++) {
            DataNode dataNodes = response.getDataNodes(i);
            String hostname = dataNodes.getHostname();
            int port = dataNodes.getNioPort();
            NetClient netClient = new NetClient("FSClient-DataNode-" + hostname, defaultScheduler);
            FileTransportClient fileTransportClient = new FileTransportClient(netClient);
            netClient.connect(hostname, port);
            netClient.ensureConnected();
            if (log.isDebugEnabled()) {
                log.debug("开始上传文件到：[node={}:{}, filename={}]", hostname, port, filename);
            }
            fileTransportClient.sendFile(response.getRealFileName(), file.getAbsolutePath(), onMultiFileProgressListener, true);
            fileTransportClient.shutdown();
            if (log.isDebugEnabled()) {
                log.debug("完成上传文件到：[node={}:{}, filename={}]", hostname, port, filename);
            }
        }
        /*
         * 文件上传是上传到DataNode节点，客户端上传到DataNode之后，DataNode再上报给NameNode节点中间有一个时间差
         * 为了达到强一致性，保证文件上传后，立马是可以读取文件的，需要等待NameNode收到DataNode上报的信息，才认为是上传成功的。
         * 但是这样一来会降低上传文件的吞吐量。 因为会占用NameNode一个线程池的线程在哪里hang住等待3秒，
         * 有可能让DataNode上报的请求在队列里面一直等待，最终出现超时错误。这里有两种方案可以选择：
         *
         * 1、 客户端可以配置让NameNode确认是否等待，如果开启确认等待，则吞吐量会下降，但是保证强一致性。如果不开启确认等待，则吞吐量比较高，
         *     但是一致性不能保证，就是说上传完毕后有可能立即读文件读不到
         *
         * 2、 在NameNode那边等待的过程，不要直接在线程里面等待，而是建立一个任务Task，保存在集合中，后台起一个线程，就无限循环的去判断
         *     这个Task是否完成，如果完成才写回响应。这种方式可以保证强一致性，并且不会阻塞线程池中的线程。
         *
         * 目前我们先采用第一种方式实现，第二种后面可以考虑优化实现。
         *
         */
        NettyPacket confirmRequest = NettyPacket.buildPacket(request.toByteArray(), PacketType.CREATE_FILE_CONFIRM);
        confirmRequest.setTimeoutInMs(-1);
        confirmRequest.setAck(fsClientConfig.getAck());
        safeSendSync(confirmRequest);
    }

    @Override
    public void get(String filename, String absolutePath) throws Exception {
        get(filename, absolutePath, null);
    }

    @Override
    public void get(String filename, String absolutePath, OnProgressListener listener) throws Exception {
        validate(filename);
        GetDataNodeForFileRequest request = GetDataNodeForFileRequest.newBuilder()
                .setFilename(filename)
                .build();
        NettyPacket nettyPacket = NettyPacket.buildPacket(request.toByteArray(), PacketType.GET_DATA_NODE_FOR_FILE);
        NettyPacket resp = safeSendSync(nettyPacket);
        GetDataNodeForFileResponse response = GetDataNodeForFileResponse.parseFrom(resp.getBody());
        DataNode dataNode = response.getDataNode();
        NetClient netClient = new NetClient("FSClient-DataNode-" + dataNode.getHostname(), defaultScheduler);
        FileTransportClient fileTransportClient = new FileTransportClient(netClient);
        netClient.connect(dataNode.getHostname(), dataNode.getNioPort());
        netClient.ensureConnected();
        fileTransportClient.readFile(response.getRealFileName(), absolutePath, new OnProgressListener() {
            @Override
            public void onProgress(long total, long current, float progress, int currentReadBytes) {
                if (listener != null) {
                    listener.onProgress(total, current, progress, currentReadBytes);
                }
            }

            @Override
            public void onCompleted() {
                if (listener != null) {
                    listener.onCompleted();
                }
                fileTransportClient.shutdown();
            }
        });
    }

    @Override
    public void remove(String filename) throws Exception {
        validate(filename);
        RemoveFileRequest request = RemoveFileRequest.newBuilder()
                .setFilename(filename)
                .build();
        NettyPacket nettyPacket = NettyPacket.buildPacket(request.toByteArray(), PacketType.REMOVE_FILE);
        safeSendSync(nettyPacket);
    }

    @Override
    public Map<String, String> readAttr(String filename) throws Exception {
        validate(filename);
        ReadAttrRequest request = ReadAttrRequest.newBuilder()
                .setFilename(filename)
                .build();
        NettyPacket nettyPacket = NettyPacket.buildPacket(request.toByteArray(), PacketType.READ_ATTR);
        NettyPacket resp = safeSendSync(nettyPacket);
        ReadAttrResponse response = ReadAttrResponse.parseFrom(resp.getBody());
        return response.getAttrMap();
    }

    @Override
    public void close() {
        this.defaultScheduler.shutdown();
        this.netClient.shutdown();
    }

    @Override
    public List<FsFile> listFile(String path) throws Exception {
        validate(path);
        ListFileRequest listFileRequest = ListFileRequest.newBuilder()
                .setPath(path)
                .build();
        NettyPacket nettyPacket = NettyPacket.buildPacket(listFileRequest.toByteArray(), PacketType.CLIENT_LIST_FILES);
        NettyPacket responsePackage = safeSendSync(nettyPacket);
        INode node = INode.parseFrom(responsePackage.getBody());
        return FsFile.parse(node);
    }

    @Override
    public ClientNameNodeInfo nameNodeInfo() throws Exception {
        NettyPacket nettyPacket = NettyPacket.buildPacket(new byte[0], PacketType.CLIENT_READ_NAME_NODE_INFO);
        NettyPacket responseNettyPacket = safeSendSync(nettyPacket);
        return ClientNameNodeInfo.parseFrom(responseNettyPacket.getBody());
    }

    @Override
    public ClientDataNodeInfo dataNodeInfo() throws Exception {
        NettyPacket nettyPacket = NettyPacket.buildPacket(new byte[0], PacketType.CLIENT_READ_DATA_NODE_INFO);
        NettyPacket responseNettyPacket = safeSendSync(nettyPacket);
        return ClientDataNodeInfo.parseFrom(responseNettyPacket.getBody());
    }

    @Override
    public ReadStorageInfoResponse readStorageInfo(String filename) throws Exception {
        validate(filename);
        ReadStorageInfoRequest request = ReadStorageInfoRequest.newBuilder()
                .setFilename(filename)
                .build();
        NettyPacket nettyPacket = NettyPacket.buildPacket(request.toByteArray(), PacketType.CLIENT_READ_STORAGE_INFO);
        NettyPacket responseNettyPacket = safeSendSync(nettyPacket);
        return ReadStorageInfoResponse.parseFrom(responseNettyPacket.getBody());
    }

    @Override
    public PreCalculateResponse preCalculatePath(String path) throws Exception {
        validate(path);
        PreCalculateRequest request = PreCalculateRequest.newBuilder()
                .setPath(path)
                .build();
        NettyPacket nettyPacket = NettyPacket.buildPacket(request.toByteArray(), PacketType.CLIENT_PRE_CALCULATE);
        NettyPacket responseNettyPacket = safeSendSync(nettyPacket);
        return PreCalculateResponse.parseFrom(responseNettyPacket.getBody());
    }

    @Override
    public GetAllFilenameResponse getAllFilenameByPath(String path) throws Exception {
        validate(path);
        GetAllFilenameRequest request = GetAllFilenameRequest.newBuilder()
                .setPath(path)
                .build();
        NettyPacket nettyPacket = NettyPacket.buildPacket(request.toByteArray(), PacketType.CLIENT_GET_ALL_FILENAME);
        nettyPacket.setSupportChunked(true);
        NettyPacket responsePackage = safeSendSync(nettyPacket);
        return GetAllFilenameResponse.parseFrom(responsePackage.getBody());
    }

    /**
     * 确保返回的结果不是无权限
     */
    private NettyPacket safeSendSync(NettyPacket nettyPacket) throws DfsClientException,
            InterruptedException, RequestTimeoutException {
        nettyPacket.setUsername(fsClientConfig.getUsername());
        if (fsClientConfig.getUserToken() != null) {
            nettyPacket.setUserToken(fsClientConfig.getUserToken());
        }
        NettyPacket resp = netClient.sendSync(nettyPacket);
        if (resp.isError()) {
            throw new DfsClientException(resp.getError());
        }
        return resp;
    }

    /**
     * 验证文件名称合法,校验连接已经认证通过
     *
     * @param filename 文件名称
     */
    private void validate(String filename) throws Exception {
        boolean ret = StringUtils.validateFileName(filename);
        if (!ret) {
            throw new DfsClientException("不合法的文件名：" + filename);
        }
        while (authStatus == AUTH_INIT) {
            synchronized (this) {
                wait(10);
            }
        }
        if (authStatus != AUTH_SUCCESS) {
            throw new DfsClientException("未通过认证，不能发送请求");
        }
    }

    /**
     * 认证成功后，唤醒等待的线程
     */
    private void notifyAuthenticate() {
        synchronized (this) {
            notifyAll();
        }
    }
}
