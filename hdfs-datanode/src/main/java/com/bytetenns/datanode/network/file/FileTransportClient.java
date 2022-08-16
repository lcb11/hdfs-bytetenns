package com.bytetenns.datanode.network.file;

import com.bytetenns.datanode.netty.NettyPacket;
import com.bytetenns.datanode.enums.PacketType;
import com.bytetenns.datanode.network.NetClient;
import com.bytetenns.dfs.model.common.GetFileRequest;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 支持文件上传、下载的客户端
 *
 * @author Sun Dasheng
 */
@Slf4j
public class FileTransportClient {

    private NetClient netClient;
    private Map<String, String> filePathMap = new ConcurrentHashMap<>();
    private Map<String, OnProgressListener> listeners = new ConcurrentHashMap<>();

    public FileTransportClient(NetClient netClient) {
        this(netClient, true);
    }

    public FileTransportClient(NetClient netClient, boolean getFile) {
        this.netClient = netClient;
        if (getFile) {
            FileTransportCallback callback = new FileTransportCallback() {
                @Override
                public String getPath(String filename) {
                    return filePathMap.remove(filename);
                }

                @Override
                public void onProgress(String filename, long total, long current, float progress, int currentWriteBytes) {
                    OnProgressListener listener = listeners.get(filename);
                    if (listener != null) {
                        listener.onProgress(total, current, progress, currentWriteBytes);
                    }
                }

                @Override
                public void onCompleted(FileAttribute fileAttribute) {
                    OnProgressListener listener = listeners.remove(fileAttribute.getFilename());
                    if (listener != null) {
                        listener.onCompleted();
                    }
                }
            };
            FileReceiveHandler fileReceiveHandler = new FileReceiveHandler(callback);
            this.netClient.addNettyPackageListener(requestWrapper -> {
                NettyPacket request = requestWrapper.getRequest();
                if (request.getPacketType() == PacketType.TRANSFER_FILE.getValue()) {
                    FilePacket filePacket = FilePacket.parseFrom(requestWrapper.getRequest().getBody());
                    fileReceiveHandler.handleRequest(filePacket);
                }
            });
        }
    }

    /**
     * 上传文件
     *
     * @param absolutePath 本地文件绝对路径
     * @throws Exception 文件不存在
     */
    public void sendFile(String absolutePath) throws Exception {
        sendFile(absolutePath, absolutePath, null, false);
    }

    /**
     * 上传文件
     *
     * @param filename     服务器文件名称
     * @param absolutePath 本地文件绝对路径
     * @throws Exception 文件不存在
     */
    public void sendFile(String filename, String absolutePath, OnProgressListener listener, boolean force) throws Exception {
        File file = new File(absolutePath);
        if (!file.exists()) {
            throw new FileNotFoundException("文件不存在：" + absolutePath);
        }
        DefaultFileSendTask fileSender = new DefaultFileSendTask(file, filename, netClient.socketChannel(), listener);
        fileSender.execute(force);
    }

    /**
     * 下载文件
     *
     * @param filename     文件名
     * @param absolutePath 本地文件绝对路径
     * @param listener     进度监听器
     */
    public void readFile(String filename, String absolutePath, OnProgressListener listener) throws InterruptedException {
        if (listener != null) {
            listeners.put(filename, listener);
        }
        filePathMap.put(filename, absolutePath);
        GetFileRequest request = GetFileRequest.newBuilder()
                .setFilename(filename)
                .build();
        NettyPacket nettyPacket = NettyPacket.buildPacket(request.toByteArray(), PacketType.GET_FILE);
        netClient.send(nettyPacket);
    }

    /**
     * 优雅关闭
     */
    public void shutdown() {
        listeners.clear();
        filePathMap.clear();
        netClient.shutdown();
    }
}
