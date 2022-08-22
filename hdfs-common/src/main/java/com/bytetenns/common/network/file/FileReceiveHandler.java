package com.bytetenns.common.network.file;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 文件接收处理器
 */
@Slf4j
public class FileReceiveHandler {

    private FileTransportCallback fileTransportCallback;
    private AtomicLong lastCheckpoint;
    private Map<String, FileAppender> fileAppenderMap = new ConcurrentHashMap<>();

    public FileReceiveHandler(FileTransportCallback fileTransportCallback) {
        this.fileTransportCallback = fileTransportCallback;
        this.lastCheckpoint = new AtomicLong(System.currentTimeMillis() + 30 * 60 * 1000);
    }

    /**
     * 处理文件传输
     */
    public void handleRequest(FilePacket filePacket) {
        FileAppender fileAppender = null;
        try {
            Map<String, String> fileMetaData = filePacket.getFileMetaData();
            FileAttribute fileAttribute = new FileAttribute(fileMetaData);
            String id = fileMetaData.get("id");
            if (FilePacket.HEAD == filePacket.getType()) {
                fileAppender = new FileAppender(fileAttribute, fileTransportCallback);
                fileAppenderMap.put(id, fileAppender);
            } else if (FilePacket.BODY == filePacket.getType()) {
                fileAppender = fileAppenderMap.get(id);
                byte[] fileData = filePacket.getBody();
                fileAppender.append(fileData);
            } else if (FilePacket.TAIL == filePacket.getType()) {
                fileAppender = fileAppenderMap.remove(id);
                fileAppender.completed();
            }
        } catch (Throwable e) {
            log.info("文件传输异常：", e);
        } finally {
            if (fileAppender != null && FilePacket.TAIL == filePacket.getType()) {
                fileAppender.release();
            }
            if (System.currentTimeMillis() > lastCheckpoint.getAndAdd(30 * 60 * 1000)) {
                checkFileReceiveTimeout();
            }
        }
    }

    /**
     * 定时检查，接收文件是否超时：是否有文件传输超过30分钟还没有传输完成。
     * <p>
     * 如果超时了，打印警告日志，并释放资源
     */
    public void checkFileReceiveTimeout() {
        ArrayList<FileAppender> fileReceivers = new ArrayList<>(fileAppenderMap.values());
        for (FileAppender fileReceiver : fileReceivers) {
            if (fileReceiver.isTimeout()) {
                fileReceiver.release();
                log.warn("FileReceiver is timeout: [filename={}, length={}, readLength={}]",
                        fileReceiver.getFileAttribute().getFilename(), fileReceiver.getFileAttribute().getSize(),
                        fileReceiver.getReadLength());
            }
        }
    }

}
