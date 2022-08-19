package com.bytetenns.namenode.fs;

import com.bytetenns.common.FileInfo;
import com.bytetenns.common.enums.NodeType;
import com.bytetenns.common.netty.Constants;
import com.bytetenns.namenode.datanode.DataNodeManager;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

/**
 * 默认的垃圾清除机制
 *
 * @author Sun Dasheng
 */
@Slf4j
public class TrashPolicyDefault implements Runnable {

    private DataNodeManager dataNodeManager;
    private DiskNameSystem diskNameSystem;
    private long clearStorageThreshold;

    public TrashPolicyDefault(DiskNameSystem diskNameSystem, DataNodeManager dataNodeManager) {
        this.diskNameSystem = diskNameSystem;
        this.clearStorageThreshold = diskNameSystem.getNameNodeConfig().getClearStorageThreshold();
        this.dataNodeManager = dataNodeManager;
    }

    @Override
    public void run() {
        if (log.isDebugEnabled()) {
            log.info("定时扫描垃圾箱线程启动.");
        }
        long currentTime = System.currentTimeMillis();
        Node node = diskNameSystem.listFiles("/");
        TreeMap<String, Node> children = node.getChildren();
        for (String user : children.keySet()) {
            Node userTrashNode = node.getChildren().get(user).getChildren().get(Constants.TRASH_DIR);
            if (userTrashNode != null) {
                List<String> toRemoveFilename = new LinkedList<>();
                scan(File.separator + user, userTrashNode, currentTime, toRemoveFilename);
                for (String filename : toRemoveFilename) {
                    // 下发任务给DataNode删除文件
                    String dataNodeFilename = filename.replaceAll(File.separator + Constants.TRASH_DIR, "");
                    FileInfo fileInfo = dataNodeManager.removeFileStorage(dataNodeFilename, true);
                    if (fileInfo == null) {
                        log.error("找不到文件的DataNode信息，等待下一次定时任务扫描再删除文件：[filename={}]", dataNodeFilename);
                        continue;
                    }
                    log.debug("删除内存目录树：[filename={}]", filename);
                    diskNameSystem.deleteFile(filename);
                    String username = fileInfo.getFileName().split("/")[1];
                    //userManager.removeStorageInfo(username, fileInfo.getFileSize());
                }
            }
        }
    }

    private void scan(String path, Node node, long currentTime, List<String> toRemoveFilename) {
        String basePath = path + File.separator + node.getPath();
        if (node.getChildren().isEmpty()) {
            String deleteTime = node.getAttr().get(Constants.ATTR_FILE_DEL_TIME);
            if (deleteTime == null) {
                return;
            }
            long delTime = Long.parseLong(deleteTime);
            boolean isFile = NodeType.FILE.getValue() == node.getType();
            if (currentTime - clearStorageThreshold > delTime && isFile) {
                toRemoveFilename.add(basePath);
            }
        } else {
            for (String key : node.getChildren().keySet()) {
                Node children = node.getChildren().get(key);
                scan(basePath, children, currentTime, toRemoveFilename);
            }
        }
    }
}
