package com.bytetenns.namenode.datanode;

import com.bytetenns.common.FileInfo;
import com.bytetenns.common.exception.NameNodeException;
import com.bytetenns.common.netty.Constants;
import com.bytetenns.common.utils.PrettyCodes;
import com.bytetenns.dfs.model.datanode.RegisterRequest;
import com.bytetenns.namenode.NameNodeConfig;
import com.bytetenns.namenode.fs.DiskNameSystem;
import com.bytetenns.common.scheduler.DefaultScheduler;
import com.bytetenns.common.utils.DateUtils;
import com.bytetenns.namenode.fs.Node;
import com.bytetenns.namenode.rebalance.RemoveReplicaTask;
import com.bytetenns.namenode.rebalance.ReplicaTask;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
  * @Author lcb
  * @Description 负责管理dataNode
  * @Date 2022/8/19
  * @Param
  * @return
  **/
@Slf4j
public class DataNodeManager {

    //private final UserManager userManager;
    //采用hashmap保存每个datanodeInfo的信息
    private final Map<String, DataNodeInfo> dataNodes = new ConcurrentHashMap<>();
    //副本任务的读写锁
    private final ReentrantReadWriteLock replicaLock = new ReentrantReadWriteLock();
    /**
     * <pre>
     * 每个文件对应存储的Datanode信息
     *
     * 比如文件aaa.png，存储在datanode01、datanode02
     *
     *    aaa.png : [
     *        datanode01,
     *        datanode02
     *    ]
     * </pre>
     */
    private final Map<String, List<DataNodeInfo>> replicaByFilename = new ConcurrentHashMap<>();

    /**
     * <pre>
     * 每个DataNode 存储的文件列表
     *
     * 比如datanode01存储有文件：aaa.jpg、bbb.jpg
     *
     *    datanode01 : [
     *        aaa.jpg,
     *        bbb.jpg
     *    ]
     * </pre>
     */
    private final Map<String, Map<String, FileInfo>> filesByDataNode = new ConcurrentHashMap<>();
    private final NameNodeConfig nameNodeConfig;
    private DiskNameSystem diskNameSystem;

    public DataNodeManager(NameNodeConfig nameNodeConfig, DefaultScheduler defaultScheduler) {
        this.nameNodeConfig = nameNodeConfig;
        long dataNodeAliveThreshold = nameNodeConfig.getDataNodeAliveCheckInterval();
        //DataNodeAliveMonitor()监控datanode是否存活线程
        defaultScheduler.schedule("DataNode存活检测", new DataNodeAliveMonitor(),
                dataNodeAliveThreshold, dataNodeAliveThreshold, TimeUnit.MILLISECONDS);
    }


    public void setDiskNameSystem(DiskNameSystem diskNameSystem) {
        this.diskNameSystem = diskNameSystem;
    }

    /**
     * dataNode进行注册
     *
     * @param request 请求信息
     */
    public boolean register(RegisterRequest request) {
        if (dataNodes.containsKey(request.getHostname())) {
            return false;
        }
        DataNodeInfo dataNode = new DataNodeInfo(request.getHostname(), request.getNioPort(), request.getHttpPort(),
                System.currentTimeMillis() + nameNodeConfig.getDataNodeHeartbeatTimeout());
        dataNode.setStoredDataSize(request.getStoredDataSize());
        dataNode.setFreeSpace(request.getFreeSpace());
        dataNode.setNodeId(request.getNodeId());
        log.info("收到DataNode注册请求：[hostname={}, storageSize={}, freeSpace={}]",
                request.getHostname(), request.getStoredDataSize(), request.getFreeSpace());
        dataNodes.put(request.getHostname(), dataNode);
        return true;
    }

    /**
     * dataNode进行心跳
     *
     * @param hostname 主机名
     */
    public Boolean heartbeat(String hostname) {
        DataNodeInfo dataNode = dataNodes.get(hostname);
        if (dataNode == null) {
            return false;
        }
        long latestHeartbeatTime = System.currentTimeMillis() + nameNodeConfig.getDataNodeHeartbeatTimeout();
        if (log.isDebugEnabled()) {
            log.debug("收到DataNode的心跳：[hostname={}, latestHeartbeatTime={}]", hostname, DateUtils.format(new Date(latestHeartbeatTime)));
        }
        dataNode.setLatestHeartbeatTime(latestHeartbeatTime);
        return true;
    }

    /**
     * 获取已经Ready并且排序的DataNode列表
     *
     * @return DataNode列表
     */
    private List<DataNodeInfo> getSortedReadyDataNode() {
        return dataNodes.values().stream()
                .filter(dataNodeInfo -> dataNodeInfo.getStatus() == DataNodeInfo.STATUS_READY)
                .sorted()
                .collect(Collectors.toList());
    }

    /**
     * 从DataNode集合中选择节点，但是要排除已经包含该文件的DataNode节点
     *
     * @param dataNodeList      DataNode节点列表
     * @param requiredNodeCount 需要的DataNode节点数量
     * @param filename          文件名
     */
    private List<DataNodeInfo> selectDataNodeFromList(List<DataNodeInfo> dataNodeList, int requiredNodeCount,
                                                      String filename) throws NameNodeException {
        //已经存在datanode的数量
        int existCount = 0;
        //最小存储节点的大小
        long minStoredDataSize = -1;
        /*
         * 在上传小文件的时候，文件大小较小，为了避免流量都打到同一台DataNode机器。
         *
         * 所以如果多台DataNode之间的存储空间大小误差在1G范围内，直接随机取一台。
         *
         * 如果误差范围小于1G,则按存储空间大小从低到高进行获取
         *
         */
        //能存当前副本任务的datanode
        List<DataNodeInfo> candidateNodes = new ArrayList<>(10);
        //1G的大小
        long delta = 1024 * 1024 * 1024;
        for (DataNodeInfo dataNodeInfo : dataNodeList) {
            //如果当前datanode存在filename，existCount加一，跳过当前datanode
            if (dataNodeContainsFiles(dataNodeInfo.getHostname(), filename)) {
                existCount++;
                continue;
            }
            //获取当前datanode已经存了多大的空间
            long storedDataSize = dataNodeInfo.getStoredDataSize();
            if (minStoredDataSize < 0) {
                minStoredDataSize = storedDataSize;
            }
            //这一步主要保证各个datanode之间已经存储空间的误差在1G以内
            if (dataNodeInfo.getStoredDataSize() - minStoredDataSize <= delta) {
                //将该datanode加入到候选集合中
                candidateNodes.add(dataNodeInfo);
            }
        }
        int findCount = 0;
        if (candidateNodes.size() == requiredNodeCount) {
            // 误差在1G以内的DataNode数量刚好和需要的节点数量一样
            return candidateNodes;
        } else if (candidateNodes.size() < requiredNodeCount) {
            // 误差在1G以内的DataNode数量小于需要的节点数量，则需要从datanode列表中继续取到足够的节点。
            //还需要多少个datanode节点
            int remainNodeCount = requiredNodeCount - candidateNodes.size();
            for (DataNodeInfo dataNodeInfo : dataNodeList) {
                //如果候选集合中存在当前dataNodeInfo，或者该datanode存有当前文件则跳过
                if (candidateNodes.contains(dataNodeInfo) ||
                        dataNodeContainsFiles(dataNodeInfo.getHostname(), filename)) {
                    continue;
                }
                //将该datanode加入到候选集合中
                candidateNodes.add(dataNodeInfo);
                //需要datanode的数量减一
                remainNodeCount--;
                //找到的datanode数量加一
                findCount++;
                if (remainNodeCount <= 0) {
                    return candidateNodes;
                }
            }
        } else {
            // 误差在1G以内的DataNode数量很多，超过所需的节点数量，则随机取几个
            Random random = new Random();
            List<DataNodeInfo> selectedDataNodes = new ArrayList<>();
            for (int i = 0; i < requiredNodeCount; i++) {
                int index = random.nextInt(candidateNodes.size());
                DataNodeInfo dataNodeInfo = candidateNodes.get(index);
                if (selectedDataNodes.contains(dataNodeInfo) ||
                        dataNodeContainsFiles(dataNodeInfo.getHostname(), filename)) {
                    continue;
                }
                selectedDataNodes.add(dataNodeInfo);
            }
            return selectedDataNodes;

        }
        log.error("DataNode数量不足：[datanodeList={}]", dataNodes.values());
        throw new NameNodeException("DataNode数量不足: [applyCount=" + requiredNodeCount +
                ", findCount=" + findCount +
                ", existsFileNodeCount=" + existCount +
                ", filename=" + filename);
    }

    /**
     * 设置DataNode状态为Ready
     *
     * @param hostname DataNode主机名
     */
    public void setDataNodeReady(String hostname) {
        DataNodeInfo dataNode = dataNodes.get(hostname);
        if (dataNode != null) {
            dataNode.setStatus(DataNodeInfo.STATUS_READY);
        }
    }

    /**
     * 创建丢失副本的复制任务
     *
     * @param dataNodeInfo 宕机的DataNode
     */
    private void createLostReplicaTask(DataNodeInfo dataNodeInfo) {
        // 获取需要复制的副本列表
        Map<String, FileInfo> filesByDataNode = removeFileByDataNode(dataNodeInfo.getHostname());
        if (filesByDataNode == null) {
            return;
        }
        for (FileInfo fileInfo : filesByDataNode.values()) {
            // 找到一个可读取文件的DataNode，即存有当前文件的datanode
            DataNodeInfo sourceDataNode = chooseReadableDataNodeByFileName(fileInfo.getFileName(), dataNodeInfo);
            if (sourceDataNode == null) {
                log.warn("警告：找不到适合的DataNode用来获取文件：" + fileInfo.getFileName());
                continue;
            }
            //为复制任务申请副本，申请的dataNode需要排除目标DataNode，sourceDataNode为需要排除的datanode
            DataNodeInfo destDataNode = allocateReplicateDataNodes(fileInfo, sourceDataNode);
            if (destDataNode == null) {
                log.warn("警告：找不到适合的DataNode用来Rebalance");
                continue;
            }
            //将该datanode和文件加入到副本复制任务
            ReplicaTask task = new ReplicaTask(fileInfo.getFileName(), sourceDataNode.getHostname(), sourceDataNode.getNioPort());
            log.info("创建副本复制任务：[filename={}, from={}, to={}]", fileInfo.getFileName(),
                    sourceDataNode.getHostname(), destDataNode.getHostname());
            destDataNode.addReplicaTask(task);
        }
    }

    /**
     * 为复制任务申请副本，申请的dataNode需要排除目标DataNode的
     *
     * @param fileInfo        文件信息
     * @param excludeDataNode 排除的DataNode
     */
    private DataNodeInfo allocateReplicateDataNodes(FileInfo fileInfo, DataNodeInfo excludeDataNode) {
        //获取可以写入的datanode
        List<DataNodeInfo> dataNodeInfos = dataNodes.values().stream()
                .filter(dataNodeInfo -> !dataNodeInfo.equals(excludeDataNode) &&
                        dataNodeInfo.getStatus() == DataNodeInfo.STATUS_READY)
                .sorted()
                .collect(Collectors.toList());
        try {
            //从DataNode集合中选择节点，但是要排除已经包含该文件的DataNode节点，选择一个datanode进行副本复制
            List<DataNodeInfo> dataNodesList = selectDataNodeFromList(dataNodeInfos,
                    1, fileInfo.getFileName());
            return dataNodesList.get(0);
        } catch (Exception e) {
            log.warn("allocateReplicateDataNodes select node failed.", e);
            return null;
        }
    }

    /**
     * @param username 该文件的所属的用户
     * @param count    申请机器数量
     *                 为文件分配dataNode机器列表
     */
   /* public List<DataNodeInfo> allocateDataNodes(String username, int count, String filename) throws Exception {
        //User user = userManager.getUser(username);
        Set<String> dataNodeSet = user.getStorageInfo().getDataNodesSet();
        if (dataNodeSet.isEmpty()) {
            List<DataNodeInfo> sortedReadyDataNode = getSortedReadyDataNode();
            return selectDataNodeFromList(sortedReadyDataNode, count, filename);
        } else {
            // 这里用户是指定了DataNode的，则在指定的DataNode中查找对应的DataNode
            List<DataNodeInfo> dataNodeInfos = dataNodes.values().stream()
                    .filter(dataNodeInfo -> dataNodeInfo.getStatus() == DataNodeInfo.STATUS_READY
                            && dataNodeSet.contains(dataNodeInfo.getHostname()))
                    .sorted()
                    .collect(Collectors.toList());
            return selectDataNodeFromList(dataNodeInfos, count, filename);
        }
    }*/

    /**
     * 从内存数据结构中移除DataNode的文件列表并返回
     *
     * @param hostname DataNode名称
     * @return 该DataNode的文件列表
     */
    public Map<String, FileInfo> removeFileByDataNode(String hostname) {
        replicaLock.writeLock().lock();
        try {
            //filesByDataNode每个DataNode 存储的文件列表，将改datanode中保存的文件从datanodemanager中移除
            return filesByDataNode.remove(hostname);
        } finally {
            replicaLock.writeLock().unlock();
        }
    }


    public boolean dataNodeContainsFiles(String hostname, String filename) {
        //获取副本读锁
        replicaLock.readLock().lock();
        try {
            //获取当前名为hostname的datanode的所有文件
            Map<String, FileInfo> files = filesByDataNode.getOrDefault(hostname, new HashMap<>(PrettyCodes.trimMapSize()));
            return files.containsKey(filename);
        } finally {
            //释放锁
            replicaLock.readLock().unlock();
        }
    }

    /**
     * 增加一个副本
     *
     * @param fileInfo 文件信息
     */
    public void addReplica(FileInfo fileInfo) {
        replicaLock.writeLock().lock();
        try {
            // 获取该文件所属的DataNode
            DataNodeInfo dataNode = dataNodes.get(fileInfo.getHostname());

            // 获取该文件对应的DataNode列表
            List<DataNodeInfo> dataNodeInfos = replicaByFilename.computeIfAbsent(fileInfo.getFileName(),
                    k -> new ArrayList<>());

            // 文件目录树有可能被移动到.Trash目录下面了，所以除了正常查找一遍文件目录树之外，还要查一遍垃圾箱。
            Node node = maybeInTrash(fileInfo.getFileName());
            if (node == null) {
                log.warn("收到DataNode上报的存储信息，但是在内存目录树中不存在文件,下发命令让DataNode删除文件: [hostname={}, filename={}]",
                        fileInfo.getHostname(), fileInfo.getFileName());
                RemoveReplicaTask task = new RemoveReplicaTask(fileInfo.getHostname(), fileInfo.getFileName());
                dataNode.addRemoveReplicaTask(task);
                return;
            }
            int replicaNum = Integer.parseInt(node.getAttr().getOrDefault(Constants.ATTR_REPLICA_NUM,
                    String.valueOf(nameNodeConfig.getReplicaNum())));
            // 如果该文件的副本数量超过配置的数量，则让该DataNode删除文件
            if (dataNodeInfos.size() >= replicaNum) {
                RemoveReplicaTask task = new RemoveReplicaTask(dataNode.getHostname(), fileInfo.getFileName());
                log.info("下发副本删除任务：[hostname={}, filename={}]", dataNode.getHostname(), fileInfo.getFileName());
                dataNode.addRemoveReplicaTask(task);
                return;
            }

            // 副本数量没有超过，将文件信息维护起来
            dataNodeInfos.add(dataNode);
            Map<String, FileInfo> files = filesByDataNode.computeIfAbsent(fileInfo.getHostname(), k -> new HashMap<>(PrettyCodes.trimMapSize()));
            files.put(fileInfo.getFileName(), fileInfo);
            if (log.isDebugEnabled()) {
                log.debug("收到DataNode文件上报：[hostname={}, filename={}]", fileInfo.getHostname(), fileInfo.getFileName());
            }
        } finally {
            replicaLock.writeLock().unlock();
        }
    }

    private Node maybeInTrash(String fileName) {
        Node node = diskNameSystem.listFiles(fileName);
        if (node != null) {
            return node;
        }
        String[] split = fileName.split("/");
        String[] newSplit = new String[split.length + 1];
        newSplit[0] = split[0];
        newSplit[1] = split[1];
        newSplit[2] = Constants.TRASH_DIR;
        System.arraycopy(split, 2, newSplit, 3, split.length - 2);
        String trashPath = String.join("/", newSplit);
        return diskNameSystem.listFiles(trashPath);
    }

    /**
     * 根据文件名选择一个可读的DataNode，并把不可读的DataNode从文件对应的DataNode数据结构中删除
     *
     * @param filename 文件名
     * @return 可读的DataNode
     */
    public DataNodeInfo chooseReadableDataNodeByFileName(String filename) {
        return chooseReadableDataNodeByFileName(filename, null);
    }

    /**
     * 根据文件名选择一个可读的DataNode，并把不可读的DataNode从文件对应的DataNode数据结构中删除
     *
     * @param filename         文件名
     * @param toRemoveDataNode 不可读的DataNode
     * @return 可读的DataNode
     */
    public DataNodeInfo chooseReadableDataNodeByFileName(String filename, DataNodeInfo toRemoveDataNode) {
        //保证当前datanode只有一个副本任务在读写
        replicaLock.readLock().lock();
        try {
            //通过文件名获取当前文件所存储在的datanode
            List<DataNodeInfo> dataNodeInfos = replicaByFilename.get(filename);
            if (dataNodeInfos == null) {
                return null;
            }
            if (toRemoveDataNode != null) {
                dataNodeInfos.remove(toRemoveDataNode);
            }
            if (dataNodeInfos.isEmpty()) {
                return null;
            }
            //获取存储了当前文件的所有datanode，即有几个datanode存储了当前文件
            int size = dataNodeInfos.size();
            Random random = new Random();
            int i = random.nextInt(size);
            //在这几个datanode中随机返回一个datanode
            return dataNodeInfos.get(i);
        } finally {
            //释放锁
            replicaLock.readLock().unlock();
        }
    }

    /**
     * 根据文件名获取DataNode列表
     *
     * @param filename 文件名
     * @return DataNode信息
     */
    public List<DataNodeInfo> getDataNodeByFileName(String filename) {
        replicaLock.readLock().lock();
        try {
            return replicaByFilename.getOrDefault(filename, new ArrayList<>(PrettyCodes.trimListSize()));
        } finally {
            replicaLock.readLock().unlock();
        }
    }


    public DataNodeInfo getDataNode(String hostname) {
        return dataNodes.get(hostname);
    }

    public List<DataNodeInfo> getDataNodeInfoList() {
        return new ArrayList<>(dataNodes.values());
    }


    /**
     * 删除对应文件存储信息
     *
     * @param filename 文件名称
     * @return 被删除的文件信息
     */
    public FileInfo removeFileStorage(String filename, boolean delReplica) {
        replicaLock.writeLock().lock();
        try {
            List<DataNodeInfo> fileDataNodes = replicaByFilename.remove(filename);
            if (fileDataNodes == null) {
                return null;
            }
            FileInfo ret = null;
            for (Map<String, FileInfo> dataNodeFileInfo : filesByDataNode.values()) {
                FileInfo fileInfo = dataNodeFileInfo.remove(filename);
                if (fileInfo != null) {
                    ret = fileInfo;
                }
            }

            if (delReplica) {
                for (DataNodeInfo dataNodeInfo : fileDataNodes) {
                    DataNodeInfo dataNode = dataNodes.get(dataNodeInfo.getHostname());
                    RemoveReplicaTask task = new RemoveReplicaTask(dataNode.getHostname(), filename);
                    log.info("下发副本删除任务：[hostname={}, filename={}]", dataNode.getHostname(), filename);
                    dataNode.addRemoveReplicaTask(task);
                }
            }
            return ret;
        } finally {
            replicaLock.writeLock().unlock();
        }
    }

    public FileInfo getFileStorage(String filename) {
        replicaLock.readLock().lock();
        try {
            for (Map<String, FileInfo> map : filesByDataNode.values()) {
                FileInfo fileInfo = map.get(filename);
                if (fileInfo != null) {
                    return fileInfo;
                }
            }
            return null;
        } finally {
            replicaLock.readLock().unlock();
        }
    }

    /**
     * 等待知道DataNode上报文件
     *
     * @param filename 文件名
     */
    public void waitFileReceive(String filename, long timeout) throws NameNodeException, InterruptedException {
        long remainTimeout = timeout;
        synchronized (this) {
            while (chooseReadableDataNodeByFileName(filename) == null) {
                if (remainTimeout < 0) {
                    throw new NameNodeException("等待文件上传确认超时：" + filename);
                }
                wait(10);
                remainTimeout -= 10;
            }
        }
    }

    /**
     * 为某个文件增加副本数量
     *
     * @param username 用户名
     * @param addNum   增加副本数量
     * @param filename 文件名
     */
    /*public void addReplicaNum(String username, int addNum, String filename) throws Exception {
        List<DataNodeInfo> dataNodeInfos = allocateDataNodes(username, addNum, filename);
        DataNodeInfo sourceDataNode = chooseReadableDataNodeByFileName(filename);
        Node node = diskNameSystem.unsafeListFiles(filename);
        int replicaNum = Integer.parseInt(node.getAttr().getOrDefault(Constants.ATTR_REPLICA_NUM,
                String.valueOf(nameNodeConfig.getReplicaNum())));
        node.getAttr().put(Constants.ATTR_REPLICA_NUM, String.valueOf(replicaNum + addNum));
        for (DataNodeInfo destDataNode : dataNodeInfos) {
            ReplicaTask task = new ReplicaTask(filename, sourceDataNode.getHostname(), sourceDataNode.getNioPort());
            log.info("创建副本复制任务：[filename={}, from={}, to={}]", filename,
                    sourceDataNode.getHostname(), destDataNode.getHostname());
            destDataNode.addReplicaTask(task);
        }
    }*/

    /**
     * dataNode是否存活的监控线程
     *
     * <pre>
     *     这里存在一种情况，假设一个DataNode宕机了，从DataNode集合中摘除
     *
     * </pre>
     */
    private class DataNodeAliveMonitor implements Runnable {
        @Override
        public void run() {
            Iterator<DataNodeInfo> iterator = dataNodes.values().iterator();
            List<DataNodeInfo> toRemoveDataNode = new ArrayList<>();
            //遍历所有的dataNodeInfo
            while (iterator.hasNext()) {
                DataNodeInfo next = iterator.next();
                long currentTimeMillis = System.currentTimeMillis();
                if (currentTimeMillis < next.getLatestHeartbeatTime()) {
                    continue;
                }
                log.info("DataNode存活检测超时，被移除：[hostname={}, current={}, nodeLatestHeartbeatTime={}]",

                        next, DateUtils.format(new Date(currentTimeMillis)), DateUtils.format(new Date(next.getLatestHeartbeatTime())));
                //datanode丢失，将其从集合中移除
                iterator.remove();
                //将丢失的datanode加入到移除datanode的集合中
                toRemoveDataNode.add(next);
            }
            for (DataNodeInfo info : toRemoveDataNode) {
                //如果datanode宕机了，将保存在datanode里面的文件信息进行复制
                createLostReplicaTask(info);
            }
        }
    }
}
