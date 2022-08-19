package com.bytetenns.namenode.fs;

import com.bytetenns.common.enums.FsOpType;
import com.bytetenns.dfs.model.backup.EditLog;
import com.bytetenns.namenode.NameNodeConfig;
import com.bytetenns.namenode.datanode.DataNodeManager;
import com.bytetenns.namenode.editlog.EditLogWrapper;
import com.bytetenns.namenode.editlog.FsEditLog;
import com.bytetenns.common.scheduler.DefaultScheduler;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
   * @Author lcb
   * @Description 负责管理文件系统元数据，落地磁盘实现
   * @Date 2022/8/12
   * @Param
   * @return
   **/
@Slf4j
public class DiskNameSystem  extends AbstractFsNameSystem{
    private NameNodeConfig nameNodeConfig;
    private FsEditLog editLog;

    public DiskNameSystem(NameNodeConfig nameNodeConfig, DefaultScheduler defaultScheduler,
                          DataNodeManager dataNodeManager) {
        super();
        this.nameNodeConfig = nameNodeConfig;
        this.editLog = new FsEditLog(nameNodeConfig);
        dataNodeManager.setDiskNameSystem(this);
        TrashPolicyDefault trashPolicyDefault = new TrashPolicyDefault(this, dataNodeManager);
        defaultScheduler.schedule("定时扫描物理删除文件", trashPolicyDefault,
                nameNodeConfig.getNameNodeTrashCheckInterval(),
                nameNodeConfig.getNameNodeTrashCheckInterval(), TimeUnit.MILLISECONDS);
    }

    public NameNodeConfig getNameNodeConfig() {
        return nameNodeConfig;
    }

    @Override
    public void recoveryNamespace() throws Exception {
        try {
            //扫描最新的FSImage文件
            FsImage fsImage = scanLatestValidFsImage(nameNodeConfig.getBaseDir());
            long txId = 0L;
            if (fsImage != null) {
                //存在fsImage文件，获取
                txId = fsImage.getMaxTxId();
                applyFsImage(fsImage);
            }
            // 回放editLog文件，将editlog文件加载到内存，txId比当前fsImage更大的
            this.editLog.playbackEditLog(txId, obj -> {
                //开始回访符合条件的txid
                EditLog editLog = obj.getEditLog();
                //获取editLog的操作类型
                int opType = editLog.getOpType();
                if (opType == FsOpType.MKDIR.getValue()) {//MKDIR：创建文件夹
                    // 这里要调用super.mkdir 回放的editLog不需要再刷磁盘
                    super.mkdir(editLog.getPath(), editLog.getAttrMap());
                } else if (opType == FsOpType.CREATE.getValue()) {//CREATE：创建文件
                    //创建文件，但传入的文件名是editLog.getPath()：文件路径，最后一个元素代表文件名字
                    super.createFile(editLog.getPath(), editLog.getAttrMap());
                } else if (opType == FsOpType.DELETE.getValue()) {
                    super.deleteFile(editLog.getPath());//DELETE：删除文件
                }
            });
        } catch (Exception e) {
            log.info("NameNode恢复命名空间异常：", e);
            throw e;
        }
    }

    @Override
    public Node listFiles(String filename) {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        Node node = super.listFiles(filename);
        stopWatch.stop();
        Prometheus.gauge("namenode_fs_memory_cost", "FSDirectory操作耗时", "op", "listFiles", stopWatch.getTime());
        return node;
    }

    /**
     * 创建目录
     *
     * @param path 目录路径
     */
    @Override
    public void mkdir(String path, Map<String, String> attr) {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        super.mkdir(path, attr);
        this.editLog.logEdit(new EditLogWrapper(FsOpType.MKDIR.getValue(), path, attr));
        log.info("创建文件夹：{}", path);
        stopWatch.stop();
        Prometheus.gauge("namenode_fs_memory_cost", "FSDirectory操作耗时", "op", "mkdir", stopWatch.getTime());
    }

    /**
     * 创建文件
     *
     * @param filename 文件路径
     */
    @Override
    public boolean createFile(String filename, Map<String, String> attr) {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        if (!super.createFile(filename, attr)) {
            return false;
        }
        this.editLog.logEdit(new EditLogWrapper(FsOpType.CREATE.getValue(), filename, attr));
        Prometheus.gauge("namenode_fs_memory_cost", "FSDirectory操作耗时", "op", "createFile", stopWatch.getTime());
        return true;
    }

    @Override
    public boolean deleteFile(String filename) {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        if (!super.deleteFile(filename)) {
            return false;
        }
        this.editLog.logEdit(new EditLogWrapper(FsOpType.DELETE.getValue(), filename));
        log.info("删除文件：{}", filename);
        Prometheus.gauge("namenode_fs_memory_cost", "FSDirectory操作耗时", "op", "deleteFile", stopWatch.getTime());
        return true;
    }

    /**
     * 优雅停机
     * 强制把内存里的edits log刷入磁盘中
     */
    public void shutdown() {
        log.info("Shutdown DiskNameSystem.");
        this.editLog.flush();
    }

    /**
     * 获取EditLog
     *
     * @return editLog
     */
    public FsEditLog getEditLog() {
        return editLog;
    }

}
