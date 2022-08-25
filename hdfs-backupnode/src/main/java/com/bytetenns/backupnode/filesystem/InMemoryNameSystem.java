package com.bytetenns.backupnode.filesystem;

import com.bytetenns.backupnode.config.BackupNodeConfig;
import com.bytetenns.dfs.model.namenode.Metadata;
import com.bytetenns.namenode.fs.AbstractFsNameSystem;
import com.bytetenns.namenode.fs.FsImage;
import lombok.extern.slf4j.Slf4j;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Author jiaoyuliang
 * @Description 基于内存的文件系统
 * @Date 2022/8/17
 */
@Slf4j
public class InMemoryNameSystem extends AbstractFsNameSystem {

    // 引入BN配置文件
    private BackupNodeConfig backupNodeConfig;

    // 设置TxId
    private volatile long maxTxId = 0L;

    // 多线程 原子性
    private AtomicBoolean recovering = new AtomicBoolean(false);

    // 有参构造
    public InMemoryNameSystem(BackupNodeConfig backupNodeConfig) {
        this.backupNodeConfig = backupNodeConfig;
    }

    /**
     * 基于本地文件恢复元数据空间
     * @throws Exception IO异常
     */
    public void recoveryNamespace() throws Exception {
        try {
            if (recovering.compareAndSet(false, true)) {
                //找到最新的fsImage
                FsImage fsImage = scanLatestValidFsImage(backupNodeConfig.getBaseDir());

                // 找到最新 fsImage
                if (fsImage != null) {
                    // 获取8位maxId
                    setMaxTxId(fsImage.getMaxTxId());

                    //加载FsImage文件到内存里来进行恢复
                    applyFsImage(fsImage);
                }

                // 假如没有fsImage暂时不做操作
                recovering.compareAndSet(true, false);
            }
        } catch (Exception e) {
            log.info("BackupNode恢复命名空间异常：", e);
            throw e;
        }
    }

    /**
     * 设置当前最大的TxId
     * @param maxTxId TxId
     */
    public void setMaxTxId(Long maxTxId) {
        this.maxTxId = maxTxId;
    }

    /**
     * 获取FSImage
     * @return FsImage
     */
    public FsImage getFsImage() {
        FsImage fsImage = directory.getFsImage();
        fsImage.setMaxTxId(maxTxId);
        return fsImage;
    }

    public long getMaxTxId() {
        return maxTxId;
    }

    /**
     * 恢复过程是否完成
     */
    public boolean isRecovering() {
        return recovering.get();
    }

    @Override
    public Set<Metadata> getFilesBySlot(int slot) {
        return null;
    }

}
