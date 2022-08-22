package com.bytetenns.backupnode.fsimage;

import com.bytetenns.backupnode.filesystem.InMemoryNameSystem;
import com.bytetenns.common.utils.FileUtil;
import com.bytetenns.namenode.editlog.FsEditLog;
import com.bytetenns.namenode.fs.AbstractFsNameSystem;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import java.io.FileInputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 清理FsImage的任务
 *
 * @author Sun Dasheng
 */
@Slf4j
public class FsImageClearTask implements Runnable {

    private String baseDir;
    private AbstractFsNameSystem nameSystem;
    private FsEditLog fsEditLog;

    public FsImageClearTask(InMemoryNameSystem nameSystem, String baseDir) {
        this(nameSystem, baseDir, null);
    }

    public FsImageClearTask(AbstractFsNameSystem nameSystem, String baseDir, FsEditLog fsEditLog) {
        this.nameSystem = nameSystem;
        this.baseDir = baseDir;
        this.fsEditLog = fsEditLog;
    }

    @SneakyThrows
    @Override
    public void run() {
        Map<Long, String> timeFsImageMap = nameSystem.scanFsImageMap(baseDir);
        List<Long> sortedList = new ArrayList<>(timeFsImageMap.keySet());
        sortedList.sort((o1, o2) -> o1.equals(o2) ? 0 : (int) (o2 - o1));
        boolean findValidFsImage = false;
        long maxTxId = -1;
        for (Long time : sortedList) {
            String path = timeFsImageMap.get(time);
            if (findValidFsImage) {
                FileUtil.delete(path);
                log.info("删除FSImage: [file={}]", path);
                continue;
            }
            try (RandomAccessFile raf = new RandomAccessFile(path, "r"); FileInputStream fis =
                    new FileInputStream(raf.getFD()); FileChannel channel = fis.getChannel()) {
                maxTxId = FsImage.validate(channel, path, (int) raf.length());
                if (maxTxId > 0) {
                    findValidFsImage = true;
                    log.info("清除FSImage任务，找到最新的合法的FsImage: [file={}]", path);
                } else {
                    FileUtil.delete(path);
                    log.info("删除FSImage: [file={}]", path);
                }
            }
        }

        // 如果是NameNode，则需要清除EditLog文件
        if (findValidFsImage && fsEditLog != null) {
            fsEditLog.cleanEditLogByTxId(maxTxId);
        }
    }
}
