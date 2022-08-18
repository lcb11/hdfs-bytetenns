package com.bytetenns.backupnode.filesystem;

import com.bytetenns.backupnode.fs.FsDirectory;
import com.bytetenns.backupnode.fs.Node;
import com.bytetenns.backupnode.fsimage.FsImage;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author jiaoyuliang
 * @Description 文件系统
 * @Date 2022/8/18
 */
@Slf4j
public abstract class FsNameSystem {

    //负责管理内存文件目录树的组件
    protected FsDirectory directory;

    /**
     * 扫描最新的FSImage文件
     * @return 最新并合法的FSImage
     */
    protected FsImage scanLatestValidFsImage(String baseDir) throws IOException {
        //扫描本地文件，把所有FsImage文件扫描出来
        Map<Long, String> timeFsImageMap = scanFsImageMap(baseDir);
        //对文件进行排序
        List<Long> sortedList = new ArrayList<>(timeFsImageMap.keySet());
        sortedList.sort((o1, o2) -> o1.equals(o2) ? 0 : (int) (o2 - o1));
        //找到最新的fsImage并返回
        for (Long time : sortedList) {
            String path = timeFsImageMap.get(time);
            try (RandomAccessFile raf = new RandomAccessFile(path, "r"); FileInputStream fis =
                    new FileInputStream(raf.getFD()); FileChannel channel = fis.getChannel()) {
                FsImage fsImage = FsImage.parse(channel, path, (int) raf.length());
                if (fsImage != null) {
                    return fsImage;
                }
            }
        }
        return null;
    }

    /**
     * 扫描本地文件，把所有FsImage文件扫描出来
     * @param path 文件路径
     * @return FSImage Map
     */
    public Map<Long, String> scanFsImageMap(String path) {
        //将文件日期和名称存储为Map
        Map<Long, String> timeFsImageMap = new HashMap<>(8);
        File dir = new File(path);
        if (!dir.exists()) {
            return timeFsImageMap;
        }
        //获取所有文件
        File[] files = dir.listFiles();
        if (files == null || files.length == 0) {
            return timeFsImageMap;
        }
        //对文件名称进行解析 获取日期和名称
        for (File file : files) {
            if (file.isDirectory()) {
                continue;
            }
            if (!file.getName().contains("fsimage")) {
                continue;
            }
            String str = file.getName().split("-")[1];
            long time = Long.parseLong(str);
            timeFsImageMap.put(time, file.getAbsolutePath());
        }
        return timeFsImageMap;
    }

    /**
     * 加载FsImage文件到内存里来进行恢复
     */
    protected void applyFsImage(FsImage fsImage) {
        StopWatch stopWatch = new StopWatch();

        // 加锁
        stopWatch.start();
        log.info("Staring apply FsImage file ...");

        // 根据FSImage初始化内存目录树
        directory.applyFsImage(fsImage);

        //关闭锁
        stopWatch.stop();
        log.info("Apply FsImage File cost {} ms", stopWatch.getTime());
    }

    public void mkdir(String path, Map<String, String> attr) {
        this.directory.mkdir(path, attr);
    }

    public boolean createFile(String filename, Map<String, String> attr) {
        return this.directory.createFile(filename, attr);
    }

    public boolean deleteFile(String filename) {
        Node node = this.directory.delete(filename);
        return node != null;
    }

}
