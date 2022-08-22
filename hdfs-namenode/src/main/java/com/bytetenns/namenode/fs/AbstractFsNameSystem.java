package com.bytetenns.namenode.fs;

import com.bytetenns.common.netty.Constants;
import com.bytetenns.dfs.model.namenode.Metadata;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.*;

/**
 *
   * @Author lcb
   * @Description 文件系统
   * @Date 2022/8/19
   * @Param
   * @return
   **/
@Slf4j
public abstract class AbstractFsNameSystem implements FsNameSystem {

    /**
     * 负责管理内存文件目录树的组件
     */
    protected FsDirectory directory;

    public AbstractFsNameSystem() {
        this.directory = new FsDirectory();
    }

    /**
     * 基于本地文件恢复元数据空间
     *
     * @throws Exception IO异常
     */
    protected abstract void recoveryNamespace() throws Exception;

    @Override
    public void mkdir(String path, Map<String, String> attr) {
        this.directory.mkdir(path, attr);
    }

    @Override
    public boolean createFile(String filename, Map<String, String> attr) {
        return this.directory.createFile(filename, attr);
    }

    @Override
    public boolean deleteFile(String filename) {
        Node node = this.directory.delete(filename);
        return node != null;
    }


    @Override
    public Set<Metadata> getFilesBySlot(int slot) {
        return directory.findAllFileBySlot(slot);
    }

    /**
     * 加载FsImage文件到内存里来进行恢复
     */
    protected void applyFsImage(FsImage fsImage) {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        log.info("Staring apply FsImage file ...");
        directory.applyFsImage(fsImage);
        stopWatch.stop();
        log.info("Apply FsImage File cost {} ms", stopWatch.getTime());
    }

    /**
     * 扫描最新的FSImage文件
     *
     * @return 最新并合法的FSImage
     */
    protected FsImage scanLatestValidFsImage(String baseDir) throws IOException {
        //扫描本地文件，把所有FsImage文件扫描出来，k-V->fsimage文件名中的数字：文件的绝对地址
        Map<Long, String> timeFsImageMap = scanFsImageMap(baseDir);
        //将Map中的key转换为List数组
        List<Long> sortedList = new ArrayList<>(timeFsImageMap.keySet());
        //根据list的key对文件进行排序
        sortedList.sort((o1, o2) -> o1.equals(o2) ? 0 : (int) (o2 - o1));
        for (Long time : sortedList) {
            //获取当前遍历fsimage文件路径
            String path = timeFsImageMap.get(time);
            try (RandomAccessFile raf = new RandomAccessFile(path, "r");
                 //getFD()：返回与此流关联的不透明文件描述符对象
                 FileInputStream fis = new FileInputStream(raf.getFD());
                 FileChannel channel = fis.getChannel()) {
                //parse()：解析FsImage文件
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
     *
     * @param path 文件路径
     * @return FSImage Map
     */
    public Map<Long, String> scanFsImageMap(String path) {

        Map<Long, String> timeFsImageMap = new HashMap<>(8);
        File dir = new File(path);
        if (!dir.exists()) {
            //不存在本地文件返回null HashMap
            return timeFsImageMap;
        }
        //返回所有的文件列表
        File[] files = dir.listFiles();
        if (files == null || files.length == 0) {
            return timeFsImageMap;
        }
        for (File file : files) {
            //isDirectory()测试此文件是否为目录
            if (file.isDirectory()) {
                continue;
            }
            //文件名不包含fsimage则跳过
            if (!file.getName().contains("fsimage")) {
                continue;
            }
            //获取fsImage文件后面的数字
            String str = file.getName().split("-")[1];
            long time = Long.parseLong(str);
            //getAbsolutePath()：Returns the absolute pathname string of this abstract pathname
            timeFsImageMap.put(time, file.getAbsolutePath());
        }
        return timeFsImageMap;
    }

    /**
     * 获取文件列表
     *
     * @param filename 文件路径
     * @return 文件列表
     */
    public Node listFiles(String filename) {
        return this.directory.listFiles(filename);
    }


    /**
     * 计算文件数量
     *
     * @param path 文件路径
     * @return 文件数量
     */
    public CalculateResult calculate(String path) {
        CalculateResult result = new CalculateResult();
        result.setFileCount(0);
        result.setTotalSize(0);
        Node node = unsafeListFiles(path);
        if (node == null) {
            return result;
        } else {
            internalCalculate(node, result);
        }
        return result;
    }

    private void internalCalculate(Node node, CalculateResult result) {
        if (node.isFile()) {
            result.addFileCount();
            String fileSizeStr = node.getAttr().getOrDefault(Constants.ATTR_FILE_SIZE, "0");
            long fileSize = Long.parseLong(fileSizeStr);
            result.addTotalSize(fileSize);
        } else {
            for (String key : node.getChildren().keySet()) {
                Node children = node.getChildren().get(key);
                internalCalculate(children, result);
            }
        }
    }


    /**
     * 获取文件列表
     *
     * @param filename 文件路径
     * @return 文件列表
     */
    public Node listFiles(String filename, int level) {
        return this.directory.listFiles(filename, level);
    }

    /**
     * <pre>
     *     假设存在文件：
     *
     *     /aaa/bbb/c1.png
     *     /aaa/bbb/c2.png
     *     /bbb/ccc/c3.png
     *
     * 传入：/aaa，则返回：[/bbb/c1.png, /bbb/c2.png]
     *
     * </pre>
     * <p>
     * 返回文件名
     */
    public List<String> findAllFiles(String path) {
        return this.directory.findAllFiles(path);
    }

    /**
     * 获取文件列表
     *
     * @param filename 文件路径
     * @return 文件列表
     */
    public Node unsafeListFiles(String filename) {
        return this.directory.unsafeListFiles(filename);
    }

    /**
     * 获取文件属性
     *
     * @param filename 文件名称
     * @return 文件属性
     */
    public Map<String, String> getAttr(String filename) {
        Node node = this.directory.listFiles(filename);
        if (node == null) {
            return null;
        }
        return Collections.unmodifiableMap(node.getAttr());
    }
}
