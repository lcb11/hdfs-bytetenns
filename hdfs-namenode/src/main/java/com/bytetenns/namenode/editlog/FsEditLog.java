package com.bytetenns.namenode.editlog;

import com.bytetenns.common.utils.FileUtil;
import com.bytetenns.namenode.NameNodeConfig;
import com.bytetenns.namenode.editlog.buffer.DoubleBuffer;
import com.bytetenns.namenode.fs.PlaybackEditLogCallback;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
  * @Author lcb
  * @Description 负责管理EditLog的组件，表示EditLogs
  * @Date 2022/8/11
  * @Param
  * @return
  **/
@Slf4j
public class FsEditLog {

    private static Pattern indexPattern = Pattern.compile("(\\d+)_(\\d+)");

    private NameNodeConfig nameNodeConfig;

    /**
     * 每条editLog的id，自增
     */
    private volatile long txIdSeq = 0;

    /**
     * 双缓冲
     */
    private DoubleBuffer editLogBuffer;

    /**
     * 每个线程保存的txid
     */
    private ThreadLocal<Long> localTxId = new ThreadLocal<>();

    /**
     * 当前刷新磁盘最大的txId
     */
    private volatile long syncTxid = 0;

    /**
     * 当前是否在刷磁盘
     */
    private volatile boolean isSyncRunning = false;

    /**
     * 是否正在调度一次刷盘的操作
     */
    private volatile Boolean isSchedulingSync = false;

    /**
     * 磁盘中的editLog文件, 升序
     */
    private List<EditslogInfo> editLogInfos = null;

    public FsEditLog(NameNodeConfig nameNodeConfig) {
        this.nameNodeConfig = nameNodeConfig;
        this.editLogBuffer = new DoubleBuffer(nameNodeConfig);
        this.loadEditLogInfos();
    }


    /**
     * 写入一条editlog
     *
     * @param editLog 内容
     */
    public void logEdit(EditLogWrapper editLog) {
        synchronized (this) {
            // 刚进来就直接检查一下是否有人正在调度一次刷盘的操作
            waitSchedulingSync();

            txIdSeq++;
            // 保存txid到线程
            long txid = txIdSeq;
            localTxId.set(txid);

            // 构造一条editlog写入缓冲区
            editLog.setTxId(txid);
            try {
                editLogBuffer.write(editLog);
            } catch (IOException e) {
                e.printStackTrace();
            }

            if (!editLogBuffer.shouldForceSync()) {
                return;
            }
            // 如果代码进行到这里，就说明需要刷磁盘
            isSchedulingSync = true;
        }

        // 释放掉锁
        logSync();
    }

    /**
     * 等待正在调度的刷磁盘的操作
     */
    private void waitSchedulingSync() {
        try {
            while (isSchedulingSync) {
                wait(1000);
                // 此时就释放锁，等待一秒再次尝试获取锁，去判断
                // isSchedulingSync是否为false，就可以脱离出while循环
            }
        } catch (Exception e) {
            log.info("waitSchedulingSync has interrupted !!");
        }
    }

    /**
     * 异步刷磁盘
     */
    private void logSync() {
        synchronized (this) {
            long txId = localTxId.get();//获取到本地线程的副本
            localTxId.remove();
            /*
             * 在这种情况下需要等待：
             * 1. 有其他线程正在刷磁盘，但是其他线程刷的磁盘的最大txid比当前需要刷磁盘的线程id少。
             * 这通常表示：正在刷磁盘的线程不会把当前线程需要刷的数据刷到磁盘中
             */
            while (txId > syncTxid && isSyncRunning) {
                try {
                    wait(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            /*
             * 多个线程在上面等待，当前一个线程刷磁盘操作完成后，唤醒了一堆线程，此时只有一个线程获取到锁。
             * 这个线程会进行刷磁盘操作，当这个线程释放锁之后，其他被唤醒的线程会依次获取到锁。
             * 此时每个被唤醒的线程需要重新判断一次，自己要做的事情是不是被其他线程干完了
             */
            if (txId <= syncTxid) {
                return;
            }

            // 交换两块缓冲区
            editLogBuffer.setReadyToSync();

            // 记录最大的txid  || txid=30代表，在30之前的editslog都会刷入到磁盘中去
            syncTxid = txId;

            // 设置当前正在同步到磁盘的标志位
            isSchedulingSync = false;

            // 唤醒哪些正在wait的线程
            notifyAll();

            // 正在刷磁盘
            isSyncRunning = true;
        }

        try {
            //flush()将数据刷回磁盘
            EditslogInfo editslogInfo = editLogBuffer.flush();
            if (editslogInfo != null) {
                //将磁盘刷新的信息保存起来
                editLogInfos.add(editslogInfo);
            }
        } catch (IOException e) {
            log.info("FSEditlog刷磁盘失败：", e);
        }

        synchronized (this) {
            // 同步完了磁盘之后，就会将标志位复位，再释放锁
            isSyncRunning = false;
            notifyAll();
        }
    }

    /**
     * 强制把内存缓冲里的数据刷入磁盘中
     */
    public void flush() {
        synchronized (this) {
            try {
                editLogBuffer.setReadyToSync();
                EditslogInfo editslogInfo = editLogBuffer.flush();
                if (editslogInfo != null) {
                    editLogInfos.add(editslogInfo);
                }
            } catch (IOException e) {
                log.error("强制刷新EditLog缓冲区到磁盘失败.", e);
            }
        }
    }

    /**
     * 获取当前写editLog的缓冲区
     *
     * @return 当前写editLog的缓冲区
     */
    public List<EditLogWrapper> getCurrentEditLog() {
        synchronized (this) {
            return editLogBuffer.getCurrentEditLog();
        }
    }

    /**
     * NameNode重启时回放editLog到内存中
     *
     * @param txiId    回放比txid大的日志
     * @param callback 回放日志回调
     * @throws IOException IO异常
     */
    public void playbackEditLog(long txiId, PlaybackEditLogCallback callback) throws IOException {
        //保存当前传进来的Txid
        long currentTxSeq = txiId;
        //以当前加载editlog文件的txid值为初始自增txid
        this.txIdSeq = currentTxSeq;
        //获取比txiId更大的EditslogInfo
        List<EditslogInfo> sortedEditLogsFiles = getSortedEditLogFiles(txiId);
        StopWatch stopWatch = new StopWatch();
        //遍历获取到的所有比txid更大的EditslogInfo
        for (EditslogInfo info : sortedEditLogsFiles) {
            //如果当前EditslogInfo的txid小于currentTxSeq，跳过该条EditslogInfo
            if (info.getEnd() <= currentTxSeq) {
                continue;
            }
            //如果当前EditslogInfo的txid大于currentTxSeq,则该条EditslogInfo所包含的所有EditLogWrapper
            List<EditLogWrapper> editLogWrappers = readEditLogFromFile(info.getName());
            stopWatch.start();
            for (EditLogWrapper editLogWrapper : editLogWrappers) {
                //获取当前editLogWrapper的txid
                long tmpTxId = editLogWrapper.getTxId();
                if (tmpTxId <= currentTxSeq) {
                    continue;
                }
                //交换txid，保证currentTxSeq不会重复加载同一条editLogWrapper
                currentTxSeq = tmpTxId;
                //保证txIdSeq实时更新，不会覆盖掉别的edit log
                this.txIdSeq = currentTxSeq;
                if (callback != null) {
                    //回放符合条件的txid
                    callback.playback(editLogWrapper);
                }
            }
            stopWatch.stop();
            log.info("回放editLog文件: [file={}, cost={} s]", info.getName(), stopWatch.getTime() / 1000.0D);
            stopWatch.reset();
        }
    }

    /**
     * 从文件中读取EditLog
     *
     * @param absolutePath 绝对路径
     * @return EditLog
     * @throws IOException IO异常
     */
    public List<EditLogWrapper> readEditLogFromFile(String absolutePath) throws IOException {
        //返回一个Bytebuffer，buffer为读模式
        return EditLogWrapper.parseFrom(FileUtil.readBuffer(absolutePath));
    }


    /**
     * <pre>
     * 获取比minTxId更大的editlog文件，经过排序后的文件
     * 比如磁盘中有文件：
     *
     *      1_1000.log
     *      1001_2000.log
     *      2001_3000.log
     *
     * 如果minTxId=1500，则会返回: [1001_2000.log, 2001_3000.log]
     * </pre>
     *
     * @param minTxId 最小的txid
     * @return 排序后的文件信息
     */
    public List<EditslogInfo> getSortedEditLogFiles(long minTxId) {
        List<EditslogInfo> result = new ArrayList<>();
        for (EditslogInfo editslogInfo : editLogInfos) {
            if (editslogInfo.getEnd() <= minTxId) {
                continue;
            }
            result.add(editslogInfo);
        }
        return result;
    }

    /**
     * 从磁盘中加载所有的editslog文件信息
     */
    private void loadEditLogInfos() {
        this.editLogInfos = new CopyOnWriteArrayList<>();
        File dir = new File(nameNodeConfig.getBaseDir());
        if (!dir.isDirectory()) {
            return;
        }
        File[] files = dir.listFiles();
        if (files == null || files.length == 0) {
            return;
        }
        for (File file : files) {
            if (!file.getName().contains("edits")) {
                continue;
            }
            long[] index = getIndexFromFileName(file.getName());
            this.editLogInfos.add(new EditslogInfo(index[0], index[1],
                    nameNodeConfig.getBaseDir() + File.separator + file.getName()));
        }
        this.editLogInfos.sort(null);
    }


    /**
     * 从文件名中提取index
     *
     * @param name 文件名  1_1000.log
     * @return 数组，例如：[1,1000]
     */
    private long[] getIndexFromFileName(String name) {
        Matcher matcher = indexPattern.matcher(name);
        long[] result = new long[2];
        if (matcher.find()) {
            result[0] = Long.parseLong(matcher.group(1));
            result[1] = Long.parseLong(matcher.group(2));
        }
        return result;
    }

    /**
     * <pre>
     *
     * 清除比txid小的日志文件
     *
     * 比如磁盘中有文件：
     *
     *      1_1000.log
     *      1001_2000.log
     *      2001_3000.log
     *
     * 如果txid=1500，则会删除文件: [1_1000.log]
     *
     * </pre>
     *
     * @param txId txId
     */
    public void cleanEditLogByTxId(long txId) {
        List<EditslogInfo> toRemoveEditLog = new ArrayList<>();
        for (EditslogInfo editslogInfo : editLogInfos) {
            if (editslogInfo.getEnd() > txId) {
                continue;
            }
            File file = new File(editslogInfo.getName());
            if (file.delete()) {
                log.info("删除editLog文件: [file={}]", editslogInfo.getName());
            }
            toRemoveEditLog.add(editslogInfo);
        }
        editLogInfos.removeAll(toRemoveEditLog);
    }
}
