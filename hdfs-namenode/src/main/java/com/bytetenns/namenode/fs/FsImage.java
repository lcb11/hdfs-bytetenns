package com.bytetenns.namenode.fs;

import com.bytetenns.dfs.model.backup.INode;
import com.bytetenns.common.utils.ByteUtil;
import com.bytetenns.common.utils.FileUtil;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
  * @Author lcb
  * @Description  代表fsimage文件
  * @Date 2022/8/10
  * @Param
  * @return
  **/
@Slf4j
@Data
@AllArgsConstructor
@NoArgsConstructor
public class FsImage {

    private static final int LENGTH_OF_FILE_LENGTH_FIELD = 4;
    private static final int LENGTH_OF_MAX_TX_ID_FIELD = 8;

    /**
     * 当前最大的txId
     */
    private long maxTxId;

    /**
     * 内容
     */
    private INode iNode;

    /**
     * 4位整个文件的长度 + 8位maxId + 文件内容
     *
     * @return 二进制数组
     */
    public byte[] toByteArray() {
        byte[] body = iNode.toByteArray();
        // 文件长度包括 4位文件长度 + 8位txId + 内容长度
        int fileLength = LENGTH_OF_FILE_LENGTH_FIELD + LENGTH_OF_MAX_TX_ID_FIELD + body.length;
        byte[] ret = new byte[fileLength];
        ByteUtil.setInt(ret, 0, fileLength);
        ByteUtil.setLong(ret, LENGTH_OF_FILE_LENGTH_FIELD, maxTxId);
        System.arraycopy(body, 0, ret, LENGTH_OF_FILE_LENGTH_FIELD + LENGTH_OF_MAX_TX_ID_FIELD, body.length);
        return ret;
    }


    /**
     * 解析FsImage文件
     *
     * @param channel 文件channel
     * @param path    文件绝对路径
     * @param length  文件长度
     * @return 如果合法返回 FsImage，不合法返回null
     * @throws IOException IO异常，文件不存在
     */
    public static FsImage parse(FileChannel channel, String path, int length) throws IOException {
        //allocate()：分配新的字节缓冲区
        ByteBuffer buffer = ByteBuffer.allocate(LENGTH_OF_FILE_LENGTH_FIELD + LENGTH_OF_MAX_TX_ID_FIELD);
        //read():将此通道中的字节序列读取到给定的缓冲区中
        channel.read(buffer);
        //flip():翻转此缓冲区。限制设置为当前位置，然后位置设置为零。如果定义了标记，则它是已丢弃。
        buffer.flip();
        //remaining():Returns the number of elements between the current position and the limit.
        //todo 为什么这里要限制为4
        if (buffer.remaining() < 4) {
            log.warn("FsImage文件不完整: [file={}]", path);
            return null;
        }
        int fileLength = buffer.getInt();
        if (fileLength != length) {
            log.warn("FsImage文件不完整: [file={}]", path);
            return null;
        } else {
            //StopWatch：计时器
            StopWatch stopWatch = new StopWatch();
            //开启计时器
            stopWatch.start();
            long maxTxId = buffer.getLong();
            //todo 为什么要减去LENGTH_OF_FILE_LENGTH_FIELD || LENGTH_OF_MAX_TX_ID_FIELD;
            int bodyLength = fileLength - LENGTH_OF_FILE_LENGTH_FIELD - LENGTH_OF_MAX_TX_ID_FIELD;
            buffer = ByteBuffer.allocate(bodyLength);
            channel.read(buffer);
            buffer.flip();
            byte[] body = new byte[bodyLength];
            //此方法将字节从此缓冲区传输到给定的目标数组
            buffer.get(body);
            INode iNode;
            try {
                iNode = INode.parseFrom(body);//protobuf解析
            } catch (InvalidProtocolBufferException e) {
                log.error("Parse EditLog failed.", e);
                return null;
            }

            FsImage fsImage = new FsImage(maxTxId, iNode);
            stopWatch.stop();
            log.info("加载FSImage: [file={}, size={}, maxTxId={}, cost={} s]",
                    path, FileUtil.formatSize(length),
                    fsImage.getMaxTxId(), stopWatch.getTime() / 1000.0D);
            stopWatch.reset();
            return fsImage;
        }
    }


    /**
     * 校验FSImage是否合法
     *
     * @param channel File Channel
     * @param path    文件路径
     * @param length  文件长度
     * @return 如果合法返回MaxTxId, 如果不合法返回-1
     * @throws IOException 文件不存在
     */
    public static long validate(FileChannel channel, String path, int length) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(12);
        channel.read(buffer);
        buffer.flip();
        if (buffer.remaining() < LENGTH_OF_FILE_LENGTH_FIELD) {
            log.warn("FsImage文件不完整: [file={}]", path);
            return -1;
        }
        int fileLength = buffer.getInt();
        if (fileLength != length) {
            log.warn("FsImage文件不完整: [file={}]", path);
            return -1;
        } else {
            return buffer.getLong();
        }
    }

}
