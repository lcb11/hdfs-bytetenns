package com.bytetenns.backupnode.fsimage;

import com.bytetenns.backupnode.utils.ByteUtil;
import com.bytetenns.backupnode.utils.FileUtil;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import com.bytetenns.dfs.model.backup.INode;
import org.apache.commons.lang3.time.StopWatch;

/**
 * @Author jiaoyuliang
 * @Description 元数据内存影像
 * @Date 2022/8/17
 */
@Slf4j
@Data
@AllArgsConstructor
@NoArgsConstructor
public class FsImage {

    private static final int LENGTH_OF_FILE_LENGTH_FIELD = 4;
    private static final int LENGTH_OF_MAX_TX_ID_FIELD = 8;


    //当前最大的txId
    private long maxTxId;

    //内容
    private INode iNode;

    /**
     * 4位整个文件的长度 + 8位maxId + 文件内容
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
     * @param channel 文件channel
     * @param path    文件绝对路径
     * @param length  文件长度
     * @return 如果合法返回 FsImage，不合法返回null
     * @throws IOException IO异常，文件不存在
     */
    public static FsImage parse(FileChannel channel, String path, int length) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(LENGTH_OF_FILE_LENGTH_FIELD + LENGTH_OF_MAX_TX_ID_FIELD);
        channel.read(buffer);
        buffer.flip();
        if (buffer.remaining() < 4) {
            log.warn("FsImage文件不完整: [file={}]", path);
            return null;
        }
        int fileLength = buffer.getInt();
        if (fileLength != length) {
            log.warn("FsImage文件不完整: [file={}]", path);
            return null;
        } else {
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();
            long maxTxId = buffer.getLong();
            int bodyLength = fileLength - LENGTH_OF_FILE_LENGTH_FIELD - LENGTH_OF_MAX_TX_ID_FIELD;
            buffer = ByteBuffer.allocate(bodyLength);
            channel.read(buffer);
            buffer.flip();
            byte[] body = new byte[bodyLength];
            buffer.get(body);
            INode iNode;
            try {
                iNode = INode.parseFrom(body);
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

