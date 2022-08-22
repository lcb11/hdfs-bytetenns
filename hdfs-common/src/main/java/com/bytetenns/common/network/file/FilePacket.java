package com.bytetenns.common.network.file;

import com.google.common.base.Joiner;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Builder;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * 文件传输包
 */
@Data
@Builder
public class FilePacket {

    /**
     * 包类型，文件传输前的请求头，文件传输内容包、文件传输完成后的结尾包
     */

    public static final Integer HEAD = 1;
    public static final Integer BODY = 2;
    public static final Integer TAIL = 3;

    /**
     * 包类型
     */
    private int type;

    /**
     * 文件元数据，key-value对
     * 例如：filename=/aaa/bbb/ccc.png,md5=xxxxxxx,crc32=xxxx,size=xxx
     */
    private Map<String, String> fileMetaData;

    /**
     * 文件内容
     */
    private byte[] body;


    /**
     * 转换为ByteBuf
     *
     * @return ByteBuf
     */
    public byte[] toBytes() {
        String metaDataString = null;
        int lengthOfFileMetaData = 0;
        if (fileMetaData != null && !fileMetaData.isEmpty()) {
            metaDataString = Joiner.on("&").withKeyValueSeparator("=").join(fileMetaData);
            lengthOfFileMetaData = metaDataString.getBytes().length;
        }

        int lengthOfBody = body == null ? 0 : body.length;
        // 4个字节存包类型 + 4个字节存元数据长度 + x个字节存元数据 + 4个字节存body长度 + x个字节存body
        ByteBuf buffer = Unpooled.buffer(4 + 4 + lengthOfFileMetaData + 4 + lengthOfBody);
        buffer.writeInt(type);
        buffer.writeInt(lengthOfFileMetaData);
        if (lengthOfFileMetaData > 0) {
            buffer.writeBytes(metaDataString.getBytes());
        }
        buffer.writeInt(lengthOfBody);
        if (lengthOfBody > 0) {
            buffer.writeBytes(body);
        }
        return buffer.array();
    }

    /**
     * 将字节数组解包成网络包
     *
     * @param sources 包
     * @return 网络包
     */
    public static FilePacket parseFrom(byte[] sources) {
        ByteBuf byteBuf = Unpooled.copiedBuffer(sources);
        FilePacketBuilder builder = FilePacket.builder();
        int type = byteBuf.readInt();
        builder.type(type);
        int lengthOfFileMetaData = byteBuf.readInt();
        if (lengthOfFileMetaData > 0) {
            byte[] metaDataBytes = new byte[lengthOfFileMetaData];
            byteBuf.readBytes(metaDataBytes, 0, lengthOfFileMetaData);
            String fileMetaData = new String(metaDataBytes, 0, lengthOfFileMetaData);
            StringTokenizer st = new StringTokenizer(fileMetaData, "&");
            Map<String, String> metaData = new HashMap<>(2);
            int i;
            while (st.hasMoreTokens()) {
                String s = st.nextToken();
                i = s.indexOf("=");
                String name = s.substring(0, i);
                String value = s.substring(i + 1);
                metaData.put(name, value);
            }
            builder.fileMetaData(metaData);
        }
        int lengthOfBody = byteBuf.readInt();
        if (lengthOfBody > 0) {
            byte[] body = new byte[lengthOfBody];
            byteBuf.readBytes(body, 0, lengthOfBody);
            builder.body = body;
        }
        return builder.build();
    }
}
