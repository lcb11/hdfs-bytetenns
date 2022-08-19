package com.bytetenns.common.netty;

import com.bytetenns.dfs.model.common.NettyPacketHeader;
import com.bytetenns.common.enums.PacketType;
import com.bytetenns.common.utils.PrettyCodes;
import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import java.util.*;

/**
 * 网络传输消息
 *
 *
 *          NettyPacket数据格式
 *  +--------+-------------------------------+---------------+-----------------------------+
 *  | HeaderLength | Actual Header (18byte)  | ContentLength | Actual Content (25byte)     |
 *  | 0x0012       | Header Serialization    | 0x0019        | Body  Serialization         |
 *  +--------------+-------------------------+---------------+-----------------------------+
 *
 *
 * @author Sun Dasheng
 */
@Slf4j
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NettyPacket {

    /**
     * 消息体
     */
    protected byte[] body;

    /**
     * 请求头
     */
    private Map<String, String> header;

    public static NettyPacket copy(NettyPacket nettyPacket) {
        return new NettyPacket(nettyPacket.getBody(), new HashMap<>(nettyPacket.getHeader()));
    }

    /**
     * 请求是否需要继续广播给其他节点
     * @param broadcast 是否需要广播
     */
    public void setBroadcast(boolean broadcast) {
        header.put("broadcast", String.valueOf(broadcast));
    }

    /**
     * 请求是否需要继续广播给其他节点
     */
    public boolean getBroadcast() {
        return Boolean.parseBoolean(header.getOrDefault("broadcast", "false"));
    }

    /**
     * 设置请求序列号
     * @param sequence 请求序列号
     */
    public void setSequence(String sequence) {
        if (sequence != null) {
            header.put("sequence", sequence);
        }
    }

    /**
     * 设置请求序列号
     */
    public String getSequence() {
        return header.get("sequence");
    }

    /**
     * 请求包类型
     * @return 请求包类型
     */
    public int getPacketType() {
        return Integer.parseInt(header.getOrDefault("packetType", "0"));
    }

    /**
     * 设置请求包类型
     * @param packetType 请求包类型
     */
    public void setPacketType(int packetType) {
        header.put("packetType", String.valueOf(packetType));
    }


    public void setUserToken(String token) {
        header.put("userToken", token);
    }

    public String getUserToken() {
        return header.getOrDefault("userToken", "");
    }

    public void setUsername(String username) {
        header.put("username", username);
    }

    public String getUserName() {
        return header.getOrDefault("username", "");
    }

    public void setError(String error) {
        header.put("error", error);
    }

    public boolean isSuccess() {
        return getError() == null;
    }

    public boolean isError() {
        return !isSuccess();
    }

    public String getError() {
        return header.getOrDefault("error", null);
    }

    public void setNodeId(int nodeId) {
        header.put("nodeId", String.valueOf(nodeId));
    }

    public int getNodeId() {
        String nodeId = header.getOrDefault("nodeId", "-1");
        return Integer.parseInt(nodeId);
    }

    public void setAck(int ack) {
        header.put("ack", String.valueOf(ack));
    }

    public int getAck() {
        String ack = header.getOrDefault("ack", "0");
        return Integer.parseInt(ack);
    }

    public void setTimeoutInMs(long timeoutInMs) {
        header.put("timeoutInMs", String.valueOf(timeoutInMs));
    }

    public long getTimeoutInMs() {
        return Long.parseLong(header.getOrDefault("timeoutInMs", "0"));
    }

    public boolean isSupportChunked() {
        return Boolean.parseBoolean(header.getOrDefault("supportChunked", "false"));
    }

    public void setSupportChunked(boolean chunkedFinish) {
        header.put("supportChunked", String.valueOf(chunkedFinish));
    }


    /**
     * 创建网络请求通用请求
     * @param body        body
     * @param packetType 请求类型
     * @return 请求
     */
    public static NettyPacket buildPacket(byte[] body, PacketType packetType) {
        NettyPacketBuilder builder = NettyPacket.builder();
        builder.body = body;
        builder.header = new HashMap<>(PrettyCodes.trimMapSize());
        NettyPacket nettyPacket = builder.build();
        nettyPacket.setPacketType(packetType.value);
        return nettyPacket;
    }

    /**
     * 合并消息体
     * @param otherPackage 网络包
     */
    public void mergeChunkedBody(NettyPacket otherPackage) {
        int newBodyLength = body.length + otherPackage.getBody().length;
        byte[] newBody = new byte[newBodyLength];
        System.arraycopy(body, 0, newBody, 0, body.length);
        System.arraycopy(otherPackage.getBody(), 0, newBody, body.length, otherPackage.getBody().length);
        this.body = newBody;
    }

    /**
     * 拆分消息体
     * @param supportChunked 是否支持chunked特性
     * @param maxPackageSize 拆分包后每个包的消息体最大的数量
     * @return 拆分后的消息集合
     */
    public List<NettyPacket> partitionChunk(boolean supportChunked, int maxPackageSize) {
        if (!supportChunked) {
            return Collections.singletonList(this);
        }
        int bodyLength = body.length;
        if (bodyLength <= maxPackageSize) {
            // 不需要拆包
            return Collections.singletonList(this);
        }

        // 开始拆包
        int packageCount = bodyLength / maxPackageSize;
        if (bodyLength % maxPackageSize > 0) {
            packageCount++;
        }
        List<NettyPacket> results = new LinkedList<>();
        int remainLength = bodyLength;
        for (int i = 0; i < packageCount; i++) {
            int partitionBodyLength = Math.min(maxPackageSize, remainLength);
            byte[] partitionBody = new byte[partitionBodyLength];
            System.arraycopy(body, bodyLength - remainLength, partitionBody, 0, partitionBodyLength);
            remainLength -= partitionBodyLength;
            NettyPacket partitionPackage = new NettyPacket();
            partitionPackage.body = partitionBody;
            partitionPackage.header = this.header;
            partitionPackage.setSupportChunked(true);
            results.add(partitionPackage);
        }
        // 增加一个结束标记包
        NettyPacket tailPackage = new NettyPacket();
        tailPackage.body = new byte[0];
        tailPackage.header = this.header;
        tailPackage.setSupportChunked(true);
        results.add(tailPackage);
        return results;
    }


    /**
     * 将数据写入ByteBuf
     * @param out 输出
     */
    public void write(ByteBuf out) {
        NettyPacketHeader nettyPackageHeader = NettyPacketHeader.newBuilder()
                .putAllHeaders(header)
                .build();
        byte[] headerBytes = nettyPackageHeader.toByteArray();
        out.writeInt(headerBytes.length);
        out.writeBytes(headerBytes);
        out.writeInt(body.length);
        out.writeBytes(body);
    }


    /**
     * 解包
     * @param byteBuf 内存缓冲区
     * @return netty网络包
     */
    public static NettyPacket parsePacket(ByteBuf byteBuf) throws InvalidProtocolBufferException {
        int headerLength = byteBuf.readInt();
        byte[] headerBytes = new byte[headerLength];
        byteBuf.readBytes(headerBytes);
        NettyPacketHeader nettyPackageHeader = NettyPacketHeader.parseFrom(headerBytes);
        int bodyLength = byteBuf.readInt();
        byte[] bodyBytes = new byte[bodyLength];
        byteBuf.readBytes(bodyBytes);
        return NettyPacket.builder()
                .header(new HashMap<>(nettyPackageHeader.getHeadersMap()))
                .body(bodyBytes)
                .build();
    }

}
