package com.bytetenns.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 请求类型
 */
@Getter
@AllArgsConstructor
public enum PacketType {

    /**
     * 请求类型
     */
    UNKNOWN(0, "未知的包类型"),
    DATA_NODE_REGISTER(1, "DataNode往NameNode注册请求"),
    HEART_BRET(2, "DataNode往NameNode发送的心跳请求"),
    MKDIR(3, "客户端往NameNode发送Mkdir请求"),
    CREATE_FILE_CONFIRM(4, "客户端往DataNode上传完文件之后，再发请求往NameNode确认"),
    FETCH_EDIT_LOG(5, "BackupNode往NameNode发送抓取EditLog请求"),
    TRANSFER_FILE(6, "文件传输的二进制包类型"),
    REPORT_STORAGE_INFO(7, "DataNode往NameNode上报存储信息请求"),
    CREATE_FILE(8, "客户端端往NameNode发送创建文件请求"),
    GET_DATA_NODE_FOR_FILE(9, "客户端往NameNode发送获取文件所在DataNode的请求"),
    REPLICA_RECEIVE(10, "DataNode往NameNode上报收到一个文件的请求"),
    GET_FILE(11, "客户端从DataNode下载文件，或DataNode之间相互同步副本请求"),
    DATA_NODE_PEER_AWARE(12, "DataNode之间建立连接后发起的相互感知的请求"),
    REPORT_BACKUP_NODE_INFO(13, "BackupNode往NameNode上报自身信息"),
    FETCH_BACKUP_NODE_INFO(14, "客户端或者DataName往NameNode上发起请求获取BackupNode信息"),
    AUTHENTICATE(15, "客户端发起的认证请求"),
    GET_NAME_NODE_STATUS(17, "BackupNode感知到NameNode挂了，往客户端和DataNode确认NameNode的状态"),
    DUPLICATE_BACKUP_NODE(18, "一个NameNode只支持一个BackupNode, 重复的BackupNode异常信息类型"),
    NAME_NODE_CONTROLLER_VOTE(19, "NameNode投票选举的票据"),
    NAME_NODE_PEER_AWARE(20, "NameNode相互之间发起连接时的感知请求"),
    NAME_NODE_SLOT_BROADCAST(21, "Controller选举成功后，往所有NameNode广播Slots信息"),
    RE_BALANCE_SLOTS(22, "新节点加入集群, 申请重新分配Slots"),
    FETCH_SLOT_METADATA(23, "新节点从旧节点中拉取文件目录树元数据"),
    FETCH_SLOT_METADATA_RESPONSE(24, "旧节点往新节点发送文件目录树元数据"),
    FETCH_SLOT_METADATA_COMPLETED(25, "新节点完成了文件目录树的拉取，通知Controller"),
    FETCH_SLOT_METADATA_COMPLETED_BROADCAST(26, "Controller广播给所有NameNode，新节点完成了元数据拉取回放"),
    REMOVE_METADATA_COMPLETED(27, "旧节点删除不属于自身Slot的内存目录树之后，往Controller上报"),
    BACKUP_NODE_SLOT(28, "NameNode往BackupNode下发属于Slots分配信息"),
    CLIENT_LOGOUT(29, "客户端断开连接，清除认证消息"),
    USER_CHANGE_EVENT(30, "用户信息增删查改事件"),
    REMOVE_FILE(31, "客户端删除文件"),
    READ_ATTR(32, "客户端读取文件属性"),
    FETCH_USER_INFO(33, "NameNode往PeerNameNode获取用户信息"),
    LIST_FILES(34, "NameNode往PeerNameNode获取文件列表"),
    TRASH_RESUME(35, "文件在垃圾箱放回原处的请求"),
    NEW_PEER_NODE_INFO(36, "NameNode的Controller节点在新节点上线后发送的信息"),
    REPLICA_REMOVE(37, "DataNode往NameNode上报移除一个文件的请求"),
    FETCH_NAME_NODE_INFO(38, "获取NameNode基本信息"),
    NAME_NODE_REMOVE_FILE(39, "NameNode节点删除文件"),
    FETCH_DATA_NODE_BY_FILENAME(40, "根据文件名获取DataNode机器名"),
    ADD_REPLICA_NUM(41, "新增文件副本数量"),
    CLIENT_LIST_FILES(42, "客户端获取文件列表"),
    CLIENT_READ_NAME_NODE_INFO(43, "客户端获取NameNode基本信息"),
    CLIENT_READ_DATA_NODE_INFO(44, "客户端获取DataNode基本信息"),
    CLIENT_READ_STORAGE_INFO(45, "客户端获取文件存储信息"),
    CLIENT_PRE_CALCULATE(46, "客户端导出文件/文件夹前计算文件数量的请求"),
    CLIENT_GET_ALL_FILENAME(47, "客户端获取文件/文件夹包含的所有文件全路径"),
    ;

    public int value;
    private String description;

    public static PacketType getEnum(int value) {
        for (PacketType packetType : values()) {
            if (packetType.getValue() == value) {
                return packetType;
            }
        }
        return UNKNOWN;
    }
}
