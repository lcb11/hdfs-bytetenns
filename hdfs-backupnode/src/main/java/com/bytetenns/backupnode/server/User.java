package com.bytetenns.backupnode.server;

import com.bytetenns.common.utils.FileUtil;
import com.bytetenns.dfs.model.namenode.UserEntity;
import com.bytetenns.dfs.model.namenode.UserStorageEntity;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.*;

/**
 * 用户信息
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class User {


    /**
     * 用户名
     */
    private String username;

    /**
     * 秘钥
     */
    private String secret;

    /**
     * 创建时间
     */
    private long createTime;

    /**
     * 存储信息
     */
    private StorageInfo storageInfo;

    public static User copy(User user) {
        User ret = new User();
        ret.setUsername(user.username);
        ret.setSecret(user.secret);
        StorageInfo storageInfo = new StorageInfo();
        storageInfo.setStorageSize(user.storageInfo.storageSize);
        storageInfo.setDisplayStorageSize(FileUtil.formatSize(user.storageInfo.storageSize));
        storageInfo.setFileCount(user.storageInfo.fileCount);
        storageInfo.setDataNodes(new ArrayList<>(user.storageInfo.dataNodes));
        ret.setStorageInfo(storageInfo);
        return ret;
    }

    @Data
    @AllArgsConstructor
    public static class StorageInfo {
        /**
         * 当前存储的大小
         */
        private long storageSize;

        private String displayStorageSize;

        /**
         * 当前存储的文件数量
         */
        private int fileCount;
        /**
         * 指定的DataNode节点，如果指定这些节点，则在分配DataNode的时候就在这些节点集合中选择
         */
        private List<String> dataNodes;

        private transient Set<String> dataNodesSet;

        public StorageInfo() {
            this.storageSize = 0;
            this.fileCount = 0;
            this.dataNodes = new ArrayList<>();
        }

        public Set<String> getDataNodesSet() {
            if (dataNodesSet == null) {
                dataNodesSet = new HashSet<>(dataNodes);
            }
            return dataNodesSet;
        }

        public void addDataNode(String dataNode) {
            dataNodes.add(dataNode);
            if (dataNodesSet != null) {
                dataNodesSet.add(dataNode);
            }
        }
    }

    public UserEntity toEntity() {
        UserEntity.Builder builder = UserEntity.newBuilder();
        builder.setUsername(username);
        builder.setSecret(secret);
        builder.setCreateTime(createTime);
        UserStorageEntity storageEntity = UserStorageEntity.newBuilder()
                .setStorageSize(storageInfo.storageSize)
                .setFileCount(storageInfo.fileCount)
                .addAllDataNodes(storageInfo.dataNodes)
                .build();
        builder.setStorage(storageEntity);
        return builder.build();
    }

    public static User parse(UserEntity entity) {
        User user = new User();
        user.username = entity.getUsername();
        user.secret = entity.getSecret();
        user.createTime = entity.getCreateTime();
        UserStorageEntity storage = entity.getStorage();
        StorageInfo storageInfo = new StorageInfo();
        storageInfo.storageSize = storage.getStorageSize();
        storageInfo.fileCount = storage.getFileCount();
        storageInfo.dataNodes = storage.getDataNodesList();
        user.storageInfo = storageInfo;
        return user;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        User that = (User) o;
        return Objects.equals(username, that.username) &&
                Objects.equals(secret, that.secret);
    }

    @Override
    public int hashCode() {
        return Objects.hash(username, secret);
    }

}
