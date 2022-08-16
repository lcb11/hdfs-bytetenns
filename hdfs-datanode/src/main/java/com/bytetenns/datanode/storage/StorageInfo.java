package com.bytetenns.datanode.storage;



import lombok.Data;
import com.bytetenns.datanode.file.FileInfo;

import java.util.List;

/**
 * 存储信息
 *
 * @author gongwei
 */
@Data
public class StorageInfo {

    /**
     * 文件信息
     */
    private List<FileInfo> files;
    /**
     * 已用空间
     */
    private long storageSize;
    /**
     * 可用空间
     */
    private long freeSpace;

    public StorageInfo() {
        this.storageSize = 0L;
    }
}

