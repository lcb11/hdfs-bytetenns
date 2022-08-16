package com.ruyuan.dfs.datanode.server.locate;

import com.ruyuan.dfs.common.utils.NetUtils;
import com.ruyuan.dfs.common.utils.StringUtils;
import lombok.extern.slf4j.Slf4j;

import java.io.File;

/**
 * 抽象文件路径定位器
 *
 * @author Sun Dasheng
 */
@Slf4j
public abstract class AbstractFileLocator implements FileLocator {

    private int hashSize;
    private String basePath;

    public AbstractFileLocator(String basePath, int hashSize) {
        this.basePath = basePath;
        this.hashSize = hashSize;
        this.encodeFileName(NetUtils.getHostName());
    }


    @Override
    public String locate(String filename) {
        String afterTransferPath = encodeFileName(filename);
        int hash = StringUtils.hash(afterTransferPath, hashSize * hashSize);
        int parent = hash / hashSize;
        int child = hash % hashSize;
        String parentPath = StringUtils.format(parent);
        String childPath = StringUtils.format(child);
        return basePath + File.separator + parentPath + File.separator + childPath + File.separator + afterTransferPath;
    }


    /**
     * 对文件名转码
     *
     * @param filename 文件名
     * @return 返回文件名
     */
    protected abstract String encodeFileName(String filename);
}
