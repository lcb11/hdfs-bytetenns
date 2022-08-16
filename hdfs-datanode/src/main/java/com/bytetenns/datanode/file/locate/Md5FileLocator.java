package com.ruyuan.dfs.datanode.server.locate;


import com.ruyuan.dfs.common.utils.StringUtils;

/**
 * 基于MD5 HASH 算法定位文件
 *
 * @author Sun Dasheng
 */
public class Md5FileLocator extends AbstractFileLocator {

    public Md5FileLocator(String basePath, int hashSize) {
        super(basePath, hashSize);
    }

    @Override
    protected String encodeFileName(String filename) {
        return StringUtils.md5(filename);
    }
}
