package com.ruyuan.dfs.datanode.server.locate;

import org.apache.commons.codec.digest.DigestUtils;

/**
 * 基于Sha-1算法查找文件存储位置
 *
 * @author Sun Dasheng
 */
public class Sha1FileLocator extends AbstractFileLocator {

    public Sha1FileLocator(String basePath, int hashSize) {
        super(basePath, hashSize);
    }

    @Override
    protected String encodeFileName(String filename) {
        return DigestUtils.sha1Hex(filename.getBytes());
    }
}
