package com.ruyuan.dfs.datanode.server.locate;

import com.ruyuan.dfs.common.utils.NetUtils;
import com.ruyuan.dfs.common.utils.StringUtils;
import lombok.extern.slf4j.Slf4j;

/**
 * 基于AES加密算法的 文件路径定位器
 *
 * @author Sun Dasheng
 */
@Slf4j
public class AesFileLocator extends AbstractFileLocator {

    private String key;

    public AesFileLocator(String basePath, int hashSize) {
        super(basePath, hashSize);
    }

    @Override
    protected String encodeFileName(String filename) {
        try {
            return StringUtils.aesEncrypt(getKey(), filename);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String getKey() {
        if (key == null) {
            key = NetUtils.getHostName();
        }
        return key;
    }
}
