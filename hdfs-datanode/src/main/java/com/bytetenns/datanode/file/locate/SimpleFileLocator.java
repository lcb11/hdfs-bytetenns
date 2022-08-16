package com.ruyuan.dfs.datanode.server.locate;

import java.io.File;

/**
 * 简单路径定位器：
 * <p>
 * 将文件名的 "/" 改为 "-"
 *
 * @author Sun Dasheng
 */
public class SimpleFileLocator extends AbstractFileLocator {

    public SimpleFileLocator(String basePath, int hashSize) {
        super(basePath, hashSize);
    }

    @Override
    protected String encodeFileName(String filename) {
        if (filename.startsWith(File.separator)) {
            filename = filename.substring(1);
        }
        return filename.replaceAll("/", "-");
    }
}
