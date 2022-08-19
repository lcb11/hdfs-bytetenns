package com.ruyuan.dfs.client;

import com.ruyuan.dfs.client.config.FsClientConfig;

/**
 * 文件系统客户端
 *
 * @author Sun Dasheng
 */
public class FsClient {

    public static FileSystem getFileSystem(FsClientConfig fsClientConfig) throws Exception {
        FileSystemImpl fileSystem = new FileSystemImpl(fsClientConfig);
        fileSystem.start();
        return fileSystem;
    }
}
