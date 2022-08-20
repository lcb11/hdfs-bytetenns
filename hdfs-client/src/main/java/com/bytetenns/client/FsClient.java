package com.bytetenns.client;

import com.bytetenns.client.config.FsClientConfig;

/**
 * 文件系统客户端
 *
 * @author Li Zhirun
 */
public class FsClient {

    public static FileSystem getFileSystem(FsClientConfig fsClientConfig) throws Exception {
        FileSystemImpl fileSystem = new FileSystemImpl(fsClientConfig);
        fileSystem.start();
        return fileSystem;
    }
}
