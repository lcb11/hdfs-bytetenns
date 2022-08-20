package com.bytetenns.client.tools.command;

import org.jline.reader.LineReader;

import com.bytetenns.client.FileSystem;

/**
 * 命令
 *
 * @author Sun Dasheng
 */
public interface Command {

    /**
     * 执行命令
     *
     * @param fileSystem 文件系统
     * @param lineReader 读取输入
     * @throws Exception 异常
     */
    void execute(FileSystem fileSystem, LineReader lineReader) throws Exception;
}
