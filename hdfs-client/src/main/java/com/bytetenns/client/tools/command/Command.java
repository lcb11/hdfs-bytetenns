package com.ruyuan.dfs.client.tools.command;

import com.ruyuan.dfs.client.FileSystem;
import org.jline.reader.LineReader;

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
