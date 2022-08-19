package com.ruyuan.dfs.client.tools.command;

import com.ruyuan.dfs.client.FileSystem;
import org.jline.reader.LineReader;

/**
 * 无操作命令
 *
 * @author Sun Dasheng
 */
public class NoOpCommand implements Command {
    @Override
    public void execute(FileSystem fileSystem, LineReader lineReader) {
    }
}
