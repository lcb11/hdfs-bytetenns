package com.bytetenns.client.tools.command;

import com.bytetenns.client.FileSystem;
import org.jline.reader.LineReader;

/**
 * 无操作命令
 *
 * @author LiZhirun
 */
public class NoOpCommand implements Command {
    @Override
    public void execute(FileSystem fileSystem, LineReader lineReader) {
    }
}
