package com.bytetenns.client.tools.command;

import com.bytetenns.client.FileSystem;
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
