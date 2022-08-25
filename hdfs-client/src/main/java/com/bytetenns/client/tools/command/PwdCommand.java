package com.bytetenns.client.tools.command;

import com.bytetenns.client.FileSystem;
import org.jline.reader.LineReader;

/**
 * 实现PWD功能, 显示当前所处工作目录的全路径
 *
 * @author LiZhirun
 */
public class PwdCommand extends AbstractCommand {

    public PwdCommand(String currentPath, String command) {
        super(currentPath, command);
    }

    @Override
    public void execute(FileSystem fileSystem, LineReader lineReader) {
        System.out.println(currentPath);
    }
}
