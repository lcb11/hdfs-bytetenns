package com.bytetenns.client.tools.command;

import com.bytetenns.client.FileSystem;
import com.bytetenns.client.tools.CommandReader;
import com.bytetenns.utils.StringUtils;
import org.jline.reader.LineReader;

import java.io.File;

/**
 * cd 命令
 * <p>
 * 例如：<br/>
 * <p>cd ..<p/>
 * <p>cd /absolute/path<p/>
 * <p>cd relative/path<p/>
 *
 * @author Sun Dasheng
 */
public class CdCommand extends AbstractCommand {

    private CommandReader commandReader;

    public CdCommand(String currentPath, String command, CommandReader commandReader) {
        super(currentPath, command);
        this.commandReader = commandReader;
    }

    @Override
    public void execute(FileSystem fileSystem, LineReader lineReader) {
        String nextPath = getArgs(0);
        if (nextPath == null) {
            System.out.println("无法识别命令：" + command);
            return;
        }
        String path = concatPath(nextPath);
        if (path == null) {
            return;
        }
        commandReader.setCurrentPath(path);
    }
}
