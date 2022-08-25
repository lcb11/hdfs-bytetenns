package com.bytetenns.client.tools.command;

import org.jline.reader.LineReader;

import com.bytetenns.client.FileSystem;

import java.util.Map;

/**
 * 读取文件属性命令
 * <p>
 * 例如：<br/>
 * <p>read /usr/local/file.txt<p/>
 *
 * @author LiZhirun
 */
public class AttrCommand extends AbstractCommand {

    public AttrCommand(String currentPath, String command) {
        super(currentPath, command);
    }

    @Override
    public void execute(FileSystem fileSystem, LineReader lineReader) throws Exception {
        String cmd = getCmd();
        if (cmd == null) {
            System.out.println("无法识别命令：" + command);
            return;
        }
        String filename = getArgs(0);
        filename = concatPath(filename);
        if (filename == null) {
            return;
        }
        Map<String, String> attr = fileSystem.readAttr(filename);
        printMap(attr);
    }
}
