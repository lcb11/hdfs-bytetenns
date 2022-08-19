package com.ruyuan.dfs.client.tools.command;

import com.ruyuan.dfs.client.FileSystem;
import org.jline.reader.LineReader;

import java.util.Map;

/**
 * 读取文件属性命令
 * <p>
 * 例如：<br/>
 * <p>read /usr/local/file.txt<p/>
 *
 * @author Sun Dasheng
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
