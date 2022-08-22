package com.bytetenns.client.tools.command;

import com.bytetenns.client.FileSystem;
import com.bytetenns.common.utils.StringUtils;
import org.jline.reader.LineReader;

import java.io.File;

/**
 * 获取文件请求
 *
 * @author Sun Dasheng
 */
public class GetCommand extends AbstractCommand {

    public GetCommand(String currentPath, String command) {
        super(currentPath, command);
    }

    @Override
    public void execute(FileSystem fileSystem, LineReader lineReader) throws Exception {
        String src = getArgs(0);
        String dest = getArgs(1);
        if (src == null || dest == null) {
            System.out.println("无法识别命令：" + command);
            return;
        }
        src = concatPath(src);
        boolean ret = StringUtils.validateFileName(src);
        if (!ret) {
            System.out.println("文件名不合法：" + src);
            return;
        }
        ret = StringUtils.validateFileName(dest);
        if (!ret) {
            System.out.println("文件名不合法：" + dest);
            return;
        }
        download(fileSystem, src, dest);
    }
}
