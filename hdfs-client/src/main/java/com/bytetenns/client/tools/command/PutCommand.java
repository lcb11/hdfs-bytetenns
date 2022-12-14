package com.bytetenns.client.tools.command;

import com.bytetenns.client.FileSystem;
import com.bytetenns.common.utils.StringUtils;
import org.jline.reader.LineReader;

import java.io.File;

/**
 * 上传文件
 * @author LiZhirun
 * 把localFile以filename的名字上传到系统中
 */
public class PutCommand extends AbstractCommand {

    public PutCommand(String currentPath, String command) {
        super(currentPath, command);
    }

    @Override
    public void execute(FileSystem fileSystem, LineReader lineReader) throws Exception {
        String localFile = getArgs(0);
        String filename = getArgs(1);
        System.out.println("-----localFile = " + localFile + ", filename = " + filename);
        if (localFile == null || filename == null) {
            System.out.println("无法识别命令：" + command);
            return;
        }
        if (!filename.startsWith(File.separator)) {
            filename = currentPath + File.separator + filename;
        }
        boolean ret = StringUtils.validateFileName(filename);
        if (!ret) {
            System.out.println("文件名不合法：" + filename);
            return;
        }
        ret = StringUtils.validateFileName(localFile);
        if (!ret) {
            System.out.println("文件名不合法：" + localFile);
            return;
        }
        File file = new File(localFile);
        if (!file.exists()) {
            System.out.println("localFile = " + localFile + ", filename = " + filename);
            System.out.println("文件不存在：" + file.getAbsolutePath());
            return;
        }
        upload(fileSystem, filename, localFile);
    }
}
