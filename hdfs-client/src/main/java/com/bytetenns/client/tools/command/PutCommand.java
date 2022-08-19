package com.ruyuan.dfs.client.tools.command;

import com.ruyuan.dfs.client.FileSystem;
import com.ruyuan.dfs.client.utils.ProgressBar;
import com.ruyuan.dfs.common.network.file.OnProgressListener;
import com.ruyuan.dfs.common.utils.PrettyCodes;
import com.ruyuan.dfs.common.utils.StringUtils;
import org.jline.reader.LineReader;

import java.io.File;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;

/**
 * 上传文件
 *
 * @author Sun Dasheng
 */
public class PutCommand extends AbstractCommand {

    public PutCommand(String currentPath, String command) {
        super(currentPath, command);
    }

    @Override
    public void execute(FileSystem fileSystem, LineReader lineReader) throws Exception {
        String localFile = getArgs(0);
        String filename = getArgs(1);
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
            System.out.println("文件不存在：" + file.getAbsolutePath());
            return;
        }
        upload(fileSystem, filename, localFile);
    }
}
