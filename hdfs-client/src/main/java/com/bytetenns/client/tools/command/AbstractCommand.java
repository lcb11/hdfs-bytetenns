package com.bytetenns.client.tools.command;



import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import com.bytetenns.client.FileSystem;
import com.bytetenns.client.utils.ConsoleTable;
import com.bytetenns.client.utils.ProgressBar;
import com.bytetenns.common.network.file.OnProgressListener;
import com.bytetenns.common.utils.FileUtil;
import com.bytetenns.common.utils.PrettyCodes;
import com.bytetenns.common.utils.StringUtils;

/**
 * 抽象命令
 *
 * @author Sun Dasheng
 */
public abstract class AbstractCommand implements Command {

    private static final String PARENT_DIR = "..";
    protected String command;
    protected String currentPath;
    private String cmd;
    private String[] args;


    public AbstractCommand(String currentPath, String command) {
        this.currentPath = currentPath;
        this.command = command;
        parseCommand();
    }

    private void parseCommand() {
        if (command == null || command.length() == 0) {
            return;
        }
        List<String> argList = new ArrayList<>();
        String[] split = command.split(" ");
        for (String arg : split) {
            if (arg == null || arg.length() == 0) {
                continue;
            }
            if (cmd == null) {
                cmd = arg;
                continue;
            }
            argList.add(arg);
        }
        if (argList.size() > 0) {
            args = new String[argList.size()];
            for (int i = 0; i < argList.size(); i++) {
                args[i] = argList.get(i);
            }
        }
    }

    protected String getCmd() {
        return cmd;
    }

    protected String getArgs(int index) {
        if (args == null || args.length <= index) {
            return null;
        }
        return args[index];
    }

    /**
     * 合并路径
     */
    protected String concatPath(String path) {
        String result = null;
        // 如果以根路径\为开头的话直接返回该路径
        if (path.startsWith(File.separator)) {
            boolean ret = StringUtils.validateFileName(path);
            if (!ret) {
                System.out.println("无法识别路径：" + path);
                return null;
            }
            return path;
        } else {
        // 如果不是则考虑其他情况
            // 处理cd ..命令
            if (path.equalsIgnoreCase(PARENT_DIR)) {
                if (!currentPath.equals(File.separator)) {
                    int i = currentPath.lastIndexOf('/');
                    result = currentPath.substring(0, Math.max(1, i));
                }
            } else {
                // 路径中包含..的话直接返回报错
                if (path.contains(PARENT_DIR)) {
                    System.out.println("无法识别路径：" + path);
                    return null;
                // 如果当前路径为\(根目录)的话
                } else if (currentPath.equals(File.separator)) {
                    result = currentPath + path;
                } else {
                    // 进行路径的拼接
                    result = (currentPath.endsWith(File.separator) ? currentPath : (currentPath + File.separator)) + path;
                }
            }
        }
        return result;
    }

    /**
     * 打印Map集合
     *
     * @param source 集合
     */
    protected void printMap(Map<String, String> source) {
        List<ConsoleTable.Cell> header = new ArrayList<ConsoleTable.Cell>() {{
            add(new ConsoleTable.Cell("key"));
            add(new ConsoleTable.Cell("value"));
        }};
        List<List<ConsoleTable.Cell>> body = new ArrayList<>();
        for (Map.Entry<String, String> entry : source.entrySet()) {
            List<ConsoleTable.Cell> rows = new ArrayList<>(3);
            rows.add(new ConsoleTable.Cell(entry.getKey()));
            rows.add(new ConsoleTable.Cell(entry.getValue()));
            body.add(rows);
        }
        new ConsoleTable.ConsoleTableBuilder().addHeaders(header).addRows(body).build().print();
    }

    protected void download(FileSystem fileSystem, String src, String dest) throws Exception {
        download(fileSystem, src, dest, "");
    }

    /**
     * 下载一个文件
     *
     * @param fileSystem 文件路径
     * @param filename   源文件
     * @param localFile  本地路径
     * @throws Exception 异常
     */
    protected void download(FileSystem fileSystem, String filename, String localFile, String desc) throws Exception {
        System.out.println("下载文件【" + filename + "】到【" + localFile + "】");
        CountDownLatch latch = new CountDownLatch(1);
        ProgressBar progressBar = new ProgressBar();
        fileSystem.get(filename, localFile, new OnProgressListener() {
            @Override
            public void onProgress(long total, long current, float progress, int currentReadBytes) {
                progressBar.printProgress(FileUtil.formatSize(current), FileUtil.formatSize(total), progress, desc);
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        });
        latch.await();
    }

    protected void upload(FileSystem fileSystem, String filename, String localFile) throws Exception {
        System.out.println("上传文件【" + localFile + "】到【" + filename + "】");
        fileSystem.put(filename, new File(localFile));
    }
}
