package com.ruyuan.dfs.client.tools.command;

import com.ruyuan.dfs.client.FileSystem;
import com.ruyuan.dfs.client.utils.ProgressBar;
import com.ruyuan.dfs.common.utils.FileUtil;
import com.ruyuan.dfs.common.utils.PrettyCodes;
import org.jline.reader.LineReader;

import java.io.File;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 导入命令
 *
 * <pre>
 *
 * 导入某个目录下文件到系统中
 *
 * </pre>
 *
 * @author Sun Dasheng
 */
public class ImportCommand extends AbstractCommand {

    public ImportCommand(String currentPath, String command) {
        super(currentPath, command);
    }

    @Override
    public void execute(FileSystem fileSystem, LineReader lineReader) throws Exception {
        String importFile = getArgs(0);
        if (importFile == null) {
            System.out.println("无法识别命令：" + command);
            return;
        }
        File file = new File(importFile);
        if (!file.exists()) {
            System.out.println("文件不存在：" + importFile);
            return;
        }
        String tmpFolder = System.getProperty("java.io.tmpdir") + "ruyuan-dfs-temp";
        AtomicLong currentSize = new AtomicLong(0);
        ProgressBar progressBar = new ProgressBar();
        System.out.println("解压文件到：" + tmpFolder);
        String totalSize = FileUtil.formatSize(file.length());
        FileUtil.unzip(importFile, tmpFolder, bytes -> {
            long readLength = currentSize.addAndGet(bytes);
            float v = new BigDecimal(String.valueOf(readLength)).multiply(new BigDecimal(100))
                    .divide(new BigDecimal(String.valueOf(file.length())),
                            2, RoundingMode.HALF_UP).floatValue();
            progressBar.printProgress(FileUtil.formatSize(readLength), totalSize, v);
        });
        File folder = new File(tmpFolder);
        Map<String, String> result = new HashMap<>(PrettyCodes.trimMapSize());
        find(folder.listFiles(), "/", result);
        for (Map.Entry<String, String> entry : result.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            try {
                upload(fileSystem, value, key);
            } catch (Exception e) {
                System.out.println("上传文件失败：" + value + ", 原因是：" + e.getMessage());
            }
        }
        FileUtil.deleteDirectory(new File(tmpFolder));
        System.out.println("导入成功！！！");
    }

    private static void find(File[] files, String baseFolder, Map<String, String> result) {
        if (files == null || files.length == 0) {
            return;
        }
        for (File file : files) {
            if (file.isDirectory()) {
                find(file.listFiles(), baseFolder + file.getName() + File.separator, result);
            } else {
                result.put(file.getAbsolutePath(), baseFolder + file.getName());
            }
        }
    }
}
