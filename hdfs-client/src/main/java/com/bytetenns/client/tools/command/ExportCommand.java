package com.ruyuan.dfs.client.tools.command;

import com.google.protobuf.ProtocolStringList;
import com.ruyuan.dfs.client.FileSystem;
import com.ruyuan.dfs.client.utils.ProgressBar;
import com.ruyuan.dfs.common.utils.FileUtil;
import com.ruyuan.dfs.model.client.GetAllFilenameResponse;
import com.ruyuan.dfs.model.client.PreCalculateResponse;
import org.jline.reader.LineReader;

import java.io.File;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 导出文件命令
 * 导出某个目录下所有文件到本地目录中
 *
 * @author Sun Dasheng
 */
public class ExportCommand extends AbstractCommand {

    public ExportCommand(String currentPath, String command) {
        super(currentPath, command);
    }

    @Override
    public void execute(FileSystem fileSystem, LineReader lineReader) throws Exception {
        String path = getArgs(0);
        String localDir = getArgs(1);
        if (path == null || localDir == null) {
            System.out.println("无法识别命令：" + command);
            return;
        }
        FileUtil.mkdirs(localDir);
        File dir = new File(localDir);
        PreCalculateResponse response = fileSystem.preCalculatePath(path);
        if (response.getFileCount() < 0) {
            System.out.println("该文件夹(目录)不存在文件：" + path);
        }
        System.out.println("导出目录【" + path + "】预计文件数量：" + response.getFileCount() + ", 总文件大小："
                + FileUtil.formatSize(response.getTotalSize()));
        String ret = lineReader.readLine("确认是否继续导出? (yes/no) ");
        if (!"yes".equals(ret.trim())) {
            return;
        }
        if (path.endsWith(File.separator)) {
            path = path.substring(0, path.length() - 1);
        }
        System.out.println("获取所有该目录下的文件信息...");
        GetAllFilenameResponse allFilenameByPath = fileSystem.getAllFilenameByPath(path);
        ProtocolStringList filenameList = allFilenameByPath.getFilenameList();
        System.out.println("获取所有该目录下文件信息成功，即将开始下载...");
        String tmpFolder = System.getProperty("java.io.tmpdir") + "/ruyuan-dfs-temp";
        for (int i = 0; i < filenameList.size(); i++) {
            String filename = filenameList.get(i);
            File file = new File(tmpFolder, filename);
            download(fileSystem, filename, file.getAbsolutePath(), "[" + (i + 1) + "/" + filenameList.size() + "]");
        }
        File zipFile = new File(dir, "export.zip");
        System.out.println("压缩所有文件为：" + zipFile.getAbsolutePath());
        AtomicLong currentSize = new AtomicLong(0);
        ProgressBar progressBar = new ProgressBar();
        FileUtil.zip(tmpFolder, zipFile.getAbsolutePath(), bytes -> {
            long readLength = currentSize.addAndGet(bytes);
            float v = new BigDecimal(String.valueOf(readLength)).multiply(new BigDecimal(100))
                    .divide(new BigDecimal(String.valueOf(response.getTotalSize())),
                            2, RoundingMode.HALF_UP).floatValue();
            progressBar.printProgress(FileUtil.formatSize(readLength), FileUtil.formatSize(response.getTotalSize()), v);
        });
        System.out.println("移除临时文件....");
        FileUtil.deleteDirectory(new File(tmpFolder));
        System.out.println("导出成功！！！结果文件为：" + zipFile.getAbsolutePath());
    }
}
