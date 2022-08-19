package com.ruyuan.dfs.client.tools.command;

import com.ruyuan.dfs.client.FileSystem;
import com.ruyuan.dfs.model.client.ReadStorageInfoResponse;
import org.jline.reader.LineReader;

/**
 * 查看某个文件信息
 *
 * <pre>
 *     info /path/file.txt
 * </pre>
 *
 * @author Sun Dasheng
 */

public class InfoCommand extends AbstractCommand {

    public InfoCommand(String currentPath, String command) {
        super(currentPath, command);
    }

    @Override
    public void execute(FileSystem fileSystem, LineReader lineReader) throws Exception {
        String args = getArgs(0);
        if (args == null) {
            System.out.println("无法识别命令：" + command);
            return;
        }
        String filename = concatPath(args);
        if (filename == null) {
            return;
        }
        ReadStorageInfoResponse readStorageInfoResponse = fileSystem.readStorageInfo(filename);
        String datanodes = readStorageInfoResponse.getDatanodes();
        int replica = readStorageInfoResponse.getReplica();
        System.out.println("Replica: [num=" + replica + ", datanodes=" + datanodes + "]");
    }
}
