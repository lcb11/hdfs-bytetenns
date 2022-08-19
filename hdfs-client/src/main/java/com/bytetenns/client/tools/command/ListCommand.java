package com.ruyuan.dfs.client.tools.command;

import com.ruyuan.dfs.client.FileSystem;
import com.ruyuan.dfs.client.FsFile;
import com.ruyuan.dfs.client.utils.ConsoleTable;
import com.ruyuan.dfs.common.enums.NodeType;
import org.jline.reader.LineReader;

import java.util.ArrayList;
import java.util.List;

/**
 * 列出当前目录的命令
 *
 * @author Sun Dasheng
 */
public class ListCommand extends AbstractCommand {

    private String currentPath;

    public ListCommand(String currentPath, String command) {
        super(currentPath, command);
        this.currentPath = currentPath;
    }

    @Override
    public void execute(FileSystem fileSystem, LineReader lineReader) throws Exception {
        List<FsFile> fsFiles = fileSystem.listFile(currentPath);
        List<ConsoleTable.Cell> header = new ArrayList<ConsoleTable.Cell>() {{
            add(new ConsoleTable.Cell("name"));
            add(new ConsoleTable.Cell("type"));
            add(new ConsoleTable.Cell("size"));
        }};
        List<List<ConsoleTable.Cell>> body = new ArrayList<>();
        for (FsFile fsFile : fsFiles) {
            List<ConsoleTable.Cell> rows = new ArrayList<>(3);
            rows.add(new ConsoleTable.Cell(fsFile.getPath()));
            rows.add(new ConsoleTable.Cell(fsFile.getType() == NodeType.FILE.getValue() ? "file" : "dir"));
            rows.add(new ConsoleTable.Cell(fsFile.getType() == NodeType.FILE.getValue() ? fsFile.getFileSize() : "--"));
            body.add(rows);
        }
        new ConsoleTable.ConsoleTableBuilder().addHeaders(header).addRows(body).build().print();
    }
}
