package com.bytetenns.client;


import com.bytetenns.Constants;
import com.bytetenns.dfs.model.backup.INode;
import com.bytetenns.utils.FileUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * 文件信息
 *
 * @author Sun Dasheng
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class FsFile {
    private int type;
    private String path;
    private String fileSize;

    public static List<FsFile> parse(INode node) {
        if (node == null) {
            return new ArrayList<>();
        }
        List<INode> childrenList = node.getChildrenList();
        if (childrenList.isEmpty()) {
            return new ArrayList<>();
        }
        List<FsFile> fsFiles = new ArrayList<>();
        for (INode iNode : childrenList) {
            FsFile fsFile = new FsFile();
            fsFile.setPath(iNode.getPath());
            fsFile.setType(iNode.getType());
            long fileSize = Long.parseLong(iNode.getAttrMap().getOrDefault(Constants.ATTR_FILE_SIZE, "0"));
            fsFile.setFileSize(FileUtil.formatSize(fileSize));
            fsFiles.add(fsFile);
        }
        return fsFiles;
    }
}
