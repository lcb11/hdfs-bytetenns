package com.bytetenns.client;


import com.bytetenns.Constants;
import com.bytetenns.common.utils.FileUtil;
import com.bytetenns.dfs.model.backup.INode;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * 文件信息
 *
 * @author Li Zhirun
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class FsFile {
    private int type;
    private String path;
    private String fileSize;

    /**
     * 把INode节点解析为FsFile文件的类型，FsFile包括文件类型，
     * 文件路径和文件大小
     */
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
