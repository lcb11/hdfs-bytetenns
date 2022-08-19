package com.bytetenns.namenode.fs;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
  * @Author lcb
  * @Description 计算结果
  * @Date 2022/8/19
  * @Param
  * @return
  **/
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CalculateResult {

    private int fileCount;
    private long totalSize;

    public void addFileCount() {
        this.fileCount++;
    }

    public void addTotalSize(long size) {
        this.totalSize += size;
    }

    public void addFileCount(int fileCount) {
        this.fileCount += fileCount;
    }
}
