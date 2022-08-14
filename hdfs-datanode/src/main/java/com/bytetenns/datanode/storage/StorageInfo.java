package com.bytetenns.datanode.storage;



import lombok.Data;

import java.util.List;

/**
 * 存储信息
 *
 * @author Sun Dasheng
 */
@Data
public class StorageInfo {

    /**
     * 文件信息
     */
    private List<FileInfo> files;
    /**
     * 已用空间
     */
    private long storageSize;
    /**
     * 可用空间
     */
    private long freeSpace;

    public StorageInfo() {
        this.storageSize = 0L;
    }
}

 class FileInfo {

    private String hostname;

    private String fileName;

    private long fileSize;

     public FileInfo(String hostname, String fileName, long fileSize) {
         this.hostname = hostname;
         this.fileName = fileName;
         this.fileSize = fileSize;
     }

     public FileInfo() {
     }

     public String getHostname() {
         return hostname;
     }

     public void setHostname(String hostname) {
         this.hostname = hostname;
     }

     public String getFileName() {
         return fileName;
     }

     public void setFileName(String fileName) {
         this.fileName = fileName;
     }

     public long getFileSize() {
         return fileSize;
     }

     public void setFileSize(long fileSize) {
         this.fileSize = fileSize;
     }

     @Override
     public String toString() {
         return "FileInfo{" +
                 "hostname='" + hostname + '\'' +
                 ", fileName='" + fileName + '\'' +
                 ", fileSize=" + fileSize +
                 '}';
     }
 }
