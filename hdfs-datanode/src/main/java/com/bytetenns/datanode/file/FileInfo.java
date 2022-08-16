package com.bytetenns.datanode.file;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 文件信息
 *
 * @author Sun Dasheng
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class FileInfo {

    private String hostname;

    private String fileName;

    private long fileSize;

}
