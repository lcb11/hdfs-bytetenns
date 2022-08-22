package com.bytetenns.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
  * @Author byte tens
  * @Description 文件信息
  * @Date 2022/8/19
  * @Param
  * @return
  **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class FileInfo {

    private String hostname;

    private String fileName;

    private long fileSize;

}
