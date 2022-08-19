package com.bytetenns.common.network.file;


import com.bytetenns.common.utils.PrettyCodes;

import java.util.HashMap;
import java.util.Map;

/**
 * 文件属性
 */
public class FileAttribute {

    private Map<String, String> attr;

    public FileAttribute() {
        this(new HashMap<>(PrettyCodes.trimMapSize()));
    }

    public FileAttribute(Map<String, String> attr) {
        this.attr = attr;
    }

    public Map<String, String> getAttr() {
        return attr;
    }

    public String getFilename() {
        return attr.get("filename");
    }


    public void setFileName(String filename) {
        attr.put("filename", filename);
    }

    public long getSize() {
        return Integer.parseInt(attr.get("size"));
    }

    public void setSize(long size) {
        attr.put("size", String.valueOf(size));
    }

    public String getId() {
        return attr.get("id");
    }

    public void setId(String id) {
        attr.put("id", id);
    }

    public String getMd5() {
        return attr.get("md5");
    }

    public void setMd5(String md5) {
        attr.put("md5", md5);
    }

    public void setAbsolutePath(String absolutePath) {
        attr.put("absolutePath", absolutePath);
    }

    public String getAbsolutePath() {
        return attr.get("absolutePath");
    }
}
