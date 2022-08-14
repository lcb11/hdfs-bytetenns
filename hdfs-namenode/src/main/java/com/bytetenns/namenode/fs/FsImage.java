package com.bytetenns.namenode.fs;

/**
  * @Author lcb
  * @Description  代表fsimage文件
  * @Date 2022/8/10
  * @Param
  * @return
  **/

public class FsImage {

    //当前最大的txid
    private long maxTxid;
    //fsImage文件内容
    private String fsImageJson;

    public FsImage() {
    }

    public FsImage(long maxTxid, String fsImageJson) {
        this.maxTxid = maxTxid;
        this.fsImageJson = fsImageJson;
    }

    public long getMaxTxid() {
        return maxTxid;
    }

    public void setMaxTxid(long maxTxid) {
        this.maxTxid = maxTxid;
    }

    public String getFsImageJson() {
        return fsImageJson;
    }

    public void setFsImageJson(String fsImageJson) {
        this.fsImageJson = fsImageJson;
    }
}
