package com.bytetenns.namenode.editlog;


import com.alibaba.fastjson.JSONObject;

/**
  * @Author lcb
  * @Description  edit log 日志,代表一条EditLog
  * @Date 2022/8/10
  * @Param
  * @return
  **/
public class EditLogWrapper {
    //每条edit log的id
    private long txid;
    //每条edit log的类容
    private String content;

    public long getTxid() {
        return txid;
    }

    public void setTxid(long txid) {
        this.txid = txid;
        //每次设置txid时，由于网络通信数据包为json格式，所以每次需要讲content对应的json串进行更新
        JSONObject jsonObject = JSONObject.parseObject(content);
        jsonObject.put("txid",txid);
        content=jsonObject.toJSONString();
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}
