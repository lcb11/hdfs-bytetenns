package com.bytetenns.namenode.editlog;


import com.alibaba.fastjson.JSONObject;
import com.bytetenns.common.utils.ByteUtil;
import com.bytetenns.common.utils.PrettyCodes;
import com.bytetenns.dfs.model.backup.EditLog;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
  * @Author lcb
  * @Description  edit log 日志,代表一条EditLog
  * @Date 2022/8/10
  * @Param
  * @return
  **/
@Slf4j
public class EditLogWrapper {//一条editlog占4个字节

    private EditLog editLog;

    public EditLogWrapper(int opType, String path) {
        this(opType, path, new HashMap<>(PrettyCodes.trimMapSize()));
    }

    public EditLogWrapper(int opType, String path, Map<String, String> attr) {
        this.editLog = EditLog.newBuilder()
                .setOpType(opType)
                .setPath(path)
                .putAllAttr(attr)
                .build();
    }

    public EditLogWrapper(EditLog editLog) {
        this.editLog = editLog;
    }

    public EditLog getEditLog() {
        return editLog;
    }

    public void setTxId(long txId) {
        this.editLog = this.editLog.toBuilder()
                .setTxId(txId)
                .build();
    }

    public long getTxId() {
        return this.editLog.getTxId();
    }

    public byte[] toByteArray() {
        byte[] body = editLog.toByteArray();
        int bodyLength = body.length;
        byte[] ret = new byte[body.length + 4];
        ByteUtil.setInt(ret, 0, bodyLength);
        System.arraycopy(body, 0, ret, 4, bodyLength);
        return ret;
    }

    public static List<EditLogWrapper> parseFrom(byte[] bytes) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        return parseFrom(byteBuffer);
    }

    public static List<EditLogWrapper> parseFrom(ByteBuffer byteBuffer) {
        List<EditLogWrapper> ret = new LinkedList<>();
        //判断当前byteBuffer是否还有元素
        while (byteBuffer.hasRemaining()) {
            try {
                //获取当前位置后面的4个字节
                int bodyLength = byteBuffer.getInt();
                //初始化一个字节数组
                byte[] body = new byte[bodyLength];
                //将byteBuffer里面的值给boby
                byteBuffer.get(body);
                //将boby信息反序列化，得到editLog
                EditLog editLog = EditLog.parseFrom(body);
                ret.add(new EditLogWrapper(editLog));
            } catch (Exception e) {
                log.error("Parse EditLog failed.", e);
            }
        }
        return ret;
    }


}
