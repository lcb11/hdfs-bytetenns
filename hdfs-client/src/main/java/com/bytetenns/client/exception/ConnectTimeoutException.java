package com.ruyuan.dfs.client.exception;

/**
 * 连接超时异常
 *
 * @author Sun Dasheng
 */
public class ConnectTimeoutException extends Exception {

    public ConnectTimeoutException(String msg) {
        super(msg);
    }
}
