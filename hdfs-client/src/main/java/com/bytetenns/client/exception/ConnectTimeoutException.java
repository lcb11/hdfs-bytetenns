package com.bytetenns.client.exception;

/**
 * 连接超时异常
 *
 * @author LiZhirun
 */
public class ConnectTimeoutException extends Exception {

    public ConnectTimeoutException(String msg) {
        super(msg);
    }
}
