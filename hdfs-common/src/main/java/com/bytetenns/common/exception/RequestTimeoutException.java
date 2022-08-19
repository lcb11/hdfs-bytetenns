package com.bytetenns.common.exception;

/**
 * 请求超时异常
 */
public class RequestTimeoutException extends Exception {
    public RequestTimeoutException(String s) {
        super(s);
    }
}
