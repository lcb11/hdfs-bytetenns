package com.bytetenns.scheduler;

import lombok.extern.slf4j.Slf4j;

/**
 * 基础线程，添加了一些日志参数
 */
@Slf4j
public class DefaultThread extends Thread {

    public DefaultThread(final String name, boolean daemon) {
        super(name);
        configureThread(name, daemon);
    }

    public DefaultThread(final String name, Runnable runnable, boolean daemon) {
        super(runnable, name);
        configureThread(name, daemon);
    }


    private void configureThread(String name, boolean daemon) {
        setDaemon(daemon);
        setUncaughtExceptionHandler((t, e) -> log.error("Uncaught exception in thread '{}':", name, e));
    }
}
