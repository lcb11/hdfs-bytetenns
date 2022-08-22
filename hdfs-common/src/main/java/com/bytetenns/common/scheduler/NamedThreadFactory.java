package com.bytetenns.common.scheduler;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 线程工厂
 */
public class NamedThreadFactory implements ThreadFactory {

    private boolean daemon;
    private String prefix;
    private AtomicInteger threadId = new AtomicInteger();

    public NamedThreadFactory(String prefix) {
        this(prefix, true);
    }

    public NamedThreadFactory(String prefix, boolean daemon) {
        this.prefix = prefix;
        this.daemon = daemon;
    }

    @Override
    public Thread newThread(Runnable r) {
        return new DefaultThread(prefix + threadId.getAndIncrement(), r, daemon);
    }
}
