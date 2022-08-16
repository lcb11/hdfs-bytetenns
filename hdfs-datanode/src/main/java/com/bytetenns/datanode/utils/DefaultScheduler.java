package com.bytetenns.datanode.utils;


import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.MDC;

import java.util.Random;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 调度器
 *
 * @author gongwei
 */
@Slf4j
public class DefaultScheduler {

    private ScheduledThreadPoolExecutor executor;
    private AtomicInteger schedulerThreadId = new AtomicInteger(0);
    private AtomicBoolean shutdown = new AtomicBoolean(true);

    public DefaultScheduler(String threadNamePrefix) {
        this(threadNamePrefix, Runtime.getRuntime().availableProcessors() * 2, true);
    }

    public DefaultScheduler(String threadNamePrefix, int threads) {
        this(threadNamePrefix, threads, true);
    }

    public DefaultScheduler(String threadNamePrefix, int threads, boolean daemon) {
        if (shutdown.compareAndSet(true, false)) {
            executor = new ScheduledThreadPoolExecutor(threads,
                    r -> new DefaultThread(threadNamePrefix + schedulerThreadId.getAndIncrement(), r, daemon));
            executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
            executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        }
    }

    /**
     * 调度任务
     *
     * @param name 任务名称
     * @param r    任务
     */
    public void scheduleOnce(String name, Runnable r) {
        scheduleOnce(name, r, 0);
    }

    /**
     * 调度任务
     *
     * @param name  任务名称
     * @param r     任务
     * @param delay 延迟
     */
    public void scheduleOnce(String name, Runnable r, long delay) {
        schedule(name, r, delay, 0, TimeUnit.MILLISECONDS);
    }

    /**
     * 调度任务
     *
     * @param name     任务名称
     * @param r        任务
     * @param delay    延迟执行时间
     * @param period   调度周期
     * @param timeUnit 时间单位
     */
    public void schedule(String name, Runnable r, long delay, long period, TimeUnit timeUnit) {
        if (log.isDebugEnabled()) {
            log.debug("Scheduling task {} with initial delay {} ms and period {} ms.", name, delay, period);
        }
        Runnable delegate = () -> {
            try {
                if (log.isTraceEnabled()) {
                    log.trace("Beginning execution of scheduled task {}.", name);
                }
                String loggerId = DigestUtils.md5Hex("" + System.nanoTime() + new Random().nextInt());
                MDC.put("logger_id", loggerId);
                r.run();
            } catch (Throwable e) {
                log.error("Uncaught exception in scheduled task {} :", name, e);
            } finally {
                if (log.isTraceEnabled()) {
                    log.trace("Completed execution of scheduled task {}.", name);
                }
                MDC.remove("logger_id");
            }
        };
        if (shutdown.get()) {
            return;
        }
        if (period > 0) {
            executor.scheduleWithFixedDelay(delegate, delay, period, timeUnit);
        } else {
            executor.schedule(delegate, delay, timeUnit);
        }
    }

    /**
     * 优雅停止
     */
    public void shutdown() {
        if (shutdown.compareAndSet(false, true)) {
            log.info("Shutdown DefaultScheduler.");
            executor.shutdown();
        }
    }

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
}
