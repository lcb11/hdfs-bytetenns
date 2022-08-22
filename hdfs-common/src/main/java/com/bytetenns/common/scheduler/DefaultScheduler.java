package com.bytetenns.common.scheduler;

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
        if (shutdown.compareAndSet(true, false)) {  //自定义：DefaultThread
            executor = new ScheduledThreadPoolExecutor(threads,
                    r -> new DefaultThread(threadNamePrefix + schedulerThreadId.getAndIncrement(), r, daemon));
            // 当是true时，在cheduleAtFisedRate方法和scheduleWithFixedDelay方法提交的任务会继续循环执行
            executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);  // 上两个方法提交的任务不会被循环执行，但是会将等待的任务执行完毕，然后进程被销毁
            executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false); //放弃等待的任务，正在运行的任务一旦完成，则进程被销毁
            // true 时 当前正在等待的任务的和正在运行的任务需要被执行完，然后进程被销毁
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
     * 调度任务  在UserManager初始化：刷新用户数据到磁盘
     *          在DataNodeManager初始化：DataNode存活检测
     *          在DiskNameSystem初始化：定时扫描物理删除文件
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
}
