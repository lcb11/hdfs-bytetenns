package com.bytetenns.common.metrics;

import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;

/**
  * @Author lcb
  * @Description 统计时间窗口次数工具
  * @Date 2022/8/20
  * @Param
  * @return
  **/
public class RollingWindow {

    /*** 槽组*/
    private Bucket[] buckets;

    /*** 时间片*/
    private long bucketTimeSlice;

    /*** 目标槽位下标*/
    private volatile Integer targetBucketPosition;

    /*** 临界跨槽时的时间点*/
    private volatile long lastPassTimeCloseToTargetBucket;

    /*** 刷新槽位时使用的锁*/
    private ReentrantLock enterNextBucketLock;

    /*** 固定窗口的最大qps*/
    private volatile long maxSummary;

    public RollingWindow() {
        this(60, 1000);
    }

    private RollingWindow(int bucketNum, int millerSecond) {
        this.buckets = new Bucket[bucketNum];
        this.bucketTimeSlice = millerSecond;
        this.enterNextBucketLock = new ReentrantLock();
        this.lastPassTimeCloseToTargetBucket = System.currentTimeMillis() - (2 * bucketTimeSlice);
        this.maxSummary = 0;
        for (int i = 0; i < bucketNum; i++) {
            this.buckets[i] = new Bucket();
        }
    }

    /**
     * 获取当前秒的QPS
     */
    public long getCurrentQps() {
        long time = System.currentTimeMillis();
        int currentBucketIndex = (int) (time / bucketTimeSlice) % buckets.length;
        /*
         * qps统计后 lastPassTimeCloseToTargetBucket 会逐步趋近到槽界界点
         */
        long qps = 0;
        if (time - lastPassTimeCloseToTargetBucket >= bucketTimeSlice) {
            // 如果当前时间窗口和上一次统计的时间窗口相隔超过设定的时间窗口，表示最近一个时间窗口没有统计，直接返回0.
            // 可以避免 比如：有个10个槽，每个槽1秒，1-10秒都有请求到来， 10-20秒没有任何请求，在15秒的时候获取qps，
            // 此时得到的是槽1中记录的是4-5秒的qps，而不是14-15秒的qps
            return qps;
        } else {
            // 上一个时间窗口下标
            int lastIndex = currentBucketIndex == 0 ? buckets.length - 1 : currentBucketIndex - 1;
            // 计算从当前时间窗口已经过来多少秒，比如一个时间窗口是1秒，当前已经过了0.3s
            long duration = (time - lastPassTimeCloseToTargetBucket) % bucketTimeSlice;

            // 假设当前时间窗口已经过了0.3s，则总的qps等于上一秒的0.7s数量 + 当前时间窗口的0.3秒的和。这里就是计算上一秒的0.7s的qps，用总qps乘以相应比例即可
            long slideCount = (long) (buckets[lastIndex].sum() * ((bucketTimeSlice - duration) % bucketTimeSlice) * 0.001);

            // 当前时间窗口0.3s的qps
            long currentSum = buckets[currentBucketIndex].sum();

            // 总的qps等于上一秒的0.7s数量 + 当前时间窗口的0.3秒的和
            qps = slideCount + currentSum;
        }
        return qps;
    }

    /**
     * 历史最大槽值统计
     */
    public long getMaxQps() {
        if (maxSummary == 0) {
            return getCurrentQps();
        }
        return maxSummary;
    }


    /**
     * 计数
     */
    public void hit(int number) {
        long currentTime = System.currentTimeMillis();
        if (targetBucketPosition == null) {
            // 定位到当前时间代表的时间槽
            targetBucketPosition = (int) (currentTime / bucketTimeSlice) % buckets.length;
        }
        Bucket currentBucket = buckets[targetBucketPosition];
        if (currentTime - lastPassTimeCloseToTargetBucket < bucketTimeSlice) {
            // 没有跨时间槽
            currentBucket.incr(number);
            return;
        }
        if (enterNextBucketLock.isLocked()) {
            currentBucket.incr(number);
            return;
        }
        enterNextBucketLock.lock();
        try {
            // 可以尝试用tryLock
            if (currentTime - lastPassTimeCloseToTargetBucket >= bucketTimeSlice) {
                // 如果跨时间槽，重新获取时间槽
                int nextTargetBucketPosition = (int) (currentTime / bucketTimeSlice) % buckets.length;
                Bucket nextBucket = buckets[nextTargetBucketPosition];
                if (!nextBucket.equals(currentBucket)) {
                    // 跨槽
                    long summary = buckets[targetBucketPosition].sum();
                    if (summary > maxSummary) {
                        maxSummary = summary;
                    }
                    nextBucket.reset(currentTime);
                    //目标槽位变动
                    targetBucketPosition = nextTargetBucketPosition;
                }
                lastPassTimeCloseToTargetBucket = currentTime;
                currentBucket = nextBucket;
            } else {
                currentBucket = buckets[targetBucketPosition];
            }
            currentBucket.incr(number);
        } finally {
            enterNextBucketLock.unlock();
        }
    }

    private static final class Bucket {
        /*** 槽内计数器*/
        private LongAdder adder;
        /*** 第一次时间，只记录一次*/
        private long firstPassTime;

        public Bucket() {
            adder = new LongAdder();
            firstPassTime = System.currentTimeMillis();
        }

        /*** 计数*/
        public void incr(int number) {
            adder.add(number);
        }

        /*** 重制*/
        public void reset(long time) {
            adder.reset();
            firstPassTime = time;
        }

        /***
         * 统计
         */
        public long sum() {
            return adder.sum();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Bucket bucket = (Bucket) o;
            return firstPassTime == bucket.firstPassTime &&
                    Objects.equals(adder, bucket.adder);
        }

        @Override
        public int hashCode() {
            return Objects.hash(adder, firstPassTime);
        }
    }
}