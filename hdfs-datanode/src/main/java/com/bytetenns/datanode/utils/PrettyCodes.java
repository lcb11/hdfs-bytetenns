package com.bytetenns.datanode.utils;


/**
 * 时间工具类
 *
 * @author gongwei
 */
public final class PrettyCodes {
    public static int trimMapSize(int count) {
        return (int) ((float) count / 0.75F + 1.0F);
    }

    public static int trimMapSize() {
        return 32;
    }

    public static int trimListSize() {
        return 10;
    }


    private PrettyCodes() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }
}

