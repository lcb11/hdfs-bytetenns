package com.bytetenns.common.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 时间工具类
 */
public class DateUtils {

    public static final String PATTER_DEFAULT = "yyyy-MM-dd HH:mm:ss";

    public static final String PATTERN_DATE = "yyyy-MM-dd";

    public static final String PATTERN_DATE_1 = "yyyyMMdd";

    public static final String PATTERN_DATE_2 = "yyyyMM";

    public static final String PATTERN_DATE_3 = "yyyy-MM-dd HH";

    public static final String PATTERN_TIME = "HH:mm";

    public static final String PATTERN_MONTY_DAY = "MM-dd";

    /**
     * 将日期格式化为字符串
     *
     * @param date 日期
     * @return 字符串
     */
    public static String format(Date date) {
        return format(date, PATTER_DEFAULT);
    }

    /**
     * 将日期格式化为字符串
     *
     * @param date 日期
     * @return 字符串
     */
    public static String format(Date date, String pattern) {
        SimpleDateFormat format = new SimpleDateFormat(pattern);
        return format.format(date);
    }

    /**
     * 格式化时间
     *
     * @param dateStr 时间字符串
     * @return Date对象
     */
    public static Date parse(String dateStr) {
        return parse(dateStr, PATTER_DEFAULT);
    }


    /**
     * 格式化时间
     *
     * @param dateStr 时间字符串
     * @return Date对象
     */
    public static Date parse(String dateStr, String pattern) {
        SimpleDateFormat format = new SimpleDateFormat(pattern);
        try {
            return format.parse(dateStr);
        } catch (Exception ignore) {
            return new Date();
        }
    }
}
