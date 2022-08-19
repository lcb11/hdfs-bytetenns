package com.ruyuan.dfs.client.utils;

import java.math.BigDecimal;

/**
 * 打印工具
 *
 * @author Sun Dasheng
 */
public class ProgressBar {

    private int lastOutputLength = 0;
    private volatile boolean isFinish = false;

    private String name;

    public ProgressBar() {
        this("Progress");
    }

    public ProgressBar(String name) {
        this.name = name;
    }

    private String getNChar(int num, char ch, boolean inProgress) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < num; i++) {
            if (i == num - 1 && inProgress) {
                builder.append(">");
            } else {
                builder.append(ch);
            }
        }
        return builder.toString();
    }

    public void printProgress(String current, String total, float progress) {
        printProgress(current, total, progress, "");
    }

    public void printProgress(String current, String total, float progress, String desc) {
        if (isFinish) {
            return;
        }
        int progressInt = (int) progress;
        BigDecimal b = new BigDecimal(progress);
        progress = b.setScale(2, BigDecimal.ROUND_HALF_UP).floatValue();
        String finish = getNChar(progressInt, '=', progressInt < 100);
        String unFinish = getNChar(100 - progressInt, ' ', false);
        String target = String.format("%s: %s [%s%s] %s/%s %s", name, progress < 100 ? "downloading" : "", finish, unFinish, current, total, desc);
        System.out.print(getNChar(lastOutputLength, '\b', false));
        lastOutputLength = target.length();
        System.out.print(target);
        if (progress >= 100) {
            System.out.println();
            isFinish = true;
        }
    }
}