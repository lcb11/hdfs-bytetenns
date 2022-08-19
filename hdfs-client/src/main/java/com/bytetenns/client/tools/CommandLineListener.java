package com.ruyuan.dfs.client.tools;

/**
 * 命令行监听器
 *
 * @author Sun Dasheng
 */
public interface CommandLineListener {

    /**
     * 连接失败监听器
     */
    void onConnectFailed();

    /**
     * 认证结果
     *
     * @param result 结果
     */
    void onAuthResult(boolean result);

}
