package com.bytetenns.client.config;

import lombok.Builder;
import lombok.Data;

/**
 * 客户端配置
 *
 * @author LiZhirun
 */
@Data
@Builder
public class FsClientConfig {
    private String server;
    private int port;
    private String username;
    private String secret;
    private volatile String userToken;
    private int connectRetryTime;
    private int ack = 0;
}
