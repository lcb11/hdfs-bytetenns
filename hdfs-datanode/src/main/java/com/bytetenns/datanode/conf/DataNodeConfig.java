package com.bytetenns.datanode.conf;

import lombok.Data;
import lombok.Builder;

import java.util.Properties;

/**
 *
 * @author gongwei
 * @description datanode 配置
 * @date 2022/8/16
 * @param
 * @return
 **/

@Data
@Builder
public class DataNodeConfig {
  // 基础目录
  private String baseDir;
  // NameNode机器节点地址
  private String nameNodeServers;
  // DataNode监听的上传文件的服务器地址
  private String dataNodeTransportServer;
  // DataNode监听的下载文件的请球服务地址
  private String dataNodeHttpServer ;

  // DataNode心跳间隔
  private int heartbeatInterval;
  // DataNode的ID，每一个DataNode的ID都是不同的
  private int dataNodeId;
  // 根据文件名定位到本地磁盘目录文件位置的方式，可选有：simple、md5、sha1、aes
  private String fileLocatorType;
  // 上传下载文件的工作线程数量
  private int dataNodeWorkerThreads;

  public static DataNodeConfig parse(Properties properties) {
    String baseDir = (String) properties.get("base.dir");
    String nameNodeServers = (String) properties.get("namenode.servers");
    String dataNodeTransportServer = (String) properties.get("datanode.transport.server");
    String dataNodeHttpServer = (String) properties.get("datanode.http.server");
    int heartbeatInterval = Integer.parseInt((String) properties.get("datanode.heartbeat.interval"));
    int dataNodeId = Integer.parseInt((String) properties.get("datanode.id"));
    String fileLocatorType = (String) properties.get("file.locator.type");
    int dataNodeWorkerThreads = Integer.parseInt((String) properties.get("datanode.worker.threads"));
    return DataNodeConfig.builder()
            .baseDir(baseDir)
            .nameNodeServers(nameNodeServers)
            .dataNodeTransportServer(dataNodeTransportServer)
            .dataNodeHttpServer(dataNodeHttpServer)
            .heartbeatInterval(heartbeatInterval)
            .dataNodeId(dataNodeId)
            .fileLocatorType(fileLocatorType)
            .dataNodeWorkerThreads(dataNodeWorkerThreads)
            .build();
}

  public int getNameNodePort() {
    return Integer.parseInt(nameNodeServers.split(":")[1]);
  }

  public int getDataNodeHttpPort() {
    return Integer.parseInt(dataNodeHttpServer.split(":")[1]);
  }

  public String getNameNodeAddr() {
    return nameNodeServers.split(":")[0];
  }

  public int getDataNodeTransportPort() {
    return Integer.parseInt(dataNodeTransportServer.split(":")[1]);
  }

  public String getDataNodeTransportAddr() {
    return dataNodeTransportServer.split(":")[0];
  }
}
