package com.bytetenns.datanode;

import com.bytetenns.datanode.namenode.NameNodeClient;
import com.bytetenns.datanode.replica.PeerDataNodes;
import com.bytetenns.datanode.server.DataNodeServer;
import com.bytetenns.datanode.storage.StorageManager;
// import com.ruyuan.dfs.common.metrics.Prometheus;
import com.bytetenns.datanode.utils.DefaultScheduler;
import com.bytetenns.datanode.conf.DataNodeConfig;
import com.bytetenns.datanode.server.DataNodeServer.*;
import com.bytetenns.datanode.server.DataNodeServer;
import com.bytetenns.datanode.server.DefaultFileTransportCallback;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
   * @Author byte tens
   * @Description dataNode 核心启动类
   * @Date 2022/8/12
   * @Param
   * @return
   **/
public class DataNode {

    //同级dataNode
    private PeerDataNodes peerDataNodes;
    //存储信息管理
    private StorageManager storageManager;
    //负责跟NameNode进行通讯
    private NameNodeClient nameNodeClient;
    //同级节点通讯
    private DataNodeServer dataNodeServer;

    public static void main(String[] args) {
      if (args == null || args.length == 0) {
          throw new IllegalArgumentException("配置文件不能为空");
      }
      // 1. 初始化配置文件
      DataNodeConfig dataNodeConfig = null;
      try {
          Path path = Paths.get(args[0]);
          try (InputStream inputStream = Files.newInputStream(path)) {
              Properties properties = new Properties();
              properties.load(inputStream);
              dataNodeConfig = DataNodeConfig.parse(properties);
          }
          log.info("DameNode启动配置文件: {}", path.toAbsolutePath());
      } catch (Exception e) {
          log.error("无法加载配置文件 : ", e);
          System.exit(1);
      }
      parseOption(args, dataNodeConfig);
      try {
          DataNode datanode = new DataNode(dataNodeConfig);
          Runtime.getRuntime().addShutdownHook(new Thread(datanode::shutdown));
          datanode.start();
      } catch (Exception e) {
          log.error("启动DataNode失败：", e);
          System.exit(1);
      }
  }


  private static void parseOption(String[] args, DataNodeConfig dataNodeConfig) {
      OptionParser parser = new OptionParser();
      OptionSpec<String> baseDirOpt = parser.accepts("baseDir").withOptionalArg().ofType(String.class);
      OptionSpec<String> datanodeTransportServerOpt = parser.accepts("datanodeTransportServer").withOptionalArg().ofType(String.class);
      OptionSpec<String> datanodeHttpServerOpt = parser.accepts("datanodeHttpServer").withOptionalArg().ofType(String.class);
      OptionSpec<String> nameNodeServersOpt = parser.accepts("nameNodeServers").withOptionalArg().ofType(String.class);
      OptionSpec<Integer> dataNodeIdOpt = parser.accepts("dataNodeId").withOptionalArg().ofType(Integer.class);
      OptionSpec<Integer> workerThreadOpt = parser.accepts("workerThread").withOptionalArg().ofType(Integer.class);
      parser.allowsUnrecognizedOptions();
      OptionSet parse = parser.parse(args);
      if (parse.has(baseDirOpt)) {
          String baseDir = parse.valueOf(baseDirOpt);
          dataNodeConfig.setBaseDir(baseDir);
          log.info("从参数读取到配置进行替换：[key={}, value={}]", "baseDir", baseDir);
      }
      if (parse.has(datanodeTransportServerOpt)) {
          String datanodeTransportServer = parse.valueOf(datanodeTransportServerOpt);
          dataNodeConfig.setDataNodeTransportServer(datanodeTransportServer);
          log.info("从参数读取到配置进行替换：[key={}, value={}]", "datanodeTransportServer", datanodeTransportServer);
      }
      if (parse.has(datanodeHttpServerOpt)) {
          String datanodeHttpServer = parse.valueOf(datanodeHttpServerOpt);
          dataNodeConfig.setDataNodeHttpServer(datanodeHttpServer);
          log.info("从参数读取到配置进行替换：[key={}, value={}]", "datanodeHttpServer", datanodeHttpServer);
      }
      if (parse.has(nameNodeServersOpt)) {
          String nameNodeServers = parse.valueOf(nameNodeServersOpt);
          dataNodeConfig.setNameNodeServers(nameNodeServers);
          log.info("从参数读取到配置进行替换：[key={}, value={}]", "nameNodeServers", nameNodeServers);
      }
      if (parse.has(dataNodeIdOpt)) {
          Integer dataNodeId = parse.valueOf(dataNodeIdOpt);
          dataNodeConfig.setDataNodeId(dataNodeId);
          log.info("从参数读取到配置进行替换：[key={}, value={}]", "dataNodeId", dataNodeId);
      }

      if (parse.has(workerThreadOpt)) {
          Integer workerThread = parse.valueOf(workerThreadOpt);
          dataNodeConfig.setDataNodeWorkerThreads(workerThread);
          log.info("从参数读取到配置进行替换：[key={}, value={}]", "workerThread", workerThread);
      }
  }


  public DataNode(DataNodeConfig dataNodeConfig) {
      this.defaultScheduler = new DefaultScheduler("DataNode-Scheduler-");
      this.storageManager = new StorageManager(dataNodeConfig);
      DefaultFileTransportCallback defaultFileTransportCallback = new DefaultFileTransportCallback(storageManager);
      DataNodeApis dataNodeApis = new DataNodeApis(dataNodeConfig, defaultFileTransportCallback, defaultScheduler);
      this.peerDataNodes = new PeerDataNodes(defaultScheduler, dataNodeConfig, dataNodeApis);
      this.nameNodeClient = new NameNodeClient(storageManager, dataNodeConfig, defaultScheduler, peerDataNodes);
      this.dataNodeServer = new DataNodeServer(dataNodeConfig, defaultScheduler, storageManager, peerDataNodes, dataNodeApis);
  }

  /**
   * 启动DataNode
   *
   * @throws InterruptedException 中断异常
   */
  private void start() throws InterruptedException {
      if (started.compareAndSet(false, true)) {
          this.nameNodeClient.start();
          this.dataNodeServer.start();
      }
  }

  /**
   * 优雅停止
   */
  public void shutdown() {
      if (started.compareAndSet(true, false)) {
          this.defaultScheduler.shutdown();
          this.nameNodeClient.shutdown();
          this.dataNodeServer.shutdown();
      }
  }
}
