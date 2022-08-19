package com.bytetenns.datanode.server;

import com.bytetenns.datanode.network.NetServer;
import com.bytetenns.datanode.utils.DefaultScheduler;
import com.bytetenns.datanode.conf.DataNodeConfig;
import com.bytetenns.datanode.replica.PeerDataNodes;
import lombok.extern.slf4j.Slf4j;
import com.google.protobuf.InvalidProtocolBufferException;
import com.bytetenns.datanode.netty.NettyPacket;
import com.bytetenns.datanode.enums.PacketType;
// import com.bytetenns.datanode.common.metrics.Prometheus;
import com.bytetenns.datanode.network.AbstractChannelHandler;
import com.bytetenns.datanode.network.RequestWrapper;
import com.bytetenns.datanode.network.file.DefaultFileSendTask;
import com.bytetenns.datanode.network.file.FilePacket;
import com.bytetenns.datanode.network.file.FileReceiveHandler;
import com.bytetenns.datanode.namenode.NameNodeClient;
import com.bytetenns.datanode.model.common.GetFileRequest;
import com.bytetenns.datanode.model.datanode.PeerNodeAwareRequest;
import com.bytetenns.datanode.storage.StorageManager;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.SocketChannel;

// import com.bytetenns.dfs.common.metrics.MetricsHandler;
import com.bytetenns.datanode.network.BaseChannelInitializer;
import com.bytetenns.datanode.server.HttpFileServerHandler;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.stream.ChunkedWriteHandler;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import java.util.Arrays;
import java.util.Collections;

/**
 * 用于上传、下载文件的Netty服务端
 *
 * @author gongwei
 */

@Slf4j
public class DataNodeServer {

  private DataNodeApis dataNodeApis;
  private StorageManager storageManager;
  private NetServer netServer;
  private DataNodeConfig dataNodeConfig;
  private PeerDataNodes peerDataNodes;

  public DataNodeServer(DataNodeConfig dataNodeConfig, DefaultScheduler defaultScheduler, StorageManager storageManager,
      PeerDataNodes peerDataNodes, DataNodeApis dataNodeApis) {
    this.dataNodeConfig = dataNodeConfig;
    this.peerDataNodes = peerDataNodes;
    this.storageManager = storageManager;
    this.dataNodeApis = dataNodeApis;
    this.netServer = new NetServer("DataNode-Server", defaultScheduler, dataNodeConfig.getDataNodeWorkerThreads());
  }

  /**
   * 启动
   */
  public void start() throws InterruptedException {
    // 用于接收PeerDataNode发过来的通知信息
    MultiPortChannelInitializer multiPortChannelInitializer = new MultiPortChannelInitializer(dataNodeConfig,
        storageManager);
    multiPortChannelInitializer.addHandlers(Collections.singletonList(dataNodeApis));
    this.netServer.setChannelInitializer(multiPortChannelInitializer);
    this.netServer.bind(Arrays.asList(dataNodeConfig.getDataNodeTransportPort(), dataNodeConfig.getDataNodeHttpPort()));
  }

  /**
   * 优雅停止
   */
  public void shutdown() {
    log.info("Shutdown DataNodeServer");
    this.netServer.shutdown();
    this.peerDataNodes.shutdown();
  }


  /**
   * 绑定多端口的渠道处理器
   *
   * @author Sun Dasheng
   */
  public class MultiPortChannelInitializer extends BaseChannelInitializer {

    private StorageManager storageManager;
    private DataNodeConfig dataNodeConfig;

    public MultiPortChannelInitializer(DataNodeConfig dataNodeConfig, StorageManager storageManager) {
      this.dataNodeConfig = dataNodeConfig;
      this.storageManager = storageManager;
    }

    @Override
    protected void initChannel(SocketChannel ch) {
      int localPort = ch.localAddress().getPort();
      if (localPort == dataNodeConfig.getDataNodeTransportPort()) {
        super.initChannel(ch);
      } else if (localPort == dataNodeConfig.getDataNodeHttpPort()) {
        ch.pipeline().addLast(new HttpServerCodec());
        ch.pipeline().addLast(new HttpObjectAggregator(65536));
        ch.pipeline().addLast(new ChunkedWriteHandler());
        ch.pipeline().addLast(new HttpFileServerHandler(storageManager));
      }
    }
  }

}
