package com.bytetenns.datanode.server;

import com.bytetenns.common.network.NetServer;
import com.bytetenns.common.scheduler.DefaultScheduler;
import com.bytetenns.datanode.conf.DataNodeConfig;
import com.bytetenns.datanode.replica.PeerDataNodes;
import lombok.extern.slf4j.Slf4j;
import com.google.protobuf.InvalidProtocolBufferException;
import com.bytetenns.common.netty.NettyPacket;
import com.bytetenns.common.enums.PacketType;
// import com.bytetenns.datanode.common.metrics.Prometheus;
import com.bytetenns.common.network.AbstractChannelHandler;
import com.bytetenns.common.network.RequestWrapper;
import com.bytetenns.common.network.file.DefaultFileSendTask;
import com.bytetenns.common.network.file.FilePacket;
import com.bytetenns.common.network.file.FileReceiveHandler;
import com.bytetenns.datanode.namenode.NameNodeClient;
import com.bytetenns.dfs.model.common.GetFileRequest;
import com.bytetenns.dfs.model.datanode.PeerNodeAwareRequest;
import com.bytetenns.datanode.storage.StorageManager;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.SocketChannel;

// import com.bytetenns.dfs.common.metrics.MetricsHandler;
import com.bytetenns.common.network.BaseChannelInitializer;
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
    // 用于创建channel的pipeline，绑定了2个端口，分别用于下载和上传
    MultiPortChannelInitializer multiPortChannelInitializer = new MultiPortChannelInitializer(dataNodeConfig,
        storageManager);
    // 添加handlers，从而对不同类型的请求进行处理
    multiPortChannelInitializer.addHandlers(Collections.singletonList(dataNodeApis));
    // 配置MultiPortChannelInitializer
    this.netServer.setChannelInitializer(multiPortChannelInitializer);
    // 绑定本地端口，从而接受文件上传请求和Http下载请求
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
      // 获取channel所连接的本地端口号
      int localPort = ch.localAddress().getPort();
      // 对channel的端口号进行判断
      // 文件上传端口
      if (localPort == dataNodeConfig.getDataNodeTransportPort()) {
        super.initChannel(ch);
        // 文件下载端口
      } else if (localPort == dataNodeConfig.getDataNodeHttpPort()) {
        // Inbound事件触发时正序执行，只执行Inbound Handler；Outbound事件触发时逆序执行，只执行Outbound Handler

        // 用于解析http请求中的request line
        ch.pipeline().addLast(new HttpServerCodec()); // Inbound & Outbound
        // 用于解析http请求中的message body，具体详见https://blog.csdn.net/m0_45406092/article/details/104895032
        ch.pipeline().addLast(new HttpObjectAggregator(65536)); // Inbound & Outbound
        // 将文件分块写入channel
        ch.pipeline().addLast(new ChunkedWriteHandler()); // Inbound & Outbound，这里只处理Outbound，也就是将文件数据传给请求端
        // 真正实现文件下载处理，包括文件的查找和发送文件数据
        ch.pipeline().addLast(new HttpFileServerHandler(storageManager)); // Inbound
      }
    }
  }

}
