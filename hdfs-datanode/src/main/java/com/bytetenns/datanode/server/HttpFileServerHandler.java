package com.bytetenns.datanode.server;

// import com.bytetenns.datanode.metrics.MetricsHandler;
// import com.ruyuan.dfs.common.metrics.Prometheus;
import com.bytetenns.datanode.storage.StorageManager;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.stream.ChunkedFile;
import io.netty.util.CharsetUtil;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.net.URLDecoder;

import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * 用于HTTP下载文件的Netty服务端
 *
 * @author gongwei
 */
@Slf4j
public class HttpFileServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private StorageManager storageManager;
    // private MetricsHandler metricsHandler;

    public HttpFileServerHandler(StorageManager storageManager) {
        this.storageManager = storageManager;
        // this.metricsHandler = new MetricsHandler();
    }

    /*
     * Called when a message is received from the server
     */
    @Override
    public void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
        String filename = URLDecoder.decode(request.uri(), "UTF-8");
        // if (metricsHandler.sendMetrics(ctx, request)) {
        // return;
        // }
        if (request.method().equals(HttpMethod.GET)) {
            log.debug("收到文件下载请求：[filename={}]", filename);
            // Prometheus.incCounter("datanode_http_get_file_count", "DataNode收到的下载文件请求数量");
            String absolutePath = storageManager.getAbsolutePathByFileName(filename);
            String name = filename.substring(filename.lastIndexOf("/") + 1);
            File file = new File(absolutePath);
            if (!file.exists()) {
                sendError(ctx, HttpResponseStatus.NOT_FOUND, String.format("file not found: %s", filename));
                return;
            }
            try {
                RandomAccessFile raf = new RandomAccessFile(file, "r");
                // 创建Http response
                HttpResponse response = new DefaultHttpResponse(HTTP_1_1, HttpResponseStatus.OK);
                response.headers().set(HttpHeaderNames.CONTENT_LENGTH, raf.length());
                response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/octet-stream");
                response.headers().add(HttpHeaderNames.CONTENT_DISPOSITION, "attachment; filename=\"" +
                        new String(name.getBytes("GBK"), "ISO8859-1") + "\"");
                ctx.write(response);
                ChannelFuture sendFileFuture;
                // 判断pipeline中是否有SslHandler
                if (ctx.pipeline().get(SslHandler.class) == null) {
                    // 传输文件时使用FileRegion来进行传输，写到channel中
                    sendFileFuture = ctx.write(new DefaultFileRegion(raf.getChannel(), 0,
                            raf.length()), ctx.newProgressivePromise());
                } else {
                    // 使用ChunkedFile（使用了SSL的安全传输方式）分块传输文件
                    sendFileFuture = ctx.write(new ChunkedFile(raf, 0,
                            raf.length(), 8192), ctx.newProgressivePromise());
                }
                // 增加listener，从而实现异步获取传输文件后client端的返回结果
                sendFileFuture.addListener(new ChannelProgressiveFutureListener() {

                    private long lastProgress = 0;

                    @Override
                    public void operationComplete(ChannelProgressiveFuture future)
                            throws Exception {
                        log.debug("file transfer complete. [filename={}]", filename);
                        raf.close();
                    }

                    @Override
                    public void operationProgressed(ChannelProgressiveFuture future, long progress, long total) {
                        if (total < 0) {
                            log.warn("file transfer progress: [filename={}, progress={}]", file.getName(), progress);
                        } else {
                            // int deltaProgress = (int) (progress - lastProgress);
                            // lastProgress = progress;
                            // Prometheus.hit("datanode_disk_read_bytes", "DataNode瞬时写磁盘大小", deltaProgress);
                            log.debug("file transfer progress: [filename={}, progress={}]", file.getName(),
                                    progress / total);
                        }
                    }
                });
                ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
            } catch (FileNotFoundException e) {
                log.warn("file not found:  [filename={}]", file.getPath());
                sendError(ctx, HttpResponseStatus.NOT_FOUND, String.format("file not found: %s", file.getPath()));
            } catch (IOException e) {
                log.warn("file has a IOException:  [filename={}, err={}]", file.getName(), e.getMessage());
                sendError(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR, String.format("读取文件发生异常：%s", absolutePath));
            }
        } else {
            log.info("Ignore http request : [uri={}, method={}]", request.uri(), request.method());
        }
    }

    private static void sendError(ChannelHandlerContext ctx, HttpResponseStatus status, String msg) {
        FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1,
                status, Unpooled.copiedBuffer("Failure: " + status.toString()
                        + "\r\n" + msg, CharsetUtil.UTF_8));
        response.headers().set("Content-Type", "text/plain; charset=UTF-8");
        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }
}
