package com.echodb.http;

import com.echodb.core.EchoDB;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.echodb.election.CASLeaderElection;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;

import io.netty.channel.Channel;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class EchoDBHttpServer {
    private final EchoDB db;
    private final CASLeaderElection leaderElection;
    private final String nodeId;
    private final ObjectMapper objectMapper;
    private final int port;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;
    
    public EchoDBHttpServer(EchoDB db, CASLeaderElection leaderElection, String nodeId, int port) {
        this.db = db;
        this.leaderElection = leaderElection;
        this.nodeId = nodeId;
        this.port = port;
        this.objectMapper = new ObjectMapper();
    }
    
    public void start() throws Exception {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();
        
        try {
            ServerBootstrap bootstrap = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new HttpServerCodec());
                        pipeline.addLast(new HttpObjectAggregator(1048576)); // 1MB max
                        pipeline.addLast(new SlateDBHttpHandler());
                    }
                })
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.TCP_NODELAY, true);
            
            ChannelFuture future = bootstrap.bind(port).sync();
            serverChannel = future.channel();
            
            System.out.println("🚀 SlateDB HTTP Server started on port " + port);
            
        } catch (Exception e) {
            shutdown();
            throw e;
        }
    }
    
    public void shutdown() {
        if (serverChannel != null) {
            serverChannel.close();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
    }
    
    private class SlateDBHttpHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
        
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
            // ✅ Retain the request for async processing
            request.retain();

            String uri = request.uri();
            HttpMethod method = request.method();

            CompletableFuture<FullHttpResponse> responseFuture;

            try {
                // Route requests
                if (method == HttpMethod.POST && uri.equals("/api/put")) {
                    responseFuture = handlePut(request);
                } else if (method == HttpMethod.GET && uri.startsWith("/api/get/")) {
                    String key = uri.substring("/api/get/".length());
                    responseFuture = handleGet(key);
                } else if (method == HttpMethod.POST && uri.equals("/api/flush")) {
                    responseFuture = handleFlush();
                } else if (method == HttpMethod.GET && uri.equals("/api/status")) {
                    responseFuture = handleStatus();
                } else {
                    responseFuture = CompletableFuture.completedFuture(
                            createResponse(HttpResponseStatus.NOT_FOUND, "{\"error\":\"Not found\"}"));
                }

                // ✅ Send response and release request
                responseFuture.whenComplete((response, throwable) -> {
                    try {
                        if (throwable != null) {
                            FullHttpResponse errorResponse = createResponse(
                                    HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                    "{\"error\":\"" + throwable.getMessage() + "\"}");
                            ctx.writeAndFlush(errorResponse).addListener(ChannelFutureListener.CLOSE);
                        } else {
                            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
                        }
                    } finally {
                        // ✅ CRITICAL: Release the retained request
                        request.release();
                    }
                });

            } catch (Exception e) {
                // ✅ Release request on immediate error
                request.release();
                FullHttpResponse errorResponse = createResponse(
                        HttpResponseStatus.INTERNAL_SERVER_ERROR,
                        "{\"error\":\"" + e.getMessage() + "\"}");
                ctx.writeAndFlush(errorResponse).addListener(ChannelFutureListener.CLOSE);
            }
        }
        
        private CompletableFuture<FullHttpResponse> handlePut(FullHttpRequest request) {
            // ✅ Only leader can handle writes
            if (!leaderElection.isLeader()) {
                return CompletableFuture.completedFuture(
                    createResponse(HttpResponseStatus.SERVICE_UNAVAILABLE, 
                                 "{\"error\":\"Not the leader\"}"));
            }

            return CompletableFuture.supplyAsync(() -> {
                try {
                    // ✅ CRITICAL: Retain the content before async processing
                    ByteBuf content = request.content().retain();

                    try {
                        String body = content.toString(CharsetUtil.UTF_8);
                        Map<String, String> requestData = objectMapper.readValue(body, Map.class);

                        String key = requestData.get("key");
                        String value = requestData.get("value");

                        db.put(key, value.getBytes());

                        return createResponse(HttpResponseStatus.OK, "{\"status\":\"success\"}");

                    } finally {
                        // ✅ CRITICAL: Release the retained content
                        content.release();
                    }

                } catch (Exception e) {
                    return createResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                            "{\"error\":\"" + e.getMessage() + "\"}");
                }
            });
        }
        
        private CompletableFuture<FullHttpResponse> handleGet(String key) {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    Optional<byte[]> result = db.get(key);
                    
                    if (result.isPresent()) {
                        String value = new String(result.get());
                        String response = "{\"key\":\"" + key + "\",\"value\":\"" + value + "\"}";
                        return createResponse(HttpResponseStatus.OK, response);
                    } else {
                        return createResponse(HttpResponseStatus.NOT_FOUND, 
                                            "{\"error\":\"Key not found\"}");
                    }
                    
                } catch (Exception e) {
                    return createResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, 
                                        "{\"error\":\"" + e.getMessage() + "\"}");
                }
            });
        }
        
        private CompletableFuture<FullHttpResponse> handleFlush() {
            if (!leaderElection.isLeader()) {
                return CompletableFuture.completedFuture(
                    createResponse(HttpResponseStatus.SERVICE_UNAVAILABLE, 
                                 "{\"error\":\"Not the leader\"}"));
            }
            
            return CompletableFuture.supplyAsync(() -> {
                try {
                    db.flush().get();
                    return createResponse(HttpResponseStatus.OK, "{\"status\":\"flushed\"}");
                } catch (Exception e) {
                    return createResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, 
                                        "{\"error\":\"" + e.getMessage() + "\"}");
                }
            });
        }

        private CompletableFuture<FullHttpResponse> handleStatus() {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    String role = leaderElection.isLeader() ? "LEADER" : "FOLLOWER";
                    String response = String.format("{\"role\":\"%s\",\"nodeId\":\"%s\"}", role, nodeId);
                    return createResponse(HttpResponseStatus.OK, response);
                } catch (Exception e) {
                    return createResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                            "{\"error\":\"" + e.getMessage() + "\"}");
                }
            });
        }
        
        private FullHttpResponse createResponse(HttpResponseStatus status, String content) {
            ByteBuf buffer = Unpooled.copiedBuffer(content, CharsetUtil.UTF_8);
            FullHttpResponse response = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1, status, buffer);
            
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json");
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, buffer.readableBytes());
            
            return response;
        }
    }
}
