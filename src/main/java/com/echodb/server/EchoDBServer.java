package com.echodb.server;

import com.echodb.config.EchoDBConfig;
import com.echodb.core.EchoDB;
import com.echodb.election.CASLeaderElection;
import com.echodb.client.http.EchoDBRequest;
import com.echodb.client.http.EchoDBResponse;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Optional;

/**
 * HTTP server for SlateDB nodes
 */
public class EchoDBServer {
    private final EchoDBConfig config;
    private final EchoDB db;
    private final CASLeaderElection leaderElection;
    private final HttpServer server;
    private final ObjectMapper objectMapper;
    private final String nodeId;
    
    public EchoDBServer(EchoDBConfig config, String nodeId, int port) throws Exception {
        this.config = config;
        this.nodeId = nodeId;
        this.leaderElection = new CASLeaderElection(config, nodeId);
        this.db = new EchoDB(config);
        this.objectMapper = new ObjectMapper();
        
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        setupEndpoints();
    }
    
    private void setupEndpoints() {
        server.createContext("/api/put", new PutHandler());
        server.createContext("/api/get", new GetHandler());
        server.createContext("/api/delete", new DeleteHandler());
        server.createContext("/health", new HealthHandler());
    }
    
    public void start() {
        leaderElection.start();
        server.start();
        System.out.println("SlateDB server started on port " + server.getAddress().getPort());
    }
    
    public void stop() {
        server.stop(0);
        leaderElection.stop();
        try {
            db.close();
        } catch (Exception e) {
            System.err.println("Error closing database: " + e.getMessage());
        }
    }
    
    private class PutHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!leaderElection.isLeader()) {
                // ✅ Use public static method
                sendResponse(exchange, EchoDBResponse.error("Not the leader"), 503);
                return;
            }
            
            try {
                EchoDBRequest request = parseRequest(exchange);
                db.put(request.getKey(), request.getValueBytes());
                // ✅ Use public static method
                sendResponse(exchange, EchoDBResponse.success((byte[]) null), 200);
            } catch (Exception e) {
                sendResponse(exchange, EchoDBResponse.error(e.getMessage()), 500);
            }
        }
    }
    
    private class GetHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                EchoDBRequest request = parseRequest(exchange);
                Optional<byte[]> result = db.get(request.getKey());
                
                if (result.isPresent()) {
                    sendResponse(exchange, EchoDBResponse.success(result.get()), 200);
                } else {
                    sendResponse(exchange, EchoDBResponse.success((byte[]) null), 404);
                }
            } catch (Exception e) {
                sendResponse(exchange, EchoDBResponse.error(e.getMessage()), 500);
            }
        }
    }
    
    private class DeleteHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!leaderElection.isLeader()) {
                sendResponse(exchange, EchoDBResponse.error("Not the leader"), 503);
                return;
            }
            
            try {
                EchoDBRequest request = parseRequest(exchange);
                db.delete(request.getKey());
                sendResponse(exchange, EchoDBResponse.success((byte[]) null), 200);
            } catch (Exception e) {
                sendResponse(exchange, EchoDBResponse.error(e.getMessage()), 500);
            }
        }
    }
    
    private class HealthHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String status = leaderElection.isLeader() ? "LEADER" : "FOLLOWER";
            String response = "{\"status\":\"" + status + "\",\"nodeId\":\"" + nodeId + "\"}";
            
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, response.length());
            
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response.getBytes());
            }
        }
    }
    
    private EchoDBRequest parseRequest(HttpExchange exchange) throws IOException {
        try (InputStream is = exchange.getRequestBody()) {
            return objectMapper.readValue(is, EchoDBRequest.class);
        }
    }
    
    private void sendResponse(HttpExchange exchange, EchoDBResponse response, int statusCode)
            throws IOException {
        String responseBody = objectMapper.writeValueAsString(response);
        
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(statusCode, responseBody.length());
        
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(responseBody.getBytes());
        }
    }
}
