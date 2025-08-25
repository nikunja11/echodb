package com.echodb.client;

import com.echodb.config.EchoDBConfig;
import com.echodb.election.CASLeaderElection;
import com.echodb.exception.EchoDBException;
import com.echodb.client.http.HttpClient;
import com.echodb.client.http.EchoDBRequest;
import com.echodb.client.http.EchoDBResponse;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.List;

/**
 * Client for connecting to SlateDB cluster
 */
public class EchoDBClient {
    private final EchoDBConfig config;
    private final CASLeaderElection leaderElection;
    private final HttpClient httpClient;
    private final List<String> nodeEndpoints;
    private volatile String currentLeader;
    
    public EchoDBClient(EchoDBConfig config) {
        this.config = config;
        this.leaderElection = new CASLeaderElection(config, "client-" + System.currentTimeMillis());
        this.httpClient = new HttpClient();
        this.nodeEndpoints = config.getNodeEndpoints();
        
        // ✅ Add validation for node endpoints
        if (nodeEndpoints.isEmpty()) {
            System.err.println("⚠️ Warning: No node endpoints configured. Add nodes using config.addNode()");
        }
    }
    
    public CompletableFuture<Void> put(String key, byte[] value) {
        return CompletableFuture.runAsync(() -> {
            try {
                String leader = findCurrentLeader();
                if (leader == null) {
                    throw new EchoDBException("No leader available");
                }
                
                String leaderEndpoint = getNodeEndpoint(leader);
                EchoDBRequest request = new EchoDBRequest("PUT", key, value);
                
                EchoDBResponse response = httpClient.sendRequest(leaderEndpoint + "/api/put", request);
                
                if (!response.isSuccess()) {
                    throw new EchoDBException("PUT failed: " + response.getError());
                }
                
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
    
    public CompletableFuture<Optional<byte[]>> get(String key) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                // Try leader first for consistency
                String leader = findCurrentLeader();
                if (leader != null) {
                    try {
                        String leaderEndpoint = getNodeEndpoint(leader);
                        EchoDBRequest request = new EchoDBRequest("GET", key, (byte[]) null);
                        
                        EchoDBResponse response = httpClient.sendRequest(leaderEndpoint + "/api/get", request);
                        
                        if (response.isSuccess() && response.getValue() != null) {
                            // ✅ Use getValueBytes() method
                            return Optional.of(response.getValueBytes());
                        }
                    } catch (Exception e) {
                        System.err.println("Failed to read from leader, trying followers: " + e.getMessage());
                    }
                }
                
                // Try follower nodes if leader failed
                for (String endpoint : nodeEndpoints) {
                    try {
                        EchoDBRequest request = new EchoDBRequest("GET", key, (byte[]) null);
                        EchoDBResponse response = httpClient.sendRequest(endpoint + "/api/get", request);
                        
                        if (response.isSuccess() && response.getValue() != null) {
                            return Optional.of(response.getValueBytes());
                        }
                    } catch (Exception e) {
                        // Try next node
                        continue;
                    }
                }
                
                return Optional.empty();
                
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
    
    public CompletableFuture<Void> delete(String key) {
        return CompletableFuture.runAsync(() -> {
            try {
                String leader = findCurrentLeader();
                if (leader == null) {
                    throw new EchoDBException("No leader available");
                }
                
                String leaderEndpoint = getNodeEndpoint(leader);
                // ✅ Fix: Cast null to byte[] to resolve constructor ambiguity
                EchoDBRequest request = new EchoDBRequest("DELETE", key, (byte[]) null);
                
                EchoDBResponse response = httpClient.sendRequest(leaderEndpoint + "/api/delete", request);
                
                if (!response.isSuccess()) {
                    throw new EchoDBException("DELETE failed: " + response.getError());
                }
                
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
    
    private String findCurrentLeader() {
        try {
            return leaderElection.getCurrentLeader();
        } catch (Exception e) {
            return null;
        }
    }
    
    private String getNodeEndpoint(String nodeId) {
        // Map node ID to endpoint - in production this would be from service discovery
        String endpoint = config.getNodeEndpoint(nodeId);
        if (endpoint == null) {
            System.err.println("⚠️ Warning: No endpoint found for node: " + nodeId);
            // Fallback to first available endpoint
            return nodeEndpoints.isEmpty() ? "http://localhost:8080" : nodeEndpoints.get(0);
        }
        return endpoint;
    }
    
    public void close() {
        leaderElection.stop();
        httpClient.close();
    }
}
