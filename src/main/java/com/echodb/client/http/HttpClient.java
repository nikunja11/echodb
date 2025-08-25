package com.echodb.client.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URI;
import java.time.Duration;

/**
 * HTTP client for SlateDB communication
 */
public class HttpClient {
    private final java.net.http.HttpClient client;
    private final ObjectMapper objectMapper;
    
    public HttpClient() {
        this.client = java.net.http.HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();
        this.objectMapper = new ObjectMapper();
    }
    
    public EchoDBResponse sendRequest(String url, EchoDBRequest request) throws Exception {
        String requestBody = objectMapper.writeValueAsString(request);
        
        HttpRequest httpRequest = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .header("Content-Type", "application/json")
            .timeout(Duration.ofSeconds(30))
            .POST(HttpRequest.BodyPublishers.ofString(requestBody))
            .build();
        
        HttpResponse<String> response = client.send(httpRequest, 
            HttpResponse.BodyHandlers.ofString());
        
        if (response.statusCode() == 200) {
            return objectMapper.readValue(response.body(), EchoDBResponse.class);
        } else {
            // ✅ Use static factory method instead of constructor
            return EchoDBResponse.error("HTTP " + response.statusCode());
        }
    }
    
    public void close() {
        // HTTP client doesn't need explicit closing in Java 11+
    }
}
