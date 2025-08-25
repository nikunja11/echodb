package com.echodb.client.http;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Base64;

/**
 * Request object for SlateDB HTTP API
 */
public class EchoDBRequest {
    private final String operation;
    private final String key;
    private final String value; // Base64 encoded
    
    // ✅ Main constructor for byte[] value (including null)
    public EchoDBRequest(String operation, String key, byte[] value) {
        this.operation = operation;
        this.key = key;
        this.value = value != null ? Base64.getEncoder().encodeToString(value) : null;
    }
    
    // ✅ Constructor for String value (for JSON deserialization)
    @JsonCreator
    public EchoDBRequest(
            @JsonProperty("operation") String operation,
            @JsonProperty("key") String key,
            @JsonProperty("value") String value) {
        this.operation = operation;
        this.key = key;
        this.value = value;
    }
    
    @JsonProperty("operation")
    public String getOperation() { return operation; }
    
    @JsonProperty("key")
    public String getKey() { return key; }
    
    @JsonProperty("value")
    public String getValue() { return value; }
    
    public byte[] getValueBytes() {
        return value != null ? Base64.getDecoder().decode(value) : null;
    }
}
