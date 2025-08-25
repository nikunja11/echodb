package com.echodb.client.http;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Base64;

/**
 * Response object for SlateDB HTTP API
 */
public class EchoDBResponse {
    private final boolean success;
    private final String value; // Base64 encoded
    private final String error;
    
    @JsonCreator
    public EchoDBResponse(
            @JsonProperty("success") boolean success,
            @JsonProperty("value") byte[] value,
            @JsonProperty("error") String error) {
        this.success = success;
        this.value = value != null ? Base64.getEncoder().encodeToString(value) : null;
        this.error = error;
    }
    
    // ✅ Add constructor for String value (for JSON deserialization)
    @JsonCreator
    public EchoDBResponse(
            @JsonProperty("success") boolean success,
            @JsonProperty("value") String value,
            @JsonProperty("error") String error) {
        this.success = success;
        this.value = value;
        this.error = error;
    }
    
    // ✅ Add constructor for HttpClient usage (boolean, null, String)
    public EchoDBResponse(boolean success, Object nullValue, String error) {
        this.success = success;
        this.value = null;
        this.error = error;
    }
    
    // ✅ Add public static factory methods
    public static EchoDBResponse success(byte[] value) {
        return new EchoDBResponse(true, value, null);
    }
    
    public static EchoDBResponse success(String value) {
        return new EchoDBResponse(true, value, null);
    }
    
    public static EchoDBResponse error(String errorMessage) {
        return new EchoDBResponse(false, (String) null, errorMessage);
    }
    
    @JsonProperty("success")
    public boolean isSuccess() { return success; }
    
    @JsonProperty("value")
    public String getValue() { return value; }
    
    @JsonProperty("error")
    public String getError() { return error; }
    
    public byte[] getValueBytes() {
        return value != null ? Base64.getDecoder().decode(value) : null;
    }
}
