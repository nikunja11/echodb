package com.echodb.election;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

/**
 * Leader information for election coordination
 */
public class LeaderInfo {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    private final String nodeId;
    private final long leaseStart;
    private final long leaseExpiry;
    
    @JsonCreator
    public LeaderInfo(
            @JsonProperty("nodeId") String nodeId,
            @JsonProperty("leaseStart") long leaseStart,
            @JsonProperty("leaseExpiry") long leaseExpiry) {
        this.nodeId = nodeId;
        this.leaseStart = leaseStart;
        this.leaseExpiry = leaseExpiry;
    }
    
    @JsonProperty("nodeId")
    public String getNodeId() { return nodeId; }
    
    @JsonProperty("leaseStart")
    public long getLeaseStart() { return leaseStart; }
    
    @JsonProperty("leaseExpiry")
    public long getLeaseExpiry() { return leaseExpiry; }
    
    public byte[] serialize() {
        try {
            return objectMapper.writeValueAsBytes(this);
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize LeaderInfo", e);
        }
    }
    
    public static LeaderInfo deserialize(byte[] data) {
        try {
            return objectMapper.readValue(data, LeaderInfo.class);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize LeaderInfo", e);
        }
    }
}
