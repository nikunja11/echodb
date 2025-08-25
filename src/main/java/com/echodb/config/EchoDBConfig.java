package com.echodb.config;

import com.echodb.cache.EvictionPolicy;

import java.time.Duration;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

/**
 * Configuration for SlateDB
 */
public class EchoDBConfig {
    private final String s3Bucket;
    private final String s3Region;
    private final String walPrefix;
    private final String dataPrefix;
    private final long memtableSize;
    private final int maxMemtables;
    private final long cacheSize;
    private final EvictionPolicy evictionPolicy;
    private final Duration walFlushInterval;
    private final Duration compactionInterval;
    private final Duration checkpointInterval;
    private final List<String> nodeEndpoints;  // ✅ Add node endpoints
    private final Map<String, String> nodeIdToEndpoint;  // ✅ Add node mapping

    private EchoDBConfig(String s3Bucket, String s3Region, String walPrefix, String dataPrefix,
                         long memtableSize, int maxMemtables, long cacheSize, EvictionPolicy evictionPolicy,
                         Duration walFlushInterval, Duration compactionInterval, Duration checkpointInterval,
                         List<String> nodeEndpoints, Map<String, String> nodeIdToEndpoint) {
        this.s3Bucket = s3Bucket;
        this.s3Region = s3Region;
        this.walPrefix = walPrefix;
        this.dataPrefix = dataPrefix;
        this.memtableSize = memtableSize;
        this.maxMemtables = maxMemtables;
        this.cacheSize = cacheSize;
        this.evictionPolicy = evictionPolicy;
        this.walFlushInterval = walFlushInterval;
        this.compactionInterval = compactionInterval;
        this.checkpointInterval = checkpointInterval;
        this.nodeEndpoints = nodeEndpoints != null ? new ArrayList<>(nodeEndpoints) : new ArrayList<>();
        this.nodeIdToEndpoint = nodeIdToEndpoint != null ? new HashMap<>(nodeIdToEndpoint) : new HashMap<>();
    }
    
    public static class Builder {
        private String s3Bucket;
        private String s3Region = "us-east-1";
        private String walPrefix = "wal/";
        private String dataPrefix = "data/";
        private long memtableSize = 64 * 1024 * 1024; // 64MB
        private int maxMemtables = 3;
        private long cacheSize = 256 * 1024 * 1024; // 256MB
        private EvictionPolicy evictionPolicy = EvictionPolicy.LRU;
        private Duration walFlushInterval = Duration.ofSeconds(5);
        private Duration compactionInterval = Duration.ofMinutes(10);
        private Duration checkpointInterval = Duration.ofMinutes(5);
        private List<String> nodeEndpoints = new ArrayList<>();  // ✅ Add to builder
        private Map<String, String> nodeIdToEndpoint = new HashMap<>();  // ✅ Add to builder
        
        // ✅ Add builder methods for node configuration
        public Builder addNodeEndpoint(String endpoint) {
            this.nodeEndpoints.add(endpoint);
            return this;
        }
        
        public Builder addNode(String nodeId, String endpoint) {
            this.nodeIdToEndpoint.put(nodeId, endpoint);
            this.nodeEndpoints.add(endpoint);
            return this;
        }
        
        public Builder nodeEndpoints(List<String> endpoints) {
            this.nodeEndpoints = new ArrayList<>(endpoints);
            return this;
        }
        
        public Builder s3Bucket(String bucket) {
            this.s3Bucket = bucket;
            return this;
        }

        public Builder s3Region(String region) {
            this.s3Region = region;
            return this;
        }
        
        public Builder evictionPolicy(EvictionPolicy policy) {
            this.evictionPolicy = policy;
            return this;
        }
        
        public Builder checkpointInterval(Duration interval) {
            this.checkpointInterval = interval;
            return this;
        }
        
        public EchoDBConfig build() {
            return new EchoDBConfig(s3Bucket, s3Region, walPrefix, dataPrefix,
                                   memtableSize, maxMemtables, cacheSize, evictionPolicy,
                                   walFlushInterval, compactionInterval, checkpointInterval,
                                   nodeEndpoints, nodeIdToEndpoint);
        }
    }
    
    // Getters
    public String getS3Bucket() { return s3Bucket; }
    public String getS3Region() { return s3Region; }
    public String getWalPrefix() { return walPrefix; }
    public String getDataPrefix() { return dataPrefix; }
    public long getMemtableSize() { return memtableSize; }
    public int getMaxMemtables() { return maxMemtables; }
    public long getCacheSize() { return cacheSize; }
    public EvictionPolicy getEvictionPolicy() { return evictionPolicy; }
    public Duration getWalFlushInterval() { return walFlushInterval; }
    public Duration getCompactionInterval() { return compactionInterval; }
    public Duration getCheckpointInterval() { return checkpointInterval; }
    public List<String> getNodeEndpoints() {
        return new ArrayList<>(nodeEndpoints);
    }
    
    public String getNodeEndpoint(String nodeId) {
        return nodeIdToEndpoint.get(nodeId);
    }
}
