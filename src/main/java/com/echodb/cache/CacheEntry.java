package com.echodb.cache;

/**
 * Cache entry with metadata
 */
public class CacheEntry {
    private final byte[] value;
    private final long timestamp;
    private final long size;
    
    public CacheEntry(byte[] value) {
        this.value = value;
        this.timestamp = System.currentTimeMillis();
        this.size = value.length + 8; // value + timestamp
    }
    
    public byte[] getValue() {
        return value;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    public long getSize() {
        return size;
    }
}