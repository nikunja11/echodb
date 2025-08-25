package com.echodb.wal;

/**
 * Write-Ahead Log entry
 */
public class WALEntry {
    public enum Type {
        PUT, DELETE
    }
    
    private final long sequenceNumber;
    private final Type type;
    private final String key;
    private final byte[] value;
    private final long timestamp;
    
    public WALEntry(long sequenceNumber, Type type, String key, byte[] value, long timestamp) {
        this.sequenceNumber = sequenceNumber;
        this.type = type;
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }
    
    public long getSequenceNumber() { return sequenceNumber; }
    public Type getType() { return type; }
    public String getKey() { return key; }
    public byte[] getValue() { return value; }
    public long getTimestamp() { return timestamp; }
}