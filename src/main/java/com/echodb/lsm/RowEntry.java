package com.echodb.lsm;

/**
 * Unified entry structure for both WAL and Memtable
 */
public class RowEntry {
    public enum Type {
        PUT, DELETE
    }
    
    private final long sequenceNumber;
    private final Type type;
    private final String key;
    private final byte[] value;
    private final long timestamp;
    
    public RowEntry(long sequenceNumber, Type type, String key, byte[] value, long timestamp) {
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
    public boolean isDeleted() { return type == Type.DELETE; }
    
    public long getSize() {
        return (value != null ? value.length : 0) + 
               8 + // sequenceNumber
               1 + // type
               (key != null ? key.length() : 0) + 
               8;  // timestamp
    }
}