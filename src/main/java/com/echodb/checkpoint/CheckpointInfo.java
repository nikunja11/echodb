package com.echodb.checkpoint;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Checkpoint information for WAL recovery
 */
public class CheckpointInfo {
    private final long lastFlushedSequence;
    private final long lastFlushedWalOffset;
    private final long checkpointTimestamp;
    
    public CheckpointInfo(long lastFlushedSequence, long lastFlushedWalOffset, long checkpointTimestamp) {
        this.lastFlushedSequence = lastFlushedSequence;
        this.lastFlushedWalOffset = lastFlushedWalOffset;
        this.checkpointTimestamp = checkpointTimestamp;
    }
    
    public long getLastFlushedSequence() { return lastFlushedSequence; }
    public long getLastFlushedWalOffset() { return lastFlushedWalOffset; }
    public long getCheckpointTimestamp() { return checkpointTimestamp; }
    
    /**
     * Serialize checkpoint to bytes
     */
    public byte[] serialize() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
            
            dos.writeLong(lastFlushedSequence);
            dos.writeLong(lastFlushedWalOffset);
            dos.writeLong(checkpointTimestamp);
            
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize checkpoint", e);
        }
    }
    
    /**
     * Deserialize checkpoint from bytes
     */
    public static CheckpointInfo deserialize(byte[] data) {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
             DataInputStream dis = new DataInputStream(bais)) {
            
            long lastFlushedSequence = dis.readLong();
            long lastFlushedWalOffset = dis.readLong();
            long checkpointTimestamp = dis.readLong();
            
            return new CheckpointInfo(lastFlushedSequence, lastFlushedWalOffset, checkpointTimestamp);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize checkpoint", e);
        }
    }
    
    @Override
    public String toString() {
        return String.format("CheckpointInfo{sequence=%d, offset=%d, timestamp=%d}", 
                           lastFlushedSequence, lastFlushedWalOffset, checkpointTimestamp);
    }
}