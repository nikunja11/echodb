package com.echodb.recovery;

import com.echodb.checkpoint.CheckpointInfo;
import com.echodb.checkpoint.Checkpointer;
import com.echodb.lsm.RowEntry;
import com.echodb.lsm.LSMTree;
import com.echodb.storage.S3StorageManager;
import com.echodb.exception.EchoDBException;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.io.DataInputStream;
import java.io.ByteArrayInputStream;

/**
 * Handles WAL recovery using checkpoint information
 */
public class WALRecovery {
    private final S3StorageManager storageManager;
    private final Checkpointer checkpointer;
    private final LSMTree lsmTree;
    
    public WALRecovery(S3StorageManager storageManager, Checkpointer checkpointer, LSMTree lsmTree) {
        this.storageManager = storageManager;
        this.checkpointer = checkpointer;
        this.lsmTree = lsmTree;
    }

    /**
     * Recover database state from WAL using checkpoint
     */
    public void recover() throws EchoDBException, IOException {
        CheckpointInfo checkpoint = checkpointer.getCurrentCheckpoint();
        
        System.out.println("Starting WAL recovery from checkpoint: " + checkpoint);
        
        // Get all WAL files after the checkpoint
        List<String> walFiles = getWALFilesAfterCheckpoint(checkpoint);
        
        int recoveredEntries = 0;
        for (String walFile : walFiles) {
            recoveredEntries += recoverFromWALFile(walFile, checkpoint);
        }
        
        System.out.println("WAL recovery completed. Recovered " + recoveredEntries + " entries");
    }
    
    /**
     * Recover entries from a single WAL file
     */

    private int recoverFromWALFile(String walFile, CheckpointInfo checkpoint) throws EchoDBException, IOException {
        byte[] walData = storageManager.get(walFile)
                .orElseThrow(() -> new EchoDBException("WAL file not found: " + walFile));

        List<RowEntry> entries = deserializeWALEntries(walData);
        int recoveredCount = 0;

        System.out.println("📋 Found " + entries.size() + " total entries in " + walFile);

        for (RowEntry entry : entries) {
            // ✅ Only replay entries after the checkpoint sequence
            if (entry.getSequenceNumber() > checkpoint.getLastFlushedSequence()) {
                System.out.println("🔄 Replaying entry: seq=" + entry.getSequenceNumber() +
                        ", key=" + entry.getKey() + ", type=" + entry.getType());
                replayEntry(entry);
                recoveredCount++;
            } else {
                System.out.println("⏭️ Skipping already flushed entry: seq=" + entry.getSequenceNumber());
            }
        }

        System.out.println("✅ Recovered " + recoveredCount + " new entries from " + walFile);
        return recoveredCount;
    }
    
    /**
     * Replay a single WAL entry to the LSM tree
     */
    private void replayEntry(RowEntry entry) throws EchoDBException, IOException {
        if (entry.getType() == RowEntry.Type.PUT) {
            lsmTree.putWithSequence(entry.getKey(), entry.getValue(), entry.getSequenceNumber());
        } else if (entry.getType() == RowEntry.Type.DELETE) {
            lsmTree.deleteWithSequence(entry.getKey(), entry.getSequenceNumber());
        }
    }
    
    /**
     * Get WAL files that contain entries after the checkpoint
     */
    private List<String> getWALFilesAfterCheckpoint(CheckpointInfo checkpoint) throws EchoDBException {
        // ✅ Use the new listFiles method
        //List<String> allWalFiles = storageManager.listFiles("wal/");
        List<String> allWalFiles = safeListFiles("wal/");
        List<String> relevantFiles = new ArrayList<>();
        
        for (String walFile : allWalFiles) {
            // Parse timestamp from filename and compare with checkpoint
            if (isWALFileRelevant(walFile, checkpoint)) {
                relevantFiles.add(walFile);
            }
        }
        
        // Sort by timestamp to ensure proper replay order
        relevantFiles.sort((a, b) -> {
            long timestampA = extractTimestampFromWALFile(a);
            long timestampB = extractTimestampFromWALFile(b);
            return Long.compare(timestampA, timestampB);
        });
        
        return relevantFiles;
    }

    // helper that won’t throw if prefix doesn’t exist
    private List<String> safeListFiles(String prefix) {
        try {
            List<String> files = storageManager.listFiles(prefix);
            return files != null ? files : Collections.emptyList();
        } catch (EchoDBException e) {
            // Ignore "prefix not found" errors, only log others
            if (!e.getMessage().contains("NoSuchKey") && !e.getMessage().contains("404")) {
                System.err.println("⚠️ Could not list files for prefix " + prefix + ": " + e.getMessage());
            }
            return Collections.emptyList();
        }
    }

    private boolean isWALFileRelevant(String walFile, CheckpointInfo checkpoint) {
        // Extract timestamp from WAL filename (e.g., "wal/wal-1234567890-1")
        try {
            long walTimestamp = extractTimestampFromWALFile(walFile);
            return walTimestamp >= checkpoint.getCheckpointTimestamp();
        } catch (NumberFormatException e) {
            // If we can't parse, include it to be safe
            System.err.println("⚠️ Could not parse WAL timestamp from: " + walFile);
            return true;
        }
    }

    private long extractTimestampFromWALFile(String walFile) {
        // Extract timestamp from filename like "wal/wal-1234567890-1" or "wal-1234567890"
        String filename = walFile.substring(walFile.lastIndexOf('/') + 1);
        String[] parts = filename.split("-");
        
        if (parts.length >= 2) {
            return Long.parseLong(parts[1]);
        }
        
        throw new NumberFormatException("Invalid WAL filename format: " + walFile);
    }
    
    private List<RowEntry> deserializeWALEntries(byte[] walData) throws EchoDBException {
        List<RowEntry> entries = new ArrayList<>();
        
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(walData))) {
            while (dis.available() > 0) {
                // Read sequence number
                long sequenceNumber = dis.readLong();
                
                // Read type
                byte typeOrdinal = dis.readByte();
                RowEntry.Type type = RowEntry.Type.values()[typeOrdinal];
                
                // Read key
                int keyLength = dis.readInt();
                byte[] keyBytes = new byte[keyLength];
                dis.readFully(keyBytes);
                String key = new String(keyBytes);
                
                // Read value (might be null for DELETE)
                int valueLength = dis.readInt();
                byte[] value = null;
                if (valueLength > 0) {
                    value = new byte[valueLength];
                    dis.readFully(value);
                }
                
                // Read timestamp
                long timestamp = dis.readLong();
                
                RowEntry entry = new RowEntry(sequenceNumber, type, key, value, timestamp);
                entries.add(entry);
            }
            
        } catch (IOException e) {
            throw new EchoDBException("Failed to deserialize WAL entries", e);
        }
        
        return entries;
    }
}
