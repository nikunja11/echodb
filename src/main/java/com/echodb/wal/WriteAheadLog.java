package com.echodb.wal;

import com.echodb.config.EchoDBConfig;
import com.echodb.core.SequenceManager;
import com.echodb.storage.S3StorageManager;
import com.echodb.exception.EchoDBException;
import com.echodb.lsm.RowEntry;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Write-Ahead Log implementation with S3 storage
 */
public class WriteAheadLog {
    private final EchoDBConfig config;
    private final S3StorageManager storageManager;
    private final ConcurrentLinkedQueue<RowEntry> buffer; // Use RowEntry
    private final ReentrantLock flushLock;
    private final ScheduledExecutorService scheduler;
    private final SequenceManager sequenceManager; // Shared sequence manager
    private volatile boolean closed = false;
    
    public WriteAheadLog(EchoDBConfig config, S3StorageManager storageManager,
                         SequenceManager sequenceManager) {
        this.config = config;
        this.storageManager = storageManager;
        this.sequenceManager = sequenceManager;
        this.buffer = new ConcurrentLinkedQueue<>();
        this.flushLock = new ReentrantLock();
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
    }
    
    public void append(String key, byte[] value) throws EchoDBException {
        if (closed) {
            throw new EchoDBException("WAL is closed");
        }
        
        RowEntry entry = new RowEntry(
            sequenceManager.nextSequence(),
            RowEntry.Type.PUT,
            key,
            value,
            System.currentTimeMillis()
        );
        
        buffer.offer(entry);
    }

    public void appendWithSequence(String key, byte[] value, long sequence) throws EchoDBException {
        if (closed) {
            throw new EchoDBException("WAL is closed");
        }
        
        RowEntry entry = new RowEntry(
            sequence,
            RowEntry.Type.PUT,
            key,
            value,
            System.currentTimeMillis()
        );
        
        buffer.offer(entry);
    }
    
    public void appendTombstone(String key) throws EchoDBException {
        if (closed) {
            throw new EchoDBException("WAL is closed");
        }
        
        RowEntry entry = new RowEntry(
            sequenceManager.nextSequence(),
            RowEntry.Type.DELETE,
            key,
            null,
            System.currentTimeMillis()
        );
        
        buffer.offer(entry);
    }

    public void appendTombstoneWithSequence(String key, long sequence) throws EchoDBException {
        if (closed) {
            throw new EchoDBException("WAL is closed");
        }
        
        RowEntry entry = new RowEntry(
            sequence,
            RowEntry.Type.DELETE,
            key,
            null,
            System.currentTimeMillis()
        );
        
        buffer.offer(entry);
    }
    
    public void flush() throws EchoDBException, IOException {
        flushLock.lock();
        try {
            if (buffer.isEmpty()) {
                return;
            }
            
            List<RowEntry> entries = new ArrayList<>();
            RowEntry entry;
            while ((entry = buffer.poll()) != null) {
                entries.add(entry);
            }
            
            // ✅ Serialize all entries into one WAL file
            byte[] walData = serializeEntries(entries);
            String walKey = config.getWalPrefix() + "wal-" + System.currentTimeMillis();
            storageManager.put(walKey, walData);
            
            System.out.println("💾 WAL flushed: " + entries.size() + " entries to " + walKey);
            
        } finally {
            flushLock.unlock();
        }
    }
    
    public void startPeriodicFlush() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                flush();
            } catch (EchoDBException | IOException e) {
                // Log error
                System.err.println("WAL flush failed: " + e.getMessage());
            }
        }, 0, config.getWalFlushInterval().toSeconds(), TimeUnit.SECONDS);
    }
    
    // ✅ Add method to serialize multiple entries
    private byte[] serializeEntries(List<RowEntry> entries) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        
        for (RowEntry entry : entries) {
            byte[] serialized = serializeEntry(entry);
            baos.write(serialized);
        }
        
        return baos.toByteArray();
    }

    private byte[] serializeEntry(RowEntry entry) {
        ByteBuffer buffer = ByteBuffer.allocate(
            8 + // sequence number
            1 + // type
            4 + entry.getKey().length() + // key length + key
            4 + (entry.getValue() != null ? entry.getValue().length : 0) + // value length + value
            8   // timestamp
        );
        
        buffer.putLong(entry.getSequenceNumber());
        buffer.put((byte) entry.getType().ordinal());
        buffer.putInt(entry.getKey().length());
        buffer.put(entry.getKey().getBytes());
        
        if (entry.getValue() != null) {
            buffer.putInt(entry.getValue().length);
            buffer.put(entry.getValue());
        } else {
            buffer.putInt(0);
        }
        
        buffer.putLong(entry.getTimestamp());
        return buffer.array();
    }
    
    public void close() throws EchoDBException, IOException {
        closed = true;
        flush();
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
