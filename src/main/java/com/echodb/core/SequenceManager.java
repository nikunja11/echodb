package com.echodb.core;

import com.echodb.storage.S3StorageManager;
import com.echodb.exception.EchoDBException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.nio.ByteBuffer;

/**
 * Global sequence number generator for consistency with persistence
 */
public class SequenceManager {
    private static final String SEQUENCE_KEY = "system/sequence/global";
    private static final long SEQUENCE_BATCH_SIZE = 1000; // Reserve sequences in batches
    
    private final AtomicLong globalSequence;
    private final AtomicLong lastPersistedSequence;
    private final S3StorageManager storageManager;
    private final ScheduledExecutorService persistenceScheduler;
    private volatile boolean closed = false;
    
    public SequenceManager(S3StorageManager storageManager) throws EchoDBException {
        this.storageManager = storageManager;
        this.persistenceScheduler = Executors.newSingleThreadScheduledExecutor();
        
        // Load last persisted sequence from S3
        long lastSequence = loadLastSequenceFromStorage();
        
        // Start from next batch to avoid collisions
        long startSequence = lastSequence + SEQUENCE_BATCH_SIZE;
        
        this.globalSequence = new AtomicLong(startSequence);
        this.lastPersistedSequence = new AtomicLong(lastSequence);
        
        System.out.println("SequenceManager initialized: lastPersisted=" + lastSequence +
                          ", starting=" + startSequence);
        
        // Periodically persist current sequence
        startPeriodicPersistence();
    }
    
    public SequenceManager(S3StorageManager storageManager, long startSequence) throws EchoDBException {
        this.storageManager = storageManager;
        this.persistenceScheduler = Executors.newSingleThreadScheduledExecutor();
        this.globalSequence = new AtomicLong(startSequence);
        this.lastPersistedSequence = new AtomicLong(startSequence);
        
        // Persist the initial sequence
        persistCurrentSequence();
        
        startPeriodicPersistence();
    }
    
    public long nextSequence() {
        if (closed) {
            throw new IllegalStateException("SequenceManager is closed");
        }
        
        long nextSeq = globalSequence.incrementAndGet();
        
        // If we're approaching the end of our reserved batch, persist immediately
        long lastPersisted = lastPersistedSequence.get();
        if (nextSeq - lastPersisted > SEQUENCE_BATCH_SIZE - 100) {
            // Async persistence to avoid blocking
            persistenceScheduler.execute(this::persistCurrentSequence);
        }
        
        return nextSeq;
    }
    
    public long getCurrentSequence() {
        return globalSequence.get();
    }
    
    /**
     * Force persistence of current sequence (useful before shutdown)
     */
    public void forceSync() throws EchoDBException {
        persistCurrentSequence();
    }
    
    public void close() throws EchoDBException {
        closed = true;
        
        // Final persistence before shutdown
        persistCurrentSequence();
        
        // Shutdown scheduler
        persistenceScheduler.shutdown();
        try {
            if (!persistenceScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                persistenceScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            persistenceScheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        System.out.println("📊 SequenceManager closed: finalSequence=" + getCurrentSequence());
    }
    
    private long loadLastSequenceFromStorage() throws EchoDBException {
        try {
            return storageManager.get(SEQUENCE_KEY)
                .map(data -> ByteBuffer.wrap(data).getLong())
                .orElse(0L);
        } catch (Exception e) {
            System.err.println("⚠️ Failed to load sequence from storage, starting from 0: " + e.getMessage());
            return 0L;
        }
    }
    
    private void persistCurrentSequence() {
        try {
            long currentSeq = globalSequence.get();
            byte[] data = ByteBuffer.allocate(8).putLong(currentSeq).array();
            
            storageManager.put(SEQUENCE_KEY, data);
            lastPersistedSequence.set(currentSeq);
            
            //System.out.println("💾 Persisted sequence: " + currentSeq);
            
        } catch (EchoDBException e) {
            System.err.println("❌ Failed to persist sequence: " + e.getMessage());
            // Don't throw - this is async operation
        }
    }
    
    private void startPeriodicPersistence() {
        // Persist sequence every 30 seconds
        persistenceScheduler.scheduleAtFixedRate(
            this::persistCurrentSequence,
            30, 30, TimeUnit.SECONDS
        );
    }
}
