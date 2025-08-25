package com.echodb.checkpoint;

import com.echodb.config.EchoDBConfig;
import com.echodb.storage.S3StorageManager;
import com.echodb.exception.EchoDBException;

import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Manages checkpoint state for WAL recovery
 */
public class Checkpointer {
    private static final String CHECKPOINT_KEY = "checkpoint/latest";
    
    private final EchoDBConfig config;
    private final S3StorageManager storageManager;
    private final ScheduledExecutorService scheduler;
    private final ReentrantLock checkpointLock;
    
    private volatile CheckpointInfo currentCheckpoint;
    private volatile boolean running = false;
    
    public Checkpointer(EchoDBConfig config, S3StorageManager storageManager) {
        this.config = config;
        this.storageManager = storageManager;
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.checkpointLock = new ReentrantLock();
        this.currentCheckpoint = new CheckpointInfo(0L, 0L, System.currentTimeMillis());
    }
    
    /**
     * Start periodic checkpointing
     */
    public void start() {
        running = true;
        
        // Load existing checkpoint
        loadLatestCheckpoint();
        
        // Schedule periodic checkpointing
        scheduler.scheduleAtFixedRate(
            this::performCheckpoint,
            0,
            config.getCheckpointInterval().toSeconds(),
            TimeUnit.SECONDS
        );
    }
    
    /**
     * Update checkpoint to mark that all WAL entries up to this sequence
     * have been flushed to SSTables
     */

    public void updateCheckpoint(long lastFlushedSequence, long lastFlushedWalOffset) {
        checkpointLock.lock();
        try {
            if (lastFlushedSequence > currentCheckpoint.getLastFlushedSequence()) {
                CheckpointInfo newCheckpoint = new CheckpointInfo(
                        lastFlushedSequence,
                        lastFlushedWalOffset,
                        System.currentTimeMillis()
                );

                System.out.println("📍 Updating checkpoint: " +
                        "old_seq=" + currentCheckpoint.getLastFlushedSequence() +
                        " -> new_seq=" + lastFlushedSequence);

                currentCheckpoint = newCheckpoint;

                // ✅ Immediately persist the checkpoint
                persistCheckpoint();
            }
        } finally {
            checkpointLock.unlock();
        }
    }

    /**
     * Persist checkpoint to storage immediately
     */
    private void persistCheckpoint() {
        try {
            byte[] checkpointData = currentCheckpoint.serialize();
            storageManager.put(CHECKPOINT_KEY, checkpointData);

            System.out.println("💾 Checkpoint persisted: sequence=" +
                    currentCheckpoint.getLastFlushedSequence() +
                    ", offset=" + currentCheckpoint.getLastFlushedWalOffset());

        } catch (EchoDBException e) {
            System.err.println("❌ Failed to persist checkpoint: " + e.getMessage());
        }
    }
    
    /**
     * Get current checkpoint info
     */
    public CheckpointInfo getCurrentCheckpoint() {
        return currentCheckpoint;
    }
    
    /**
     * Persist checkpoint to storage
     */
    private void performCheckpoint() {
        if (!running) return;

        checkpointLock.lock();
        try {
            // ✅ Just call the persist method
            persistCheckpoint();
        } finally {
            checkpointLock.unlock();
        }
    }
    
    /**
     * Load latest checkpoint from storage
     */
    private void loadLatestCheckpoint() {
        try {
            Optional<byte[]> checkpointData = storageManager.get(CHECKPOINT_KEY);
            if (checkpointData.isPresent()) {
                currentCheckpoint = CheckpointInfo.deserialize(checkpointData.get());
                System.out.println("Loaded checkpoint: sequence=" + 
                                 currentCheckpoint.getLastFlushedSequence() + 
                                 ", offset=" + currentCheckpoint.getLastFlushedWalOffset());
            } else {
                System.out.println("No existing checkpoint found, starting from beginning");
            }
        } catch (EchoDBException e) {
            System.err.println("Failed to load checkpoint: " + e.getMessage());
            // Continue with default checkpoint
        }
    }
    
    /**
     * Force checkpoint immediately
     */
    public void forceCheckpoint() {
        performCheckpoint();
    }
    
    public void stop() {
        running = false;
        
        // Save final checkpoint
        forceCheckpoint();
        
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}