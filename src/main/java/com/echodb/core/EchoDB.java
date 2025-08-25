package com.echodb.core;

import com.echodb.cache.CacheManager;
import com.echodb.checkpoint.CheckpointInfo;
import com.echodb.checkpoint.Checkpointer;
import com.echodb.config.EchoDBConfig;
import com.echodb.lsm.LSMTree;
import com.echodb.storage.S3StorageManager;
import com.echodb.wal.WriteAheadLog;
import com.echodb.exception.EchoDBException;
import com.echodb.recovery.WALRecovery;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Main SlateDB database interface with compute-storage separation
 */
public class EchoDB {
    private final EchoDBConfig config;
    private final S3StorageManager storageManager;
    private final CacheManager cacheManager;
    private final WriteAheadLog wal;
    private final LSMTree lsmTree;
    private final SequenceManager sequenceManager; // Global sequence manager
    private final Checkpointer checkpointer;  // Add checkpointer field
    private final ReadWriteLock dbLock;

    
    public EchoDB(EchoDBConfig config) throws EchoDBException {
        this.config = config;
        this.storageManager = new S3StorageManager(config);
        this.cacheManager = new CacheManager(config);
        this.sequenceManager = new SequenceManager(storageManager); // Single source of truth
        this.checkpointer = new Checkpointer(config, storageManager);  // Create checkpointer
        this.wal = new WriteAheadLog(config, storageManager, sequenceManager);
        this.lsmTree = new LSMTree(config, storageManager, cacheManager, sequenceManager, checkpointer);  // Pass checkpointer
        this.dbLock = new ReentrantReadWriteLock();

        initialize();
    }
    
    public void put(String key, byte[] value) throws EchoDBException, IOException {
        dbLock.readLock().lock();
        try {
            // CRITICAL: Use same sequence number for both WAL and Memtable
            long sequence = sequenceManager.nextSequence();
            
            // Write to WAL first with the sequence
            wal.appendWithSequence(key, value, sequence);
            
            // Write to LSM tree with same sequence
            lsmTree.putWithSequence(key, value, sequence);
            
            // Update cache
            //cacheManager.put(key, value); - don't update cache here
            // it should rather be updated in the read path
        } finally {
            dbLock.readLock().unlock();
        }
    }
    
    public Optional<byte[]> get(String key) throws EchoDBException {
        dbLock.readLock().lock();
        System.out.println("Get from SlateDB===");
        try {
            // Check cache first
            Optional<byte[]> cached = cacheManager.get(key);
            if (cached.isPresent()) {
                //return cached; -- read from cache
            }
            
            // Check LSM tree (memory + disk)
            System.out.println("Get from LSM===");
            Optional<byte[]> result = lsmTree.get(key);
            
            // Cache the result if found
            result.ifPresent(value -> cacheManager.put(key, value));
            
            return result;
        } finally {
            dbLock.readLock().unlock();
        }
    }
    
    public void delete(String key) throws EchoDBException {
        dbLock.readLock().lock();
        try {
            // ✅ CRITICAL: Use same sequence number for both WAL and LSM tree
            long sequence = sequenceManager.nextSequence();
            
            // ✅ Write tombstone to WAL with sequence
            wal.appendTombstoneWithSequence(key, sequence);
            
            // ✅ Delete from LSM tree with same sequence
            lsmTree.deleteWithSequence(key, sequence);
            
            // Remove from cache
            cacheManager.evict(key);
        } finally {
            dbLock.readLock().unlock();
        }
    }
    
    public CompletableFuture<Void> flush() {
        return CompletableFuture.runAsync(() -> {
            dbLock.writeLock().lock();
            try {
                wal.flush();
                lsmTree.flush();
            } catch (EchoDBException | IOException e) {
                throw new RuntimeException(e);
            } finally {
                dbLock.writeLock().unlock();
            }
        });
    }
    
    private void initialize() throws EchoDBException {

        // ✅ Both leader and follower discover existing SST files
        lsmTree.discoverSSTFiles();

        // ✅ Only leader recovers from WAL (followers don't have memtables)
        // recoverFromStorage(); // Remove this

        // ✅ Load data from WAL and SST files on startup
        recoverFromStorage();

        // Start checkpointer
        checkpointer.start();

        // Start background tasks
        lsmTree.startSSTDiscovery();
        wal.startPeriodicFlush();
        lsmTree.startCompaction();
    }


    // ✅ Add recovery method
    private void recoverFromStorage() throws EchoDBException {
        try {
            System.out.println("🔄 Recovering data from storage...");
            
            // First recover SST files
            lsmTree.recover();
            
            // Then recover from WAL files using WALRecovery
            com.echodb.recovery.WALRecovery walRecovery =
                new com.echodb.recovery.WALRecovery(storageManager, checkpointer, lsmTree);
            walRecovery.recover();
            
            System.out.println("✅ Recovery completed");
            
        } catch (Exception e) {
            System.err.println("❌ Recovery failed: " + e.getMessage());
            throw new EchoDBException("Failed to recover from storage", e);
        }
    }
    
    public void close() throws EchoDBException, IOException {
        dbLock.writeLock().lock();
        try {
            wal.close();
            lsmTree.close();
            checkpointer.stop();  // Stop checkpointer
            cacheManager.close();
            storageManager.close();
        } finally {
            dbLock.writeLock().unlock();
        }
    }

    // ✅ Add getter methods for admin commands
    public S3StorageManager getStorageManager() {
        return storageManager;
    }

    public CacheManager getCacheManager() {
        return cacheManager;
    }

    public long getMemtableSize() {
        return lsmTree.getActiveMemtableSize();
    }

    /*
    WAL Recovery
    */

    public void recoverFromWAL() throws EchoDBException {
        try {
            System.out.println("🔄 Starting checkpoint-based WAL recovery...");

            // ✅ Get current checkpoint to know what's already been flushed
            CheckpointInfo checkpoint = checkpointer.getCurrentCheckpoint();
            System.out.println("📍 Current checkpoint: " + checkpoint);

            // ✅ Use the existing WALRecovery class which already handles checkpoints
            WALRecovery walRecovery = new WALRecovery(storageManager, checkpointer, lsmTree);
            walRecovery.recover();

            System.out.println("✅ Checkpoint-based WAL recovery completed");

        } catch (Exception e) {
            throw new EchoDBException("WAL recovery failed", e);
        }
    }

}
