package com.echodb.lsm;

import com.echodb.cache.CacheManager;
import com.echodb.checkpoint.Checkpointer;
import com.echodb.config.EchoDBConfig;
import com.echodb.core.SequenceManager;
import com.echodb.storage.S3StorageManager;
import com.echodb.exception.EchoDBException;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * LSM Tree implementation with S3 storage
 */
public class LSMTree {
    private final EchoDBConfig config;
    private final S3StorageManager storageManager;
    private final CacheManager cacheManager;
    private final ReadWriteLock treeLock;
    private final ScheduledExecutorService compactionExecutor;
    
    private volatile Memtable activeMemtable;
    private final List<Memtable> immutableMemtables;
    private final List<SSTable> l0Tables;
    private final Map<Integer, List<SSTable>> levels;
    private final Checkpointer checkpointer;


    public LSMTree(EchoDBConfig config, S3StorageManager storageManager,
                   CacheManager cacheManager, SequenceManager sequenceManager,
                   Checkpointer checkpointer) {
        this.config = config;
        this.storageManager = storageManager;
        this.cacheManager = cacheManager;
        this.treeLock = new ReentrantReadWriteLock();
        this.compactionExecutor = Executors.newSingleThreadScheduledExecutor();
        
        this.activeMemtable = new Memtable(config.getMemtableSize());
        this.immutableMemtables = new ArrayList<>();
        this.l0Tables = new ArrayList<>();
        this.levels = new ConcurrentHashMap<>();
        this.checkpointer = checkpointer;
    }

    // ✅ Discover SST files without loading data (just metadata)
    public void discoverSSTFiles() throws EchoDBException {
        treeLock.writeLock().lock();
        try {
            System.out.println("🔍 LSMTree: Discovering SST files in S3...");

            // Discover L0 SST files
            //List<String> l0Files = storageManager.listFiles("data/l0/");
            List<String> l0Files = safeListFiles("data/l0/");
            System.out.println("📂 Found " + l0Files.size() + " L0 files in S3");

            for (String file : l0Files) {
                if (file.endsWith(".data")) {
                    String tableId = extractTableId(file);
                    String indexKey = file.replace(".data", ".index");

                    // ✅ Create SSTable reference (doesn't load data, just metadata)
                    SSTable sstable = new SSTable(tableId, file, indexKey, storageManager, cacheManager);
                    l0Tables.add(sstable);
                    System.out.println("📋 Registered L0 SSTable: " + tableId);
                }
            }

            // Discover other levels
            for (int level = 1; level <= 10; level++) {
                //List<String> levelFiles = storageManager.listFiles("data/l" + level + "/");
                List<String> levelFiles = safeListFiles("data/l" + level + "/");
                if (!levelFiles.isEmpty()) {
                    List<SSTable> levelTables = new ArrayList<>();
                    for (String file : levelFiles) {
                        if (file.endsWith(".data")) {
                            String tableId = extractTableId(file);
                            String indexKey = file.replace(".data", ".index");

                            SSTable sstable = new SSTable(tableId, file, indexKey, storageManager, cacheManager);
                            levelTables.add(sstable);
                            System.out.println("📋 Registered L" + level + " SSTable: " + tableId);
                        }
                    }
                    levels.put(level, levelTables);
                }
            }

            System.out.println("✅ SST discovery completed. L0: " + l0Tables.size() + ", Other levels: " + levels.size());

        } finally {
            treeLock.writeLock().unlock();
        }
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

    // ✅ Periodically discover new SST files (for followers)
    public void startSSTDiscovery() {
        ScheduledExecutorService discoveryExecutor = Executors.newSingleThreadScheduledExecutor();

        discoveryExecutor.scheduleWithFixedDelay(() -> {
            try {
                discoverNewSSTFiles();
            } catch (Exception e) {
                System.err.println("❌ SST discovery failed: " + e.getMessage());
            }
        }, 10, 10, TimeUnit.SECONDS);
    }

    private void discoverNewSSTFiles() throws EchoDBException {
        // Only discover new files, don't reload existing ones
        //List<String> l0Files = storageManager.listFiles("data/l0/");
        List<String> l0Files = safeListFiles("data/l0/");

        for (String file : l0Files) {
            if (file.endsWith(".data")) {
                String tableId = extractTableId(file);

                boolean alreadyKnown = l0Tables.stream()
                        .anyMatch(table -> table.getTableId().equals(tableId));

                if (!alreadyKnown) {
                    String indexKey = file.replace(".data", ".index");
                    SSTable sstable = new SSTable(tableId, file, indexKey, storageManager, cacheManager);
                    l0Tables.add(sstable);
                    //System.out.println("🆕 Discovered new SSTable: " + tableId);
                }
            }
        }
    }


    
//    public void put(String key, byte[] value) throws EchoDBException {
//        treeLock.readLock().lock();
//        try {
//            if (activeMemtable.size() >= config.getMemtableSize()) {
//                rotateMemtable();
//            }
//            activeMemtable.put(key, value);
//        } finally {
//            treeLock.readLock().unlock();
//        }
//    }

    public void putWithSequence(String key, byte[] value, long sequence) throws EchoDBException, IOException {
        treeLock.readLock().lock();
        try {
            if (activeMemtable.size() >= config.getMemtableSize()) {
                rotateMemtable();
            }
            activeMemtable.putWithSequence(key, value, sequence);
        } finally {
            treeLock.readLock().unlock();
        }
    }
    
    public Optional<byte[]> get(String key) throws EchoDBException {
        treeLock.readLock().lock();
        try {
            // Check active memtable
            Optional<byte[]> result = activeMemtable.get(key);
            if (result.isPresent()) {
                System.out.println("Returning from active memtable");
                return result;
            }
            
            // Check immutable memtables
            for (Memtable memtable : immutableMemtables) {
                result = memtable.get(key);
                if (result.isPresent()) {
                    System.out.println("Returning from immutable memtable");
                    return result;
                }
            }
            
            // Check L0 tables
            System.out.println("l0Tables size :"+l0Tables.size());
            for (SSTable table : l0Tables) {
                System.out.println("Checking loTables...");
                result = table.get(key);
                if (result.isPresent()) {
                    return result;
                }
            }
            
            // Check other levels
            for (int level = 1; level <= getMaxLevel(); level++) {
                List<SSTable> levelTables = levels.get(level);
                if (levelTables != null) {
                    for (SSTable table : levelTables) {
                        result = table.get(key);
                        if (result.isPresent()) {
                            return result;
                        }
                    }
                }
            }
            
            return Optional.empty();
        } finally {
            treeLock.readLock().unlock();
        }
    }
    
//    public void delete(String key) throws EchoDBException {
//        treeLock.readLock().lock();
//        try {
//            if (activeMemtable.size() >= config.getMemtableSize()) {
//                rotateMemtable();
//            }
//            activeMemtable.delete(key);
//        } finally {
//            treeLock.readLock().unlock();
//        }
//    }

    public void deleteWithSequence(String key, long sequence) throws EchoDBException {
        treeLock.readLock().lock();
        try {
            if (activeMemtable.size() >= config.getMemtableSize()) {
                rotateMemtable();
            }
            activeMemtable.deleteWithSequence(key, sequence);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            treeLock.readLock().unlock();
        }
    }
    
    public void flush() throws EchoDBException, IOException {
        treeLock.writeLock().lock();
        try {
            // ✅ Move active memtable to immutable first if it has data
            if (!activeMemtable.isEmpty()) {
                immutableMemtables.add(activeMemtable);
                activeMemtable = new Memtable(config.getMemtableSize());
            }
            
            long maxSequenceFlushed = 0L;
            
            // Flush immutable memtables to L0
            Iterator<Memtable> iterator = immutableMemtables.iterator();
            while (iterator.hasNext()) {
                Memtable memtable = iterator.next();
                
                // ✅ Only flush if memtable has data
                if (!memtable.isEmpty()) {
                    SSTable sstable = flushMemtableToSSTable(memtable);
                    l0Tables.add(sstable);
                    
                    // Track the highest sequence number flushed
                    maxSequenceFlushed = Math.max(maxSequenceFlushed, 
                                                memtable.getMaxSequenceNumber());
                    
                    System.out.println("✅ Flushed memtable to SSTable: " + sstable.getTableId());
                }
                
                iterator.remove();
            }
            
            // Update checkpoint after successful flush
            if (maxSequenceFlushed > 0) {
                checkpointer.updateCheckpoint(maxSequenceFlushed, getCurrentWALOffset());
                System.out.println("📍 Updated checkpoint after flush batch: sequence=" + maxSequenceFlushed);
            }

        } finally {
            treeLock.writeLock().unlock();
        }
    }
    
    private void rotateMemtable() throws EchoDBException, IOException {
        treeLock.writeLock().lock();
        try {
            immutableMemtables.add(activeMemtable);
            activeMemtable = new Memtable(config.getMemtableSize());
            
            if (immutableMemtables.size() >= config.getMaxMemtables()) {
                flush();
            }
        } finally {
            treeLock.writeLock().unlock();
        }
    }

    private SSTable flushMemtableToSSTable(Memtable memtable) throws EchoDBException, IOException {
        String tableId = "sstable-" + System.currentTimeMillis() + "-" + UUID.randomUUID();
        String dataKey = config.getDataPrefix() + "l0/" + tableId + ".data";
        String indexKey = config.getDataPrefix() + "l0/" + tableId + ".index";

        // ✅ Get the highest sequence number from memtable before flushing
        long highestSequence = memtable.getMaxSequenceNumber();

        // ✅ Use consistent indexInterval (every 10th key for L0)
        int indexInterval = 10;
        SSTableBuilder builder = new SSTableBuilder(indexInterval);
        memtable.forEach(builder::add);

        SSTableBuilder.SSTableData sstableData = builder.build();

        storageManager.put(dataKey, sstableData.getData());
        storageManager.put(indexKey, sstableData.getIndex());

        // ✅ Update checkpoint after successful flush
        if (checkpointer != null && highestSequence > 0) {
            checkpointer.updateCheckpoint(highestSequence, getCurrentWALOffset());
            System.out.println("📍 Updated checkpoint after flushing memtable: sequence=" + highestSequence);
        }

        return new SSTable(tableId, dataKey, indexKey, storageManager, cacheManager);
    }


    
    public void startCompaction() {
        compactionExecutor.scheduleAtFixedRate(() -> {
            try {
                performCompaction();
            } catch (EchoDBException | IOException e) {
                System.err.println("Compaction failed: " + e.getMessage());
            }
        }, 0, config.getCompactionInterval().toMinutes(), TimeUnit.MINUTES);
    }
    
    private void performCompaction() throws EchoDBException, IOException {
        // Simple compaction strategy: compact L0 when it has too many files
        if (l0Tables.size() > 4) {
            compactL0ToL1();
        }
    }
    
    private void compactL0ToL1() throws EchoDBException, IOException {
        treeLock.writeLock().lock();
        try {
            List<SSTable> l1Tables = levels.computeIfAbsent(1, k -> new ArrayList<>());
            
            // Merge L0 tables with L1 tables
            List<SSTable> allTables = new ArrayList<>(l0Tables);
            allTables.addAll(l1Tables);
            
            SSTable compactedTable = mergeTables(allTables, 1);
            
            // Replace old tables with compacted table
            l0Tables.clear();
            l1Tables.clear();
            l1Tables.add(compactedTable);
        } finally {
            treeLock.writeLock().unlock();
        }
    }
    
    private SSTable mergeTables(List<SSTable> tables, int level) throws EchoDBException, IOException {
        String tableId = "sstable-l" + level + "-" + System.currentTimeMillis();
        String dataKey = config.getDataPrefix() + "l" + level + "/" + tableId + ".data";
        String indexKey = config.getDataPrefix() + "l" + level + "/" + tableId + ".index";
        
        // ✅ Use larger indexInterval for higher levels (fewer index entries)
        int indexInterval = Math.min(50, 10 * level);  // L1=10, L2=20, L3=30, etc., max 50
        SSTableBuilder builder = new SSTableBuilder(indexInterval);
        
        // Merge all tables (simplified - should use proper merge iterator)
        Map<String, byte[]> mergedData = new TreeMap<>();
        for (SSTable table : tables) {
            table.forEach(mergedData::put);
        }
        
        mergedData.forEach(builder::add);
        
        SSTableBuilder.SSTableData sstableData = builder.build();
        storageManager.put(dataKey, sstableData.getData());
        storageManager.put(indexKey, sstableData.getIndex());
        
        return new SSTable(tableId, dataKey, indexKey, storageManager, cacheManager);
    }
    
    private int getMaxLevel() {
        return levels.keySet().stream().mapToInt(Integer::intValue).max().orElse(0);
    }
    
    public void close() throws EchoDBException, IOException {
        flush();
        compactionExecutor.shutdown();
        try {
            if (!compactionExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                compactionExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            compactionExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
    
    private long getCurrentWALOffset() {
        // This would need to be coordinated with WAL to get current offset
        return System.currentTimeMillis(); // Simplified for now
    }

    // ✅ Add getter for active memtable size
    public long getActiveMemtableSize() {
        treeLock.readLock().lock();
        try {
            return activeMemtable.size();
        } finally {
            treeLock.readLock().unlock();
        }
    }

    // ✅ Add getter for total memtable count
    public int getTotalMemtableCount() {
        treeLock.readLock().lock();
        try {
            return 1 + immutableMemtables.size(); // active + immutable
        } finally {
            treeLock.readLock().unlock();
        }
    }

    // ✅ Add recovery method to load existing SST files
    public void recover() throws EchoDBException {
        treeLock.writeLock().lock();
        try {
            System.out.println("🔄 LSMTree: Loading existing SST files...");
            
            // Load L0 SST files
            //List<String> l0Files = storageManager.listFiles("data/l0/");
            List<String> l0Files = safeListFiles("data/l0/");
            for (String file : l0Files) {
                if (file.endsWith(".data")) {
                    String tableId = extractTableId(file);
                    String indexKey = file.replace(".data", ".index");
                    SSTable sstable = new SSTable(tableId, file, indexKey, storageManager, cacheManager);
                    l0Tables.add(sstable);
                    System.out.println("📂 Loaded L0 SSTable: " + tableId);
                }
            }
            
            // Load other levels
            for (int level = 1; level <= 7; level++) {
                List<String> levelFiles = safeListFiles("data/l" + level + "/");
                //List<String> levelFiles = storageManager.listFiles("data/l" + level + "/");
                if (!levelFiles.isEmpty()) {
                    List<SSTable> levelTables = new ArrayList<>();
                    for (String file : levelFiles) {
                        if (file.endsWith(".data")) {
                            String tableId = extractTableId(file);
                            String indexKey = file.replace(".data", ".index");
                            SSTable sstable = new SSTable(tableId, file, indexKey, storageManager, cacheManager);
                            levelTables.add(sstable);
                            System.out.println("📂 Loaded L" + level + " SSTable: " + tableId);
                        }
                    }
                    levels.put(level, levelTables);
                }
            }
            
        } finally {
            treeLock.writeLock().unlock();
        }
    }

    private String extractTableId(String filePath) {
        // Extract table ID from path like "data/l0/sstable-123456.data"
        String fileName = filePath.substring(filePath.lastIndexOf('/') + 1);
        return fileName.replace(".data", "");
    }
}
