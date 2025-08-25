package com.echodb.lsm;

import com.echodb.cache.CacheManager;
import com.echodb.storage.S3StorageManager;
import com.echodb.exception.EchoDBException;

import java.io.*;
import java.util.*;
import java.util.function.BiConsumer;

/**
 * Sorted String Table stored in S3
 */
public class SSTable {
    private final String tableId;
    private final String dataKey;
    private final String indexKey;  // ✅ Separate index key
    private final S3StorageManager storageManager;
    private final CacheManager cacheManager;
    private volatile SSTableIndex index;
    
    public SSTable(String tableId, String dataKey, String indexKey, 
                   S3StorageManager storageManager, CacheManager cacheManager) {
        this.tableId = tableId;
        this.dataKey = dataKey;
        this.indexKey = indexKey;
        this.storageManager = storageManager;
        this.cacheManager = cacheManager;
    }
    
    public Optional<byte[]> get(String key) throws EchoDBException {
        // Check cache first
        System.out.println("SSTable GET");
        String cacheKey = "sstable:" + tableId + ":" + key;
        Optional<byte[]> cached = cacheManager.get(cacheKey);
        if (cached.isPresent()) {
            System.out.println("cachekey : "+cacheKey);
            System.out.println("Returning from cache...");
            return cached;
        }
        
        // ✅ Load index if not already loaded (much smaller file!)
        if (index == null) {
            System.out.println("Loading index from S3");
            loadIndexFromS3();
        }
        
        // Use index to find approximate location
        Optional<Long> offset = index.getOffset(key);
        System.out.println("Approx Offset: "+offset);
        if (offset.isEmpty()) {
            return Optional.empty();
        }
        
        // Read from S3 and search
        Optional<byte[]> result = searchInBlock(key, offset.get());
        
        // Cache the result
        result.ifPresent(value -> cacheManager.put(cacheKey, value));
        
        return result;
    }
    
    public void forEach(BiConsumer<String, byte[]> consumer) throws EchoDBException {
        // ✅ Use dataKey instead of s3Key
        Optional<byte[]> data = storageManager.get(dataKey);
        if (data.isEmpty()) {
            return;
        }
        
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data.get()))) {
            while (dis.available() > 0) {
                String key = dis.readUTF();
                int valueLength = dis.readInt();
                byte[] value = new byte[valueLength];
                dis.readFully(value);
                
                consumer.accept(key, value);
            }
        } catch (IOException e) {
            throw new EchoDBException("Failed to iterate SSTable", e);
        }
    }
    
    private void loadIndexFromS3() throws EchoDBException {
        // ✅ Load only the small index file, not the entire data!
        System.out.println("Index key :"+indexKey);
        Optional<byte[]> indexData = storageManager.get(indexKey);
        if (indexData.isEmpty()) {
            System.out.println("Index data is null");
            this.index = new SSTableIndex(new TreeMap<>());
            return;
        }
        
        Map<String, Long> indexMap = new TreeMap<>();
        
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(indexData.get()))) {
            while (dis.available() > 0) {
                String key = dis.readUTF();
                long offset = dis.readLong();
                indexMap.put(key, offset);
            }
            
            this.index = new SSTableIndex(indexMap);
            
        } catch (IOException e) {
            throw new EchoDBException("Failed to load SSTable index", e);
        }
    }
    
    private Optional<byte[]> searchInBlock(String key, long startOffset) throws EchoDBException {
        // ✅ Now we only read the data file from the specific offset
        Optional<byte[]> data = storageManager.get(dataKey);
        if (data.isEmpty()) {
            return Optional.empty();
        }
        
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data.get()))) {
            dis.skipBytes((int) startOffset);
            
            // Linear search from the offset (could be optimized with binary search)
            while (dis.available() > 0) {
                String currentKey = dis.readUTF();
                int valueLength = dis.readInt();
                
                if (currentKey.equals(key)) {
                    byte[] value = new byte[valueLength];
                    dis.readFully(value);
                    return Optional.of(value);
                } else if (currentKey.compareTo(key) > 0) {
                    // Keys are sorted, so we've passed our target
                    break;
                } else {
                    // Skip this value and continue
                    dis.skipBytes(valueLength);
                }
            }
            
            return Optional.empty();
            
        } catch (IOException e) {
            throw new EchoDBException("Failed to search SSTable", e);
        }
    }
    
    public String getTableId() {
        return tableId;
    }
    
    public String getS3Key() {
        // ✅ Return dataKey (the main data file)
        return dataKey;
    }
    
    private static class SSTableIndex {
        private final TreeMap<String, Long> index;
        
        public SSTableIndex(Map<String, Long> index) {
            this.index = new TreeMap<>(index);
        }
        
        public Optional<Long> getOffset(String key) {
            // Find the largest key <= target key
            Map.Entry<String, Long> entry = index.floorEntry(key);
            return entry != null ? Optional.of(entry.getValue()) : Optional.of(0L);
        }
    }
}
