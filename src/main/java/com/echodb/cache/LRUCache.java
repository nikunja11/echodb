package com.echodb.cache;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * LRU Cache implementation
 */
public class LRUCache implements Cache {
    private final long maxSize;
    private final LinkedHashMap<String, CacheEntry> cache;
    private long currentSize;
    
    public LRUCache(long maxSize) {
        this.maxSize = maxSize;
        this.currentSize = 0;
        this.cache = new LinkedHashMap<String, CacheEntry>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, CacheEntry> eldest) {
                if (currentSize > LRUCache.this.maxSize) {
                    currentSize -= eldest.getValue().getSize();
                    return true;
                }
                return false;
            }
        };
    }
    
    @Override
    public Optional<byte[]> get(String key) {
        CacheEntry entry = cache.get(key);
        return entry != null ? Optional.of(entry.getValue()) : Optional.empty();
    }
    
    @Override
    public void put(String key, byte[] value) {
        CacheEntry oldEntry = cache.get(key);
        if (oldEntry != null) {
            currentSize -= oldEntry.getSize();
        }
        
        CacheEntry newEntry = new CacheEntry(value);
        cache.put(key, newEntry);
        currentSize += newEntry.getSize();
    }
    
    @Override
    public void evict(String key) {
        CacheEntry entry = cache.remove(key);
        if (entry != null) {
            currentSize -= entry.getSize();
        }
    }
    
    @Override
    public void clear() {
        cache.clear();
        currentSize = 0;
    }
    
    @Override
    public long size() {
        return currentSize;
    }

    @Override
    public boolean isEmpty() {
        return cache.isEmpty();
    }
}