package com.echodb.cache;

import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Two-choice eviction cache implementation
 */
public class TwoChoiceCache implements Cache {
    private final long maxSize;
    private final Map<String, CacheEntry> cache;
    private final AtomicLong currentSize;
    private final Random random;
    
    public TwoChoiceCache(long maxSize) {
        this.maxSize = maxSize;
        this.cache = new ConcurrentHashMap<>();
        this.currentSize = new AtomicLong(0);
        this.random = new Random();
    }
    
    @Override
    public Optional<byte[]> get(String key) {
        CacheEntry entry = cache.get(key);
        if (entry != null) {
            entry.incrementAccessCount();
            return Optional.of(entry.getValue());
        }
        return Optional.empty();
    }
    
    @Override
    public void put(String key, byte[] value) {
        CacheEntry newEntry = new CacheEntry(value);
        CacheEntry oldEntry = cache.put(key, newEntry);
        
        if (oldEntry != null) {
            currentSize.addAndGet(-oldEntry.getSize());
        }
        currentSize.addAndGet(newEntry.getSize());
        
        // Evict if necessary
        while (currentSize.get() > maxSize && !cache.isEmpty()) {
            evictTwoChoice();
        }
    }
    
    @Override
    public void evict(String key) {
        CacheEntry entry = cache.remove(key);
        if (entry != null) {
            currentSize.addAndGet(-entry.getSize());
        }
    }
    
    private void evictTwoChoice() {
        String[] keys = cache.keySet().toArray(new String[0]);
        if (keys.length < 2) {
            if (keys.length == 1) {
                evict(keys[0]);
            }
            return;
        }
        
        // Pick two random keys
        String key1 = keys[random.nextInt(keys.length)];
        String key2 = keys[random.nextInt(keys.length)];
        
        CacheEntry entry1 = cache.get(key1);
        CacheEntry entry2 = cache.get(key2);
        
        if (entry1 == null && entry2 == null) {
            return;
        }
        
        // Evict the one with lower access count
        if (entry1 == null || (entry2 != null && entry1.getAccessCount() > entry2.getAccessCount())) {
            evict(key2);
        } else {
            evict(key1);
        }
    }
    
    @Override
    public void clear() {
        cache.clear();
        currentSize.set(0);
    }
    
    @Override
    public long size() {
        return currentSize.get();
    }
    
    @Override
    public boolean isEmpty() {
        return cache.isEmpty();
    }
    
    private static class CacheEntry {
        private final byte[] value;
        private volatile long accessCount;
        private volatile long lastAccessed;
        
        CacheEntry(byte[] value) {
            this.value = value;
            this.accessCount = 1;
            this.lastAccessed = System.currentTimeMillis();
        }
        
        void incrementAccessCount() {
            accessCount++;
            lastAccessed = System.currentTimeMillis();
        }
        
        long getAccessCount() {
            return accessCount;
        }
        
        long getLastAccessed() {
            return lastAccessed;
        }
        
        byte[] getValue() {
            return value;
        }
        
        long getSize() {
            return value != null ? value.length : 0;
        }
    }
}
