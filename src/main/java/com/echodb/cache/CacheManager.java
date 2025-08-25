package com.echodb.cache;

import com.echodb.config.EchoDBConfig;

import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Cache manager with configurable eviction policies
 */
public class CacheManager {
    private final EchoDBConfig config;
    private final Cache cache;
    private final ReadWriteLock cacheLock;
    
    public CacheManager(EchoDBConfig config) {
        this.config = config;
        this.cacheLock = new ReentrantReadWriteLock();
        
        switch (config.getEvictionPolicy()) {
            case LRU:
                this.cache = new LRUCache(config.getCacheSize());
                break;
            case TWO_CHOICE:
                this.cache = new TwoChoiceCache(config.getCacheSize());
                break;
            default:
                throw new IllegalArgumentException("Unsupported eviction policy: " + config.getEvictionPolicy());
        }
    }
    
    public Optional<byte[]> get(String key) {
        cacheLock.readLock().lock();
        try {
            return cache.get(key);
        } finally {
            cacheLock.readLock().unlock();
        }
    }
    
    public void put(String key, byte[] value) {
        cacheLock.writeLock().lock();
        try {
            cache.put(key, value);
        } finally {
            cacheLock.writeLock().unlock();
        }
    }
    
    public void evict(String key) {
        cacheLock.writeLock().lock();
        try {
            cache.evict(key);
        } finally {
            cacheLock.writeLock().unlock();
        }
    }
    
    public void close() {
        cacheLock.writeLock().lock();
        try {
            // Clear cache on close
            if (cache instanceof LRUCache) {
                ((LRUCache) cache).clear();
            } else if (cache instanceof TwoChoiceCache) {
                ((TwoChoiceCache) cache).clear();
            }
        } finally {
            cacheLock.writeLock().unlock();
        }
    }
    
    public long size() {
        cacheLock.readLock().lock();
        try {
            return cache.size();
        } finally {
            cacheLock.readLock().unlock();
        }
    }
}
