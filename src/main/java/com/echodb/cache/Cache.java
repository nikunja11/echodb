package com.echodb.cache;

import java.util.Optional;

/**
 * Cache interface for SlateDB
 */
public interface Cache {
    Optional<byte[]> get(String key);
    void put(String key, byte[] value);
    void evict(String key);
    
    // ✅ Add missing methods
    void clear();
    long size();
    boolean isEmpty();
}
