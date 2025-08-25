package com.echodb.lsm;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

/**
 * In-memory table for LSM Tree
 */
public class Memtable {
    private final ConcurrentSkipListMap<String, RowEntry> data;
    private final AtomicLong size; // It’s actually tracking the approximate memory footprint
    /*
     When you insert/overwrite:
    if (oldEntry != null) {
        size.addAndGet(-oldEntry.getSize());
    }
    size.addAndGet(newEntry.getSize());

    First subtract the old entry’s size (if key existed).
    Then add the new entry’s size.
     */
    private final long maxSize;
    private volatile long maxSequenceNumber = 0;  // ✅ Track max sequence
    
    public Memtable(long maxSize) {
        this.data = new ConcurrentSkipListMap<>();
        this.size = new AtomicLong(0);
        this.maxSize = maxSize;
    }
    
//    public void put(String key, byte[] value) {
//        RowEntry oldEntry = data.get(key);
//        RowEntry newEntry = new RowEntry(
//            sequenceManager.nextSequence(),
//            RowEntry.Type.PUT,
//            key,
//            value,
//            System.currentTimeMillis()
//        );
//
//        data.put(key, newEntry);
//
//        if (oldEntry != null) {
//            size.addAndGet(-oldEntry.getSize());
//        }
//        size.addAndGet(newEntry.getSize());
//    }

    public void putWithSequence(String key, byte[] value, long sequence) {
        RowEntry oldEntry = data.get(key);
        RowEntry newEntry = new RowEntry(
            sequence,
            RowEntry.Type.PUT,
            key,
            value,
            System.currentTimeMillis()
        );
        
        data.put(key, newEntry);
        
        // ✅ Update max sequence
        maxSequenceNumber = Math.max(maxSequenceNumber, sequence);
        
        if (oldEntry != null) {
            size.addAndGet(-oldEntry.getSize());
        }
        size.addAndGet(newEntry.getSize());
    }
    
//    public void delete(String key) {
//        RowEntry oldEntry = data.get(key);
//        RowEntry tombstone = new RowEntry(
//            sequenceManager.nextSequence(),
//            RowEntry.Type.DELETE,
//            key,
//            null,
//            System.currentTimeMillis()
//        );
//
//        data.put(key, tombstone);
//
//        if (oldEntry != null) {
//            size.addAndGet(-oldEntry.getSize());
//        }
//        size.addAndGet(tombstone.getSize());
//    }

    public void deleteWithSequence(String key, long sequence) {
        RowEntry oldEntry = data.get(key);
        RowEntry tombstone = new RowEntry(
                sequence,
                RowEntry.Type.DELETE,
                key,
                null,
                System.currentTimeMillis()
        );

        data.put(key, tombstone);
        
        // ✅ Update max sequence
        maxSequenceNumber = Math.max(maxSequenceNumber, sequence);

        if (oldEntry != null) {
            size.addAndGet(-oldEntry.getSize());
        }
        size.addAndGet(tombstone.getSize());
    }
    
    public Optional<byte[]> get(String key) {
        RowEntry entry = data.get(key);
        if (entry == null || entry.isDeleted()) {
            return Optional.empty();
        }
        return Optional.of(entry.getValue());
    }
    
    public long size() {
        return data.size();
    }

    public long getMemorySize() {
        return size.get();
    }

    public boolean isEmpty() {
        return data.isEmpty();
    }
    
    public void forEach(BiConsumer<String, byte[]> consumer) {
        data.forEach((key, entry) -> {
            if (!entry.isDeleted()) {
                consumer.accept(key, entry.getValue());
            }
        });
    }
    
    public Iterator<Map.Entry<String, RowEntry>> iterator() {
        return data.entrySet().iterator();
    }
    
    // ✅ Add getter for max sequence
    public long getMaxSequenceNumber() {
        return data.values().stream()
            .mapToLong(RowEntry::getSequenceNumber)
            .max()
            .orElse(0L);
    }

}
