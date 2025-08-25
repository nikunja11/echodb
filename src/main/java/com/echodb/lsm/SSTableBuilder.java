package com.echodb.lsm;

import java.io.*;
import java.util.*;

/**
 * Builder for creating SSTable files
 */
public class SSTableBuilder {
    private final Map<String, byte[]> data;
    private final Map<String, Long> sparseIndex;  // ✅ Build index during construction
    private final int indexInterval;
    
    public SSTableBuilder(int indexInterval) {
        this.data = new TreeMap<>();
        this.sparseIndex = new TreeMap<>();
        this.indexInterval = indexInterval;
    }
    
    public void add(String key, byte[] value) {
        data.put(key, value);
    }
    
    public SSTableData build() throws IOException {
        ByteArrayOutputStream dataStream = new ByteArrayOutputStream();
        ByteArrayOutputStream indexStream = new ByteArrayOutputStream();
        
        try (DataOutputStream dataDos = new DataOutputStream(dataStream);
             DataOutputStream indexDos = new DataOutputStream(indexStream)) {
            
            long offset = 0;
            int entryCount = 0;
            
            // ✅ Build data AND index simultaneously
            for (Map.Entry<String, byte[]> entry : data.entrySet()) {
                String key = entry.getKey();
                byte[] value = entry.getValue();
                
                // Add to sparse index every N entries
                if (entryCount % indexInterval == 0) {
                    sparseIndex.put(key, offset);
                }
                
                // Write data
                dataDos.writeUTF(key);
                dataDos.writeInt(value.length);
                dataDos.write(value);
                
                // Update offset
                offset += 2 + key.getBytes("UTF-8").length + 4 + value.length;
                entryCount++;
            }
            
            // ✅ Serialize sparse index
            for (Map.Entry<String, Long> indexEntry : sparseIndex.entrySet()) {
                indexDos.writeUTF(indexEntry.getKey());
                indexDos.writeLong(indexEntry.getValue());
            }
        }
        
        return new SSTableData(dataStream.toByteArray(), indexStream.toByteArray());
    }
    
    public static class SSTableData {
        private final byte[] data;
        private final byte[] index;
        
        public SSTableData(byte[] data, byte[] index) {
            this.data = data;
            this.index = index;
        }
        
        public byte[] getData() { return data; }
        public byte[] getIndex() { return index; }
    }
}
