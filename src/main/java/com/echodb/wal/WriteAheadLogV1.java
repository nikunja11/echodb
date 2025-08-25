package com.echodb.wal;


import com.echodb.config.EchoDBConfig;
import com.echodb.core.SequenceManager;
import com.echodb.storage.S3StorageManager;
import com.echodb.exception.EchoDBException;
import com.echodb.lsm.RowEntry;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Write-Ahead Log implementation with local fsync + periodic S3 upload
 */
public class WriteAheadLogV1 {
    private final EchoDBConfig config;
    private final S3StorageManager storageManager;
    private final SequenceManager sequenceManager;
    private final ScheduledExecutorService scheduler;
    private final ReentrantLock writeLock;

    private final Path localWalPath;
    private RandomAccessFile walFile;
    private FileChannel walChannel;
    private volatile boolean closed = false;

    public WriteAheadLogV1(EchoDBConfig config, S3StorageManager storageManager,
                           SequenceManager sequenceManager) throws IOException {
        this.config = config;
        this.storageManager = storageManager;
        this.sequenceManager = sequenceManager;
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.writeLock = new ReentrantLock();

        // Create local WAL directory
        Path walDir = Paths.get(config.getWalPrefix());
        if (!Files.exists(walDir)) {
            Files.createDirectories(walDir);
        }

        // Create WAL file
        this.localWalPath = walDir.resolve("wal-" + System.currentTimeMillis() + ".log");
        this.walFile = new RandomAccessFile(localWalPath.toFile(), "rw");
        this.walChannel = walFile.getChannel();
    }

    public void append(String key, byte[] value) throws EchoDBException, IOException {
        if (closed) {
            throw new EchoDBException("WAL is closed");
        }
        RowEntry entry = new RowEntry(
                sequenceManager.nextSequence(),
                RowEntry.Type.PUT,
                key,
                value,
                System.currentTimeMillis()
        );
        writeEntry(entry);
    }

    public void appendTombstone(String key) throws EchoDBException, IOException {
        if (closed) {
            throw new EchoDBException("WAL is closed");
        }
        RowEntry entry = new RowEntry(
                sequenceManager.nextSequence(),
                RowEntry.Type.DELETE,
                key,
                null,
                System.currentTimeMillis()
        );
        writeEntry(entry);
    }

    private void writeEntry(RowEntry entry) throws IOException {
        writeLock.lock();
        try {
            byte[] serialized = serializeEntry(entry);
            walChannel.write(ByteBuffer.wrap(serialized));
            walChannel.force(true); // fsync: ensure durability
        } finally {
            writeLock.unlock();
        }
    }

    private byte[] serializeEntry(RowEntry entry) {
        ByteBuffer buffer = ByteBuffer.allocate(
                8 + // sequence number
                        1 + // type
                        4 + entry.getKey().length() +
                        4 + (entry.getValue() != null ? entry.getValue().length : 0) +
                        8 // timestamp
        );

        buffer.putLong(entry.getSequenceNumber());
        buffer.put((byte) entry.getType().ordinal());
        buffer.putInt(entry.getKey().length());
        buffer.put(entry.getKey().getBytes());

        if (entry.getValue() != null) {
            buffer.putInt(entry.getValue().length);
            buffer.put(entry.getValue());
        } else {
            buffer.putInt(0);
        }

        buffer.putLong(entry.getTimestamp());
        return buffer.array();
    }

    /**
     * Periodically uploads WAL file to S3 for persistence
     */
    public void startPeriodicUpload() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                uploadToS3();
            } catch (Exception e) {
                System.err.println("WAL upload failed: " + e.getMessage());
            }
        }, 0, config.getWalFlushInterval().toSeconds(), TimeUnit.SECONDS);
    }

    private void uploadToS3() throws IOException, EchoDBException {
        writeLock.lock();
        try {
            walChannel.force(true); // extra safety before upload
            byte[] fileBytes = Files.readAllBytes(localWalPath);
            String walKey = config.getWalPrefix() + localWalPath.getFileName().toString();
            storageManager.put(walKey, fileBytes);

            System.out.println("☁️ WAL uploaded to S3: " + walKey);
        } finally {
            writeLock.unlock();
        }
    }

    public void close() throws IOException, EchoDBException {
        closed = true;
        uploadToS3();
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        walChannel.close();
        walFile.close();
    }
}

