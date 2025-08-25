package com.echodb.election;

import com.echodb.config.EchoDBConfig;
import com.echodb.storage.S3StorageManager;
import com.echodb.exception.EchoDBException;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * CAS-based leader election using S3 as coordination service
 */
public class CASLeaderElection {
    private static final String LEADER_KEY = "leader/current";
    private static final long LEASE_DURATION_SECONDS = 30;
    private static final long HEARTBEAT_INTERVAL_SECONDS = 10;
    
    private final EchoDBConfig config;
    private final S3StorageManager storageManager;
    private final String nodeId;
    private final AtomicBoolean isLeader;
    private final ScheduledExecutorService scheduler;
    private volatile boolean running;
    private final boolean designatedLeader;

    // ✅ Add callback for WAL recovery
    private Runnable walRecoveryCallback;

    public CASLeaderElection(EchoDBConfig config, String nodeId) {
        this.config = config;
        this.storageManager = new S3StorageManager(config);
        this.nodeId = nodeId;
        this.isLeader = new AtomicBoolean(false);
        // ✅ Fix: Use newScheduledThreadPool instead of newScheduledExecutorService
        this.scheduler = Executors.newScheduledThreadPool(2);
        this.running = false;
        this.designatedLeader = false;
    }

    public CASLeaderElection(EchoDBConfig config, String nodeId, boolean designatedLeader) {
        this.config = config;
        this.storageManager = new S3StorageManager(config);
        this.nodeId = nodeId;
        this.isLeader = new AtomicBoolean(false);
        // ✅ Fix: Use newScheduledThreadPool instead of newScheduledExecutorService
        this.scheduler = Executors.newScheduledThreadPool(2);
        this.running = false;
        this.designatedLeader = designatedLeader;
    }

    public void setWALRecoveryCallback(Runnable callback) {
        this.walRecoveryCallback = callback;
    }

    public void start() {
        running = true;
        if (designatedLeader) {
            // For designated leader, just start heartbeat
            isLeader.set(true);
            //scheduler.scheduleAtFixedRate(this::sendHeartbeat, 0, HEARTBEAT_INTERVAL_SECONDS, TimeUnit.SECONDS);
            // Already sending heartbeat in Main
            System.out.println("✅ Started as designated leader: " + nodeId);
        } else {
            // Start election process
            scheduler.scheduleAtFixedRate(this::tryBecomeLeader, 0, 5, TimeUnit.SECONDS);
            System.out.println("Started leader election for node: " + nodeId);
        }
    }
    
    public void startHeartbeat() {
        if (!isLeader.get()) {
            throw new IllegalStateException("Cannot start heartbeat - not a leader");
        }
        
        scheduler.scheduleAtFixedRate(this::sendHeartbeat, 
            HEARTBEAT_INTERVAL_SECONDS, HEARTBEAT_INTERVAL_SECONDS, TimeUnit.SECONDS);
        
        System.out.println("Started heartbeat for leader: " + nodeId);
    }
    
//    private void tryBecomeLeader() {
//        if (!running) return;
//
//        try {
//            Optional<LeaderInfo> currentLeader = getCurrentLeaderInfo();
//
//            if (currentLeader.isEmpty() || isLeaderExpired(currentLeader.get())) {
//                // Try to become leader using CAS
//                if (attemptLeadership()) {
//                    isLeader.set(true);
//                    System.out.println("Successfully became leader: " + nodeId);
//                }
//            } else if (currentLeader.get().getNodeId().equals(nodeId)) {
//                // We are already the leader
//                isLeader.set(true);
//            } else {
//                // Another node is leader
//                isLeader.set(false);
//            }
//
//        } catch (SlateDBException e) {
//            System.err.println("Error during leader election: " + e.getMessage());
//            isLeader.set(false);
//        }
//    }

    private void tryBecomeLeader() {
        if (!running) return;

        try {
            Optional<LeaderInfo> currentLeader = getCurrentLeaderInfo();

            if (currentLeader.isEmpty() || isLeaderExpired(currentLeader.get())) {
                // Try to become leader using CAS
                System.out.println("Current leader info is empty");
                if (attemptLeadership()) {
                    isLeader.set(true);
                    System.out.println("✅ Successfully became leader: " + nodeId);

                    // Start heartbeat immediately
                    scheduler.scheduleAtFixedRate(this::sendHeartbeat, 0, HEARTBEAT_INTERVAL_SECONDS, TimeUnit.SECONDS);
                } else {
                    isLeader.set(false);
                    // System.out.println("❌ Failed to become leader: " + nodeId);
                }
            } else if (currentLeader.get().getNodeId().equals(nodeId)) {
                // We are already the leader
                if (!isLeader.get()) {
                    System.out.println("✅ Confirmed leadership: " + nodeId);
                }
                isLeader.set(true);
            } else {
                // Another node is leader
                if (isLeader.get()) {
                    System.out.println("👥 Stepping down, another leader exists: " + currentLeader.get().getNodeId());
                }
                isLeader.set(false);
            }

        } catch (EchoDBException e) {
            System.err.println("❌ Error during leader election: " + e.getMessage());
            isLeader.set(false);
        }
    }
    
//    private boolean attemptLeadership() throws SlateDBException {
//        LeaderInfo newLeaderInfo = new LeaderInfo(
//            nodeId,
//            Instant.now().getEpochSecond(),
//            Instant.now().getEpochSecond() + LEASE_DURATION_SECONDS
//        );
//        try {
//            // Try to create leader key (CAS operation)
//            // In real S3, you'd use conditional puts or DynamoDB for true CAS
//            Optional<byte[]> existing = storageManager.get(LEADER_KEY);
//
//            if (existing.isEmpty()) {
//                // No current leader, try to claim
//                storageManager.put(LEADER_KEY, newLeaderInfo.serialize());
//
//                // Verify we actually got it (simple check)
//                Optional<byte[]> verification = storageManager.get(LEADER_KEY);
//                if (verification.isPresent()) {
//                    LeaderInfo verified = LeaderInfo.deserialize(verification.get());
//                    return verified.getNodeId().equals(nodeId);
//                }
//            }
//
//            return false;
//
//        } catch (Exception e) {
//            throw new SlateDBException("Failed to attempt leadership", e);
//        }
//    }

    private boolean attemptLeadership() throws EchoDBException {
        LeaderInfo newLeaderInfo = new LeaderInfo(
                nodeId,
                Instant.now().getEpochSecond(),
                Instant.now().getEpochSecond() + LEASE_DURATION_SECONDS
        );

        try {
            // ✅ Add random delay to reduce election conflicts
            Thread.sleep((long) (Math.random() * 1000));

            // Try to create leader key (CAS operation)
            Optional<byte[]> existing;
            try {
                existing = storageManager.get(LEADER_KEY);
            } catch (EchoDBException e) {
                existing = Optional.empty();
            }

            if (existing.isEmpty()) {
                // No current leader, try to claim
                storageManager.put(LEADER_KEY, newLeaderInfo.serialize());

                // ✅ Add small delay and verify we actually got it
                Thread.sleep(100);
                Optional<byte[]> verification = storageManager.get(LEADER_KEY);
                if (verification.isPresent()) {
                    LeaderInfo verified = LeaderInfo.deserialize(verification.get());
                    boolean success = verified.getNodeId().equals(nodeId);
                    if (success) {
                        System.out.println("🎯 " + nodeId + " claimed leadership!");
                        // ✅ Trigger WAL recovery when becoming leader
                        triggerWALRecovery();
                    }
                    return success;
                }
            } else {
                // Check if existing leader is expired
                LeaderInfo existingLeader = LeaderInfo.deserialize(existing.get());
                if (isLeaderExpired(existingLeader)) {
                    // Try to replace expired leader
                    storageManager.put(LEADER_KEY, newLeaderInfo.serialize());

                    Thread.sleep(100);
                    Optional<byte[]> verification = storageManager.get(LEADER_KEY);
                    if (verification.isPresent()) {
                        LeaderInfo verified = LeaderInfo.deserialize(verification.get());
                        boolean success = verified.getNodeId().equals(nodeId);
                        if (success) {
                            System.out.println("🎯 " + nodeId + " replaced expired leader!");
                            // ✅ Trigger WAL recovery when becoming leader
                            triggerWALRecovery();
                        }
                        return success;
                    }
                }
            }
            return false;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (Exception e) {
            throw new EchoDBException("Failed to attempt leadership", e);
        }
    }

    // ✅ Add WAL recovery trigger
    private void triggerWALRecovery() {
        if (walRecoveryCallback != null) {
            try {
                System.out.println("🔄 Triggering WAL recovery for new leader: " + nodeId);
                walRecoveryCallback.run();
            } catch (Exception e) {
                System.err.println("❌ WAL recovery failed: " + e.getMessage());
            }
        }
    }

    private void sendHeartbeat() {
        if (!isLeader.get() || !running) return;
        
        try {
            LeaderInfo heartbeat = new LeaderInfo(
                nodeId,
                Instant.now().getEpochSecond(),
                Instant.now().getEpochSecond() + LEASE_DURATION_SECONDS
            );
            
            storageManager.put(LEADER_KEY, heartbeat.serialize());
            
        } catch (EchoDBException e) {
            System.err.println("Failed to send heartbeat: " + e.getMessage());
            isLeader.set(false);
        }
    }
    
//    private Optional<LeaderInfo> getCurrentLeaderInfo() {
//        try {
//            Optional<byte[]> data = storageManager.get(LEADER_KEY);
//            return data.map(LeaderInfo::deserialize);
//        } catch (EchoDBException e) {
//            return Optional.empty();
//        }
//
//    }

    private Optional<LeaderInfo> getCurrentLeaderInfo() {

        try {
            Optional<byte[]> data = storageManager.get(LEADER_KEY);
            return data.map(LeaderInfo::deserialize);
        } catch (EchoDBException e) {
            return Optional.empty();
        }
    }

    private boolean isLeaderExpired(LeaderInfo leaderInfo) {
        return Instant.now().getEpochSecond() > leaderInfo.getLeaseExpiry();
    }
    
    public boolean isLeader() {
        return isLeader.get();
    }

    public String getCurrentLeader() {
        Optional<LeaderInfo> leader = getCurrentLeaderInfo();
        return leader.map(LeaderInfo::getNodeId).orElse(null);
    }

    public void stepDown() {
        isLeader.set(false);
        try {
            storageManager.delete(LEADER_KEY);
            System.out.println("Stepped down as leader: " + nodeId);
        } catch (EchoDBException e) {
            System.err.println("Error stepping down: " + e.getMessage());
        }
    }
    
    public void stop() {
        running = false;
        if (isLeader.get()) {
            stepDown();
        }
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        storageManager.close();
    }
}
