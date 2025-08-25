package com.echodb.cluster;

import com.echodb.config.EchoDBConfig;
import com.echodb.storage.S3StorageManager;
import com.echodb.election.LeaderInfo;
import com.echodb.exception.EchoDBException;

/**
 * Simple leader registry for designated leader/follower setup
 */
public class LeaderRegistry {
    private static final String LEADER_PREFIX = "cluster/leaders/";
    
    private final S3StorageManager storageManager;
    
    public LeaderRegistry(EchoDBConfig config) {
        this.storageManager = new S3StorageManager(config);
    }
    
    /**
     * Register this node as the designated leader
     */
    public void registerAsLeader(String nodeId, String host, int port) throws EchoDBException {
        LeaderInfo leaderInfo = new LeaderInfo(
            nodeId,
            System.currentTimeMillis(),
            System.currentTimeMillis() + 30000 // 30 second lease
        );
        
        String key = LEADER_PREFIX + nodeId;
        storageManager.put(key, leaderInfo.serialize());
        
        System.out.println("✅ Registered as leader: " + nodeId + " at " + host + ":" + port);
    }
    
    /**
     * Send heartbeat to maintain leadership
     */
    public void sendHeartbeat(String nodeId) throws EchoDBException {
        LeaderInfo heartbeat = new LeaderInfo(
            nodeId,
            System.currentTimeMillis(),
            System.currentTimeMillis() + 30000
        );
        
        String key = LEADER_PREFIX + nodeId;
        storageManager.put(key, heartbeat.serialize());
    }
    
    /**
     * Get leader information
     */
    public LeaderInfo getLeaderInfo(String nodeId) throws EchoDBException {
        String key = LEADER_PREFIX + nodeId;
        return storageManager.get(key)
            .map(data -> {
                try {
                    return LeaderInfo.deserialize(data);
                } catch (Exception e) {
                    return null;
                }
            })
            .orElse(null);
    }
    
    /**
     * Step down as leader
     */
    public void stepDown(String nodeId) throws EchoDBException {
        String key = LEADER_PREFIX + nodeId;
        storageManager.delete(key);
        System.out.println("👋 Stepped down as leader: " + nodeId);
    }
}