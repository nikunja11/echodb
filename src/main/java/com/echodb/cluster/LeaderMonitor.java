package com.echodb.cluster;

import com.echodb.election.LeaderInfo;
import com.echodb.exception.EchoDBException;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Monitors leader health for followers
 */
public class LeaderMonitor {
    private final LeaderRegistry leaderRegistry;
    private final String leaderNodeId;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean leaderAlive;
    private volatile boolean running;
    
    public LeaderMonitor(LeaderRegistry leaderRegistry, String leaderNodeId) {
        this.leaderRegistry = leaderRegistry;
        this.leaderNodeId = leaderNodeId;
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.leaderAlive = new AtomicBoolean(false);
        this.running = false;
    }
    
    public void start() {
        running = true;
        scheduler.scheduleAtFixedRate(this::checkLeaderHealth, 0, 5, TimeUnit.SECONDS);
        System.out.println("👀 Started monitoring leader: " + leaderNodeId);
    }
    
    private void checkLeaderHealth() {
        if (!running) return;
        
        try {
            LeaderInfo leaderInfo = leaderRegistry.getLeaderInfo(leaderNodeId);
            
            if (leaderInfo == null) {
                leaderAlive.set(false);
                System.out.println("💀 Leader not found: " + leaderNodeId);
            } else if (System.currentTimeMillis() > leaderInfo.getLeaseExpiry()) {
                leaderAlive.set(false);
                System.out.println("⏰ Leader lease expired: " + leaderNodeId);
            } else {
                if (!leaderAlive.get()) {
                    System.out.println("💚 Leader is alive: " + leaderNodeId + "\n");
                }
                leaderAlive.set(true);
            }

            /*
             *  leaderAlive is an AtomicBoolean that tracks the last known state.
                If it was previously false (meaning last check said leader was dead), then we now just saw it come alive again.
                So, it prints "💚 Leader is alive" once, the moment the state flips from dead ➝ alive.
                🔄 Think of it as a state transition detector.
             */
            
        } catch (EchoDBException e) {
            leaderAlive.set(false);
            System.err.println("❌ Error checking leader health: " + e.getMessage());
        }
    }
    
    public boolean isLeaderAlive() {
        return leaderAlive.get();
    }
    
    public void stop() {
        running = false;
        scheduler.shutdown();
    }
}
