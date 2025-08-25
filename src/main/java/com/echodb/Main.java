package com.echodb;

import com.echodb.cluster.LeaderMonitor;
import com.echodb.cluster.LeaderRegistry;
import com.echodb.cache.EvictionPolicy;
import com.echodb.config.EchoDBConfig;
import com.echodb.core.EchoDB;
import com.echodb.election.CASLeaderElection;
import com.echodb.election.LeaderInfo;
import com.echodb.exception.EchoDBException;
import com.echodb.http.EchoDBHttpServer;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Main application demonstrating SlateDB usage with CAS-based leader election
 */
public class Main {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: java Main <nodeId> <role> [options]");
            System.out.println("Roles: leader, follower");
            System.out.println("Options: --port=<port>, --http-server, --console");
            System.out.println("Examples:");
            System.out.println("  java Main node-1 leader --port=8081 --console");
            System.out.println("  java Main node-1 leader --port=8081 --http-server");
            System.out.println("  java Main node-2 follower --leader=node-1 --port=8082");
            System.exit(1);
        }
        
        String nodeId = args[0];
        String role = args[1].toLowerCase();
        
        // Parse additional options
        Map<String, String> options = parseOptions(args);
        int port = Integer.parseInt(options.getOrDefault("port", "8080"));
        boolean httpServer = options.containsKey("http-server");
        boolean console = options.containsKey("console");

        // Default behavior: console mode unless http-server is specified
        if (!httpServer && !console) {
            console = true;
        }

        try {
            String s3Bucket = System.getenv("S3_BUCKET");
            EchoDBConfig config = new EchoDBConfig.Builder()
                .s3Bucket(s3Bucket)
                .s3Region("us-east-1")
                .evictionPolicy(EvictionPolicy.LRU)
                .addNode("node-1", "http://localhost:8081")
                .addNode("node-2", "http://localhost:8082") 
                .addNode("node-3", "http://localhost:8083")
                .build();
            
            switch (role) {
                case "leader":
                    runAsDesignatedLeader(nodeId, port, config, httpServer, console);
                    break;
                case "follower":
                    String leaderNodeId = options.get("leader");
                    if (leaderNodeId == null) {
                        System.err.println("Follower nodes must specify --leader=<nodeId>");
                        System.exit(1);
                    }
                    runAsDesignatedFollower(nodeId, port, leaderNodeId, config, httpServer, console);
                    break;
                case "auto":
                    runWithLeaderElection(nodeId, port, config, httpServer, console);
                    break;
                default:
                    System.err.println("Invalid role: " + role + ". Use 'leader' or 'follower'");
                    System.exit(1);
            }
            
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void runWithLeaderElection(String nodeId, int port, EchoDBConfig config,
                                              boolean httpServer, boolean console)
            throws EchoDBException, IOException {
        System.out.println("🗳️ Starting node with automatic leader election: " + nodeId);

        EchoDB db = new EchoDB(config);

        // ✅ Create leader election (not designated leader)
        CASLeaderElection leaderElection = new CASLeaderElection(config, nodeId, false);

        // ✅ Set WAL recovery callback
        leaderElection.setWALRecoveryCallback(() -> {
            try {
                System.out.println("🔄 Starting WAL recovery for new leader: " + nodeId);
                db.recoverFromWAL();
                System.out.println("✅ WAL recovery completed for: " + nodeId);
            } catch (Exception e) {
                System.err.println("❌ WAL recovery failed for " + nodeId + ": " + e.getMessage());
            }
        });

        leaderElection.start();

        // ✅ Monitor leadership changes
        ScheduledExecutorService monitorScheduler = Executors.newSingleThreadScheduledExecutor();
        AtomicBoolean wasLeader = new AtomicBoolean(false);

        monitorScheduler.scheduleAtFixedRate(() -> {
            boolean isCurrentlyLeader = leaderElection.isLeader();
            boolean previouslyLeader = wasLeader.get();

            if (isCurrentlyLeader && !previouslyLeader) {
                System.out.println("🎯 " + nodeId + " became LEADER!");
                wasLeader.set(true);
            } else if (!isCurrentlyLeader && previouslyLeader) {
                System.out.println("👥 " + nodeId + " became FOLLOWER");
                wasLeader.set(false);
            }
        }, 1, 2, TimeUnit.SECONDS);

        // ✅ Start HTTP server if requested
        if (httpServer) {
            startHttpServer(db, nodeId, leaderElection, port);
            //startHttpServer(db, leaderElection, port);
        }

        // ✅ Start console if requested
        if (console) {
            runAutoConsole(db, leaderElection);
        } else {
            // ✅ Keep the process alive for HTTP server mode
            System.out.println("🚀 Node running in auto-election mode on port " + port);
            System.out.println("📋 Press Ctrl+C to shutdown");

            // Wait for shutdown signal
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\n🛑 Shutting down node " + nodeId + "...");
                monitorScheduler.shutdown();
                leaderElection.stop();
                try {
                    db.close();
                } catch (Exception e) {
                    System.err.println("Error during shutdown: " + e.getMessage());
                }
            }));

            // Keep main thread alive
            try {
                Thread.currentThread().join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        if (console) {
            monitorScheduler.shutdown();
            leaderElection.stop();
            db.close();
        }
    }

    private static void runAutoConsole(EchoDB db, CASLeaderElection leaderElection) {
        Scanner scanner = new Scanner(System.in);
        System.out.println("🎮 SlateDB Auto-Election Console");
        System.out.println("Commands: put <key> <value>, get <key>, status, quit");

        while (true) {
            System.out.print("> ");
            String input = scanner.nextLine().trim();

            if (input.equals("quit")) {
                break;
            }

            String[] parts = input.split(" ", 3);
            String command = parts[0].toLowerCase();

            try {
                switch (command) {
                    case "put":
                        if (parts.length != 3) {
                            System.out.println("Usage: put <key> <value>");
                            continue;
                        }
                        if (!leaderElection.isLeader()) {
                            System.out.println("❌ Cannot write - not the leader");
                            continue;
                        }
                        db.put(parts[1], parts[2].getBytes());
                        System.out.println("✅ Put: " + parts[1] + " = " + parts[2]);
                        break;

                    case "get":
                        if (parts.length != 2) {
                            System.out.println("Usage: get <key>");
                            continue;
                        }
                        Optional<byte[]> result = db.get(parts[1]);
                        if (result.isPresent()) {
                            System.out.println("✅ " + parts[1] + " = " + new String(result.get()));
                        } else {
                            System.out.println("❌ Key not found: " + parts[1]);
                        }
                        break;

                    case "status":
                        String role = leaderElection.isLeader() ? "LEADER" : "FOLLOWER";
                        System.out.println("📊 Node status: " + role);
                        break;

                    default:
                        System.out.println("Unknown command: " + command);
                }
            } catch (Exception e) {
                System.out.println("❌ Error: " + e.getMessage());
            }
        }
    }
    
    private static void runAsDesignatedLeaderOld(String nodeId, int port, EchoDBConfig config)
            throws EchoDBException, IOException {
        System.out.println("🎯 Starting as DESIGNATED LEADER: " + nodeId);
        
        EchoDB db = new EchoDB(config);
        LeaderRegistry leaderRegistry = new LeaderRegistry(config);
        leaderRegistry.registerAsLeader(nodeId, "localhost", port);
        
        ScheduledExecutorService heartbeatScheduler = Executors.newSingleThreadScheduledExecutor();
        heartbeatScheduler.scheduleAtFixedRate(() -> {
            try {
                leaderRegistry.sendHeartbeat(nodeId);
            } catch (EchoDBException e) {
                System.err.println("Failed to send heartbeat: " + e.getMessage());
            }
        }, 0, 10, TimeUnit.SECONDS);
        
        runLeaderConsole(db);
        
        heartbeatScheduler.shutdown();
        leaderRegistry.stepDown(nodeId);
        db.close();
    }

    private static void runAsDesignatedLeader(String nodeId, int port, EchoDBConfig config,
                                              boolean httpServer, boolean console)
            throws EchoDBException, IOException {
        System.out.println("🎯 Starting as DESIGNATED LEADER: " + nodeId);
        System.out.println("🎯 httpServer: " + httpServer);

        EchoDB db = new EchoDB(config);
        LeaderRegistry leaderRegistry = new LeaderRegistry(config);
        leaderRegistry.registerAsLeader(nodeId, "localhost", port);

        ScheduledExecutorService heartbeatScheduler = Executors.newSingleThreadScheduledExecutor();
        heartbeatScheduler.scheduleAtFixedRate(() -> {
            try {
                leaderRegistry.sendHeartbeat(nodeId);
            } catch (EchoDBException e) {
                System.err.println("Failed to send heartbeat: " + e.getMessage());
            }
        }, 0, 10, TimeUnit.SECONDS);

        // ✅ Start HTTP server if requested
        if (httpServer) {
            CASLeaderElection leaderElection = new CASLeaderElection(config, nodeId, true);
            leaderElection.start(); // forcing it to become leader
            startHttpServer(db, nodeId, leaderElection, port);
        }

        // ✅ Start console if requested
        if (console) {
            runLeaderConsole(db);
        } else {
            // ✅ Keep the process alive for HTTP server mode
            System.out.println("🚀 Leader running in HTTP server mode on port " + port);
            System.out.println("📋 Press Ctrl+C to shutdown");

            // Wait for shutdown signal
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\n🛑 Shutting down leader...");
                heartbeatScheduler.shutdown();
                try {
                    leaderRegistry.stepDown(nodeId);
                    db.close();
                } catch (Exception e) {
                    System.err.println("Error during shutdown: " + e.getMessage());
                }
            }));

            // Keep main thread alive
            try {
                Thread.currentThread().join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        if (console) {
            heartbeatScheduler.shutdown();
            leaderRegistry.stepDown(nodeId);
            db.close();
        }
    }
    
    private static void runAsDesignatedFollowerOld(String nodeId, int port, String leaderNodeId,
                                              EchoDBConfig config) throws EchoDBException, IOException {
        System.out.println("👥 Starting as DESIGNATED FOLLOWER: " + nodeId + " (leader: " + leaderNodeId + ")");
        
        EchoDB db = new EchoDB(config);
        LeaderRegistry leaderRegistry = new LeaderRegistry(config);
        LeaderInfo leaderInfo = leaderRegistry.getLeaderInfo(leaderNodeId);
        
        if (leaderInfo == null) {
            System.err.println("Cannot find leader: " + leaderNodeId);
            System.exit(1);
        }
        
        LeaderMonitor leaderMonitor = new LeaderMonitor(leaderRegistry, leaderNodeId);
        leaderMonitor.start();
        
        runFollowerConsole(db, leaderMonitor);
        
        leaderMonitor.stop();
        db.close();
    }

    private static void runAsDesignatedFollower(String nodeId, int port, String leaderNodeId,
                                                EchoDBConfig config, boolean httpServer, boolean console)
            throws EchoDBException, IOException {
        System.out.println("👥 Starting as DESIGNATED FOLLOWER: " + nodeId + " (leader: " + leaderNodeId + ")");

        EchoDB db = new EchoDB(config);
        LeaderRegistry leaderRegistry = new LeaderRegistry(config);
        LeaderInfo leaderInfo = leaderRegistry.getLeaderInfo(leaderNodeId);

        if (leaderInfo == null) {
            System.err.println("Cannot find leader: " + leaderNodeId);
            System.exit(1);
        }

        LeaderMonitor leaderMonitor = new LeaderMonitor(leaderRegistry, leaderNodeId);
        leaderMonitor.start();

        // ✅ Start HTTP server if requested
        if (httpServer) {
            CASLeaderElection leaderElection = new CASLeaderElection(config, nodeId);
            startHttpServer(db, nodeId, leaderElection, port);
        }

        // ✅ Start console if requested
        if (console) {
            runFollowerConsole(db, leaderMonitor);
        } else {
            // ✅ Keep the process alive for HTTP server mode
            System.out.println("🚀 Follower running in HTTP server mode on port " + port);
            System.out.println("📋 Press Ctrl+C to shutdown");

            // Wait for shutdown signal
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\n🛑 Shutting down follower...");
                leaderMonitor.stop();
                try {
                    db.close();
                } catch (Exception e) {
                    System.err.println("Error during shutdown: " + e.getMessage());
                }
            }));

            // Keep main thread alive
            try {
                Thread.currentThread().join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        if (console) {
            leaderMonitor.stop();
            db.close();
        }
    }
    
    private static void runLeaderConsole(EchoDB db) {
        Scanner scanner = new Scanner(System.in);
        System.out.println("\n🎯 SlateDB Leader Console");
        System.out.println("Commands: put <key> <value>, get <key>, delete <key>, flush, list-wal, list-sst, storage-stats, help, quit");
        
        while (true) {
            try {
                System.out.print("leader> ");
                String input = scanner.nextLine().trim();
                
                if (input.equals("quit")) break;
                
                String[] parts = input.split("\\s+", 3);
                if (parts.length == 0) continue;
                
                switch (parts[0].toLowerCase()) {
                    case "put":
                        if (parts.length >= 3) {
                            db.put(parts[1], parts[2].getBytes());
                            System.out.println("✅ PUT " + parts[1] + " = " + parts[2]);
                        } else {
                            System.out.println("Usage: put <key> <value>");
                        }
                        break;
                        
                    case "get":
                        if (parts.length >= 2) {
                            var result = db.get(parts[1]);
                            if (result.isPresent()) {
                                System.out.println("📖 GET " + parts[1] + " = " + new String(result.get()));
                            } else {
                                System.out.println("❌ GET " + parts[1] + " = NOT_FOUND");
                            }
                        } else {
                            System.out.println("Usage: get <key>");
                        }
                        break;
                        
                    case "delete":
                        if (parts.length >= 2) {
                            db.delete(parts[1]);
                            System.out.println("🗑️ DELETE " + parts[1]);
                        } else {
                            System.out.println("Usage: delete <key>");
                        }
                        break;
                        
                    case "flush":
                        db.flush().get();
                        System.out.println("💾 FLUSH completed - Check for SST files now!");
                        break;
                        
                    case "list-wal":
                        listWALFiles(db);
                        break;
                        
                    case "list-sst":
                        listSSTFiles(db);
                        break;
                        
                    case "storage-stats":
                        showStorageStats(db);
                        break;
                        
                    case "help":
                        printLeaderHelp();
                        break;
                        
                    default:
                        if(!parts[0].isEmpty())
                            System.out.println("❓ Unknown command: " + parts[0]);
                }
                
            } catch (Exception e) {
                System.err.println("❌ Error: " + e.getMessage());
            }
        }
        
        scanner.close();
        System.exit(0);
    }
    
    private static void runFollowerConsole(EchoDB db, LeaderMonitor leaderMonitor) {
        Scanner scanner = new Scanner(System.in);
        System.out.println("\n👥 SlateDB Follower Console (Read-Only)");
        System.out.println("Commands: get <key>, list-wal, list-sst, storage-stats, leader-status, help, quit");
        
        while (true) {
            try {
                System.out.print("follower> ");
                String input = scanner.nextLine().trim();
                
                if (input.equals("quit")) break;
                
                String[] parts = input.split("\\s+", 3);
                if (parts.length == 0) continue;
                
                switch (parts[0].toLowerCase()) {
                    case "get":
                        if (parts.length >= 2) {
                            var result = db.get(parts[1]);
                            if (result.isPresent()) {
                                System.out.println("📖 GET " + parts[1] + " = " + new String(result.get()));
                            } else {
                                System.out.println("❌ GET " + parts[1] + " = NOT_FOUND");
                            }
                        } else {
                            System.out.println("Usage: get <key>");
                        }
                        break;
                    
                    case "list-wal":
                        listWALFiles(db);
                        break;
                    
                    case "list-sst":
                        listSSTFiles(db);
                        break;
                    
                    case "storage-stats":
                        showStorageStats(db);
                        break;
                    
                    case "leader-status":
                        boolean alive = leaderMonitor.isLeaderAlive();
                        System.out.println("👑 Leader Status: " + (alive ? "ALIVE ✅" : "DEAD ❌"));
                        break;
                    
                    case "help":
                        printFollowerHelp();
                        break;
                    
                    default:
                        if(!parts[0].isEmpty())
                            System.out.println("❓ Unknown command: " + parts[0] + ". Type 'help' for available commands.");
                }
                
            } catch (Exception e) {
                System.err.println("❌ Error: " + e.getMessage());
            }
        }
        
        scanner.close();
        System.exit(0);
    }

    private static void printFollowerHelp() {
        System.out.println("📋 Follower Commands:");
        System.out.println("  get <key>          - Read value for key");
        System.out.println("  list-wal           - List WAL files in S3");
        System.out.println("  list-sst           - List SST files in S3");
        System.out.println("  storage-stats      - Show storage statistics");
        System.out.println("  leader-status      - Check if leader is alive");
        System.out.println("  help               - Show this help");
        System.out.println("  quit               - Exit the program");
    }
    
    private static void printLeaderHelp() {
        System.out.println("📋 Leader Commands:");
        System.out.println("  put <key> <value>  - Write key-value pair");
        System.out.println("  get <key>          - Read value for key");
        System.out.println("  delete <key>       - Delete key");
        System.out.println("  flush              - Force flush memtables to SST files");
        System.out.println("  list-wal           - List WAL files in S3");
        System.out.println("  list-sst           - List SST files in S3");
        System.out.println("  storage-stats      - Show storage statistics");
        System.out.println("  help               - Show this help");
        System.out.println("  quit               - Exit the program");
    }

    private static void listWALFiles(EchoDB db) {
        try {
            List<String> walFiles = db.getStorageManager().listWALFiles();
            System.out.println("📝 WAL Files (" + walFiles.size() + "):");
            for (String file : walFiles) {
                System.out.println("  " + file);
            }
        } catch (Exception e) {
            System.out.println("❌ Error listing WAL files: " + e.getMessage());
        }
    }

    private static void listSSTFiles(EchoDB db) {
        try {
            // ✅ Try multiple prefixes to find SST files
            List<String> l0Files = db.getStorageManager().listFiles("data/l0/");
            List<String> dataFiles = db.getStorageManager().listFiles("data/");
            
            System.out.println("🗃️ SST Files:");
            System.out.println("  L0 Files (" + l0Files.size() + "):");
            for (String file : l0Files) {
                System.out.println("    " + file);
            }
            System.out.println("  All Data Files (" + dataFiles.size() + "):");
            for (String file : dataFiles) {
                System.out.println("    " + file);
            }
        } catch (Exception e) {
            System.out.println("❌ Error listing SST files: " + e.getMessage());
        }
    }

    private static void showStorageStats(EchoDB db) {
        try {
            List<String> walFiles = db.getStorageManager().listWALFiles();
            List<String> sstFiles = db.getStorageManager().listFiles("data/");
            
            System.out.println("📊 Storage Statistics:");
            System.out.println("  WAL Files: " + walFiles.size());
            System.out.println("  SST Files: " + sstFiles.size());
            System.out.println("  Cache Size: " + db.getCacheManager().size());
            System.out.println("  Memtable Size: " + db.getMemtableSize());
        } catch (Exception e) {
            System.out.println("❌ Error getting storage stats: " + e.getMessage());
        }
    }
    
    private static Map<String, String> parseOptions(String[] args) {
        Map<String, String> options = new HashMap<>();

        for (int i = 2; i < args.length; i++) {
            String arg = args[i];
            if (arg.startsWith("--")) {
                String[] parts = arg.substring(2).split("=", 2);
                if (parts.length == 2) {
                    options.put(parts[0], parts[1]);
                } else {
                    // flag with no value -> store as "true"
                    options.put(parts[0], "true");
                }
            }
        }
        return options;
    }


    private static void startHttpServer(EchoDB db, String nodeId,
                                        CASLeaderElection leaderElection, int port) {
        try {
            System.out.println("🌐 HTTP Server would start here on port " + port);
            EchoDBHttpServer httpServer = new EchoDBHttpServer(db, leaderElection, nodeId, port);
            httpServer.start();
        } catch (Exception e) {
            System.err.println("Failed to start HTTP server: " + e.getMessage());
        }
    }
}
