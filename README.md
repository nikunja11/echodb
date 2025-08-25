# SlateDB - Distributed Database with CAS-based Leader Election

A distributed database with compute-storage separation, using S3 for storage and CAS-based leader election.

## Quick Start

### 1. Local Development with LocalStack

```bash
Set AWS Credentials : This is required for Localstack to run
aws configure list
aws configure set aws_access_key_id test
aws configure set aws_secret_access_key test
aws configure set region us-east-1


Terminal 1
# Start LocalStack if not running
docker-compose up -d localstack

# Create S3 bucket
aws --endpoint-url=http://localhost:4566 s3 mb s3://echodb-test 2>/dev/null

Terminal 2
# ✅ Set S3 endpoint for LocalStack
export S3_ENDPOINT=http://localhost:4566
export S3_BUCKET=echodb-test 

Start The leader on terminal 1
mvn exec:java \
    -Dexec.mainClass="com.echodb.Main" \
    -Dexec.args="node-1 leader --port=8081"

Terminal 3
Start The Follower 
# ✅ Set S3 endpoint for LocalStack
export S3_ENDPOINT=http://localhost:4566
export S3_BUCKET=echodb-test
mvn exec:java \
    -Dexec.mainClass="com.echodb.Main" \
    -Dexec.args="node-2 follower --leader=node-1 --port=8082"


Teminal 2(Leader)
leader> put db:123 "Echo"
leader> put db:125 "DB"
leader> put db:130 "LSMTree"

The entries are in WAL and Memtable. Flush to persist in SSTable

leader> flush

Terminal 3
follower> GET db:123

```

### 2. Multi-Node Cluster 

```bash
#Start each node in auto mode, let leader election process decide the leader

1. Start LocalStack (if not already running)
docker-compose up -d localstack

2. Create S3 bucket
aws --endpoint-url=http://localhost:4566 s3 mb s3://echodb-test 2>/dev/null || true

3. Start 3 nodes in auto mode
This triggers leader election. One node will be elected as the leader, while the others act as followers.
👉 Only the leader can accept writes, since the system follows a single-writer design.

Start Node 1
export S3_ENDPOINT=http://localhost:4566
export S3_BUCKET=echodb-test 
mvn exec:java \
  -Dexec.mainClass="com.echodb.Main" \
  -Dexec.args="node-1 auto --port=8081 --http-server"

Start Node 2
export S3_ENDPOINT=http://localhost:4566
export S3_BUCKET=echodb-test 
mvn exec:java \
  -Dexec.mainClass="com.echodb.Main" \
  -Dexec.args="node-2 auto --port=8082 --http-server"

Start Node 3
export S3_ENDPOINT=http://localhost:4566
export S3_BUCKET=echodb-test 
mvn exec:java \
  -Dexec.mainClass="com.echodb.Main" \
  -Dexec.args="node-3 auto --port=8083 --http-server"

4. Verify Leader Election
Check the status of each node:

curl http://localhost:8081/api/status
curl http://localhost:8082/api/status
curl http://localhost:8083/api/status

5. Write Data to Leader
(Assume Node 1 on port 8081 is elected leader)

curl -X POST http://localhost:8081/api/put \
  -H "Content-Type: application/json" \
  -d '{"key": "test:1", "value": "Hello World"}'

curl -X POST http://localhost:8081/api/put \
  -H "Content-Type: application/json" \
  -d '{"key": "test:2", "value": "Leader Election Test"}'

6. Trigger Manual Flush
A flush normally occurs when the immutable Memtable size exceeds the 
configured threshold.
You can manually trigger it to persist data into SSTables and confirm it is readable 
from followers:

curl -X POST http://localhost:8081/api/flush

7. Verify Reads From Followers After Flush

curl -s http://localhost:8082/api/get/test:1
curl -s http://localhost:8083/api/get/test:1


# Or run multiple nodes manually
java -jar slatedb.jar --node-id=node-1 --port=8081
java -jar slatedb.jar --node-id=node-2 --port=8082
java -jar slatedb.jar --node-id=node-3 --port=8083
```

### 3. Using the Client

```java
// ✅ Updated client configuration example
SlateDBConfig config = new SlateDBConfig.Builder()
    .s3Bucket("my-bucket")
    .evictionPolicy(EvictionPolicy.LRU)
    .addNode("node-1", "http://localhost:8081")
    .addNode("node-2", "http://localhost:8082")
    .addNode("node-3", "http://localhost:8083")
    .build();

SlateDBClient client = new SlateDBClient(config);

// Write operations (go to leader)
client.put("user:123", "John Doe".getBytes()).get();

// Read operations (can go to any node)
Optional<byte[]> result = client.get("user:123").get();

// Delete operations (go to leader)
client.delete("user:123").get();
```

## Architecture

### Leader Election
- Uses S3 as coordination service for CAS operations
- Lease-based leadership with automatic failover
- No complex consensus protocol needed

### Write Path
1. Client finds current leader
2. Leader writes to WAL (S3)
3. Leader updates LSM tree
4. Leader updates cache
5. Background flush to S3

### Read Path
1. Check local cache
2. Check LSM tree (memory)
3. Check LSM tree (disk/S3)
4. Cache result

## Configuration

```java
SlateDBConfig config = new SlateDBConfig.Builder()
    .s3Bucket("my-slatedb-bucket")
    .s3Region("us-east-1")
    .evictionPolicy(EvictionPolicy.TWO_CHOICE) // or LRU
    .memtableSize(64 * 1024 * 1024) // 64MB
    .cacheSize(256 * 1024 * 1024)   // 256MB
    .walFlushInterval(Duration.ofSeconds(5))
    .build();
```

## Fault Tolerance

- **Leader Failure**: Automatic re-election via CAS
- **Network Partitions**: Lease expiration prevents split-brain
- **Data Durability**: WAL and LSM trees in S3
- **Cache Failures**: Automatic fallback to storage

## Monitoring

Check leader status:
```bash
curl http://localhost:8081/health
curl http://localhost:8081/metrics
```
