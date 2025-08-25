#!/bin/bash
echo "🧪 Testing Leader Failure Recovery..."

# 1. Start services
echo "1️⃣ Starting Leader and Follower..."
./run-leader-http.sh
sleep 3
./run-follower-http.sh
sleep 5

# 2. Write some data to leader
echo "2️⃣ Writing initial data to leader..."
curl -X POST http://localhost:8081/api/put \
  -H "Content-Type: application/json" \
  -d '{"key": "user:1", "value": "John Doe"}'

curl -X POST http://localhost:8081/api/put \
  -H "Content-Type: application/json" \
  -d '{"key": "user:2", "value": "Jane Smith"}'

echo "✅ Initial data written"

# 3. Force flush to create checkpoint
echo "3️⃣ Creating checkpoint..."
curl -X POST http://localhost:8081/api/flush

# 4. Write more data (this will be in WAL only, not flushed)
echo "4️⃣ Writing delta data (WAL only)..."
curl -X POST http://localhost:8081/api/put \
  -H "Content-Type: application/json" \
  -d '{"key": "user:3", "value": "Bob Wilson"}'

curl -X POST http://localhost:8081/api/put \
  -H "Content-Type: application/json" \
  -d '{"key": "user:4", "value": "Alice Brown"}'

echo "✅ Delta data written to WAL"

# 5. Verify data exists in leader
echo "5️⃣ Verifying data in leader..."
curl http://localhost:8081/api/get/user:3
curl http://localhost:8081/api/get/user:4

# 6. Kill leader (simulate failure)
echo "6️⃣ 💥 KILLING LEADER (simulating failure)..."
kill $(cat leader.pid)
rm leader.pid
sleep 2

# 7. Check S3 state
echo "7️⃣ Checking S3 state after leader failure..."
echo "📂 WAL files:"
aws --endpoint-url=http://localhost:4566 s3 ls s3://slatedb-test/wal/ --recursive

echo "📂 Data files:"
aws --endpoint-url=http://localhost:4566 s3 ls s3://slatedb-test/data/ --recursive

echo "📂 Checkpoint:"
aws --endpoint-url=http://localhost:4566 s3 ls s3://slatedb-test/checkpoint --recursive

# 8. Promote follower to leader
echo "8️⃣ Promoting follower to leader..."
# Kill current follower
kill $(cat follower.pid) 2>/dev/null || true

# Start as new leader (will trigger recovery)
nohup java -cp target/classes:target/dependency/* \
    com.slatedb.Main node-2 leader --port=8082 \
    > new-leader.log 2>&1 &

echo $! > new-leader.pid
echo "✅ New leader started with PID: $(cat new-leader.pid)"

# 9. Wait for recovery to complete
echo "9️⃣ Waiting for WAL recovery..."
sleep 10

# 10. Test if delta data was recovered
echo "🔟 Testing if delta data was recovered..."
echo "Testing user:3 (should be recovered from WAL):"
curl http://localhost:8082/api/get/user:3

echo "Testing user:4 (should be recovered from WAL):"
curl http://localhost:8082/api/get/user:4

echo "Testing user:1 (should be from SST):"
curl http://localhost:8082/api/get/user:1

# 11. Check recovery logs
echo "📋 Recovery logs:"
grep -i "recovery\|checkpoint\|wal" new-leader.log

echo "🧪 Test completed!"