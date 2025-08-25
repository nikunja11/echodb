#!/bin/bash
echo "🧪 Testing Leader Election and Failover..."

# 1. Start cluster
echo "1️⃣ Starting 3-node cluster..."
./run-cluster.sh
sleep 10

# 2. Check initial leader
echo "2️⃣ Checking initial leader..."
echo "Node 1 status:"
curl -s http://localhost:8081/api/status | jq .

echo "Node 2 status:"
curl -s http://localhost:8082/api/status | jq .

echo "Node 3 status:"
curl -s http://localhost:8083/api/status | jq .

# 3. Find current leader
LEADER_PORT=""
for port in 8081 8082 8083; do
    ROLE=$(curl -s http://localhost:$port/api/status | jq -r .role 2>/dev/null)
    if [ "$ROLE" = "LEADER" ]; then
        LEADER_PORT=$port
        echo "✅ Current leader is on port $port"
        break
    fi
done

if [ -z "$LEADER_PORT" ]; then
    echo "❌ No leader found!"
    exit 1
fi

# 4. Write data to leader
echo "3️⃣ Writing data to leader (port $LEADER_PORT)..."
curl -X POST http://localhost:$LEADER_PORT/api/put \
  -H "Content-Type: application/json" \
  -d '{"key": "test:1", "value": "Hello World"}'

curl -X POST http://localhost:$LEADER_PORT/api/put \
  -H "Content-Type: application/json" \
  -d '{"key": "test:2", "value": "Leader Election Test"}'

# 5. Kill current leader
echo "4️⃣ 💥 KILLING CURRENT LEADER (port $LEADER_PORT)..."
if [ "$LEADER_PORT" = "8081" ]; then
    kill $(cat node-1.pid)
    rm node-1.pid
elif [ "$LEADER_PORT" = "8082" ]; then
    kill $(cat node-2.pid)
    rm node-2.pid
elif [ "$LEADER_PORT" = "8083" ]; then
    kill $(cat node-3.pid)
    rm node-3.pid
fi

# 6. Wait for new leader election
echo "5️⃣ Waiting for new leader election..."
sleep 15

# 7. Check new leader
echo "6️⃣ Checking new leader..."
NEW_LEADER_PORT=""
for port in 8081 8082 8083; do
    if [ "$port" = "$LEADER_PORT" ]; then
        continue  # Skip the killed node
    fi
    
    ROLE=$(curl -s http://localhost:$port/api/status 2>/dev/null | jq -r .role 2>/dev/null)
    if [ "$ROLE" = "LEADER" ]; then
        NEW_LEADER_PORT=$port
        echo "✅ New leader elected on port $port"
        break
    fi
done

if [ -z "$NEW_LEADER_PORT" ]; then
    echo "❌ No new leader elected!"
    exit 1
fi

# 8. Test data recovery
echo "7️⃣ Testing data recovery from new leader..."
echo "Reading test:1:"
curl -s http://localhost:$NEW_LEADER_PORT/api/get/test:1

echo "Reading test:2:"
curl -s http://localhost:$NEW_LEADER_PORT/api/get/test:2

# 9. Write new data to new leader
echo "8️⃣ Writing new data to new leader..."
curl -X POST http://localhost:$NEW_LEADER_PORT/api/put \
  -H "Content-Type: application/json" \
  -d '{"key": "test:3", "value": "New Leader Data"}'

echo "✅ Leader election test completed!"