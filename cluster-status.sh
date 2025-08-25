#!/bin/bash
echo "📊 SlateDB Cluster Status"
echo "========================"

for i in 1 2 3; do
    port=$((8080 + i))
    if [ -f "node-$i.pid" ]; then
        pid=$(cat node-$i.pid)
        if kill -0 $pid 2>/dev/null; then
            status=$(curl -s http://localhost:$port/api/status 2>/dev/null | jq -r .role 2>/dev/null || echo "UNKNOWN")
            echo "Node $i (port $port): RUNNING - $status (PID: $pid)"
        else
            echo "Node $i (port $port): DEAD (stale PID: $pid)"
        fi
    else
        echo "Node $i (port $port): NOT STARTED"
    fi
done