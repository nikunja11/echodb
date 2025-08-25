#!/bin/bash
echo "🛑 Stopping SlateDB cluster..."

# Kill all SlateDB processes
pkill -f "com.slatedb.Main" 2>/dev/null || true

# Remove PID files
rm -f node-*.pid

echo "✅ Cluster stopped"