#!/bin/bash
echo "🛑 Stopping SlateDB services..."

# Stop leader
if [ -f leader.pid ]; then
    kill $(cat leader.pid) 2>/dev/null || true
    rm leader.pid
    echo "✅ Leader stopped"
fi

# Stop follower  
if [ -f follower.pid ]; then
    kill $(cat follower.pid) 2>/dev/null || true
    rm follower.pid
    echo "✅ Follower stopped"
fi