#!/bin/bash
# Emerging Edge watchdog
set -u
REPO=/Users/martinsjogren/AI/emerging-edge
PORT=8878
HEALTH_URL="http://127.0.0.1:${PORT}/api/status"
LOG=/tmp/emerging-edge.log
WATCHDOG_LOG=/tmp/emerging-edge-watchdog.log
# Load from .env if present
if [ -f "$REPO/.env" ]; then source "$REPO/.env"; fi
export SERPER_API_KEY="${SERPER_API_KEY:-}"
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
cd "$REPO" || exit 1

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" >> "$WATCHDOG_LOG"; }

start_server() {
    PIDS=$(lsof -ti ":${PORT}" 2>/dev/null || true)
    if [ -n "$PIDS" ]; then
        log "killing stale processes on port ${PORT}: $PIDS"
        echo "$PIDS" | xargs kill -9 2>/dev/null || true
        sleep 1
    fi
    log "starting server"
    nohup /usr/bin/python3 monitor.py serve >> "$LOG" 2>&1 &
    for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do
        sleep 1
        if curl -s --max-time 3 "$HEALTH_URL" >/dev/null 2>&1; then
            log "server ready after ${i}s"
            return 0
        fi
    done
    log "server failed to become healthy after 15s"
    return 1
}

is_healthy() {
    CODE=$(curl -s -o /dev/null --max-time 8 -w "%{http_code}" "$HEALTH_URL" 2>/dev/null || echo "000")
    [ "$CODE" = "200" ]
}

log "watchdog started (pid $$)"
while true; do
    if ! is_healthy; then
        log "unhealthy response, restarting server"
        start_server || log "restart attempt failed"
    fi
    sleep 60
done
