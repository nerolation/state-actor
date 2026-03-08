#!/usr/bin/env bash
# Start all clients (geth, erigon, besu) side-by-side on the same generated state.
#
# Each client gets its own HTTP/WS/P2P/Engine ports to avoid conflicts.
# All clients share the same genesis and state root.
#
# Usage:
#   ./scripts/start-all.sh                # start all three
#   ./scripts/start-all.sh --generate     # generate state first, then start
#   ./scripts/start-all.sh --help         # show all options
#
# Environment variables (override defaults):
#   OUTDIR        Directory containing client datadirs   (default: ./data)
#   CHAIN_ID      Chain ID                               (default: 1337)
#   LOG_LEVEL     Log verbosity                           (default: 3 / INFO)
#
# Port assignments:
#   Client   HTTP    WS      Engine  P2P
#   geth     8545    8546    8551    30303
#   erigon   8645    8646    8651    30403
#   besu     8745    8746    8751    30503

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

OUTDIR="${OUTDIR:-$ROOT_DIR/data}"
CHAIN_ID="${CHAIN_ID:-1337}"
LOG_LEVEL="${LOG_LEVEL:-3}"
GENERATE=true

for arg in "$@"; do
    case "$arg" in
        --help|-h)
            head -23 "$0" | tail -21
            exit 0
            ;;
        --generate)
            GENERATE=true
            ;;
        *)
            echo "Unknown argument: $arg"
            exit 1
            ;;
    esac
done

log() { echo "[$(date +%H:%M:%S)] $*"; }

# ============================================================
# Step 1: Optionally generate state
# ============================================================
if $GENERATE; then
    log "Generating state for all clients..."
    "$SCRIPT_DIR/generate-state.sh"
    echo ""
fi

# Verify data directory exists
if [ ! -d "$OUTDIR" ]; then
    echo "ERROR: Data directory $OUTDIR not found."
    echo "Run ./scripts/generate-state.sh first, or use --generate."
    exit 1
fi

# ============================================================
# Step 2: Launch clients with offset ports
# ============================================================
PIDS=()
NAMES=()

cleanup() {
    echo ""
    log "Shutting down all clients..."
    for i in "${!PIDS[@]}"; do
        if kill -0 "${PIDS[$i]}" 2>/dev/null; then
            log "  Stopping ${NAMES[$i]} (pid ${PIDS[$i]})..."
            kill "${PIDS[$i]}" 2>/dev/null || true
        fi
    done
    # Give clients time to flush
    sleep 2
    for pid in "${PIDS[@]}"; do
        kill -9 "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
    log "All clients stopped."
}
trap cleanup EXIT INT TERM

start_client() {
    local client=$1
    local http_port=$2
    local ws_port=$3
    local authrpc_port=$4
    local p2p_port=$5

    HTTP_PORT="$http_port" \
    WS_PORT="$ws_port" \
    AUTHRPC_PORT="$authrpc_port" \
    P2P_PORT="$p2p_port" \
    OUTDIR="$OUTDIR" \
    CHAIN_ID="$CHAIN_ID" \
    LOG_LEVEL="$LOG_LEVEL" \
        "$SCRIPT_DIR/start-client.sh" "$client" \
        > "$OUTDIR/$client.log" 2>&1 &

    local pid=$!
    PIDS+=("$pid")
    NAMES+=("$client")
    log "CLIENT" $client "FOLDER" outdir
    log "Started $client (pid $pid) → HTTP :$http_port  WS :$ws_port  Engine :$authrpc_port"
}

echo "=========================================="
echo "  Starting all clients"
echo "=========================================="
echo ""

#              client   HTTP   WS     Engine P2P
start_client   geth     8545   8546   8551   30303
start_client   erigon   8645   8646   8651   30403
start_client   besu     8745   8746   8751   30503

echo ""
echo "=========================================="
echo "  All clients running"
echo "=========================================="
echo ""
echo "  geth:    http://localhost:8545    (log: $OUTDIR/geth.log)"
echo "  erigon:  http://localhost:8645    (log: $OUTDIR/erigon.log)"
echo "  besu:    http://localhost:8745    (log: $OUTDIR/besu.log)"
echo ""
echo "  Verify state roots match:"
echo "    for port in 8545 8645 8745; do"
echo "      curl -s http://localhost:\$port -X POST \\"
echo "        -H 'Content-Type: application/json' \\"
echo "        -d '{\"jsonrpc\":\"2.0\",\"method\":\"eth_getBlockByNumber\",\"params\":[\"0x0\",false],\"id\":1}' \\"
echo "        | python3 -c 'import sys,json; r=json.load(sys.stdin)[\"result\"]; print(f\"port {\$port}: stateRoot={r[\"stateRoot\"]}  hash={r[\"hash\"]}\")'"
echo "    done"
echo ""
echo "  Press Ctrl+C to stop all clients."
echo ""

# Wait for any child to exit
while true; do
    for i in "${!PIDS[@]}"; do
        if ! kill -0 "${PIDS[$i]}" 2>/dev/null; then
            log "WARNING: ${NAMES[$i]} (pid ${PIDS[$i]}) exited unexpectedly"
            log "  Check $OUTDIR/${NAMES[$i]}.log for details"
        fi
    done
    sleep 5
done
