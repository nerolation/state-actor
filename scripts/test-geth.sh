#!/usr/bin/env bash
# Quick setup: generate state and launch Geth to verify.
#
# Usage:
#   ./scripts/test-geth.sh [--accounts 1000] [--contracts 100] [--max-slots 10000]
#
# Prerequisites:
#   - geth in PATH
#   - state-actor built (go build .)

set -euo pipefail

DATADIR="${DATADIR:-/tmp/state-actor-geth}"
CHAIN_ID="${CHAIN_ID:-1337}"
HTTP_PORT="${HTTP_PORT:-18545}"
INJECT_ADDR="${INJECT_ADDR:-0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

# Forward extra flags (e.g. --accounts 1000 --contracts 100)
EXTRA_FLAGS=("$@")

cleanup() {
    echo "Stopping Geth..."
    kill "$GETH_PID" 2>/dev/null || true
    wait "$GETH_PID" 2>/dev/null || true
}

# 1. Build
echo "==> Building state-actor..."
cd "$ROOT_DIR"
go build -o state-actor .

# 2. Generate state
echo "==> Generating state into $DATADIR/geth/chaindata ..."
rm -rf "$DATADIR"
./state-actor \
    -db "$DATADIR/geth/chaindata" \
    -chain-id "$CHAIN_ID" \
    -inject-accounts "$INJECT_ADDR" \
    -verbose \
    "${EXTRA_FLAGS[@]}"

# 3. Launch Geth
echo ""
echo "==> Starting Geth (HTTP on port $HTTP_PORT)..."
geth \
    --datadir "$DATADIR" \
    --http --http.port "$HTTP_PORT" \
    --http.api eth,web3,net,debug \
    --port 0 --authrpc.port 0 \
    --networkid "$CHAIN_ID" \
    --nodiscover \
    --verbosity 3 \
    > "$DATADIR/geth.log" 2>&1 &
GETH_PID=$!
trap cleanup EXIT

echo "    Geth PID=$GETH_PID, logs: $DATADIR/geth.log"
sleep 3

# 4. Verify
echo ""
echo "==> Verifying genesis block..."
BLOCK=$(curl -s -X POST "http://localhost:$HTTP_PORT" \
    -H 'Content-Type: application/json' \
    -d '{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x0",false],"id":1}')
STATE_ROOT=$(echo "$BLOCK" | python3 -c 'import sys,json; print(json.load(sys.stdin)["result"]["stateRoot"])')
echo "    stateRoot: $STATE_ROOT"

echo ""
echo "==> Querying balance of $INJECT_ADDR ..."
BALANCE=$(curl -s -X POST "http://localhost:$HTTP_PORT" \
    -H 'Content-Type: application/json' \
    -d "{\"jsonrpc\":\"2.0\",\"method\":\"eth_getBalance\",\"params\":[\"$INJECT_ADDR\",\"0x0\"],\"id\":2}")
BAL_HEX=$(echo "$BALANCE" | python3 -c 'import sys,json; print(json.load(sys.stdin)["result"])')
BAL_ETH=$(python3 -c "print(int('$BAL_HEX', 16) / 1e18)")
echo "    balance: $BAL_HEX ($BAL_ETH ETH)"

echo ""
echo "==> Geth is running. Use it at http://localhost:$HTTP_PORT"
echo "    Press Ctrl+C to stop."
wait "$GETH_PID"
