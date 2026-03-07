#!/usr/bin/env bash
# Test state-actor with all three clients via Docker.
#
# Usage:
#   ./scripts/test-clients.sh [geth|erigon|nethermind|all]
#
# Prerequisites:
#   - Docker
#   - geth in PATH (for Geth test only)
#   - go toolchain

set -euo pipefail

CLIENT="${1:-all}"
CHAIN_ID=1337
CHAIN_ID_HEX="0x539"
INJECT_ADDR="0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
EXPECTED_BALANCE_HEX="0x33b2e3c91efc989409c0000"
EXPECTED_BALANCE_WEI="999999999000000000000000000"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
PASS=0
FAIL=0

# Collect state roots from each client for cross-verification
declare -A STATE_ROOTS

cd "$ROOT_DIR"
go build -o state-actor .

check() {
    local name=$1
    local condition=$2
    local msg=$3
    if eval "$condition"; then
        echo "  PASS: $msg"
        PASS=$((PASS + 1))
    else
        echo "  FAIL: $msg"
        FAIL=$((FAIL + 1))
    fi
}

verify_client() {
    local port=$1
    local name=$2
    local expected_eth=$3

    echo "  Querying genesis block..."
    local block
    block=$(curl -sf -X POST "http://localhost:$port" \
        -H 'Content-Type: application/json' \
        -d '{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x0",false],"id":1}') || {
        echo "  FAIL: $name RPC not reachable"
        FAIL=$((FAIL + 1))
        return
    }

    local state_root
    state_root=$(echo "$block" | python3 -c 'import sys,json; print(json.load(sys.stdin)["result"]["stateRoot"])')
    echo "  stateRoot: $state_root"
    STATE_ROOTS[$name]="$state_root"

    echo "  Querying balance of $INJECT_ADDR ..."
    local balance
    balance=$(curl -sf -X POST "http://localhost:$port" \
        -H 'Content-Type: application/json' \
        -d "{\"jsonrpc\":\"2.0\",\"method\":\"eth_getBalance\",\"params\":[\"$INJECT_ADDR\",\"0x0\"],\"id\":2}")
    local bal_hex
    bal_hex=$(echo "$balance" | python3 -c 'import sys,json; print(json.load(sys.stdin)["result"])')
    local bal_eth
    bal_eth=$(python3 -c "print(int('$bal_hex', 16) / 1e18)")
    echo "  balance: $bal_hex ($bal_eth ETH)"

    check "$name" \
        "python3 -c \"assert abs($bal_eth - $expected_eth) < 1\" 2>/dev/null" \
        "$name balance = $bal_eth ETH (expected ~$expected_eth)"
}

# ============================================================
# GETH — writes directly into Pebble DB, no init needed
# ============================================================
test_geth() {
    echo ""
    echo "=========================================="
    echo "  Testing Geth"
    echo "=========================================="
    local datadir="/tmp/state-actor-test-geth"
    local port=18545

    rm -rf "$datadir"
    echo "Generating state..."
    ./state-actor \
        -db "$datadir/geth/chaindata" \
        -chain-id $CHAIN_ID \
        -accounts 10 -contracts 5 -max-slots 10 \
        -seed 42 2>&1 | grep "State Root"

    echo "Starting Geth..."
    geth \
        --datadir "$datadir" \
        --http --http.port $port \
        --http.api eth,web3,net \
        --port 0 --authrpc.port 0 \
        --networkid $CHAIN_ID \
        --nodiscover \
        --verbosity 2 \
        > "$datadir/geth.log" 2>&1 &
    local pid=$!
    sleep 5

    if kill -0 "$pid" 2>/dev/null; then
        verify_client $port "Geth" 999999999
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    else
        echo "  FAIL: Geth exited early. Check $datadir/geth.log"
        cat "$datadir/geth.log" | tail -5
        FAIL=$((FAIL + 1))
    fi
    rm -rf "$datadir"
}

# ============================================================
# ERIGON (Docker) — needs erigon init with genesis.json
# ============================================================
test_erigon() {
    echo ""
    echo "=========================================="
    echo "  Testing Erigon (Docker)"
    echo "=========================================="
    local datadir="/tmp/state-actor-test-erigon"
    local port=18546

    local genesis_file="/tmp/state-actor-erigon-genesis.json"
    cat > "$genesis_file" << GENESIS
{
  "config": {
    "chainId": $CHAIN_ID,
    "homesteadBlock": 0, "eip150Block": 0, "eip155Block": 0, "eip158Block": 0,
    "byzantiumBlock": 0, "constantinopleBlock": 0, "petersburgBlock": 0,
    "istanbulBlock": 0, "berlinBlock": 0, "londonBlock": 0,
    "shanghaiTime": 0, "cancunTime": 0,
    "terminalTotalDifficulty": 0, "terminalTotalDifficultyPassed": true
  },
  "nonce": "0x0", "timestamp": "0x0", "extraData": "0x",
  "gasLimit": "0x1c9c380", "difficulty": "0x0",
  "alloc": {
    "$INJECT_ADDR": { "balance": "$EXPECTED_BALANCE_HEX" }
  }
}
GENESIS

    # Clean up
    docker rm -f erigon-test 2>/dev/null || true
    docker run --rm -v /tmp:/tmp alpine rm -rf "$datadir" 2>/dev/null || true
    mkdir -p "$datadir"

    echo "Initializing Erigon with genesis..."
    docker run --rm \
        -v "$genesis_file":/genesis.json:ro \
        -v "$datadir":/data \
        --user "$(id -u):$(id -g)" \
        erigontech/erigon:latest \
        init --datadir /data /genesis.json 2>&1 | grep -i "genesis"

    echo "Starting Erigon..."
    docker run -d --name erigon-test \
        -v "$datadir":/data \
        -p $port:8545 \
        --user "$(id -u):$(id -g)" \
        erigontech/erigon:latest \
        --datadir /data \
        --http --http.port 8545 --http.addr 0.0.0.0 --http.api eth,web3,net \
        --nodiscover --private.api.addr="" \
        > /dev/null 2>&1
    sleep 10

    if docker ps --format '{{.Names}}' | grep -q erigon-test; then
        verify_client $port "Erigon" 999999999
    else
        echo "  FAIL: Erigon container exited."
        docker logs erigon-test 2>&1 | tail -10
        FAIL=$((FAIL + 1))
    fi

    docker rm -f erigon-test 2>/dev/null || true
    rm -f "$genesis_file"
    docker run --rm -v /tmp:/tmp alpine rm -rf "$datadir" 2>/dev/null || true
}

# ============================================================
# NETHERMIND (Docker) — needs chainspec file
# ============================================================
test_nethermind() {
    echo ""
    echo "=========================================="
    echo "  Testing Nethermind (Docker)"
    echo "=========================================="
    local datadir="/tmp/state-actor-test-nethermind"
    local port=18547

    local chainspec_file="/tmp/state-actor-nethermind-chainspec.json"
    cat > "$chainspec_file" << CHAINSPEC
{
  "name": "StateActor",
  "engine": { "NethDev": { "params": {} } },
  "params": {
    "gasLimitBoundDivisor": "0x400",
    "accountStartNonce": "0x0",
    "maximumExtraDataSize": "0x20",
    "minGasLimit": "0x1388",
    "networkID": "$CHAIN_ID_HEX",
    "chainID": "$CHAIN_ID_HEX",
    "eip150Transition": "0x0", "eip155Transition": "0x0",
    "eip158Transition": "0x0", "eip160Transition": "0x0",
    "eip161abcTransition": "0x0", "eip161dTransition": "0x0",
    "eip140Transition": "0x0", "eip211Transition": "0x0",
    "eip214Transition": "0x0", "eip658Transition": "0x0",
    "eip145Transition": "0x0", "eip1014Transition": "0x0",
    "eip1052Transition": "0x0", "eip1344Transition": "0x0",
    "eip2028Transition": "0x0", "eip1884Transition": "0x0",
    "eip2200Transition": "0x0", "eip2929Transition": "0x0",
    "eip2930Transition": "0x0", "eip1559Transition": "0x0",
    "eip3198Transition": "0x0", "eip3529Transition": "0x0",
    "eip3541Transition": "0x0",
    "eip4895TransitionTimestamp": "0x0",
    "eip4844TransitionTimestamp": "0x0",
    "eip1153TransitionTimestamp": "0x0",
    "eip5656TransitionTimestamp": "0x0",
    "eip6780TransitionTimestamp": "0x0",
    "terminalTotalDifficulty": "0"
  },
  "genesis": {
    "seal": {
      "ethereum": {
        "nonce": "0x0000000000000000",
        "mixHash": "0x0000000000000000000000000000000000000000000000000000000000000000"
      }
    },
    "difficulty": "0x0",
    "gasLimit": "0x1c9c380",
    "timestamp": "0x0",
    "extraData": "0x",
    "baseFeePerGas": "0x3b9aca00"
  },
  "accounts": {
    "$INJECT_ADDR": {
      "balance": "$EXPECTED_BALANCE_WEI"
    }
  }
}
CHAINSPEC

    # Clean up
    docker rm -f nethermind-test 2>/dev/null || true
    docker run --rm -v /tmp:/tmp alpine rm -rf "$datadir" 2>/dev/null || true
    mkdir -p "$datadir"

    echo "Starting Nethermind..."
    docker run -d --name nethermind-test \
        -v "$chainspec_file":/chainspec.json:ro \
        -v "$datadir":/data \
        -p $port:8545 \
        nethermind/nethermind:latest \
        --data-dir /data \
        --Init.ChainSpecPath /chainspec.json \
        --JsonRpc.Enabled true \
        --JsonRpc.Port 8545 \
        --JsonRpc.Host 0.0.0.0 \
        --JsonRpc.EnabledModules "eth,web3,net" \
        --HealthChecks.Enabled false \
        > /dev/null 2>&1
    sleep 15

    if docker ps --format '{{.Names}}' | grep -q nethermind-test; then
        verify_client $port "Nethermind" 999999999
    else
        echo "  FAIL: Nethermind container exited."
        docker logs nethermind-test 2>&1 | tail -10
        FAIL=$((FAIL + 1))
    fi

    docker rm -f nethermind-test 2>/dev/null || true
    rm -f "$chainspec_file"
    docker run --rm -v /tmp:/tmp alpine rm -rf "$datadir" 2>/dev/null || true
}

# ============================================================
# Cross-client state root verification
# ============================================================
verify_state_roots() {
    echo ""
    echo "=========================================="
    echo "  Cross-client state root verification"
    echo "=========================================="

    # Geth has extra generated accounts so its root will differ.
    # Erigon and Nethermind should match (same genesis alloc).
    local erigon_root="${STATE_ROOTS[Erigon]:-}"
    local nethermind_root="${STATE_ROOTS[Nethermind]:-}"
    local geth_root="${STATE_ROOTS[Geth]:-}"

    if [[ -n "$geth_root" ]]; then
        echo "  Geth stateRoot:       $geth_root (includes generated accounts)"
    fi
    if [[ -n "$erigon_root" ]]; then
        echo "  Erigon stateRoot:     $erigon_root"
    fi
    if [[ -n "$nethermind_root" ]]; then
        echo "  Nethermind stateRoot: $nethermind_root"
    fi

    if [[ -n "$erigon_root" && -n "$nethermind_root" ]]; then
        check "cross-client" \
            "[[ '$erigon_root' == '$nethermind_root' ]]" \
            "Erigon and Nethermind state roots match"
    fi
}

# ============================================================
# Run tests
# ============================================================
case "$CLIENT" in
    geth)       test_geth ;;
    erigon)     test_erigon ;;
    nethermind) test_nethermind ;;
    all)
        test_geth
        test_erigon
        test_nethermind
        verify_state_roots
        ;;
    *)
        echo "Usage: $0 [geth|erigon|nethermind|all]"
        exit 1
        ;;
esac

echo ""
echo "=========================================="
echo "  Results: $PASS passed, $FAIL failed"
echo "=========================================="
exit $FAIL
