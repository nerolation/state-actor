#!/usr/bin/env bash
# Generate identical Ethereum state for multiple clients using bridge mode.
#
# Each client gets its own native database, all starting from the same genesis
# with the same state root and genesis block hash.
#
# Usage:
#   ./scripts/generate-state.sh                    # all clients, default config
#   ./scripts/generate-state.sh geth               # geth only
#   ./scripts/generate-state.sh geth erigon        # geth + erigon
#   ./scripts/generate-state.sh --help             # show all options
#
# Environment variables (override defaults):
#   OUTDIR        Output directory for client datadirs   (default: ./data)
#   ACCOUNTS      Number of EOA accounts                 (default: 1000)
#   CONTRACTS     Number of contracts                    (default: 100)
#   MAX_SLOTS     Max storage slots per contract         (default: 10000)
#   MIN_SLOTS     Min storage slots per contract         (default: 1)
#   TARGET_SIZE   Target DB size (e.g. "5GB")            (default: unset)
#   SEED          Random seed for reproducibility        (default: 42)
#   GENESIS       Path to genesis.json                   (default: auto-generated)
#   CHAIN_ID      Chain ID                               (default: 1337)
#   INJECT        Comma-separated addresses to fund      (default: none)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
CLIENTS_DIR="$(dirname "$ROOT_DIR")/clients"

# Defaults
OUTDIR="${OUTDIR:-$ROOT_DIR/data}"
ACCOUNTS="${ACCOUNTS:-1000}"
CONTRACTS="${CONTRACTS:-100}"
MAX_SLOTS="${MAX_SLOTS:-10000}"
MIN_SLOTS="${MIN_SLOTS:-1}"
TARGET_SIZE="${TARGET_SIZE:-}"
SEED="${SEED:-42}"
GENESIS="${GENESIS:-}"
CHAIN_ID="${CHAIN_ID:-1337}"
INJECT="${INJECT:-}"

# All supported clients
ALL_CLIENTS="geth erigon besu"

# Parse arguments
SELECTED_CLIENTS=()
VERIFY=false

for arg in "$@"; do
    case "$arg" in
        --help|-h)
            head -22 "$0" | tail -20
            echo ""
            echo "Supported clients: $ALL_CLIENTS"
            exit 0
            ;;
        --verify)
            VERIFY=true
            ;;
        geth|erigon|besu)
            SELECTED_CLIENTS+=("$arg")
            ;;
        *)
            echo "Unknown argument: $arg"
            echo "Usage: $0 [--verify] [geth] [erigon] [besu]"
            exit 1
            ;;
    esac
done

if [ ${#SELECTED_CLIENTS[@]} -eq 0 ]; then
    read -ra SELECTED_CLIENTS <<< "$ALL_CLIENTS"
fi

log() { echo "[$(date +%H:%M:%S)] $*"; }
die() { echo "ERROR: $*" >&2; exit 1; }

# ============================================================
# Step 1: Build the generator and bridges
# ============================================================
log "Building state-actor generator..."
cd "$ROOT_DIR"
go build -o "$ROOT_DIR/state-actor" .

build_geth_bridge() {
    log "Building geth-bridge..."
    (cd "$ROOT_DIR/bridges/geth" && go build -o "$ROOT_DIR/geth-bridge" .)
}

build_erigon_bridge() {
    log "Building erigon-bridge..."
    (cd "$ROOT_DIR/bridges/erigon" && go build -o "$ROOT_DIR/erigon-bridge" .)
}

build_besu_bridge() {
    log "Building besu-bridge..."
    local gradlew="$CLIENTS_DIR/besu/gradlew"
    if [ ! -x "$gradlew" ]; then
        die "Besu gradlew not found at $gradlew. Make sure the besu submodule is checked out."
    fi
    (cd "$ROOT_DIR/bridges/besu" && "$gradlew" fatJar -q)
    local jar
    jar=$(ls "$ROOT_DIR/bridges/besu/build/libs/"*-all.jar 2>/dev/null | head -1)
    if [ -z "$jar" ]; then
        die "besu-bridge fat JAR not found after build"
    fi
    # Create wrapper script
    cat > "$ROOT_DIR/besu-bridge" << WRAPPER
#!/bin/sh
exec java -jar "$jar" "\$@"
WRAPPER
    chmod +x "$ROOT_DIR/besu-bridge"
}

for client in "${SELECTED_CLIENTS[@]}"; do
    case "$client" in
        geth)   build_geth_bridge ;;
        erigon) build_erigon_bridge ;;
        besu)   build_besu_bridge ;;
    esac
done

# ============================================================
# Step 2: Generate genesis.json if not provided
# ============================================================
if [ -z "$GENESIS" ]; then
    GENESIS="$OUTDIR/genesis.json"
    mkdir -p "$OUTDIR"

    log "Generating genesis.json (chainId=$CHAIN_ID)..."

    # Build alloc section from inject addresses
    ALLOC="{}"
    if [ -n "$INJECT" ]; then
        ALLOC="{"
        first=true
        IFS=',' read -ra ADDRS <<< "$INJECT"
        for addr in "${ADDRS[@]}"; do
            addr=$(echo "$addr" | xargs) # trim whitespace
            if [ "$first" = true ]; then first=false; else ALLOC+=","; fi
            ALLOC+="\"$addr\":{\"balance\":\"0x33b2e3c9c8e4bdd4c0eb4c000000\"}"
        done
        ALLOC+="}"
    fi

    cat > "$GENESIS" << EOF
{
  "config": {
    "chainId": $CHAIN_ID,
    "homesteadBlock": 0,
    "eip150Block": 0,
    "eip155Block": 0,
    "eip158Block": 0,
    "byzantiumBlock": 0,
    "constantinopleBlock": 0,
    "petersburgBlock": 0,
    "istanbulBlock": 0,
    "berlinBlock": 0,
    "londonBlock": 0,
    "shanghaiTime": 0,
    "cancunTime": 0,
    "terminalTotalDifficulty": 0,
    "terminalTotalDifficultyPassed": true
  },
  "nonce": "0x0",
  "timestamp": "0x0",
  "extraData": "0x",
  "gasLimit": "0x1c9c380",
  "difficulty": "0x0",
  "baseFeePerGas": "0x3b9aca00",
  "alloc": $ALLOC
}
EOF
    log "Wrote $GENESIS"
fi

# ============================================================
# Step 3: Generate state for each client
# ============================================================
generate_client() {
    local client=$1
    local bridge_bin="$ROOT_DIR/${client}-bridge"
    local db_path

    case "$client" in
        geth)   db_path="$OUTDIR/geth/chaindata" ;;
        erigon) db_path="$OUTDIR/erigon" ;;
        besu)   db_path="$OUTDIR/besu" ;;
    esac

    rm -rf "$db_path"
    mkdir -p "$db_path"

    log "Generating state for $client → $db_path"

    local extra_args=()
    if [ -n "$TARGET_SIZE" ]; then
        extra_args+=("--target-size" "$TARGET_SIZE")
    fi
    if [ -n "$INJECT" ]; then
        extra_args+=("--inject-accounts" "$INJECT")
    fi

    "$ROOT_DIR/state-actor" \
        --db "$db_path" \
        --output-format bridge \
        --bridge "$bridge_bin" \
        --accounts "$ACCOUNTS" \
        --contracts "$CONTRACTS" \
        --max-slots "$MAX_SLOTS" \
        --min-slots "$MIN_SLOTS" \
        --seed "$SEED" \
        --genesis "$GENESIS" \
        --chain-id "$CHAIN_ID" \
        --verbose \
        "${extra_args[@]}"
}

declare -A STATE_ROOTS
for client in "${SELECTED_CLIENTS[@]}"; do
    echo ""
    echo "=========================================="
    echo "  $client"
    echo "=========================================="
    output=$(generate_client "$client" 2>&1)
    echo "$output"

    # Extract state root from output
    root=$(echo "$output" | grep -oP 'State Root:\s+\K0x[0-9a-fA-F]+' || true)
    if [ -n "$root" ]; then
        STATE_ROOTS[$client]="$root"
    fi
done

# ============================================================
# Step 4: Cross-verify state roots
# ============================================================
echo ""
echo "=========================================="
echo "  Cross-client verification"
echo "=========================================="

all_match=true
first_root=""
for client in "${SELECTED_CLIENTS[@]}"; do
    root="${STATE_ROOTS[$client]:-unknown}"
    echo "  $client: $root"
    if [ -z "$first_root" ]; then
        first_root="$root"
    elif [ "$root" != "$first_root" ]; then
        all_match=false
    fi
done

if [ ${#SELECTED_CLIENTS[@]} -gt 1 ]; then
    if $all_match; then
        echo ""
        echo "  ALL STATE ROOTS MATCH"
    else
        echo ""
        echo "  STATE ROOT MISMATCH!"
        exit 1
    fi
fi

# ============================================================
# Step 5: Optionally start clients and verify via RPC
# ============================================================
if $VERIFY; then
    echo ""
    echo "=========================================="
    echo "  RPC verification"
    echo "=========================================="

    verify_rpc() {
        local name=$1
        local port=$2
        echo "  Checking $name at localhost:$port ..."
        local block
        block=$(curl -sf -X POST "http://localhost:$port" \
            -H 'Content-Type: application/json' \
            -d '{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x0",false],"id":1}') || {
            echo "    FAIL: $name RPC not reachable"
            return 1
        }
        local rpc_root
        rpc_root=$(echo "$block" | python3 -c 'import sys,json; print(json.load(sys.stdin)["result"]["stateRoot"])')
        echo "    stateRoot from RPC: $rpc_root"
        if [ "$rpc_root" = "$first_root" ]; then
            echo "    PASS: matches generated root"
        else
            echo "    FAIL: expected $first_root"
            return 1
        fi
    }

    PIDS=()
    cleanup() {
        for pid in "${PIDS[@]}"; do
            kill "$pid" 2>/dev/null || true
        done
        wait 2>/dev/null || true
    }
    trap cleanup EXIT

    for client in "${SELECTED_CLIENTS[@]}"; do
        case "$client" in
            geth)
                if command -v geth &>/dev/null; then
                    log "Starting geth on port 18545..."
                    geth --datadir "$OUTDIR/geth" \
                        --http --http.port 18545 --http.api eth,web3,net \
                        --port 0 --authrpc.port 0 \
                        --networkid "$CHAIN_ID" --nodiscover \
                        --verbosity 2 > "$OUTDIR/geth.log" 2>&1 &
                    PIDS+=($!)
                    sleep 5
                    verify_rpc "geth" 18545
                else
                    echo "  geth not in PATH, skipping RPC verify"
                fi
                ;;
            erigon)
                echo "  erigon RPC verify: start manually with --datadir $OUTDIR/erigon"
                ;;
            besu)
                echo "  besu RPC verify: start manually with --data-path $OUTDIR/besu"
                ;;
        esac
    done
fi

echo ""
echo "=========================================="
echo "  Done! Client databases are in $OUTDIR/"
echo "=========================================="
for client in "${SELECTED_CLIENTS[@]}"; do
    case "$client" in
        geth)   echo "  geth:   $OUTDIR/geth/chaindata" ;;
        erigon) echo "  erigon: $OUTDIR/erigon" ;;
        besu)   echo "  besu:   $OUTDIR/besu" ;;
    esac
done
echo ""
echo "Genesis: $GENESIS"
echo "Seed:    $SEED"
echo ""
