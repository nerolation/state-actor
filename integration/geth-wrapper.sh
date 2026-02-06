#!/bin/bash
#
# Geth wrapper script for bloated devnets
# 
# This script:
# 1. Checks for pre-generated chaindata (from stategen with --genesis)
# 2. Copies it to geth's data directory
# 3. If genesis block is already present, skips geth init
# 4. Otherwise, initializes geth with the genesis file
# 5. Starts geth normally
#
# Environment variables:
#   PREGENERATED_STATE_PATH - Path to pre-generated chaindata (optional)
#   GENESIS_PATH - Path to genesis.json (required for devnets without pre-init)
#   BINARY_TRIE - Set to "true" to enable binary trie mode.
#     Passes --override.verkle=0 to geth (legacy flag name for EIP-7864 binary trie).

set -e

GETH_DATADIR="${GETH_DATADIR:-/data/geth/execution-data}"
PREGENERATED_STATE_PATH="${PREGENERATED_STATE_PATH:-/pregenerated-state/chaindata}"
GENESIS_PATH="${GENESIS_PATH:-/genesis/genesis.json}"
BINARY_TRIE="${BINARY_TRIE:-false}"
SKIP_INIT=false

log() {
    echo "[geth-wrapper] $1"
}

# Check for pre-generated state
if [[ -d "$PREGENERATED_STATE_PATH" ]] && [[ "$(ls -A "$PREGENERATED_STATE_PATH" 2>/dev/null)" ]]; then
    log "Found pre-generated state at $PREGENERATED_STATE_PATH"
    
    CHAINDATA_DST="$GETH_DATADIR/geth/chaindata"
    
    if [[ -d "$CHAINDATA_DST" ]] && [[ "$(ls -A "$CHAINDATA_DST" 2>/dev/null)" ]]; then
        log "Chaindata already exists at $CHAINDATA_DST, skipping copy"
    else
        log "Copying pre-generated state..."
        mkdir -p "$(dirname "$CHAINDATA_DST")"
        
        # Use rsync if available for progress, otherwise cp
        if command -v rsync &>/dev/null; then
            rsync -a --progress "$PREGENERATED_STATE_PATH/" "$CHAINDATA_DST/"
        else
            cp -r "$PREGENERATED_STATE_PATH" "$CHAINDATA_DST"
        fi
        
        SIZE=$(du -sh "$CHAINDATA_DST" 2>/dev/null | cut -f1 || echo "unknown")
        log "Pre-generated state installed: $SIZE"
    fi
    
    # Check if genesis block is already in database
    # Stategen with --genesis writes all necessary metadata
    # We can detect this by checking for MANIFEST file and certain keys
    if [[ -f "$CHAINDATA_DST/MANIFEST-000001" ]] || [[ -f "$CHAINDATA_DST/CURRENT" ]]; then
        # Check if the database has head block pointer (indicates genesis was written)
        # This is a heuristic - if stategen wrote the genesis block, we're ready
        log "Pre-generated state appears to include genesis block, skipping geth init"
        SKIP_INIT=true
    fi
else
    log "No pre-generated state found, starting fresh"
fi

# Initialize with genesis if needed and not already initialized
if [[ "$SKIP_INIT" != "true" ]]; then
    if [[ -f "$GENESIS_PATH" ]] && [[ ! -f "$GETH_DATADIR/geth/LOCK" ]]; then
        log "Initializing geth with genesis..."
        geth init --datadir "$GETH_DATADIR" "$GENESIS_PATH"
        log "Genesis initialization complete"
    fi
else
    log "Skipping geth init (database already initialized)"
fi

# Build extra args for binary trie mode
EXTRA_ARGS=()
if [[ "$BINARY_TRIE" == "true" ]]; then
    EXTRA_ARGS+=("--override.verkle=0")
    log "Binary trie mode enabled (--override.verkle=0)"
fi

# Execute the original geth command
log "Starting geth: $@ ${EXTRA_ARGS[*]}"
exec geth "$@" "${EXTRA_ARGS[@]}"
