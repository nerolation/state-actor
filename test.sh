#!/bin/bash
# Generate state with genesis included, writing to geth's expected directory structure.
# The --genesis flag makes state-actor write the genesis block, chain config, head pointers,
# PathDB metadata, and ancient/freezer directory — everything geth needs to start.
mkdir -p ./geth-data/geth
./state-actor \
    --db ./geth-data/geth/chaindata \
    --genesis ./examples/test-genesis.json \
    --accounts 10000 \
    --contracts 5000 \
    --seed 42

# Launch geth — no `geth init` needed!
# Must use geth v1.16+ to match the go-ethereum library version used by state-actor.
# System geth (v1.14) will fail with "freezerTableMeta" RLP errors due to format changes.
GETH=../clients/go-ethereum/build/bin/geth
$GETH --datadir ./geth-data

# === Reth ===
# Build reth-import if needed: cd reth-import && cargo build --release
./state-actor \
    --db ./reth-data \
    --output-format reth \
    --genesis ./examples/test-genesis.json \
    --accounts 10000 \
    --contracts 5000 \
    --seed 42

RETH=../clients/reth/target/release/reth
$RETH node --datadir ./reth-data
