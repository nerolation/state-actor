# Plan: Bloat Binary Trie Geth Instance to 300GB

Target: 300GB state with 20% accounts, 70% storage, 10% code on a binary trie (EIP-7864) geth instance.

## Prerequisites

- Build state-actor: `go build -o state-actor .`
- Genesis.json compatible with binary trie devnet
- Geth binary supporting `--override.verkle=0`
- Hardware: 16-32 GB RAM (with `--commit-interval`), 600GB+ disk, multi-core CPU

## Step 1 — Calibration Run

```bash
./state-actor \
    --db ./calibration-chaindata \
    --accounts 100000 \
    --contracts 10000 \
    --max-slots 50000 \
    --min-slots 1 \
    --code-size 24000 \
    --binary-trie \
    --seed 42 \
    --benchmark \
    --verbose
```

From `--benchmark` output, note:
- `Account Bytes / (accounts + contracts)` = bytes per account entry (`B_acct`)
- `Storage Bytes / Storage Slots` = bytes per storage slot (`B_slot`)
- `Code Bytes / contracts` = bytes per code entry (`B_code`)
- `du -sh ./calibration-chaindata` for Pebble overhead factor

## Step 2 — Per-Entry Size Reference

| Entry Type          | Key Size | Value Size (avg) | Total    |
|---------------------|----------|------------------|----------|
| EOA account         | 33 B     | ~12 B            | **~45 B**    |
| Contract account    | 33 B     | ~42 B            | **~75 B**    |
| Storage slot        | 65 B     | ~33 B            | **~98 B**    |
| Code entry          | 33 B     | ~36,000 B        | **~36,033 B**|

Code value averages 1.5x `--code-size` because actual size = `code_size + rand(0..code_size)`.

## Step 3 — Parameter Calculation for 300GB (20/70/10)

| Category       | Target | Formula                    | Result                   |
|----------------|--------|----------------------------|--------------------------|
| Code (10%)     | 30 GB  | `C = 30e9 / B_code`       | ~833,000 contracts       |
| Accounts (20%) | 60 GB  | `E = (60e9 - C*75) / 45`  | ~1,331,000,000 EOAs      |
| Storage (70%)  | 210 GB | `S = 210e9 / 98`          | ~2,143,000,000 slots     |
| Avg slots/contract | —  | `S / C`                    | ~2,573 slots/contract    |

Distribution parameters for power-law (alpha=1.5):
- `--min-slots` ≈ avg_slots / 3 ≈ **858**
- `--max-slots` ≈ **500,000**

## Step 4 — Generate State

```bash
./state-actor \
    --db /data/chaindata \
    --genesis /path/to/genesis.json \
    --accounts 1331000000 \
    --contracts 833000 \
    --min-slots 858 \
    --max-slots 500000 \
    --code-size 24000 \
    --distribution power-law \
    --binary-trie \
    --commit-interval 100000 \
    --seed 42 \
    --batch-size 100000 \
    --workers $(nproc) \
    --benchmark \
    --verbose
```

With `--commit-interval 100000`, the binary trie is periodically committed to a
temporary Pebble database and reopened. Peak memory stays at ~1-2 GB regardless
of total state size.

## Step 5 — Set Up Geth

```bash
# Option A: Use wrapper script
export PREGENERATED_STATE_PATH=/data/chaindata
export GETH_DATADIR=/data/geth/execution-data
export BINARY_TRIE=true
bash integration/geth-wrapper.sh --datadir "$GETH_DATADIR" --syncmode full --gcmode archive

# Option B: Manual
geth --datadir /data/geth/execution-data --override.verkle=0 --syncmode full --gcmode archive
```

No `geth init` needed — state-actor writes the genesis block when `--genesis` is provided.

## Step 6 — Verify

```bash
du -sh /data/geth/execution-data/geth/chaindata
geth attach /data/geth/execution-data/geth.ipc --exec "eth.getBlock(0).stateRoot"
```

## Memory Architecture

Streaming generation processes one account at a time, and `--commit-interval`
periodically commits the binary trie to a temporary Pebble database and reopens
it from the root hash. After reopening, all children are lazy `HashedNode`
references that resolve from disk on demand, so only the working set stays in RAM.

| Component             | No commit-interval | With --commit-interval 100000 |
|-----------------------|--------------------|-------------------------------|
| Account data in RAM   | O(1) — streaming   | O(1) — streaming              |
| Binary trie nodes     | ~300 GB (all in RAM) | ~1-2 GB (working set)       |
| pathdb buffer         | N/A                | ~256 MB                       |
| **Peak total**        | **~300 GB**        | **~2-3 GB**                   |

Between commits, ~100K insertions each resolve ~17 InternalNodes (log₂ of trie
depth). Working set: ~100K paths × 17 nodes × 65B ≈ 111MB for InternalNodes,
plus ~100K StemNodes × ~8KB ≈ 800MB. After commit + reopen, memory resets via GC.

## Recommended Parameters Summary

| Parameter       | Value           | Rationale                              |
|-----------------|-----------------|----------------------------------------|
| `--accounts`    | 1,331,000,000   | 20% of 300GB at ~45 B/EOA              |
| `--contracts`   | 833,000         | 10% of 300GB at ~36 KB avg code        |
| `--min-slots`   | 858             | Power-law mean ≈ 3 × min = 2,574      |
| `--max-slots`   | 500,000         | Long-tail for realistic distribution   |
| `--code-size`   | 24,000          | Near Ethereum's 24,576 max             |
| `--binary-trie` | (flag)          | EIP-7864 binary trie mode              |
| `--commit-interval` | 100,000     | Periodic trie commits; bounds RAM to ~2 GB |
| `--batch-size`  | 100,000         | Fewer Pebble commits                   |
| `--seed`        | 42              | Reproducible generation                |
| `--genesis`     | path            | Writes genesis block, skips geth init  |
