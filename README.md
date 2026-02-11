# State Actor

<p align="center">
  <img src="docs/logo.svg" alt="State Actor" width="200"/>
</p>

<p align="center">
  <strong>High-performance Ethereum state generator for devnet testing</strong>
</p>

<p align="center">
  <a href="#quick-start">Quick Start</a> •
  <a href="#features">Features</a> •
  <a href="#usage">Usage</a> •
  <a href="#integration">Integration</a> •
  <a href="docs/ARCHITECTURE.md">Architecture</a>
</p>

---

State Actor generates realistic Ethereum state directly into client-compatible databases. Supports both **geth** (Pebble/snapshot) and **Erigon** (MDBX/PlainState) formats. Create bloated devnets with millions of accounts and storage slots to test client behavior under mainnet-like conditions.

## Quick Start

```bash
# Install
go install github.com/nerolation/state-actor@latest

# Generate for geth (default)
state-actor \
    --db ./chaindata \
    --genesis genesis.json \
    --accounts 10000 \
    --contracts 5000 \
    --seed 42

# Generate for Erigon
state-actor \
    --db ./erigon-data \
    --accounts 10000 \
    --contracts 5000 \
    --output-format erigon \
    --seed 42

# Output:
# State Root:  0x8e170135992c...
# Genesis:     included (ready to use without geth init)
```

**No `geth init` required** — the database is ready to use immediately.

## Features

| Feature | Description |
|---------|-------------|
| ⚡ **Fast** | 350K+ storage slots/second |
| 🎯 **Realistic** | Power-law distribution mimics mainnet state |
| 🔄 **Reproducible** | Seed-based generation for consistent tests |
| 🔗 **Genesis Integration** | Merges with genesis.json, writes genesis block |
| 📦 **Ready to Use** | No `geth init` needed |
| 🐳 **Docker Ready** | Pre-built images available |
| 🔀 **Multi-Client** | Supports geth (Pebble) and Erigon (MDBX) |

## Installation

### From Source

```bash
git clone https://github.com/nerolation/state-actor.git
cd state-actor
go build -o state-actor .
```

### Using Go Install

```bash
go install github.com/nerolation/state-actor@latest
```

### Docker

```bash
docker pull ghcr.io/nerolation/state-actor:latest
# or build locally
docker build -t state-actor:latest .
```

## Usage

### Basic Usage

```bash
# Minimal: just generate random state
state-actor --db ./chaindata --accounts 1000 --contracts 500

# With genesis integration (recommended)
state-actor \
    --db ./chaindata \
    --genesis genesis.json \
    --accounts 10000 \
    --contracts 5000
```

### Command Line Options

| Flag | Default | Description |
|------|---------|-------------|
| `--db` | (required) | Output database directory |
| `--genesis` | - | Genesis JSON file (enables genesis block writing) |
| `--output-format` | geth | Output format: `geth` (Pebble) or `erigon` (MDBX) |
| `--accounts` | 1000 | Number of EOA accounts |
| `--contracts` | 100 | Number of contracts |
| `--max-slots` | 10000 | Max storage slots per contract |
| `--min-slots` | 1 | Min storage slots per contract |
| `--distribution` | power-law | Distribution: `power-law`, `uniform`, `exponential` |
| `--seed` | 0 | Random seed (0 = random) |
| `--batch-size` | 10000 | DB batch size |
| `--workers` | NumCPU | Parallel workers |
| `--code-size` | 1024 | Average contract code size |
| `--binary-trie` | false | Generate state for EIP-7864 binary trie mode |
| `--inject-accounts` | - | Comma-separated hex addresses to pre-fund with 999999999 ETH |
| `--chain-id` | 0 | Override genesis chainId (0 = use value from genesis.json) |
| `--target-size` | - | Target total DB size on disk (e.g. `5GB`, `500MB`). Stops when reached. |
| `--verbose` | false | Verbose output |
| `--benchmark` | false | Print detailed stats |

### Output Formats

State Actor supports two database formats:

#### Geth (default)
Generates a Pebble database with geth's snapshot layer format. Ready to use with `geth --db.engine=pebble`.

```bash
state-actor --db ./chaindata --output-format geth --genesis genesis.json ...
```

#### Erigon
Generates an MDBX database with Erigon's PlainState format. Ready to use with Erigon.

```bash
state-actor --db ./erigon-data --output-format erigon ...
```

| Aspect | Geth | Erigon |
|--------|------|--------|
| Database | Pebble | MDBX |
| Account key | `a` + keccak(addr) | addr (20 bytes) |
| Storage key | `o` + keccak(addr) + keccak(slot) | addr + incarnation + slot |
| Encoding | SlimAccountRLP | SerialiseV3 (fieldset) |

> **Note:** Both formats produce identical state roots for the same seed, ensuring reproducibility across clients.

### Trie Modes

By default, State Actor uses the **Merkle Patricia Trie (MPT)** for state root computation, matching standard Ethereum. To generate state for **binary trie mode** (EIP-7864), pass `--binary-trie`:

```bash
state-actor --db ./chaindata --genesis genesis.json --accounts 10000 --contracts 5000 --binary-trie
```

Binary trie state requires geth to run with `--override.verkle=0` (legacy flag name for EIP-7864).

> **Important:** Binary trie mode requires geth built from the same `go-ethereum` version
> referenced in this project's `go.mod` (the binary trie key derivation must match). Using a
> different geth version may produce incompatible state that geth cannot read.

### Recommended Configurations

#### Local Testing (Quick)
```bash
state-actor --db ./chaindata --genesis genesis.json \
    --accounts 1000 --contracts 500 --max-slots 100 --seed 1
```

#### CI/CD Pipeline
```bash
state-actor --db ./chaindata --genesis genesis.json \
    --accounts 10000 --contracts 5000 --max-slots 1000 --seed 42
```

#### Mainnet-like State
```bash
state-actor --db ./chaindata --genesis genesis.json \
    --accounts 1000000 --contracts 500000 --max-slots 50000 \
    --distribution power-law --seed 12345
```

#### Maximum Throughput
```bash
state-actor --db ./chaindata --genesis genesis.json \
    --accounts 100000 --contracts 50000 --max-slots 10000 \
    --batch-size 100000 --workers 16
```

## Genesis Integration

When `--genesis` is provided, State Actor:

1. **Loads genesis.json** — parses chain config and alloc accounts
2. **Merges accounts** — includes alloc accounts at their exact addresses
3. **Generates state** — adds random accounts/contracts
4. **Computes state root** — combined root via StackTrie
5. **Writes genesis block** — with correct state root

This eliminates the state root mismatch problem and removes the need for `geth init`.

### Supported Genesis Format

Standard geth genesis.json:

```json
{
  "config": {
    "chainId": 32382,
    "shanghaiTime": 0,
    "cancunTime": 0,
    "terminalTotalDifficulty": 0
  },
  "gasLimit": "0x1c9c380",
  "difficulty": "0x0",
  "alloc": {
    "0x123...": { "balance": "0x..." },
    "0xabc...": { "code": "0x...", "storage": {...} }
  }
}
```

## Integration with Kurtosis / ethereum-package

See [docs/KURTOSIS.md](docs/KURTOSIS.md) for detailed integration guide.

### Quick Integration

```bash
# 1. Generate state
state-actor --db ./chaindata --genesis genesis.json \
    --accounts 100000 --contracts 50000 --seed 42

# 2. Copy to geth data directory
mkdir -p ./geth-data/geth
cp -r ./chaindata ./geth-data/geth/chaindata

# 3. Start geth (no init needed)
geth --datadir ./geth-data --db.engine=pebble ...
```

## Performance

| Scale | Accounts | Contracts | Slots | Time | Throughput |
|-------|----------|-----------|-------|------|------------|
| Small | 1K | 500 | ~11K | 64ms | 170K/s |
| Medium | 10K | 5K | ~140K | 400ms | 350K/s |
| Large | 100K | 50K | ~1.4M | 4s | 350K/s |

**Estimated capacity**: ~20 million storage slots per minute.

## Distribution Types

### Power-Law (Recommended)

Pareto distribution where most contracts have few slots, but some have many. Accurately mimics real Ethereum state distribution.

```bash
--distribution power-law
```

### Uniform

All contracts have similar slot counts. Useful for specific test scenarios.

```bash
--distribution uniform
```

### Exponential

Exponential decay in slot counts. Middle ground between power-law and uniform.

```bash
--distribution exponential
```

## Architecture

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for detailed architecture documentation.

```
┌─────────────────┐     ┌─────────────────┐
│   CLI (main.go) │────▶│  genesis.json   │
└────────┬────────┘     └────────┬────────┘
         │                       │
         ▼                       ▼
┌─────────────────────────────────────────┐
│              Generator                  │
│  • Genesis accounts + Generated state   │
│  • StackTrie (MPT) or BinaryTrie root   │
│  • StateWriter abstraction              │
└────────────────────┬────────────────────┘
                     │
         ┌───────────┴───────────┐
         ▼                       ▼
┌─────────────────┐     ┌─────────────────┐
│   GethWriter    │     │  ErigonWriter   │
│   (Pebble)      │     │   (MDBX)        │
│   Snapshot fmt  │     │  PlainState fmt │
└─────────────────┘     └─────────────────┘
```

## Database Schema

### Geth Format (Pebble)

Snapshot layer format:

| Key | Value |
|-----|-------|
| `a` + keccak(addr) | SlimAccountRLP |
| `o` + keccak(addr) + keccak(slot) | RLP(value) |
| `c` + keccak(code) | bytecode |
| `SnapshotRoot` | state root |

Plus genesis metadata when `--genesis` is provided.

### Erigon Format (MDBX)

PlainState format:

| Table | Key | Value |
|-------|-----|-------|
| PlainState | addr (20 bytes) | SerialiseV3 account |
| PlainState | addr + incarnation (8) + slot (32) | trimmed value |
| Code | codeHash (32 bytes) | bytecode |
| Config | `StateRoot` | state root |

## Testing

```bash
# Run all tests
go test -v ./...

# With race detector
go test -race ./...

# Run benchmarks
go test -bench=. ./generator
```

## Contributing

Contributions welcome! Please read [CONTRIBUTING.md](CONTRIBUTING.md) first.

## License

MIT License - see [LICENSE](LICENSE)

## Acknowledgments

- [go-ethereum](https://github.com/ethereum/go-ethereum) for the database and state primitives
- [ethereum-package](https://github.com/ethpandaops/ethereum-package) for Kurtosis integration patterns
