# Client Bridges

Bridge programs allow the state-actor generator to write state directly into
any Ethereum client's native database format. Each bridge is a standalone
binary that uses the client's own libraries to handle DB writes and state root
computation.

## Architecture

```
┌──────────────┐   stdin (binary protocol)   ┌──────────────┐
│   generator   │ ──────────────────────────▶ │  geth-bridge  │
│  (main tool)  │ ◀────────────────────────── │  (PebbleDB)   │
└──────────────┘   stdout (responses)         └──────────────┘
```

The generator spawns a bridge process and streams state writes (accounts,
storage slots, code) over stdin using a compact binary protocol. Synchronous
commands (Flush, ComputeRoot, WriteGenesis) get responses on stdout. Logs go
to stderr.

This approach is forward-compatible: when a client changes its DB layout, only
its bridge binary needs updating.

## Quick Start with Geth

### 1. Build the geth-bridge

```bash
cd bridges/geth
go build -o geth-bridge .
```

### 2. Run the generator in bridge mode

```bash
# Minimal example: 1000 EOAs + 100 contracts
go run . \
  -output-format bridge \
  -bridge ./bridges/geth/geth-bridge \
  -db /tmp/geth-chaindata \
  -accounts 1000 \
  -contracts 100

# With genesis file (produces a ready-to-use database)
go run . \
  -output-format bridge \
  -bridge ./bridges/geth/geth-bridge \
  -db /tmp/geth-chaindata \
  -genesis genesis.json \
  -accounts 10000 \
  -contracts 1000 \
  -max-slots 50000 \
  -inject-accounts 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266

# Target a specific DB size
go run . \
  -output-format bridge \
  -bridge ./bridges/geth/geth-bridge \
  -db /tmp/geth-chaindata \
  -genesis genesis.json \
  -target-size 5GB \
  -contracts 10000
```

### 3. Start Geth with the generated database

```bash
geth --datadir /tmp/geth-chaindata --networkid <chain-id>
```

No `geth init` is needed — the database already contains the genesis block,
chain config, snapshot layer, and trie nodes.

## How It Works

The generator uses `--output-format bridge` to route all writes through a
`BridgeWriter`, which spawns the binary specified by `--bridge` and passes
`-db <path>` as arguments.

### Wire Protocol

Messages use a simple length-prefixed binary format:

```
Request:  [1B cmd][4B LE payload length][payload...]
Response: [1B status][4B LE payload length][payload...]
```

**Fire-and-forget commands** (no response, for throughput):
- `PutAccount` (0x01): `[20B addr][8B nonce LE][32B balance BE][32B codeHash]`
- `PutStorage` (0x02): `[20B addr][32B slot][32B value]`
- `PutCode` (0x03): `[32B codeHash][variable code bytes]`

**Synchronous commands** (response required):
- `Flush` (0x04): Commit buffered writes to disk
- `ComputeRoot` (0x05): Build the state trie and return the 32-byte root hash
- `WriteGenesis` (0x06): JSON payload with `chainConfig`, `stateRoot`, `gasLimit`, `baseFee`
- `Close` (0x07): Flush and shut down

### Data Flow

1. Generator creates account/contract metadata (addresses, balances, slot counts)
2. For each account, storage slots and code are streamed to the bridge via `PutStorage`/`PutCode`/`PutAccount`
3. After all state is written, `Flush` ensures data is on disk
4. `ComputeRoot` asks the bridge to build the MPT and return the state root
5. If genesis is configured, `WriteGenesis` writes the genesis block with the computed root
6. `Close` shuts down the bridge process

## Adding a New Client Bridge

To add support for a new client (e.g. Erigon, Nethermind):

1. Create `bridges/<client>/` with its own `go.mod` (or language-appropriate build)
2. Import the protocol package: `github.com/nerolation/state-actor/bridges/protocol`
3. Implement the command loop: read commands from stdin, write responses to stdout
4. Handle state root computation using the client's own trie implementation
5. Build the bridge binary and use it with `--bridge ./bridges/<client>/<client>-bridge`

See `bridges/geth/main.go` for a complete reference implementation.
