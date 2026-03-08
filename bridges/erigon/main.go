// erigon-bridge is a bridge program that accepts state write commands on stdin
// and writes them to an Erigon-compatible MDBX database using Erigon as a library.
//
// Usage:
//
//	erigon-bridge -db /path/to/datadir
//
// It reads the binary protocol from stdin and writes responses to stdout.
// Logs go to stderr.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	erigonlog "github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state/genesiswrite"
	"github.com/erigontech/erigon/execution/types"
	"github.com/nerolation/state-actor/bridges/protocol"
)

var dataDirPath = flag.String("db", "", "Path to Erigon datadir (required)")

func main() {
	flag.Parse()
	log.SetOutput(os.Stderr)
	log.SetPrefix("[erigon-bridge] ")

	if *dataDirPath == "" {
		log.Fatal("-db flag is required")
	}

	bridge := &erigonBridge{
		datadir:  *dataDirPath,
		accounts: make(map[common.Address]*acctRecord),
		code:     make(map[common.Hash][]byte),
	}

	in := bufio.NewReaderSize(os.Stdin, 4*1024*1024)  // 4MB input buffer
	out := bufio.NewWriterSize(os.Stdout, 4*1024*1024) // 4MB output buffer

	log.Printf("ready, datadir=%s", *dataDirPath)

	for {
		cmd, payload, err := protocol.ReadMsg(in)
		if err != nil {
			if err == io.EOF {
				log.Printf("stdin closed, exiting")
				break
			}
			log.Fatalf("read: %v", err)
		}

		switch cmd {
		case protocol.CmdPutAccount:
			if err := bridge.putAccount(payload); err != nil {
				bridge.lastErr = err
			}

		case protocol.CmdPutStorage:
			if err := bridge.putStorage(payload); err != nil {
				bridge.lastErr = err
			}

		case protocol.CmdPutCode:
			if err := bridge.putCode(payload); err != nil {
				bridge.lastErr = err
			}

		case protocol.CmdFlush:
			// No-op for erigon-bridge (all state is buffered in memory)
			if err := bridge.respond(out); err != nil {
				log.Fatalf("write response: %v", err)
			}

		case protocol.CmdComputeRoot:
			root, err := bridge.computeRoot()
			if err != nil {
				bridge.lastErr = err
			}
			if err := bridge.respondWithHash(out, root); err != nil {
				log.Fatalf("write response: %v", err)
			}

		case protocol.CmdWriteGenesis:
			hash, err := bridge.writeGenesis(payload)
			if err != nil {
				bridge.lastErr = err
			}
			if err := bridge.respondWithHash(out, hash); err != nil {
				log.Fatalf("write response: %v", err)
			}

		case protocol.CmdClose:
			if err := bridge.respond(out); err != nil {
				log.Fatalf("write response: %v", err)
			}
			out.Flush()
			return

		default:
			log.Printf("unknown command 0x%02x, ignoring", cmd)
		}
	}
}

// acctRecord tracks an account for state root computation.
type acctRecord struct {
	nonce    uint64
	balance  *uint256.Int
	codeHash common.Hash
	storage  map[common.Hash]common.Hash
}

type erigonBridge struct {
	datadir  string
	accounts map[common.Address]*acctRecord
	code     map[common.Hash][]byte
	lastErr  error
}

var emptyCodeHash = common.HexToHash("c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470")

func (b *erigonBridge) putAccount(payload []byte) error {
	addr, nonce, balBytes, codeHash, err := protocol.DecodePutAccount(payload)
	if err != nil {
		return err
	}

	address := common.Address(addr)
	balance := new(uint256.Int).SetBytes32(balBytes[:])
	ch := common.Hash(codeHash)

	// Preserve storage that may have arrived first
	rec, ok := b.accounts[address]
	if !ok {
		rec = &acctRecord{storage: make(map[common.Hash]common.Hash)}
		b.accounts[address] = rec
	}
	rec.nonce = nonce
	rec.balance = balance
	rec.codeHash = ch

	return nil
}

func (b *erigonBridge) putStorage(payload []byte) error {
	addr, slot, value, err := protocol.DecodePutStorage(payload)
	if err != nil {
		return err
	}

	address := common.Address(addr)
	// Storage may arrive before the account
	rec, ok := b.accounts[address]
	if !ok {
		rec = &acctRecord{storage: make(map[common.Hash]common.Hash)}
		b.accounts[address] = rec
	}
	rec.storage[common.Hash(slot)] = common.Hash(value)

	return nil
}

func (b *erigonBridge) putCode(payload []byte) error {
	codeHash, code, err := protocol.DecodePutCode(payload)
	if err != nil {
		return err
	}

	b.code[common.Hash(codeHash)] = code
	return nil
}

// computeRoot builds a Genesis.Alloc from buffered state and uses Erigon's
// commitment computation (hex patricia hashed trie) to compute the state root.
func (b *erigonBridge) computeRoot() (common.Hash, error) {
	// Build Genesis.Alloc from buffered state
	alloc := make(types.GenesisAlloc, len(b.accounts))
	for addr, rec := range b.accounts {
		ga := types.GenesisAccount{
			Nonce:   rec.nonce,
			Balance: rec.balance.ToBig(),
		}
		if rec.codeHash != emptyCodeHash && rec.codeHash != (common.Hash{}) {
			ga.Code = b.code[rec.codeHash]
		}
		if len(rec.storage) > 0 {
			ga.Storage = make(map[common.Hash]common.Hash, len(rec.storage))
			for k, v := range rec.storage {
				ga.Storage[k] = v
			}
		}
		alloc[addr] = ga
	}

	// Use a minimal genesis config for root computation.
	// Chain config does NOT affect the state root — it only affects block header fields.
	g := &types.Genesis{
		Config:     chain.AllProtocolChanges,
		GasLimit:   30_000_000,
		Difficulty: new(uint256.Int),
		Alloc:      alloc,
	}

	dirs := datadir.New(b.datadir)
	logger := erigonlog.New()

	// WriteGenesisState computes the state root using Erigon's commitment
	// (hex patricia hashed trie) via a temporary in-memory MDBX.
	// No persistent state is written — only the root is computed.
	block, _, err := genesiswrite.WriteGenesisState(g, dirs, logger)
	if err != nil {
		return common.Hash{}, fmt.Errorf("compute root: %w", err)
	}

	root := block.Root()
	log.Printf("computed state root: %s (%d accounts)", root.Hex(), len(b.accounts))
	return root, nil
}

// writeGenesis writes the genesis block and chain metadata to Erigon's MDBX.
func (b *erigonBridge) writeGenesis(payload []byte) (common.Hash, error) {
	// Parse the genesis request
	var req struct {
		ChainConfig json.RawMessage `json:"chainConfig"`
		StateRoot   common.Hash     `json:"stateRoot"`
		GasLimit    uint64          `json:"gasLimit"`
		BaseFee     uint64          `json:"baseFee"`
		Timestamp   uint64          `json:"timestamp"`
	}
	if err := json.Unmarshal(payload, &req); err != nil {
		return common.Hash{}, fmt.Errorf("unmarshal genesis config: %w", err)
	}

	// Deserialize chain config into Erigon's format
	var cfg chain.Config
	if err := json.Unmarshal(req.ChainConfig, &cfg); err != nil {
		return common.Hash{}, fmt.Errorf("unmarshal chain config: %w", err)
	}

	// Build genesis struct (empty alloc — state root was already computed)
	g := &types.Genesis{
		Config:     &cfg,
		GasLimit:   req.GasLimit,
		Difficulty: new(uint256.Int),
		Timestamp:  req.Timestamp,
		Alloc:      make(types.GenesisAlloc),
	}
	if cfg.IsLondon(0) {
		if req.BaseFee > 0 {
			g.BaseFee = uint256.NewInt(req.BaseFee)
		} else {
			g.BaseFee = uint256.NewInt(1_000_000_000) // 1 gwei
		}
	}

	// Build the genesis header and set the pre-computed state root
	head, withdrawals := genesiswrite.GenesisWithoutStateToBlock(g)
	head.Root = req.StateRoot

	block := types.NewBlock(head, nil, nil, nil, withdrawals)

	// Open MDBX and write genesis block metadata
	dirs := datadir.New(b.datadir)
	logger := erigonlog.New()
	db := mdbx.New(dbcfg.ChainDB, logger).Path(dirs.Chaindata).MustOpen()
	defer db.Close()

	tx, err := db.BeginRw(context.Background())
	if err != nil {
		return common.Hash{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if err := genesiswrite.WriteGenesisBesideState(block, tx, g); err != nil {
		return common.Hash{}, fmt.Errorf("write genesis: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return common.Hash{}, fmt.Errorf("commit genesis: %w", err)
	}

	log.Printf("wrote genesis block %s", block.Hash().Hex())
	return block.Hash(), nil
}

func (b *erigonBridge) respond(out *bufio.Writer) error {
	if b.lastErr != nil {
		err := b.lastErr
		b.lastErr = nil
		if e := protocol.WriteError(out, err.Error()); e != nil {
			return e
		}
		return out.Flush()
	}
	if err := protocol.WriteOK(out, nil); err != nil {
		return err
	}
	return out.Flush()
}

func (b *erigonBridge) respondWithHash(out *bufio.Writer, hash common.Hash) error {
	if b.lastErr != nil {
		err := b.lastErr
		b.lastErr = nil
		if e := protocol.WriteError(out, err.Error()); e != nil {
			return e
		}
		return out.Flush()
	}
	if err := protocol.WriteOK(out, hash[:]); err != nil {
		return err
	}
	return out.Flush()
}
