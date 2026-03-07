// geth-bridge is a bridge program that accepts state write commands on stdin
// and writes them to a Geth-compatible PebbleDB using go-ethereum as a library.
//
// Usage:
//
//	geth-bridge -db /path/to/chaindata
//
// It reads the binary protocol from stdin and writes responses to stdout.
// Logs go to stderr.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/big"
	"os"
	"sort"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/pebble"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/holiman/uint256"
	"github.com/nerolation/state-actor/bridges/protocol"
)

var dbPath = flag.String("db", "", "Path to PebbleDB chaindata directory (required)")

func main() {
	flag.Parse()
	log.SetOutput(os.Stderr)
	log.SetPrefix("[geth-bridge] ")

	if *dbPath == "" {
		log.Fatal("-db flag is required")
	}

	db, err := pebble.New(*dbPath, 512, 256, "geth-bridge/", false)
	if err != nil {
		log.Fatalf("open pebble: %v", err)
	}
	defer db.Close()

	bridge := &gethBridge{
		db:       db,
		batch:    db.NewBatch(),
		code:     make(map[common.Hash][]byte),
		accounts: make(map[common.Address]*acctRecord),
	}

	in := bufio.NewReaderSize(os.Stdin, 4*1024*1024)  // 4MB input buffer
	out := bufio.NewWriterSize(os.Stdout, 4*1024*1024) // 4MB output buffer

	log.Printf("ready, db=%s", *dbPath)

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
			if err := bridge.flush(); err != nil {
				bridge.lastErr = err
			}
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
			_ = bridge.flush()
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

type gethBridge struct {
	db       ethdb.KeyValueStore
	batch    ethdb.Batch
	batchN   int
	code     map[common.Hash][]byte
	accounts map[common.Address]*acctRecord
	lastErr  error
}

func (b *gethBridge) putAccount(payload []byte) error {
	addr, nonce, balBytes, codeHash, err := protocol.DecodePutAccount(payload)
	if err != nil {
		return err
	}

	address := common.Address(addr)
	balance := new(uint256.Int).SetBytes32(balBytes[:])
	ch := common.Hash(codeHash)

	// Write to snapshot layer
	acc := &types.StateAccount{
		Nonce:    nonce,
		Balance:  balance,
		Root:     types.EmptyRootHash,
		CodeHash: ch[:],
	}
	slimData := types.SlimAccountRLP(*acc)
	addrHash := crypto.Keccak256Hash(address[:])
	key := append([]byte("a"), addrHash.Bytes()...)
	if err := b.batch.Put(key, slimData); err != nil {
		return err
	}

	// Track for root computation
	b.accounts[address] = &acctRecord{
		nonce:    nonce,
		balance:  balance,
		codeHash: ch,
		storage:  make(map[common.Hash]common.Hash),
	}

	b.batchN++
	return b.maybeFlushBatch()
}

func (b *gethBridge) putStorage(payload []byte) error {
	addr, slot, value, err := protocol.DecodePutStorage(payload)
	if err != nil {
		return err
	}

	address := common.Address(addr)
	slotHash := common.Hash(slot)
	valHash := common.Hash(value)

	// Write to snapshot layer
	addrHash := crypto.Keccak256Hash(address[:])
	slotKeccak := crypto.Keccak256Hash(slotHash[:])

	trimmed := trimLeftZeroes(valHash[:])
	if len(trimmed) == 0 {
		return nil
	}
	encoded, err := rlp.EncodeToBytes(trimmed)
	if err != nil {
		return err
	}

	buf := make([]byte, 1+common.HashLength+common.HashLength)
	buf[0] = 'o'
	copy(buf[1:], addrHash.Bytes())
	copy(buf[1+common.HashLength:], slotKeccak.Bytes())
	if err := b.batch.Put(buf, encoded); err != nil {
		return err
	}

	// Track for root computation
	if rec, ok := b.accounts[address]; ok {
		rec.storage[slotHash] = valHash
	}

	b.batchN++
	return b.maybeFlushBatch()
}

func (b *gethBridge) putCode(payload []byte) error {
	codeHash, code, err := protocol.DecodePutCode(payload)
	if err != nil {
		return err
	}

	ch := common.Hash(codeHash)
	b.code[ch] = code

	key := append([]byte("c"), ch.Bytes()...)
	if err := b.batch.Put(key, code); err != nil {
		return err
	}

	b.batchN++
	return b.maybeFlushBatch()
}

func (b *gethBridge) maybeFlushBatch() error {
	if b.batchN >= 50000 {
		return b.flushBatch()
	}
	return nil
}

func (b *gethBridge) flushBatch() error {
	if b.batchN == 0 {
		return nil
	}
	if err := b.batch.Write(); err != nil {
		return err
	}
	b.batch.Reset()
	b.batchN = 0
	return nil
}

func (b *gethBridge) flush() error {
	return b.flushBatch()
}

// computeRoot builds the MPT from tracked accounts and returns the state root.
func (b *gethBridge) computeRoot() (common.Hash, error) {
	// Sort addresses by keccak hash (trie key order)
	type addrEntry struct {
		addr     common.Address
		addrHash common.Hash
		rec      *acctRecord
	}
	entries := make([]addrEntry, 0, len(b.accounts))
	for addr, rec := range b.accounts {
		entries = append(entries, addrEntry{
			addr:     addr,
			addrHash: crypto.Keccak256Hash(addr[:]),
			rec:      rec,
		})
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].addrHash.Hex() < entries[j].addrHash.Hex()
	})

	// Build the state trie using StackTrie
	var trieNodeWriter trieNodeDB
	trieNodeWriter.db = b.db
	trieNodeWriter.batch = b.db.NewBatch()

	accountTrie := trie.NewStackTrie(func(path []byte, hash common.Hash, blob []byte) {
		rawdb.WriteTrieNode(trieNodeWriter.batch, common.Hash{}, path, hash, blob, rawdb.HashScheme)
		trieNodeWriter.count++
		if trieNodeWriter.count%50000 == 0 {
			trieNodeWriter.batch.Write()
			trieNodeWriter.batch.Reset()
		}
	})

	for _, e := range entries {
		rec := e.rec

		// Compute storage root for this account
		storageRoot := types.EmptyRootHash
		if len(rec.storage) > 0 {
			storageRoot = b.computeStorageRoot(rec.storage, e.addrHash, &trieNodeWriter)
		}

		acc := &types.StateAccount{
			Nonce:    rec.nonce,
			Balance:  rec.balance,
			Root:     storageRoot,
			CodeHash: rec.codeHash[:],
		}

		accRLP, err := rlp.EncodeToBytes(acc)
		if err != nil {
			return common.Hash{}, fmt.Errorf("RLP encode account %s: %w", e.addr.Hex(), err)
		}

		accountTrie.Update(e.addrHash[:], accRLP)
	}

	root := accountTrie.Hash()

	// Flush remaining trie nodes
	if trieNodeWriter.batch.ValueSize() > 0 {
		trieNodeWriter.batch.Write()
	}

	// Write snapshot root marker
	if err := b.db.Put([]byte("SnapshotRoot"), root[:]); err != nil {
		return common.Hash{}, err
	}

	log.Printf("computed state root: %s (%d accounts)", root.Hex(), len(entries))
	return root, nil
}

func (b *gethBridge) computeStorageRoot(storage map[common.Hash]common.Hash, addrHash common.Hash, tnw *trieNodeDB) common.Hash {
	type slotEntry struct {
		slotHash common.Hash
		value    common.Hash
	}
	slots := make([]slotEntry, 0, len(storage))
	for slot, val := range storage {
		slots = append(slots, slotEntry{
			slotHash: crypto.Keccak256Hash(slot[:]),
			value:    val,
		})
	}
	sort.Slice(slots, func(i, j int) bool {
		return slots[i].slotHash.Hex() < slots[j].slotHash.Hex()
	})

	storageTrie := trie.NewStackTrie(func(path []byte, hash common.Hash, blob []byte) {
		rawdb.WriteTrieNode(tnw.batch, addrHash, path, hash, blob, rawdb.HashScheme)
		tnw.count++
		if tnw.count%50000 == 0 {
			tnw.batch.Write()
			tnw.batch.Reset()
		}
	})

	for _, s := range slots {
		trimmed := trimLeftZeroes(s.value[:])
		if len(trimmed) == 0 {
			continue
		}
		encoded, _ := rlp.EncodeToBytes(trimmed)
		storageTrie.Update(s.slotHash[:], encoded)
	}

	return storageTrie.Hash()
}

type trieNodeDB struct {
	db    ethdb.KeyValueStore
	batch ethdb.Batch
	count int
}

// writeGenesis writes the genesis block and chain metadata.
func (b *gethBridge) writeGenesis(payload []byte) (common.Hash, error) {
	// payload is JSON: {"chainConfig": {...}, "stateRoot": "0x..."}
	var req struct {
		ChainConfig *params.ChainConfig `json:"chainConfig"`
		StateRoot   common.Hash         `json:"stateRoot"`
		GasLimit    uint64              `json:"gasLimit"`
		BaseFee     *big.Int            `json:"baseFee"`
		Timestamp   uint64              `json:"timestamp"`
	}
	if err := json.Unmarshal(payload, &req); err != nil {
		return common.Hash{}, fmt.Errorf("unmarshal genesis config: %w", err)
	}

	cfg := req.ChainConfig
	if cfg == nil {
		return common.Hash{}, fmt.Errorf("missing chainConfig")
	}

	header := &types.Header{
		Number:     new(big.Int),
		Nonce:      types.EncodeNonce(0),
		GasLimit:   req.GasLimit,
		Difficulty: big.NewInt(0),
		Time:       req.Timestamp,
		Root:       req.StateRoot,
	}
	if header.GasLimit == 0 {
		header.GasLimit = params.GenesisGasLimit
	}
	if cfg.IsLondon(common.Big0) {
		if req.BaseFee != nil {
			header.BaseFee = req.BaseFee
		} else {
			header.BaseFee = new(big.Int).SetUint64(params.InitialBaseFee)
		}
	}

	num := big.NewInt(0)
	if cfg.IsShanghai(num, req.Timestamp) {
		h := types.EmptyWithdrawalsHash
		header.WithdrawalsHash = &h
	}
	if cfg.IsCancun(num, req.Timestamp) {
		header.ParentBeaconRoot = new(common.Hash)
		header.ExcessBlobGas = new(uint64)
		header.BlobGasUsed = new(uint64)
	}
	if cfg.IsPrague(num, req.Timestamp) {
		h := types.EmptyRequestsHash
		header.RequestsHash = &h
	}

	var withdrawals []*types.Withdrawal
	if cfg.IsShanghai(num, req.Timestamp) {
		withdrawals = make([]*types.Withdrawal, 0)
	}

	block := types.NewBlock(header, &types.Body{Withdrawals: withdrawals}, nil, trie.NewStackTrie(nil))

	batch := b.db.NewBatch()
	rawdb.WriteBlock(batch, block)
	rawdb.WriteReceipts(batch, block.Hash(), 0, nil)
	rawdb.WriteCanonicalHash(batch, block.Hash(), 0)
	rawdb.WriteHeadBlockHash(batch, block.Hash())
	rawdb.WriteHeadFastBlockHash(batch, block.Hash())
	rawdb.WriteHeadHeaderHash(batch, block.Hash())
	writeChainConfigCompat(batch, block.Hash(), cfg)

	if err := batch.Write(); err != nil {
		return common.Hash{}, fmt.Errorf("write genesis: %w", err)
	}

	log.Printf("wrote genesis block %s", block.Hash().Hex())
	return block.Hash(), nil
}

func writeChainConfigCompat(db ethdb.KeyValueWriter, hash common.Hash, cfg *params.ChainConfig) {
	data, err := json.Marshal(cfg)
	if err != nil {
		return
	}
	if cfg.TerminalTotalDifficulty != nil && cfg.TerminalTotalDifficulty.Sign() == 0 {
		var raw map[string]json.RawMessage
		if json.Unmarshal(data, &raw) == nil {
			raw["terminalTotalDifficultyPassed"] = json.RawMessage("true")
			if d, err := json.Marshal(raw); err == nil {
				data = d
			}
		}
	}
	key := append([]byte("ethereum-config-"), hash.Bytes()...)
	db.Put(key, data)
}

func (b *gethBridge) respond(out *bufio.Writer) error {
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

func (b *gethBridge) respondWithHash(out *bufio.Writer, hash common.Hash) error {
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

func trimLeftZeroes(s []byte) []byte {
	for i, v := range s {
		if v != 0 {
			return s[i:]
		}
	}
	return nil
}
