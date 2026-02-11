package generator

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	mrand "math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/pebble"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/holiman/uint256"
)

// Generator handles state generation.
type Generator struct {
	config Config
	db     ethdb.KeyValueStore // Pebble DB for geth format or temp operations
	writer StateWriter         // Abstracted writer for output format
	rng    *mrand.Rand
}

// New creates a new state generator.
func New(config Config) (*Generator, error) {
	// Validate trie mode
	switch config.TrieMode {
	case TrieModeMPT, TrieModeBinary, "":
		// valid
	default:
		return nil, fmt.Errorf("unsupported trie mode: %q", config.TrieMode)
	}

	// Default to geth format
	if config.OutputFormat == "" {
		config.OutputFormat = OutputGeth
	}

	var db ethdb.KeyValueStore
	var writer StateWriter
	var err error

	switch config.OutputFormat {
	case OutputErigon:
		// For Erigon, we still need a Pebble DB for binary trie temp storage
		// and for trie node writes if WriteTrieNodes is enabled
		if config.TrieMode == TrieModeBinary || config.WriteTrieNodes {
			db, err = pebble.New(config.DBPath+".geth-temp", 512, 256, "stategen/", false)
			if err != nil {
				return nil, fmt.Errorf("failed to open temp database: %w", err)
			}
		}
		writer, err = NewErigonWriter(config.DBPath)
		if err != nil {
			if db != nil {
				db.Close()
			}
			return nil, fmt.Errorf("failed to create erigon writer: %w", err)
		}

	case OutputGeth:
		fallthrough
	default:
		// Geth format: use GethWriter which wraps Pebble
		gethWriter, err := NewGethWriter(config.DBPath, config.BatchSize, config.Workers)
		if err != nil {
			return nil, fmt.Errorf("failed to create geth writer: %w", err)
		}
		writer = gethWriter
		db = gethWriter.DB() // Share the underlying DB for genesis/trie operations
	}

	return &Generator{
		config: config,
		db:     db,
		writer: writer,
		rng:    mrand.New(mrand.NewSource(config.Seed)),
	}, nil
}

// Close closes the generator and its database.
func (g *Generator) Close() error {
	var errs []error

	if g.writer != nil {
		if err := g.writer.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close writer: %w", err))
		}
	}

	// For Erigon format, db may be a separate temp DB
	if g.db != nil && g.config.OutputFormat == OutputErigon {
		if err := g.db.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close temp db: %w", err))
		}
	}

	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// DB returns the underlying database for external writes (e.g., genesis block).
func (g *Generator) DB() ethdb.KeyValueStore {
	return g.db
}

// Generate generates the state and returns statistics.
// Both MPT and binary trie modes now use streaming approaches to support
// states larger than available RAM.
func (g *Generator) Generate() (*Stats, error) {
	if g.config.TrieMode == TrieModeBinary {
		return g.generateStreamingBinary()
	}
	// MPT mode also uses streaming to support large states
	return g.generateStreamingMPT()
}

// generateStreamingMPT generates state for MPT mode using a streaming approach.
// Instead of generating all accounts/contracts in memory, it:
// 1. Generates lightweight metadata (addresses, slot counts)
// 2. Processes each account one at a time: generate storage → compute storage root → write → discard
// This reduces peak memory from O(total_storage_slots) to O(max_slots_per_contract).
func (g *Generator) generateStreamingMPT() (*Stats, error) {
	stats := &Stats{}
	start := time.Now()

	// Phase 1: Generate account metadata (no storage data yet)
	metas, err := g.generateAccountMetas(stats)
	if err != nil {
		return nil, fmt.Errorf("failed to generate account metadata: %w", err)
	}

	stats.GenerationTime = time.Since(start)
	writeStart := time.Now()

	// Phase 2: Stream-write state in sorted order
	if err := g.streamWriteStateMPT(metas, stats); err != nil {
		return nil, fmt.Errorf("failed to write state: %w", err)
	}

	stats.DBWriteTime = time.Since(writeStart)
	stats.TotalBytes = stats.AccountBytes + stats.StorageBytes + stats.CodeBytes

	return stats, nil
}

// accountMeta holds lightweight account metadata without storage data.
// This allows us to generate addresses and slot counts without holding
// all storage in memory.
type accountMeta struct {
	address  common.Address
	addrHash common.Hash
	nonce    uint64
	balance  *uint256.Int
	// Contract fields
	isContract bool
	numSlots   int   // Number of storage slots to generate
	codeSize   int   // Code size to generate
	codeSeed   int64 // Seed for reproducible code/storage generation
	// Genesis account fields (pre-defined data)
	genesisAccount *types.StateAccount
	genesisStorage map[common.Hash]common.Hash
	genesisCode    []byte
	genesisCH      common.Hash // pre-computed code hash for genesis
}

// generateAccountMetas creates lightweight metadata for all accounts.
// Storage is NOT generated here — only slot counts are recorded.
func (g *Generator) generateAccountMetas(stats *Stats) ([]*accountMeta, error) {
	totalAccounts := g.config.NumAccounts + g.config.NumContracts + len(g.config.GenesisAccounts)
	metas := make([]*accountMeta, 0, totalAccounts)

	usedAddresses := make(map[common.Address]bool)
	genesisEOAs, genesisContracts := 0, 0

	// Genesis accounts
	for addr, acc := range g.config.GenesisAccounts {
		usedAddresses[addr] = true

		meta := &accountMeta{
			address:        addr,
			addrHash:       crypto.Keccak256Hash(addr[:]),
			genesisAccount: acc,
		}

		if storage, ok := g.config.GenesisStorage[addr]; ok {
			meta.genesisStorage = storage
			meta.isContract = true
			stats.StorageSlotsCreated += len(storage)
		}

		if code, ok := g.config.GenesisCode[addr]; ok {
			meta.genesisCode = code
			meta.genesisCH = crypto.Keccak256Hash(code)
			meta.isContract = true
		}

		if meta.isContract {
			genesisContracts++
		} else {
			genesisEOAs++
		}

		metas = append(metas, meta)
	}

	if g.config.Verbose && len(g.config.GenesisAccounts) > 0 {
		log.Printf("Included %d genesis alloc accounts (%d EOAs, %d contracts)",
			len(g.config.GenesisAccounts), genesisEOAs, genesisContracts)
	}

	// Update live stats
	if g.config.LiveStats != nil {
		g.config.LiveStats.SetPhase("accounts")
	}

	// Generate EOA metadata
	for i := 0; i < g.config.NumAccounts; i++ {
		var addr common.Address
		g.rng.Read(addr[:])
		for usedAddresses[addr] {
			g.rng.Read(addr[:])
		}
		usedAddresses[addr] = true

		metas = append(metas, &accountMeta{
			address:    addr,
			addrHash:   crypto.Keccak256Hash(addr[:]),
			nonce:      uint64(g.rng.Intn(1000)),
			balance:    new(uint256.Int).Mul(uint256.NewInt(uint64(g.rng.Intn(1000))), uint256.NewInt(1e18)),
			isContract: false,
		})

		if g.config.LiveStats != nil {
			g.config.LiveStats.AddAccount()
		}
	}
	stats.AccountsCreated = genesisEOAs + g.config.NumAccounts

	// Update live stats
	if g.config.LiveStats != nil {
		g.config.LiveStats.SetPhase("contracts")
	}

	// Generate contract metadata (slot counts only)
	slotDistribution := g.generateSlotDistribution()

	for i := 0; i < g.config.NumContracts; i++ {
		var addr common.Address
		g.rng.Read(addr[:])
		for usedAddresses[addr] {
			g.rng.Read(addr[:])
		}
		usedAddresses[addr] = true

		numSlots := slotDistribution[i]
		stats.StorageSlotsCreated += numSlots

		metas = append(metas, &accountMeta{
			address:    addr,
			addrHash:   crypto.Keccak256Hash(addr[:]),
			nonce:      uint64(g.rng.Intn(1000)),
			balance:    new(uint256.Int).Mul(uint256.NewInt(uint64(g.rng.Intn(100))), uint256.NewInt(1e18)),
			isContract: true,
			numSlots:   numSlots,
			codeSize:   g.config.CodeSize + g.rng.Intn(g.config.CodeSize),
			codeSeed:   g.rng.Int63(),
		})

		if g.config.LiveStats != nil {
			g.config.LiveStats.AddContract(numSlots)
		}
	}
	stats.ContractsCreated = genesisContracts + g.config.NumContracts

	if g.config.Verbose {
		log.Printf("Generated metadata for %d accounts, %d contracts with %d total storage slots",
			stats.AccountsCreated, stats.ContractsCreated, stats.StorageSlotsCreated)
	}

	// Sort by address hash for deterministic StackTrie construction
	sort.Slice(metas, func(i, j int) bool {
		return bytes.Compare(metas[i].addrHash[:], metas[j].addrHash[:]) < 0
	})

	return metas, nil
}

// streamWriteStateMPT processes accounts one at a time in streaming fashion.
// For each account: generate storage → compute storage root → write to DB → discard.
// This keeps memory bounded by the largest single contract's storage.
func (g *Generator) streamWriteStateMPT(metas []*accountMeta, stats *Stats) error {
	accountTrie := trie.NewStackTrie(nil)

	processedCount := 0
	totalCount := len(metas)
	var lastLogTime = time.Now()

	for _, meta := range metas {
		var stateAccount types.StateAccount
		var code []byte
		var codeHash common.Hash
		var storageSlots []storageSlot

		if meta.genesisAccount != nil {
			// Genesis account
			stateAccount = *meta.genesisAccount
			code = meta.genesisCode
			codeHash = meta.genesisCH
			if meta.genesisStorage != nil {
				storageSlots = mapToSortedSlots(meta.genesisStorage)
			}
		} else if meta.isContract {
			// Generated contract: create code and storage from seed
			rng := mrand.New(mrand.NewSource(meta.codeSeed))
			code = make([]byte, meta.codeSize)
			rng.Read(code)
			codeHash = crypto.Keccak256Hash(code)

			// Generate storage slots
			storageSlots = make([]storageSlot, 0, meta.numSlots)
			for j := 0; j < meta.numSlots; j++ {
				var key, value common.Hash
				rng.Read(key[:])
				rng.Read(value[:])
				if value == (common.Hash{}) {
					value[31] = 1
				}
				storageSlots = append(storageSlots, storageSlot{Key: key, Value: value})
			}
			sort.Slice(storageSlots, func(i, j int) bool {
				return bytes.Compare(storageSlots[i].Key[:], storageSlots[j].Key[:]) < 0
			})

			stateAccount = types.StateAccount{
				Nonce:    meta.nonce,
				Balance:  meta.balance,
				Root:     types.EmptyRootHash,
				CodeHash: codeHash.Bytes(),
			}
		} else {
			// EOA
			stateAccount = types.StateAccount{
				Nonce:    meta.nonce,
				Balance:  meta.balance,
				Root:     types.EmptyRootHash,
				CodeHash: types.EmptyCodeHash.Bytes(),
			}
		}

		// Compute storage root and write storage via StateWriter
		if len(storageSlots) > 0 {
			storageTrie := trie.NewStackTrie(nil)

			// MPT requires keys sorted by Keccak256(key)
			type keyWithHash struct {
				slot    storageSlot
				keyHash common.Hash
			}
			withHashes := make([]keyWithHash, len(storageSlots))
			for i, slot := range storageSlots {
				withHashes[i] = keyWithHash{
					slot:    slot,
					keyHash: crypto.Keccak256Hash(slot.Key[:]),
				}
			}
			sort.Slice(withHashes, func(i, j int) bool {
				return bytes.Compare(withHashes[i].keyHash[:], withHashes[j].keyHash[:]) < 0
			})

			for _, kh := range withHashes {
				if err := g.writer.WriteStorage(meta.address, 0, kh.slot.Key, kh.slot.Value); err != nil {
					return fmt.Errorf("write storage: %w", err)
				}

				valueRLP, err := encodeStorageValue(kh.slot.Value)
				if err != nil {
					return err
				}
				storageTrie.Update(kh.keyHash[:], valueRLP)
			}

			stateAccount.Root = storageTrie.Hash()
		}

		// Write code
		if len(code) > 0 {
			if err := g.writer.WriteCode(codeHash, code); err != nil {
				return fmt.Errorf("write code: %w", err)
			}
		}

		// Write account
		if err := g.writer.WriteAccount(meta.address, &stateAccount, 0); err != nil {
			return fmt.Errorf("write account: %w", err)
		}

		// Add to account trie
		slimData := types.SlimAccountRLP(stateAccount)
		accountTrie.Update(meta.addrHash[:], slimData)

		// Collect sample addresses
		if meta.isContract {
			if len(stats.SampleContracts) < 3 {
				stats.SampleContracts = append(stats.SampleContracts, meta.address)
			}
		} else {
			if len(stats.SampleEOAs) < 3 {
				stats.SampleEOAs = append(stats.SampleEOAs, meta.address)
			}
		}

		processedCount++

		// Progress logging
		if g.config.Verbose && time.Since(lastLogTime) > 20*time.Second {
			lastLogTime = time.Now()
			pct := float64(processedCount) / float64(totalCount) * 100
			log.Printf("[MPT] %d/%d accounts (%.1f%%)", processedCount, totalCount, pct)
		}

		// Sync live stats
		if g.config.LiveStats != nil && processedCount%1000 == 0 {
			g.config.LiveStats.SyncBytes(g.writer.Stats())
		}
	}

	// Flush all pending writes
	if err := g.writer.Flush(); err != nil {
		return fmt.Errorf("flush writes: %w", err)
	}

	// Compute and store state root
	stateRoot := accountTrie.Hash()
	stats.StateRoot = stateRoot

	if err := g.writer.SetStateRoot(stateRoot); err != nil {
		return fmt.Errorf("failed to write state root: %w", err)
	}

	if g.config.Verbose {
		log.Printf("State root: %s", stateRoot.Hex())
	}

	writerStats := g.writer.Stats()
	stats.AccountBytes = writerStats.AccountBytes
	stats.StorageBytes = writerStats.StorageBytes
	stats.CodeBytes = writerStats.CodeBytes

	return nil
}

// generateStreamingBinary generates state for binary trie mode using a
// two-phase approach:
//
// Phase 1: Generate account/contract/storage data, write snapshot entries to
// Pebble (via batchWriter), and collect trie entries (key-value pairs) into a
// flat in-memory slice. Each entry is 64 bytes (32-byte key + 32-byte value).
//
// Phase 2: Sort entries by key, then compute the binary trie root hash via
// recursive divide-and-conquer — grouping by stem, computing StemNode hashes,
// and building the InternalNode tree. No BinaryTrie object, no disk I/O for
// trie nodes, no commit/reopen cycles.
//
// This approach is analogous to how MPT mode uses StackTrie: sorted input
// enables streaming construction. It eliminates the 35x penalty from
// HashedNode disk resolution that plagued the old commit-interval approach.
//
// Memory: O(N × 64 bytes) for the entries slice, where N is the total number
// of trie entries (accounts × 2 + storage slots + code chunks).
func (g *Generator) generateStreamingBinary() (retStats *Stats, retErr error) {
	stats := &Stats{}
	start := time.Now()

	if g.config.CommitInterval > 0 && g.config.Verbose {
		log.Printf("NOTE: --commit-interval is ignored (binary stack trie computes root from sorted entries)")
	}

	// Note: We use g.writer (StateWriter) for final output, not batchWriter

	// --- Phase 1: Generate data, write snapshots, write trie entries to temp DB ---
	//
	// Instead of collecting entries in an in-memory slice (which grows linearly
	// with state size), we write each trie entry to a temporary Pebble DB.
	// Pebble's LSM tree keeps keys sorted automatically, so Phase 2 can
	// iterate in order without an explicit sort step. Memory stays O(1).
	tempDir, err := os.MkdirTemp("", "state-actor-sort-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp dir: %w", err)
	}
	defer os.RemoveAll(tempDir)

	tempDB, err := pebble.New(tempDir, 128, 64, "temp/", false)
	if err != nil {
		return nil, fmt.Errorf("failed to create temp sort DB: %w", err)
	}
	defer tempDB.Close()

	tempBatch := tempDB.NewBatch()
	var entryCount int64

	// writeEntries writes a batch of trie entries to the temp DB.
	writeEntries := func(entries []trieEntry) error {
		for i := range entries {
			if err := tempBatch.Put(entries[i].Key[:], entries[i].Value[:]); err != nil {
				return err
			}
			entryCount++
			if tempBatch.ValueSize() >= 64*1024*1024 { // flush every 64 MB
				if err := tempBatch.Write(); err != nil {
					return err
				}
				tempBatch.Reset()
			}
		}
		return nil
	}

	var lastLogTime = time.Now()
	var lastProjectedSize uint64 // cached from most recent dirSize check
	logProgress := func(phase string, current, total int, slots int64) {
		if time.Since(lastLogTime) < 20*time.Second {
			return
		}
		lastLogTime = time.Now()
		if g.config.TargetSize > 0 && g.config.NumContracts >= math.MaxInt32 {
			pct := float64(lastProjectedSize) / float64(g.config.TargetSize) * 100
			if pct > 100 {
				pct = 100
			}
			log.Printf("[%s] %.1f%% of target (%s / %s), %d contracts, %d storage slots, %d trie entries",
				phase, pct,
				formatBytesInternal(lastProjectedSize),
				formatBytesInternal(g.config.TargetSize),
				current, slots, entryCount)
		} else {
			pct := float64(current) / float64(total) * 100
			log.Printf("[%s] %d/%d (%.1f%%), %d storage slots, %d trie entries",
				phase, current, total, pct, slots, entryCount)
		}
	}

	// Track genesis addresses for collision avoidance.
	genesisAddrs := make(map[common.Address]bool, len(g.config.GenesisAccounts))

	// Reusable entries buffer — collectAccountEntries appends to this,
	// then writeEntries drains it, then we reset to reuse the backing array.
	var entryBuf []trieEntry

	// 1a. Genesis alloc accounts.
	for addr, acc := range g.config.GenesisAccounts {
		genesisAddrs[addr] = true

		addrHash := crypto.Keccak256Hash(addr[:])
		codeHash := common.BytesToHash(acc.CodeHash)

		ad := &accountData{
			address:  addr,
			addrHash: addrHash,
			account:  acc,
		}
		if storageMap, ok := g.config.GenesisStorage[addr]; ok {
			ad.storage = mapToSortedSlots(storageMap)
			stats.StorageSlotsCreated += len(ad.storage)
		}
		if code, ok := g.config.GenesisCode[addr]; ok {
			ad.code = code
			ad.codeHash = codeHash
		}

		entryBuf = collectAccountEntries(addr, acc, len(ad.code), ad.code, ad.storage, entryBuf[:0])
		if err := writeEntries(entryBuf); err != nil {
			return nil, fmt.Errorf("failed to write genesis trie entries: %w", err)
		}
		if err := g.writeAccountSnapshot(ad); err != nil {
			return nil, fmt.Errorf("failed to write genesis account %s: %w", addr.Hex(), err)
		}

		if len(ad.code) > 0 || len(ad.storage) > 0 {
			stats.ContractsCreated++
		} else {
			stats.AccountsCreated++
		}
	}

	if g.config.Verbose && len(g.config.GenesisAccounts) > 0 {
		log.Printf("Included %d genesis alloc accounts (%d EOAs, %d contracts)",
			len(g.config.GenesisAccounts), stats.AccountsCreated, stats.ContractsCreated)
	}

	// Inject any explicitly-requested addresses (e.g. Anvil's default account).
	for _, addr := range g.config.InjectAddresses {
		if genesisAddrs[addr] {
			continue
		}
		genesisAddrs[addr] = true
		injectBalance := new(uint256.Int).Mul(uint256.NewInt(999999999), uint256.NewInt(1e18))
		injectAccount := &types.StateAccount{
			Nonce:    0,
			Balance:  injectBalance,
			Root:     types.EmptyRootHash,
			CodeHash: types.EmptyCodeHash.Bytes(),
		}
		entryBuf = collectAccountEntries(addr, injectAccount, 0, nil, nil, entryBuf[:0])
		if err := writeEntries(entryBuf); err != nil {
			return nil, fmt.Errorf("failed to write injected trie entries: %w", err)
		}
		ad := &accountData{
			address:  addr,
			addrHash: crypto.Keccak256Hash(addr[:]),
			account:  injectAccount,
		}
		if err := g.writeAccountSnapshot(ad); err != nil {
			return nil, fmt.Errorf("failed to write injected account %s: %w", addr.Hex(), err)
		}
		stats.AccountsCreated++
		if g.config.Verbose {
			log.Printf("Injected account %s with %s wei", addr.Hex(), injectBalance.String())
		}
	}

	// 1b. EOA generation.
	if g.config.LiveStats != nil {
		g.config.LiveStats.SetPhase("accounts")
	}
	for i := 0; i < g.config.NumAccounts; i++ {
		acc := g.generateEOA()
		for genesisAddrs[acc.address] {
			acc = g.generateEOA()
		}

		entryBuf = collectAccountEntries(acc.address, acc.account, 0, nil, nil, entryBuf[:0])
		if err := writeEntries(entryBuf); err != nil {
			return nil, fmt.Errorf("failed to write EOA trie entries: %w", err)
		}
		if err := g.writeAccountSnapshot(acc); err != nil {
			return nil, fmt.Errorf("failed to write EOA %d: %w", i, err)
		}
		stats.AccountsCreated++
		if g.config.LiveStats != nil {
			g.config.LiveStats.AddAccount()
			// Sync byte stats every 1000 accounts
			if stats.AccountsCreated%1000 == 0 {
				g.config.LiveStats.SyncBytes(g.writer.Stats())
			}
		}
		if len(stats.SampleEOAs) < 3 {
			stats.SampleEOAs = append(stats.SampleEOAs, acc.address)
		}
		logProgress("EOA", i+1, g.config.NumAccounts, 0)
	}

	// 1c. Contract generation via producer-consumer pipeline.
	// When --target-size governs, slot counts are generated on-demand
	// (one RNG call per contract, same sequence as generateSlotDistribution).
	// When --contracts governs, we pre-compute the distribution for the
	// known count.
	var slotDistribution []int
	if g.config.NumContracts < math.MaxInt32 {
		slotDistribution = g.generateSlotDistribution()
	}

	done := make(chan struct{})
	contractCh := make(chan *accountData, 16)
	go func() {
		defer close(contractCh)
		for i := 0; i < g.config.NumContracts; i++ {
			var numSlots int
			if slotDistribution != nil {
				numSlots = slotDistribution[i]
			} else {
				numSlots = g.generateSlotCount()
			}
			contract := g.generateContract(numSlots)
			for genesisAddrs[contract.address] {
				contract = g.generateContract(numSlots)
			}
			select {
			case contractCh <- contract:
			case <-done:
				return
			}
		}
	}()

	if g.config.LiveStats != nil {
		g.config.LiveStats.SetPhase("contracts")
	}
	targetCheckInterval := 500
	if g.config.NumContracts < 500*5 {
		targetCheckInterval = max(1, g.config.NumContracts/5)
	}
	contractIdx := 0
	targetReached := false
	for contract := range contractCh {
		entryBuf = collectAccountEntries(contract.address, contract.account, len(contract.code), contract.code, contract.storage, entryBuf[:0])
		if err := writeEntries(entryBuf); err != nil {
			return nil, fmt.Errorf("failed to write contract trie entries: %w", err)
		}
		if err := g.writeAccountSnapshot(contract); err != nil {
			return nil, fmt.Errorf("failed to write contract %d: %w", contractIdx, err)
		}
		stats.ContractsCreated++
		stats.StorageSlotsCreated += len(contract.storage)
		if g.config.LiveStats != nil {
			g.config.LiveStats.AddContract(len(contract.storage))
			// Sync byte stats every 100 contracts
			if stats.ContractsCreated%100 == 0 {
				g.config.LiveStats.SyncBytes(g.writer.Stats())
			}
		}
		if len(stats.SampleContracts) < 3 {
			stats.SampleContracts = append(stats.SampleContracts, contract.address)
		}
		contractIdx++
		logProgress("Contract", contractIdx, g.config.NumContracts, int64(stats.StorageSlotsCreated))

		// Check target size periodically using actual disk measurement.
		if g.config.TargetSize > 0 && contractIdx%targetCheckInterval == 0 {
			mainDBSize, err := dirSize(g.config.DBPath)
			if err == nil {
				// Project trie node overhead: empirically, trie nodes add
				// ~1.5× the snapshot data, so total ≈ 2.5× snapshot.
				projected := mainDBSize
				if g.config.WriteTrieNodes {
					projected = mainDBSize * 5 / 2
				}
				lastProjectedSize = projected
				if projected >= g.config.TargetSize {
					if g.config.Verbose {
						log.Printf("Target size reached: DB %s × 2.5 = %s (target: %s)",
							formatBytesInternal(mainDBSize),
							formatBytesInternal(projected),
							formatBytesInternal(g.config.TargetSize))
					}
					targetReached = true
					close(done)
					break
				}
			}
		}
	}
	// Drain producer if we broke early.
	if targetReached {
		for range contractCh {
		}
	}

	// Flush remaining temp entries.
	if tempBatch.ValueSize() > 0 {
		if err := tempBatch.Write(); err != nil {
			return nil, fmt.Errorf("failed to flush temp batch: %w", err)
		}
	}

	// Flush StateWriter
	if err := g.writer.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush writer: %w", err)
	}

	// --- Phase 2: Stream sorted entries from temp DB → compute root hash ---

	if g.config.Verbose {
		log.Printf("Computing root from %d trie entries (streaming, O(depth) memory)...", entryCount)
	}

	hashStart := time.Now()
	var nodeDB ethdb.KeyValueStore
	// Trie node storage only supported for geth format
	if g.config.WriteTrieNodes && g.config.OutputFormat == OutputGeth && g.db != nil {
		nodeDB = g.db
	}
	iter := tempDB.NewIterator(nil, nil)
	stateRoot, tnStats := computeBinaryRootStreaming(iter, nodeDB)
	if g.config.Verbose {
		log.Printf("Computed binary trie root in %v", time.Since(hashStart).Round(time.Millisecond))
	}

	stats.StateRoot = stateRoot

	// Write state root via StateWriter
	if err := g.writer.SetStateRoot(stateRoot); err != nil {
		return nil, fmt.Errorf("failed to write snapshot root: %w", err)
	}

	if g.config.Verbose {
		log.Printf("State root (binary stack trie): %s", stateRoot.Hex())
		log.Printf("Generated %d accounts, %d contracts with %d total storage slots (%d trie entries)",
			stats.AccountsCreated, stats.ContractsCreated, stats.StorageSlotsCreated, entryCount)
	}

	writerStats := g.writer.Stats()
	stats.AccountBytes = writerStats.AccountBytes
	stats.StorageBytes = writerStats.StorageBytes
	stats.CodeBytes = writerStats.CodeBytes
	stats.TrieNodeBytes = uint64(tnStats.Bytes)
	stats.TotalBytes = stats.AccountBytes + stats.StorageBytes + stats.CodeBytes + stats.TrieNodeBytes

	elapsed := time.Since(start)
	stats.GenerationTime = elapsed
	stats.DBWriteTime = elapsed

	return stats, nil
}

// writeAccountSnapshot writes snapshot entries for an account using the StateWriter.
// Handles storage, account, and code writes. This is the snapshot layer —
// separate from trie root computation.
func (g *Generator) writeAccountSnapshot(acc *accountData) error {
	// Storage: write each slot via StateWriter
	for _, slot := range acc.storage {
		if err := g.writer.WriteStorage(acc.address, 0, slot.Key, slot.Value); err != nil {
			return fmt.Errorf("write storage: %w", err)
		}
	}

	// Account: Root is always EmptyRootHash in binary trie mode
	// (binary trie doesn't use per-account storage roots like MPT).
	snapshotAcc := *acc.account
	snapshotAcc.Root = types.EmptyRootHash
	if err := g.writer.WriteAccount(acc.address, &snapshotAcc, 0); err != nil {
		return fmt.Errorf("write account: %w", err)
	}

	// Code
	if len(acc.code) > 0 {
		if err := g.writer.WriteCode(acc.codeHash, acc.code); err != nil {
			return fmt.Errorf("write code: %w", err)
		}
	}

	return nil
}

// writeAccountSnapshotLegacy writes snapshot entries for an account to the batch writer.
// Used only for binary trie temp DB operations where we need direct Pebble access.
func writeAccountSnapshotLegacy(bw *batchWriter, acc *accountData) error {
	// Storage snapshots: each slot keyed by Keccak256(slotKey), RLP-encoded value.
	for _, slot := range acc.storage {
		keyHash := crypto.Keccak256Hash(slot.Key[:])
		valueRLP, err := encodeStorageValue(slot.Value)
		if err != nil {
			return fmt.Errorf("encode storage value: %w", err)
		}
		storageKey := storageSnapshotKey(acc.addrHash, keyHash)
		if err := bw.put(storageKey, valueRLP, &bw.storageBytes); err != nil {
			return fmt.Errorf("snapshot storage write: %w", err)
		}
	}

	// Account snapshot: Root is always EmptyRootHash in binary trie mode
	// (binary trie doesn't use per-account storage roots like MPT).
	snapshotAcc := *acc.account
	snapshotAcc.Root = types.EmptyRootHash
	slimData := types.SlimAccountRLP(snapshotAcc)
	key := accountSnapshotKey(acc.addrHash)
	if err := bw.put(key, slimData, &bw.accountBytes); err != nil {
		return fmt.Errorf("snapshot account write: %w", err)
	}

	// Code snapshot (same format as MPT).
	if len(acc.code) > 0 {
		cKey := codeKey(acc.codeHash)
		if err := bw.put(cKey, acc.code, &bw.codeBytes); err != nil {
			return fmt.Errorf("snapshot code write: %w", err)
		}
	}

	return nil
}

// storageSlot is a key-value pair for deterministic storage iteration.
type storageSlot struct {
	Key   common.Hash
	Value common.Hash
}

// accountData holds generated account data.
type accountData struct {
	address   common.Address
	addrHash  common.Hash
	account   *types.StateAccount
	code      []byte
	codeHash  common.Hash
	storage   []storageSlot // pre-sorted by Key for deterministic trie insertion
}

// mapToSortedSlots converts a storage map to a sorted slice of storageSlot.
func mapToSortedSlots(m map[common.Hash]common.Hash) []storageSlot {
	slots := make([]storageSlot, 0, len(m))
	for k, v := range m {
		slots = append(slots, storageSlot{Key: k, Value: v})
	}
	sort.Slice(slots, func(i, j int) bool {
		return bytes.Compare(slots[i].Key[:], slots[j].Key[:]) < 0
	})
	return slots
}

// generateAccounts generates account and contract data.
func (g *Generator) generateAccounts(stats *Stats) ([]*accountData, []*accountData, error) {
	accounts := make([]*accountData, 0, g.config.NumAccounts+len(g.config.GenesisAccounts))
	contracts := make([]*accountData, 0, g.config.NumContracts)

	// Track addresses used by genesis alloc to avoid collisions
	usedAddresses := make(map[common.Address]bool)

	// First, include genesis alloc accounts
	for addr, acc := range g.config.GenesisAccounts {
		usedAddresses[addr] = true

		addrHash := crypto.Keccak256Hash(addr[:])
		codeHash := common.BytesToHash(acc.CodeHash)

		ad := &accountData{
			address:  addr,
			addrHash: addrHash,
			account:  acc,
		}

		// Include genesis storage if present
		if storageMap, ok := g.config.GenesisStorage[addr]; ok {
			ad.storage = mapToSortedSlots(storageMap)
			stats.StorageSlotsCreated += len(ad.storage)
		}

		// Include genesis code if present
		if code, ok := g.config.GenesisCode[addr]; ok {
			ad.code = code
			ad.codeHash = codeHash
		}

		// Classify as account or contract based on code
		if len(ad.code) > 0 || len(ad.storage) > 0 {
			contracts = append(contracts, ad)
		} else {
			accounts = append(accounts, ad)
		}
	}

	genesisAccountCount := len(accounts)
	genesisContractCount := len(contracts)

	if g.config.Verbose && len(g.config.GenesisAccounts) > 0 {
		log.Printf("Included %d genesis alloc accounts (%d EOAs, %d contracts)",
			len(g.config.GenesisAccounts), genesisAccountCount, genesisContractCount)
	}

	// Update live stats phase
	if g.config.LiveStats != nil {
		g.config.LiveStats.SetPhase("accounts")
	}

	// Generate additional EOA accounts
	for i := 0; i < g.config.NumAccounts; i++ {
		acc := g.generateEOA()
		// Ensure no collision with genesis addresses (extremely unlikely but be safe)
		for usedAddresses[acc.address] {
			acc = g.generateEOA()
		}
		usedAddresses[acc.address] = true
		accounts = append(accounts, acc)

		// Update live stats
		if g.config.LiveStats != nil {
			g.config.LiveStats.AddAccount()
		}
	}
	stats.AccountsCreated = len(accounts)

	// Update live stats phase
	if g.config.LiveStats != nil {
		g.config.LiveStats.SetPhase("contracts")
	}

	// Generate contract accounts with storage
	slotDistribution := g.generateSlotDistribution()

	for i := 0; i < g.config.NumContracts; i++ {
		numSlots := slotDistribution[i]
		contract := g.generateContract(numSlots)
		// Ensure no collision with genesis addresses
		for usedAddresses[contract.address] {
			contract = g.generateContract(numSlots)
		}
		usedAddresses[contract.address] = true
		contracts = append(contracts, contract)
		stats.StorageSlotsCreated += len(contract.storage)

		// Update live stats
		if g.config.LiveStats != nil {
			g.config.LiveStats.AddContract(len(contract.storage))
		}
	}
	stats.ContractsCreated = len(contracts)

	if g.config.Verbose {
		log.Printf("Generated %d accounts, %d contracts with %d total storage slots",
			len(accounts), len(contracts), stats.StorageSlotsCreated)
	}

	return accounts, contracts, nil
}

// generateEOA generates an Externally Owned Account.
func (g *Generator) generateEOA() *accountData {
	var addr common.Address
	g.rng.Read(addr[:])

	// Random balance between 0 and 1000 ETH
	balance := new(uint256.Int).Mul(
		uint256.NewInt(uint64(g.rng.Intn(1000))),
		uint256.NewInt(1e18),
	)

	return &accountData{
		address:  addr,
		addrHash: crypto.Keccak256Hash(addr[:]),
		account: &types.StateAccount{
			Nonce:    uint64(g.rng.Intn(1000)),
			Balance:  balance,
			Root:     types.EmptyRootHash,
			CodeHash: types.EmptyCodeHash.Bytes(),
		},
		storage: nil,
	}
}

// generateContract generates a contract account with storage.
func (g *Generator) generateContract(numSlots int) *accountData {
	var addr common.Address
	g.rng.Read(addr[:])

	// Generate random code
	codeSize := g.config.CodeSize + g.rng.Intn(g.config.CodeSize)
	code := make([]byte, codeSize)
	g.rng.Read(code)
	codeHash := crypto.Keccak256Hash(code)

	// Random balance
	balance := new(uint256.Int).Mul(
		uint256.NewInt(uint64(g.rng.Intn(100))),
		uint256.NewInt(1e18),
	)

	// Generate storage slots as a pre-sorted slice for deterministic trie insertion.
	storage := make([]storageSlot, 0, numSlots)
	for j := 0; j < numSlots; j++ {
		var key, value common.Hash
		g.rng.Read(key[:])
		g.rng.Read(value[:])
		// Ensure value is non-zero (zero values are deletions)
		if value == (common.Hash{}) {
			value[31] = 1
		}
		storage = append(storage, storageSlot{Key: key, Value: value})
	}
	sort.Slice(storage, func(i, j int) bool {
		return bytes.Compare(storage[i].Key[:], storage[j].Key[:]) < 0
	})

	return &accountData{
		address:  addr,
		addrHash: crypto.Keccak256Hash(addr[:]),
		account: &types.StateAccount{
			Nonce:    uint64(g.rng.Intn(1000)),
			Balance:  balance,
			Root:     types.EmptyRootHash, // Will be computed
			CodeHash: codeHash.Bytes(),
		},
		code:     code,
		codeHash: codeHash,
		storage:  storage,
	}
}

// generateSlotDistribution generates the number of storage slots for each contract.
func (g *Generator) generateSlotDistribution() []int {
	distribution := make([]int, g.config.NumContracts)

	switch g.config.Distribution {
	case PowerLaw:
		// Power-law distribution (Pareto) - 80/20 rule
		// Most contracts have few slots, few contracts have many
		alpha := 1.5 // Shape parameter
		for i := range distribution {
			// Inverse CDF of Pareto distribution
			u := g.rng.Float64()
			slots := float64(g.config.MinSlots) / math.Pow(1-u, 1/alpha)
			if slots > float64(g.config.MaxSlots) {
				slots = float64(g.config.MaxSlots)
			}
			distribution[i] = int(slots)
		}

	case Exponential:
		// Exponential decay
		lambda := math.Log(2) / float64(g.config.MaxSlots/4)
		for i := range distribution {
			u := g.rng.Float64()
			slots := -math.Log(1-u) / lambda
			slots = math.Max(float64(g.config.MinSlots), math.Min(slots, float64(g.config.MaxSlots)))
			distribution[i] = int(slots)
		}

	case Uniform:
		// Uniform distribution
		for i := range distribution {
			distribution[i] = g.config.MinSlots + g.rng.Intn(g.config.MaxSlots-g.config.MinSlots+1)
		}
	}

	return distribution
}

// generateSlotCount generates the slot count for a single contract using
// the configured distribution. Called from the producer goroutine (which
// owns the RNG). Each call consumes exactly 1 RNG call, so the sequence
// is identical to generateSlotDistribution for the same seed.
func (g *Generator) generateSlotCount() int {
	switch g.config.Distribution {
	case PowerLaw:
		alpha := 1.5
		u := g.rng.Float64()
		slots := float64(g.config.MinSlots) / math.Pow(1-u, 1/alpha)
		if slots > float64(g.config.MaxSlots) {
			slots = float64(g.config.MaxSlots)
		}
		return int(slots)
	case Exponential:
		lambda := math.Log(2) / float64(g.config.MaxSlots/4)
		u := g.rng.Float64()
		slots := -math.Log(1-u) / lambda
		slots = math.Max(float64(g.config.MinSlots), math.Min(slots, float64(g.config.MaxSlots)))
		return int(slots)
	case Uniform:
		return g.config.MinSlots + g.rng.Intn(g.config.MaxSlots-g.config.MinSlots+1)
	default:
		return g.config.MinSlots
	}
}

// dirSize returns the total size of all files in a directory tree.
func dirSize(path string) (uint64, error) {
	var total uint64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			total += uint64(info.Size())
		}
		return nil
	})
	return total, err
}

// writeState dispatches to the appropriate trie-mode-specific writer.
// Binary trie mode is handled by generateStreamingBinary and never reaches here.
func (g *Generator) writeState(accounts, contracts []*accountData, stats *Stats) error {
	switch g.config.TrieMode {
	case TrieModeMPT, "":
		return g.writeStateMPT(accounts, contracts, stats)
	default:
		return fmt.Errorf("unsupported trie mode: %q", g.config.TrieMode)
	}
}

// batchWriter encapsulates the parallel batch writing infrastructure
// shared by both MPT and binary trie state writers.
type batchWriter struct {
	db        ethdb.KeyValueStore
	batchSize int
	batchChan chan *batchWork
	errChan   chan error
	wg        sync.WaitGroup
	closeOnce sync.Once
	batch     ethdb.Batch
	count     int

	accountBytes atomic.Uint64
	storageBytes atomic.Uint64
	codeBytes    atomic.Uint64
}

type batchWork struct {
	batch ethdb.Batch
}

func newBatchWriter(db ethdb.KeyValueStore, batchSize, workers int) *batchWriter {
	bw := &batchWriter{
		db:        db,
		batchSize: batchSize,
		batchChan: make(chan *batchWork, workers*2),
		errChan:   make(chan error, 1),
		batch:     db.NewBatch(),
	}

	for i := 0; i < workers; i++ {
		bw.wg.Add(1)
		go func() {
			defer bw.wg.Done()
			for work := range bw.batchChan {
				if err := work.batch.Write(); err != nil {
					select {
					case bw.errChan <- err:
					default:
						log.Printf("ERROR: additional batch write failure (dropped): %v", err)
					}
					return
				}
			}
		}()
	}

	return bw
}

// put writes a key-value pair and tracks bytes under the given counter.
// Automatically flushes when batch size is reached.
func (bw *batchWriter) put(key, value []byte, counter *atomic.Uint64) error {
	if err := bw.batch.Put(key, value); err != nil {
		return err
	}
	counter.Add(uint64(len(key) + len(value)))
	bw.count++
	if bw.count >= bw.batchSize {
		return bw.flush()
	}
	return nil
}

// flush sends the current batch to workers. Uses select to detect worker
// errors early and avoid deadlocking if all workers have exited.
func (bw *batchWriter) flush() error {
	if bw.count == 0 {
		return nil
	}
	select {
	case bw.batchChan <- &batchWork{batch: bw.batch}:
	case err := <-bw.errChan:
		return fmt.Errorf("batch worker failed: %w", err)
	}
	bw.batch = bw.db.NewBatch()
	bw.count = 0
	return nil
}

// finish flushes remaining data, waits for workers, and checks for errors.
func (bw *batchWriter) finish() error {
	if err := bw.flush(); err != nil {
		return err
	}
	bw.closeOnce.Do(func() { close(bw.batchChan) })
	bw.wg.Wait()

	select {
	case err := <-bw.errChan:
		return err
	default:
	}
	return nil
}

// close releases worker goroutines without flushing. Idempotent; safe to
// call after finish() or on error paths.
func (bw *batchWriter) close() {
	bw.closeOnce.Do(func() { close(bw.batchChan) })
	bw.wg.Wait()
}

// writeStateMPT writes all state to the database using a Merkle Patricia Trie.
// Uses the StateWriter abstraction for format-agnostic output.
func (g *Generator) writeStateMPT(accounts, contracts []*accountData, stats *Stats) error {
	accountTrie := trie.NewStackTrie(nil)

	// Merge and sort all accounts by address hash for deterministic trie construction
	allAccounts := make([]*accountData, 0, len(accounts)+len(contracts))
	allAccounts = append(allAccounts, accounts...)
	allAccounts = append(allAccounts, contracts...)
	sort.Slice(allAccounts, func(i, j int) bool {
		return bytes.Compare(allAccounts[i].addrHash[:], allAccounts[j].addrHash[:]) < 0
	})

	// Process all accounts (EOAs and contracts) in sorted order
	for _, acc := range allAccounts {
		// Process storage for contracts.
		// MPT requires keys sorted by Keccak256(key), not raw key.
		if len(acc.storage) > 0 {
			storageTrie := trie.NewStackTrie(nil)

			type keyWithHash struct {
				slot    storageSlot
				keyHash common.Hash
			}
			withHashes := make([]keyWithHash, len(acc.storage))
			for i, slot := range acc.storage {
				withHashes[i] = keyWithHash{
					slot:    slot,
					keyHash: crypto.Keccak256Hash(slot.Key[:]),
				}
			}
			sort.Slice(withHashes, func(i, j int) bool {
				return bytes.Compare(withHashes[i].keyHash[:], withHashes[j].keyHash[:]) < 0
			})

			for _, kh := range withHashes {
				// Write storage via StateWriter (format-agnostic)
				if err := g.writer.WriteStorage(acc.address, 0, kh.slot.Key, kh.slot.Value); err != nil {
					return fmt.Errorf("write storage: %w", err)
				}

				// Update storage trie for root computation
				valueRLP, err := encodeStorageValue(kh.slot.Value)
				if err != nil {
					return err
				}
				storageTrie.Update(kh.keyHash[:], valueRLP)
			}

			acc.account.Root = storageTrie.Hash()
		}

		// Write contract code if present
		if len(acc.code) > 0 {
			if err := g.writer.WriteCode(acc.codeHash, acc.code); err != nil {
				return fmt.Errorf("write code: %w", err)
			}
		}

		// Write account via StateWriter (format-agnostic)
		if err := g.writer.WriteAccount(acc.address, acc.account, 0); err != nil {
			return fmt.Errorf("write account: %w", err)
		}

		// Add to account trie for root computation
		slimData := types.SlimAccountRLP(*acc.account)
		accountTrie.Update(acc.addrHash[:], slimData)
	}

	// Flush all pending writes
	if err := g.writer.Flush(); err != nil {
		return fmt.Errorf("flush writes: %w", err)
	}

	// Compute and store state root
	stateRoot := accountTrie.Hash()
	stats.StateRoot = stateRoot

	// Write state root marker via StateWriter
	if err := g.writer.SetStateRoot(stateRoot); err != nil {
		return fmt.Errorf("failed to write state root: %w", err)
	}

	if g.config.Verbose {
		log.Printf("State root: %s", stateRoot.Hex())
	}

	// Get stats from writer
	writerStats := g.writer.Stats()
	stats.AccountBytes = writerStats.AccountBytes
	stats.StorageBytes = writerStats.StorageBytes
	stats.CodeBytes = writerStats.CodeBytes

	return nil
}

// Key encoding functions matching geth's rawdb schema

var (
	snapshotAccountPrefix = []byte("a")
	snapshotStoragePrefix = []byte("o")
	codePrefix            = []byte("c")
)

func accountSnapshotKey(hash common.Hash) []byte {
	return append(snapshotAccountPrefix, hash.Bytes()...)
}

func storageSnapshotKey(accountHash, storageHash common.Hash) []byte {
	buf := make([]byte, len(snapshotStoragePrefix)+common.HashLength+common.HashLength)
	n := copy(buf, snapshotStoragePrefix)
	n += copy(buf[n:], accountHash.Bytes())
	copy(buf[n:], storageHash.Bytes())
	return buf
}

func codeKey(hash common.Hash) []byte {
	return append(codePrefix, hash.Bytes()...)
}

// encodeStorageValue encodes a storage value using RLP with leading zeros trimmed.
func encodeStorageValue(value common.Hash) ([]byte, error) {
	trimmed := trimLeftZeroes(value[:])
	if len(trimmed) == 0 {
		return nil, nil
	}
	encoded, err := rlp.EncodeToBytes(trimmed)
	if err != nil {
		return nil, fmt.Errorf("failed to RLP-encode storage value %x: %w", value, err)
	}
	return encoded, nil
}

func trimLeftZeroes(s []byte) []byte {
	for i, v := range s {
		if v != 0 {
			return s[i:]
		}
	}
	return nil
}

// Helper for encoding block numbers
func encodeBlockNumber(number uint64) []byte {
	enc := make([]byte, 8)
	binary.BigEndian.PutUint64(enc, number)
	return enc
}

func formatBytesInternal(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}
