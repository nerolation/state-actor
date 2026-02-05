package generator

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	mrand "math/rand"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/pebble"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/bintrie"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/ethereum/go-ethereum/triedb/pathdb"
	"github.com/holiman/uint256"
)

// Generator handles state generation.
type Generator struct {
	config Config
	db     ethdb.KeyValueStore
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

	// Open Pebble database with reasonable cache settings
	db, err := pebble.New(config.DBPath, 512, 256, "stategen/", false)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	return &Generator{
		config: config,
		db:     db,
		rng:    mrand.New(mrand.NewSource(config.Seed)),
	}, nil
}

// Close closes the generator and its database.
func (g *Generator) Close() error {
	return g.db.Close()
}

// DB returns the underlying database for external writes (e.g., genesis block).
func (g *Generator) DB() ethdb.KeyValueStore {
	return g.db
}

// Generate generates the state and returns statistics.
func (g *Generator) Generate() (*Stats, error) {
	// Binary trie mode uses a streaming approach that processes one account
	// at a time, avoiding the need to hold all account data in memory.
	// This reduces peak memory from O(accounts + trie) to O(trie) only.
	if g.config.TrieMode == TrieModeBinary {
		return g.generateStreamingBinary()
	}

	stats := &Stats{}
	genStart := time.Now()

	// Generate accounts and contracts
	accounts, contracts, err := g.generateAccounts(stats)
	if err != nil {
		return nil, fmt.Errorf("failed to generate accounts: %w", err)
	}

	stats.GenerationTime = time.Since(genStart)
	writeStart := time.Now()

	// Write to database
	if err := g.writeState(accounts, contracts, stats); err != nil {
		return nil, fmt.Errorf("failed to write state: %w", err)
	}

	stats.DBWriteTime = time.Since(writeStart)
	stats.TotalBytes = stats.AccountBytes + stats.StorageBytes + stats.CodeBytes

	return stats, nil
}

// generateStreamingBinary generates state for binary trie mode using a
// streaming approach. Instead of allocating all account data in memory before
// writing, each account is generated, inserted into the binary trie, written
// to the snapshot database, and then discarded. This bounds the memory used
// by account data to O(1) per account.
//
// When CommitInterval is 0, the entire binary trie resides in memory. For a
// 300 GB state target this requires ~300 GB of RAM.
//
// When CommitInterval > 0, the trie is periodically committed to a temporary
// Pebble database and reopened from its root hash. After reopening, all
// children are HashedNode references that resolve lazily from disk on insert.
// Only the nodes along recently-inserted paths stay in memory, bounding peak
// usage to ~1-2 GB regardless of total state size.
//
// Address collision detection is limited to genesis alloc addresses only.
// Collisions between randomly generated 160-bit addresses are statistically
// impossible (probability ~10^-30 for 10^9 addresses).
func (g *Generator) generateStreamingBinary() (retStats *Stats, retErr error) {
	stats := &Stats{}
	start := time.Now()

	// Choose backing store based on CommitInterval.
	// When committing incrementally, use a temporary Pebble database so trie
	// nodes survive across commit→reopen cycles. Otherwise use an in-memory DB.
	var (
		backingDB ethdb.Database
		cleanup   func()
	)
	if g.config.CommitInterval > 0 {
		tmpDir, err := os.MkdirTemp("", "bintrie-*")
		if err != nil {
			return nil, fmt.Errorf("failed to create temp dir for trie backing store: %w", err)
		}
		pdb, err := pebble.New(tmpDir, 256, 128, "triedb/", false)
		if err != nil {
			os.RemoveAll(tmpDir)
			return nil, fmt.Errorf("failed to open trie backing store: %w", err)
		}
		backingDB = rawdb.NewDatabase(pdb)
		cleanup = func() {
			pdb.Close()
			os.RemoveAll(tmpDir)
		}
		if g.config.Verbose {
			log.Printf("Using disk-backed trie (commit every %d accounts, tmpdir: %s)",
				g.config.CommitInterval, tmpDir)
		}
	} else {
		backingDB = rawdb.NewMemoryDatabase()
		cleanup = func() { backingDB.Close() }
	}
	defer cleanup()

	trieDB := triedb.NewDatabase(backingDB, &triedb.Config{
		IsVerkle: true,
		PathDB:   pathdb.Defaults,
	})
	defer func() {
		if err := trieDB.Close(); err != nil && retErr == nil {
			retErr = fmt.Errorf("failed to close trie database: %w", err)
		}
	}()

	bt, err := bintrie.NewBinaryTrie(types.EmptyBinaryHash, trieDB)
	if err != nil {
		return nil, fmt.Errorf("failed to create binary trie: %w", err)
	}

	bw := newBatchWriter(g.db, g.config.BatchSize, g.config.Workers)
	defer bw.close()

	// Incremental commit state. Between commits, only the nodes along
	// recently-inserted paths are in memory; everything else is on disk
	// as HashedNode references that resolve lazily on the next insert.
	var (
		totalProcessed int
		lastRoot       = types.EmptyBinaryHash
		blockNum       uint64
	)

	maybeCommit := func() error {
		if g.config.CommitInterval <= 0 {
			return nil
		}
		totalProcessed++
		if totalProcessed%g.config.CommitInterval != 0 {
			return nil
		}

		hash, nodeset := bt.Commit(false)
		merged := trienode.NewWithNodeSet(nodeset)
		if err := trieDB.Update(hash, lastRoot, blockNum, merged, triedb.NewStateSet()); err != nil {
			return fmt.Errorf("trie db update at %d accounts: %w", totalProcessed, err)
		}
		if err := trieDB.Commit(hash, false); err != nil {
			return fmt.Errorf("trie db commit at %d accounts: %w", totalProcessed, err)
		}

		// Reopen the trie from the committed root. The new trie's children
		// are all HashedNode references — only the root InternalNode is loaded.
		newBT, err := bintrie.NewBinaryTrie(hash, trieDB)
		if err != nil {
			return fmt.Errorf("reopen binary trie after commit: %w", err)
		}
		bt = newBT
		lastRoot = hash
		blockNum++

		// Let GC reclaim the old trie's in-memory nodes.
		runtime.GC()

		if g.config.Verbose {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			log.Printf("Committed trie at %d accounts (root: %s, heap: %s)",
				totalProcessed, hash.Hex(), formatBytesUint64(m.HeapInuse))
		}
		return nil
	}

	// Track genesis addresses for collision avoidance.
	genesisAddrs := make(map[common.Address]bool, len(g.config.GenesisAccounts))

	// Phase 1: Stream genesis alloc accounts.
	for addr, acc := range g.config.GenesisAccounts {
		genesisAddrs[addr] = true

		addrHash := crypto.Keccak256Hash(addr[:])
		codeHash := common.BytesToHash(acc.CodeHash)

		ad := &accountData{
			address:  addr,
			addrHash: addrHash,
			account:  acc,
		}
		if storage, ok := g.config.GenesisStorage[addr]; ok {
			ad.storage = storage
			stats.StorageSlotsCreated += len(storage)
		}
		if code, ok := g.config.GenesisCode[addr]; ok {
			ad.code = code
			ad.codeHash = codeHash
		}

		if err := g.processAccountBinaryTrie(bt, bw, ad); err != nil {
			return nil, fmt.Errorf("failed to process genesis account %s: %w", addr.Hex(), err)
		}
		if err := maybeCommit(); err != nil {
			return nil, err
		}

		if len(ad.code) > 0 || len(ad.storage) > 0 {
			stats.ContractsCreated++
		} else {
			stats.AccountsCreated++
		}
	}

	genesisEOAs := stats.AccountsCreated
	genesisContracts := stats.ContractsCreated
	if g.config.Verbose && len(g.config.GenesisAccounts) > 0 {
		log.Printf("Included %d genesis alloc accounts (%d EOAs, %d contracts)",
			len(g.config.GenesisAccounts), genesisEOAs, genesisContracts)
	}

	// Phase 2: Stream EOA generation.
	for i := 0; i < g.config.NumAccounts; i++ {
		acc := g.generateEOA()
		for genesisAddrs[acc.address] {
			acc = g.generateEOA()
		}

		if err := g.processAccountBinaryTrie(bt, bw, acc); err != nil {
			return nil, fmt.Errorf("failed to process EOA %d: %w", i, err)
		}
		if err := maybeCommit(); err != nil {
			return nil, err
		}
		stats.AccountsCreated++

		if g.config.Verbose && (i+1)%1_000_000 == 0 {
			log.Printf("Streamed %d / %d EOAs", i+1, g.config.NumAccounts)
		}
	}

	// Phase 3: Stream contract generation.
	slotDistribution := g.generateSlotDistribution()

	for i := 0; i < g.config.NumContracts; i++ {
		numSlots := slotDistribution[i]
		contract := g.generateContract(numSlots)
		for genesisAddrs[contract.address] {
			contract = g.generateContract(numSlots)
		}

		if err := g.processAccountBinaryTrie(bt, bw, contract); err != nil {
			return nil, fmt.Errorf("failed to process contract %d: %w", i, err)
		}
		if err := maybeCommit(); err != nil {
			return nil, err
		}
		stats.ContractsCreated++
		stats.StorageSlotsCreated += len(contract.storage)

		if g.config.Verbose && (i+1)%100_000 == 0 {
			log.Printf("Streamed %d / %d contracts (%d total slots so far)",
				i+1, g.config.NumContracts, stats.StorageSlotsCreated)
		}
	}

	if err := bw.finish(); err != nil {
		return nil, fmt.Errorf("failed to finish batch writes: %w", err)
	}

	// Compute state root from binary trie.
	stateRoot := bt.Hash()
	stats.StateRoot = stateRoot

	if err := g.db.Put([]byte("SnapshotRoot"), stateRoot[:]); err != nil {
		return nil, fmt.Errorf("failed to write snapshot root: %w", err)
	}

	if g.config.Verbose {
		log.Printf("State root (binary trie, streaming): %s", stateRoot.Hex())
		log.Printf("Generated %d accounts, %d contracts with %d total storage slots",
			stats.AccountsCreated, stats.ContractsCreated, stats.StorageSlotsCreated)
	}

	stats.AccountBytes = bw.accountBytes.Load()
	stats.StorageBytes = bw.storageBytes.Load()
	stats.CodeBytes = bw.codeBytes.Load()
	stats.TotalBytes = stats.AccountBytes + stats.StorageBytes + stats.CodeBytes

	elapsed := time.Since(start)
	stats.GenerationTime = elapsed
	stats.DBWriteTime = elapsed

	return stats, nil
}

// formatBytesUint64 formats a byte count for log output.
func formatBytesUint64(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

// processAccountBinaryTrie inserts a single account into the binary trie and
// writes its snapshot entries to the Pebble batch writer. After this function
// returns, the caller can discard the accountData — it is not retained.
func (g *Generator) processAccountBinaryTrie(bt *bintrie.BinaryTrie, bw *batchWriter, acc *accountData) error {
	// Update binary trie (handles key derivation internally).
	if err := bt.UpdateAccount(acc.address, acc.account, len(acc.code)); err != nil {
		return fmt.Errorf("binary trie UpdateAccount: %w", err)
	}
	if len(acc.code) > 0 {
		if err := bt.UpdateContractCode(acc.address, acc.codeHash, acc.code); err != nil {
			return fmt.Errorf("binary trie UpdateContractCode: %w", err)
		}
	}

	// Process storage: sort keys for deterministic insertion, update trie and snapshot.
	// Skip entirely for EOAs (no allocation, no sort overhead).
	var storageKeys []common.Hash
	if len(acc.storage) > 0 {
		storageKeys = make([]common.Hash, 0, len(acc.storage))
		for k := range acc.storage {
			storageKeys = append(storageKeys, k)
		}
		sort.Slice(storageKeys, func(i, j int) bool {
			return bytes.Compare(storageKeys[i][:], storageKeys[j][:]) < 0
		})

		for _, slotKey := range storageKeys {
			slotValue := acc.storage[slotKey]
			if err := bt.UpdateStorage(acc.address, slotKey[:], slotValue[:]); err != nil {
				return fmt.Errorf("binary trie UpdateStorage: %w", err)
			}
		}
	}

	// Write snapshot entries to Pebble (same format as MPT path).
	// In binary trie mode, Account.Root is always EmptyRootHash.
	snapshotAcc := *acc.account
	snapshotAcc.Root = types.EmptyRootHash
	slimData := types.SlimAccountRLP(snapshotAcc)
	key := accountSnapshotKey(acc.addrHash)
	if err := bw.put(key, slimData, &bw.accountBytes); err != nil {
		return fmt.Errorf("snapshot account write: %w", err)
	}

	// Storage snapshots (keccak256-keyed).
	for _, slotKey := range storageKeys {
		slotValue := acc.storage[slotKey]
		keyHash := crypto.Keccak256Hash(slotKey[:])
		valueRLP, err := encodeStorageValue(slotValue)
		if err != nil {
			return fmt.Errorf("encode storage value: %w", err)
		}
		storageKey := storageSnapshotKey(acc.addrHash, keyHash)
		if err := bw.put(storageKey, valueRLP, &bw.storageBytes); err != nil {
			return fmt.Errorf("snapshot storage write: %w", err)
		}
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

// accountData holds generated account data.
type accountData struct {
	address   common.Address
	addrHash  common.Hash
	account   *types.StateAccount
	code      []byte
	codeHash  common.Hash
	storage   map[common.Hash]common.Hash
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
		if storage, ok := g.config.GenesisStorage[addr]; ok {
			ad.storage = storage
			stats.StorageSlotsCreated += len(storage)
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

	// Generate additional EOA accounts
	for i := 0; i < g.config.NumAccounts; i++ {
		acc := g.generateEOA()
		// Ensure no collision with genesis addresses (extremely unlikely but be safe)
		for usedAddresses[acc.address] {
			acc = g.generateEOA()
		}
		usedAddresses[acc.address] = true
		accounts = append(accounts, acc)
	}
	stats.AccountsCreated = len(accounts)

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

	// Generate storage slots
	storage := make(map[common.Hash]common.Hash, numSlots)
	for j := 0; j < numSlots; j++ {
		var key, value common.Hash
		g.rng.Read(key[:])
		g.rng.Read(value[:])
		// Ensure value is non-zero (zero values are deletions)
		if value == (common.Hash{}) {
			value[31] = 1
		}
		storage[key] = value
	}

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
func (g *Generator) writeStateMPT(accounts, contracts []*accountData, stats *Stats) error {
	accountTrie := trie.NewStackTrie(nil)

	// Merge and sort all accounts by address hash for deterministic trie construction
	allAccounts := make([]*accountData, 0, len(accounts)+len(contracts))
	allAccounts = append(allAccounts, accounts...)
	allAccounts = append(allAccounts, contracts...)
	sort.Slice(allAccounts, func(i, j int) bool {
		return bytes.Compare(allAccounts[i].addrHash[:], allAccounts[j].addrHash[:]) < 0
	})

	bw := newBatchWriter(g.db, g.config.BatchSize, g.config.Workers)
	defer bw.close()

	// Process all accounts (EOAs and contracts) in sorted order
	for _, acc := range allAccounts {
		// Process storage for contracts
		if len(acc.storage) > 0 {
			storageTrie := trie.NewStackTrie(nil)

			// Collect storage keys with their hashes
			type keyWithHash struct {
				key     common.Hash
				keyHash common.Hash
			}
			storageKeys := make([]keyWithHash, 0, len(acc.storage))
			for key := range acc.storage {
				storageKeys = append(storageKeys, keyWithHash{
					key:     key,
					keyHash: crypto.Keccak256Hash(key[:]),
				})
			}
			// Sort by keyHash (StackTrie requires sorted keys)
			sort.Slice(storageKeys, func(i, j int) bool {
				return bytes.Compare(storageKeys[i].keyHash[:], storageKeys[j].keyHash[:]) < 0
			})

			for _, kh := range storageKeys {
				value := acc.storage[kh.key]
				valueRLP, err := encodeStorageValue(value)
				if err != nil {
					return err
				}

				storageKey := storageSnapshotKey(acc.addrHash, kh.keyHash)
				if err := bw.put(storageKey, valueRLP, &bw.storageBytes); err != nil {
					return err
				}

				storageTrie.Update(kh.keyHash[:], valueRLP)
			}

			// Update account's storage root
			acc.account.Root = storageTrie.Hash()
		}

		// Write contract code if present
		if len(acc.code) > 0 {
			cKey := codeKey(acc.codeHash)
			if err := bw.put(cKey, acc.code, &bw.codeBytes); err != nil {
				return err
			}
		}

		// Write account snapshot
		slimData := types.SlimAccountRLP(*acc.account)
		key := accountSnapshotKey(acc.addrHash)
		if err := bw.put(key, slimData, &bw.accountBytes); err != nil {
			return err
		}

		// Add to account trie
		accountTrie.Update(acc.addrHash[:], slimData)
	}

	if err := bw.finish(); err != nil {
		return err
	}

	// Compute and store state root
	stateRoot := accountTrie.Hash()
	stats.StateRoot = stateRoot

	// Write snapshot root marker
	if err := g.db.Put([]byte("SnapshotRoot"), stateRoot[:]); err != nil {
		return fmt.Errorf("failed to write snapshot root: %w", err)
	}

	if g.config.Verbose {
		log.Printf("State root: %s", stateRoot.Hex())
	}

	stats.AccountBytes = bw.accountBytes.Load()
	stats.StorageBytes = bw.storageBytes.Load()
	stats.CodeBytes = bw.codeBytes.Load()

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
