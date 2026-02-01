package generator

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	mrand "math/rand"
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
	db     ethdb.KeyValueStore
	rng    *mrand.Rand
}

// New creates a new state generator.
func New(config Config) (*Generator, error) {
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

// writeState writes all state to the database.
func (g *Generator) writeState(accounts, contracts []*accountData, stats *Stats) error {
	// Use a stack trie for computing the state root
	accountTrie := trie.NewStackTrie(nil)
	
	// Merge and sort all accounts by address hash for deterministic trie construction
	allAccounts := make([]*accountData, 0, len(accounts)+len(contracts))
	allAccounts = append(allAccounts, accounts...)
	allAccounts = append(allAccounts, contracts...)
	sort.Slice(allAccounts, func(i, j int) bool {
		return bytes.Compare(allAccounts[i].addrHash[:], allAccounts[j].addrHash[:]) < 0
	})
	
	// Process in batches using workers
	var wg sync.WaitGroup
	var accountBytes, storageBytes, codeBytes atomic.Uint64
	
	// Channel for batch work
	type batchWork struct {
		batch ethdb.Batch
	}
	
	batchChan := make(chan *batchWork, g.config.Workers*2)
	errChan := make(chan error, 1)
	
	// Start batch writer workers
	for i := 0; i < g.config.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for work := range batchChan {
				if err := work.batch.Write(); err != nil {
					select {
					case errChan <- err:
					default:
					}
					return
				}
			}
		}()
	}

	batch := g.db.NewBatch()
	batchCount := 0
	
	// Helper to flush batch
	flushBatch := func() error {
		if batchCount == 0 {
			return nil
		}
		batchChan <- &batchWork{batch: batch}
		batch = g.db.NewBatch()
		batchCount = 0
		return nil
	}
	
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
				valueRLP := encodeStorageValue(value)
				
				// Write storage snapshot
				storageKey := storageSnapshotKey(acc.addrHash, kh.keyHash)
				if err := batch.Put(storageKey, valueRLP); err != nil {
					return err
				}
				storageBytes.Add(uint64(len(storageKey) + len(valueRLP)))
				
				storageTrie.Update(kh.keyHash[:], valueRLP)
				
				batchCount++
				if batchCount >= g.config.BatchSize {
					if err := flushBatch(); err != nil {
						return err
					}
				}
			}
			
			// Update account's storage root
			acc.account.Root = storageTrie.Hash()
		}
		
		// Write contract code if present
		if len(acc.code) > 0 {
			cKey := codeKey(acc.codeHash)
			if err := batch.Put(cKey, acc.code); err != nil {
				return err
			}
			codeBytes.Add(uint64(len(cKey) + len(acc.code)))
			batchCount++
		}
		
		// Write account snapshot
		slimData := types.SlimAccountRLP(*acc.account)
		key := accountSnapshotKey(acc.addrHash)
		if err := batch.Put(key, slimData); err != nil {
			return err
		}
		accountBytes.Add(uint64(len(key) + len(slimData)))
		
		// Add to account trie
		accountTrie.Update(acc.addrHash[:], slimData)
		
		batchCount++
		if batchCount >= g.config.BatchSize {
			if err := flushBatch(); err != nil {
				return err
			}
		}
	}

	// Flush remaining batch
	if err := flushBatch(); err != nil {
		return err
	}
	
	// Close batch channel and wait for workers
	close(batchChan)
	wg.Wait()
	
	// Check for errors
	select {
	case err := <-errChan:
		return err
	default:
	}

	// Compute and store state root
	stateRoot := accountTrie.Hash()
	stats.StateRoot = stateRoot
	
	// Write snapshot root marker
	snapshotRootKey := []byte("SnapshotRoot")
	if err := g.db.Put(snapshotRootKey, stateRoot[:]); err != nil {
		return fmt.Errorf("failed to write snapshot root: %w", err)
	}

	if g.config.Verbose {
		log.Printf("State root: %s", stateRoot.Hex())
	}

	stats.AccountBytes = accountBytes.Load()
	stats.StorageBytes = storageBytes.Load()
	stats.CodeBytes = codeBytes.Load()

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
func encodeStorageValue(value common.Hash) []byte {
	trimmed := trimLeftZeroes(value[:])
	if len(trimmed) == 0 {
		return nil
	}
	encoded, _ := rlp.EncodeToBytes(trimmed)
	return encoded
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
