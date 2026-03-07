package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/erigontech/mdbx-go/mdbx"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb/pebble"
	"github.com/nerolation/state-actor/generator"
	"github.com/nerolation/state-actor/genesis"
)

// TestEndToEndWithGenesis tests the complete workflow:
// 1. Load a genesis file
// 2. Generate state including genesis accounts
// 3. Write genesis block with correct state root
// 4. Verify the database is complete and readable
func TestEndToEndWithGenesis(t *testing.T) {
	// Create a realistic genesis JSON
	genesisJSON := `{
		"config": {
			"chainId": 32382,
			"homesteadBlock": 0,
			"eip150Block": 0,
			"eip155Block": 0,
			"eip158Block": 0,
			"byzantiumBlock": 0,
			"constantinopleBlock": 0,
			"petersburgBlock": 0,
			"istanbulBlock": 0,
			"berlinBlock": 0,
			"londonBlock": 0,
			"mergeNetsplitBlock": 0,
			"shanghaiTime": 0,
			"cancunTime": 0,
			"terminalTotalDifficulty": 0
		},
		"nonce": "0x0",
		"timestamp": "0x0",
		"extraData": "0x",
		"gasLimit": "0x1c9c380",
		"difficulty": "0x0",
		"mixHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
		"coinbase": "0x0000000000000000000000000000000000000000",
		"alloc": {
			"0x123463a4b065722e99115d6c222f267d9cabb524": {
				"balance": "0x43c33c1937564800000"
			},
			"0x8943545177806ed17b9f23f0a21ee5948ecaa776": {
				"code": "0x60606040523615600e57600e565b5b603f806100196000396000f3006060604052361560025760025b5b60006000fd00",
				"balance": "0x1"
			},
			"0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef": {
				"code": "0x6000",
				"balance": "0x0",
				"nonce": "0x1",
				"storage": {
					"0x0000000000000000000000000000000000000000000000000000000000000001": "0x0000000000000000000000000000000000000000000000000000000000000042"
				}
			}
		},
		"number": "0x0",
		"gasUsed": "0x0",
		"parentHash": "0x0000000000000000000000000000000000000000000000000000000000000000"
	}`

	// Create temp directory structure
	dir := t.TempDir()
	genesisPath := filepath.Join(dir, "genesis.json")
	dbPath := filepath.Join(dir, "chaindata")

	// Write genesis file
	if err := os.WriteFile(genesisPath, []byte(genesisJSON), 0644); err != nil {
		t.Fatalf("Failed to write genesis file: %v", err)
	}

	// Load genesis
	gen, err := genesis.LoadGenesis(genesisPath)
	if err != nil {
		t.Fatalf("Failed to load genesis: %v", err)
	}

	// Configure generator with genesis accounts
	config := generator.Config{
		DBPath:          dbPath,
		NumAccounts:     100,
		NumContracts:    50,
		MaxSlots:        1000,
		MinSlots:        10,
		Distribution:    generator.PowerLaw,
		Seed:            12345,
		BatchSize:       10000,
		Workers:         4,
		CodeSize:        512,
		Verbose:         false,
		GenesisAccounts: gen.ToStateAccounts(),
		GenesisStorage:  gen.GetAllocStorage(),
		GenesisCode:     gen.GetAllocCode(),
	}

	// Create and run generator
	stateGen, err := generator.New(config)
	if err != nil {
		t.Fatalf("Failed to create generator: %v", err)
	}

	stats, err := stateGen.Generate()
	if err != nil {
		stateGen.Close()
		t.Fatalf("Failed to generate state: %v", err)
	}

	// Write genesis block
	block, err := genesis.WriteGenesisBlock(stateGen.DB(), gen, stats.StateRoot, false)
	if err != nil {
		stateGen.Close()
		t.Fatalf("Failed to write genesis block: %v", err)
	}

	// Close generator (flushes database)
	stateGen.Close()

	t.Logf("Generated state:")
	t.Logf("  Accounts: %d", stats.AccountsCreated)
	t.Logf("  Contracts: %d", stats.ContractsCreated)
	t.Logf("  Storage slots: %d", stats.StorageSlotsCreated)
	t.Logf("  State root: %s", stats.StateRoot.Hex())
	t.Logf("  Genesis block hash: %s", block.Hash().Hex())

	// Verify the database is readable
	// Reopen database
	db, err := pebble.New(dbPath, 128, 64, "verify/", true) // readonly
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db.Close()

	// Verify genesis accounts exist
	verifyAddress := func(addr common.Address, name string) {
		addrHash := crypto.Keccak256Hash(addr[:])
		key := append([]byte("a"), addrHash[:]...)
		data, err := db.Get(key)
		if err != nil {
			t.Errorf("Account %s (%s) not found: %v", name, addr.Hex(), err)
			return
		}
		if len(data) == 0 {
			t.Errorf("Account %s (%s) has empty data", name, addr.Hex())
		}
	}

	verifyAddress(common.HexToAddress("0x123463a4b065722e99115d6c222f267d9cabb524"), "prefunded")
	verifyAddress(common.HexToAddress("0x8943545177806ed17b9f23f0a21ee5948ecaa776"), "contract1")
	verifyAddress(common.HexToAddress("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"), "contract2")

	// Verify storage slot exists
	contractAddr := common.HexToAddress("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef")
	contractAddrHash := crypto.Keccak256Hash(contractAddr[:])
	slotKey := common.HexToHash("0x01")
	slotKeyHash := crypto.Keccak256Hash(slotKey[:])
	storageKey := append([]byte("o"), contractAddrHash[:]...)
	storageKey = append(storageKey, slotKeyHash[:]...)
	storageData, err := db.Get(storageKey)
	if err != nil {
		t.Errorf("Storage slot not found: %v", err)
	} else if len(storageData) == 0 {
		t.Error("Storage slot has empty data")
	}

	// Verify code exists
	code := []byte{0x60, 0x00} // 0x6000
	codeHash := crypto.Keccak256Hash(code)
	codeKey := append([]byte("c"), codeHash[:]...)
	codeData, err := db.Get(codeKey)
	if err != nil {
		t.Errorf("Code not found: %v", err)
	} else if string(codeData) != string(code) {
		t.Errorf("Code mismatch: got %x, want %x", codeData, code)
	}

	// Verify SnapshotRoot marker
	snapshotRoot, err := db.Get([]byte("SnapshotRoot"))
	if err != nil {
		t.Errorf("SnapshotRoot marker not found: %v", err)
	} else if common.BytesToHash(snapshotRoot) != stats.StateRoot {
		t.Errorf("SnapshotRoot mismatch: got %x, want %s", snapshotRoot, stats.StateRoot.Hex())
	}

	// Verify expected counts
	// 3 genesis accounts: 1 EOA, 2 contracts
	// Plus 100 generated EOAs, 50 generated contracts
	expectedAccounts := 1 + 100 // 1 genesis EOA + 100 generated
	expectedContracts := 2 + 50 // 2 genesis contracts + 50 generated

	if stats.AccountsCreated != expectedAccounts {
		t.Errorf("Account count mismatch: got %d, want %d", stats.AccountsCreated, expectedAccounts)
	}
	if stats.ContractsCreated != expectedContracts {
		t.Errorf("Contract count mismatch: got %d, want %d", stats.ContractsCreated, expectedContracts)
	}
}

// TestEndToEndWithGenesisBinaryTrie tests the complete workflow with binary trie mode:
// 1. Load a genesis file
// 2. Generate state with TrieModeBinary
// 3. Write genesis block with binaryTrie=true
// 4. Verify database is complete with correct config
func TestEndToEndWithGenesisBinaryTrie(t *testing.T) {
	genesisJSON := `{
		"config": {
			"chainId": 32382,
			"homesteadBlock": 0,
			"eip150Block": 0,
			"eip155Block": 0,
			"eip158Block": 0,
			"byzantiumBlock": 0,
			"constantinopleBlock": 0,
			"petersburgBlock": 0,
			"istanbulBlock": 0,
			"berlinBlock": 0,
			"londonBlock": 0,
			"mergeNetsplitBlock": 0,
			"shanghaiTime": 0,
			"cancunTime": 0,
			"terminalTotalDifficulty": 0
		},
		"nonce": "0x0",
		"timestamp": "0x0",
		"extraData": "0x",
		"gasLimit": "0x1c9c380",
		"difficulty": "0x0",
		"mixHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
		"coinbase": "0x0000000000000000000000000000000000000000",
		"alloc": {
			"0x123463a4b065722e99115d6c222f267d9cabb524": {
				"balance": "0x43c33c1937564800000"
			},
			"0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef": {
				"code": "0x6000",
				"balance": "0x0",
				"nonce": "0x1",
				"storage": {
					"0x0000000000000000000000000000000000000000000000000000000000000001": "0x0000000000000000000000000000000000000000000000000000000000000042"
				}
			}
		},
		"number": "0x0",
		"gasUsed": "0x0",
		"parentHash": "0x0000000000000000000000000000000000000000000000000000000000000000"
	}`

	dir := t.TempDir()
	genesisPath := filepath.Join(dir, "genesis.json")
	dbPath := filepath.Join(dir, "chaindata")

	if err := os.WriteFile(genesisPath, []byte(genesisJSON), 0644); err != nil {
		t.Fatalf("Failed to write genesis file: %v", err)
	}

	gen, err := genesis.LoadGenesis(genesisPath)
	if err != nil {
		t.Fatalf("Failed to load genesis: %v", err)
	}

	config := generator.Config{
		DBPath:          dbPath,
		NumAccounts:     50,
		NumContracts:    20,
		MaxSlots:        100,
		MinSlots:        5,
		Distribution:    generator.PowerLaw,
		Seed:            12345,
		BatchSize:       10000,
		Workers:         1,
		CodeSize:        256,
		Verbose:         false,
		TrieMode:        generator.TrieModeBinary,
		GenesisAccounts: gen.ToStateAccounts(),
		GenesisStorage:  gen.GetAllocStorage(),
		GenesisCode:     gen.GetAllocCode(),
	}

	stateGen, err := generator.New(config)
	if err != nil {
		t.Fatalf("Failed to create generator: %v", err)
	}

	stats, err := stateGen.Generate()
	if err != nil {
		stateGen.Close()
		t.Fatalf("Failed to generate state: %v", err)
	}

	// Write genesis block with binary trie enabled
	block, err := genesis.WriteGenesisBlock(stateGen.DB(), gen, stats.StateRoot, true)
	if err != nil {
		stateGen.Close()
		t.Fatalf("Failed to write genesis block: %v", err)
	}
	stateGen.Close()

	t.Logf("Binary trie e2e: root=%s block=%s", stats.StateRoot.Hex(), block.Hash().Hex())

	// Verify state root is non-zero
	if stats.StateRoot == (common.Hash{}) {
		t.Error("State root should not be zero")
	}

	// Reopen and verify
	db, err := pebble.New(dbPath, 128, 64, "verify/", true)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db.Close()

	// Verify genesis accounts
	for _, addr := range []common.Address{
		common.HexToAddress("0x123463a4b065722e99115d6c222f267d9cabb524"),
		common.HexToAddress("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"),
	} {
		addrHash := crypto.Keccak256Hash(addr[:])
		key := append([]byte("a"), addrHash[:]...)
		data, err := db.Get(key)
		if err != nil {
			t.Errorf("Account %s not found: %v", addr.Hex(), err)
			continue
		}
		if len(data) == 0 {
			t.Errorf("Account %s has empty data", addr.Hex())
		}
	}

	// Verify SnapshotRoot
	snapshotRoot, err := db.Get([]byte("SnapshotRoot"))
	if err != nil {
		t.Errorf("SnapshotRoot not found: %v", err)
	} else if common.BytesToHash(snapshotRoot) != stats.StateRoot {
		t.Errorf("SnapshotRoot mismatch: got %x, want %s", snapshotRoot, stats.StateRoot.Hex())
	}

	// Verify chain config has EnableVerkleAtGenesis
	chainConfig := rawdb.ReadChainConfig(db, block.Hash())
	if chainConfig == nil {
		t.Error("Chain config not found")
	} else if !chainConfig.EnableVerkleAtGenesis {
		t.Error("Chain config should have EnableVerkleAtGenesis=true for binary trie mode")
	}

	// Verify expected counts
	expectedAccounts := 1 + 50 // 1 genesis EOA + 50 generated
	expectedContracts := 1 + 20 // 1 genesis contract + 20 generated

	if stats.AccountsCreated != expectedAccounts {
		t.Errorf("Account count mismatch: got %d, want %d", stats.AccountsCreated, expectedAccounts)
	}
	if stats.ContractsCreated != expectedContracts {
		t.Errorf("Contract count mismatch: got %d, want %d", stats.ContractsCreated, expectedContracts)
	}
}

// TestDatabaseReadableByRawDB tests that the generated database can be read
// using geth's rawdb functions (requires ethdb.Database interface).
func TestDatabaseReadableByRawDB(t *testing.T) {
	// Use memory database which implements full ethdb.Database
	db := rawdb.NewMemoryDatabase()
	defer db.Close()

	genesisJSON := `{
		"config": {
			"chainId": 1337,
			"homesteadBlock": 0,
			"eip155Block": 0,
			"eip158Block": 0,
			"byzantiumBlock": 0,
			"constantinopleBlock": 0,
			"petersburgBlock": 0,
			"istanbulBlock": 0,
			"berlinBlock": 0,
			"londonBlock": 0,
			"terminalTotalDifficulty": 0
		},
		"gasLimit": "0x1c9c380",
		"difficulty": "0x0",
		"alloc": {
			"0x1111111111111111111111111111111111111111": {
				"balance": "0xde0b6b3a7640000"
			}
		}
	}`

	var gen genesis.Genesis
	if err := json.Unmarshal([]byte(genesisJSON), &gen); err != nil {
		t.Fatalf("Failed to parse genesis: %v", err)
	}

	// Write a genesis block
	stateRoot := common.HexToHash("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
	block, err := genesis.WriteGenesisBlock(db, &gen, stateRoot, false)
	if err != nil {
		t.Fatalf("Failed to write genesis block: %v", err)
	}

	// Verify using rawdb
	canonicalHash := rawdb.ReadCanonicalHash(db, 0)
	if canonicalHash != block.Hash() {
		t.Errorf("Canonical hash mismatch")
	}

	headBlockHash := rawdb.ReadHeadBlockHash(db)
	if headBlockHash != block.Hash() {
		t.Errorf("Head block hash mismatch")
	}

	chainConfig := rawdb.ReadChainConfig(db, block.Hash())
	if chainConfig == nil {
		t.Error("Chain config not found")
	} else {
		if chainConfig.ChainID.Int64() != 1337 {
			t.Errorf("Chain ID mismatch: got %d, want 1337", chainConfig.ChainID.Int64())
		}
	}

	storedBlock := rawdb.ReadBlock(db, block.Hash(), 0)
	if storedBlock == nil {
		t.Error("Block not found")
	} else {
		if storedBlock.Root() != stateRoot {
			t.Errorf("State root mismatch in stored block")
		}
	}
}

// TestEndToEndErigonFormat tests the Erigon output format.
func TestEndToEndErigonFormat(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "erigon-e2e-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := generator.Config{
		DBPath:       tmpDir,
		NumAccounts:  50,
		NumContracts: 25,
		MaxSlots:     100,
		MinSlots:     1,
		Distribution: generator.PowerLaw,
		Seed:         42,
		BatchSize:    1000,
		Workers:      4,
		CodeSize:     512,
		OutputFormat: generator.OutputErigon,
	}

	gen, err := generator.New(config)
	if err != nil {
		t.Fatalf("Failed to create generator: %v", err)
	}

	stats, err := gen.Generate()
	if err != nil {
		gen.Close()
		t.Fatalf("Failed to generate state: %v", err)
	}

	if err := gen.Close(); err != nil {
		t.Fatalf("Failed to close generator: %v", err)
	}

	// Verify the output
	if stats.AccountsCreated != 50 {
		t.Errorf("Expected 50 accounts, got %d", stats.AccountsCreated)
	}
	if stats.ContractsCreated != 25 {
		t.Errorf("Expected 25 contracts, got %d", stats.ContractsCreated)
	}
	if stats.StorageSlotsCreated == 0 {
		t.Error("Expected storage slots to be created")
	}
	if stats.StateRoot == (common.Hash{}) {
		t.Error("Expected non-zero state root")
	}

	t.Logf("Erigon e2e: accounts=%d, contracts=%d, slots=%d, root=%s",
		stats.AccountsCreated, stats.ContractsCreated, stats.StorageSlotsCreated, stats.StateRoot.Hex())

	// Verify MDBX database was created
	if _, err := os.Stat(filepath.Join(tmpDir, "mdbx.dat")); os.IsNotExist(err) {
		t.Error("MDBX database file not created")
	}
}

// TestAllFormatsProduceSameStateRoot verifies that geth, erigon, and nethermind
// formats all produce the same state root for identical input.
func TestAllFormatsProduceSameStateRoot(t *testing.T) {
	seed := int64(12345)

	formats := []struct {
		name   string
		format generator.OutputFormat
	}{
		{"geth", generator.OutputGeth},
		{"erigon", generator.OutputErigon},
		{"nethermind", generator.OutputNethermind},
	}

	type result struct {
		stats *generator.Stats
	}
	results := make(map[string]result)

	for _, f := range formats {
		t.Run(f.name, func(t *testing.T) {
			dir, err := os.MkdirTemp("", f.name+"-compare-*")
			if err != nil {
				t.Fatalf("Failed to create temp dir: %v", err)
			}
			defer os.RemoveAll(dir)

			config := generator.Config{
				DBPath:       dir,
				NumAccounts:  30,
				NumContracts: 15,
				MaxSlots:     50,
				MinSlots:     1,
				Distribution: generator.Uniform,
				Seed:         seed,
				BatchSize:    1000,
				Workers:      4,
				CodeSize:     256,
				OutputFormat: f.format,
			}

			gen, err := generator.New(config)
			if err != nil {
				t.Fatalf("Failed to create %s generator: %v", f.name, err)
			}

			stats, err := gen.Generate()
			if err != nil {
				gen.Close()
				t.Fatalf("Failed to generate %s state: %v", f.name, err)
			}
			gen.Close()

			results[f.name] = result{stats: stats}

			if stats.StateRoot == (common.Hash{}) {
				t.Errorf("%s: state root should not be zero", f.name)
			}

			t.Logf("%s: root=%s accounts=%d contracts=%d slots=%d",
				f.name, stats.StateRoot.Hex(),
				stats.AccountsCreated, stats.ContractsCreated, stats.StorageSlotsCreated)
		})
	}

	// Compare all pairs
	geth := results["geth"]
	erigon := results["erigon"]
	nethermind := results["nethermind"]

	if geth.stats == nil || erigon.stats == nil || nethermind.stats == nil {
		t.Fatal("One or more formats failed to produce stats")
	}

	if geth.stats.StateRoot != erigon.stats.StateRoot {
		t.Errorf("State root mismatch geth vs erigon:\n  geth:   %s\n  erigon: %s",
			geth.stats.StateRoot.Hex(), erigon.stats.StateRoot.Hex())
	}
	if geth.stats.StateRoot != nethermind.stats.StateRoot {
		t.Errorf("State root mismatch geth vs nethermind:\n  geth:       %s\n  nethermind: %s",
			geth.stats.StateRoot.Hex(), nethermind.stats.StateRoot.Hex())
	}

	// Verify all stats match
	for _, name := range []string{"erigon", "nethermind"} {
		other := results[name]
		if geth.stats.AccountsCreated != other.stats.AccountsCreated {
			t.Errorf("AccountsCreated mismatch geth(%d) vs %s(%d)",
				geth.stats.AccountsCreated, name, other.stats.AccountsCreated)
		}
		if geth.stats.ContractsCreated != other.stats.ContractsCreated {
			t.Errorf("ContractsCreated mismatch geth(%d) vs %s(%d)",
				geth.stats.ContractsCreated, name, other.stats.ContractsCreated)
		}
		if geth.stats.StorageSlotsCreated != other.stats.StorageSlotsCreated {
			t.Errorf("StorageSlotsCreated mismatch geth(%d) vs %s(%d)",
				geth.stats.StorageSlotsCreated, name, other.stats.StorageSlotsCreated)
		}
	}

	t.Logf("All three formats produced identical state root: %s", geth.stats.StateRoot.Hex())
}

// TestAllFormatsWithGenesisProduceSameStateRoot verifies that all three output
// formats produce the same state root when initialized from the same genesis file.
// This is the core invariant: all clients must start with identical state.
func TestAllFormatsWithGenesisProduceSameStateRoot(t *testing.T) {
	genesisJSON := `{
		"config": {
			"chainId": 32382,
			"homesteadBlock": 0,
			"eip150Block": 0,
			"eip155Block": 0,
			"eip158Block": 0,
			"byzantiumBlock": 0,
			"constantinopleBlock": 0,
			"petersburgBlock": 0,
			"istanbulBlock": 0,
			"berlinBlock": 0,
			"londonBlock": 0,
			"mergeNetsplitBlock": 0,
			"shanghaiTime": 0,
			"cancunTime": 0,
			"terminalTotalDifficulty": 0
		},
		"nonce": "0x0",
		"timestamp": "0x0",
		"extraData": "0x",
		"gasLimit": "0x1c9c380",
		"difficulty": "0x0",
		"mixHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
		"coinbase": "0x0000000000000000000000000000000000000000",
		"alloc": {
			"0x123463a4b065722e99115d6c222f267d9cabb524": {
				"balance": "0x43c33c1937564800000"
			},
			"0x8943545177806ed17b9f23f0a21ee5948ecaa776": {
				"code": "0x60606040523615600e57600e565b5b603f806100196000396000f3006060604052361560025760025b5b60006000fd00",
				"balance": "0x1"
			},
			"0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef": {
				"code": "0x6000",
				"balance": "0x0",
				"nonce": "0x1",
				"storage": {
					"0x0000000000000000000000000000000000000000000000000000000000000001": "0x0000000000000000000000000000000000000000000000000000000000000042"
				}
			}
		},
		"number": "0x0",
		"gasUsed": "0x0",
		"parentHash": "0x0000000000000000000000000000000000000000000000000000000000000000"
	}`

	// Write shared genesis file
	dir := t.TempDir()
	genesisPath := filepath.Join(dir, "genesis.json")
	if err := os.WriteFile(genesisPath, []byte(genesisJSON), 0644); err != nil {
		t.Fatalf("Failed to write genesis file: %v", err)
	}

	gen, err := genesis.LoadGenesis(genesisPath)
	if err != nil {
		t.Fatalf("Failed to load genesis: %v", err)
	}

	genesisAccounts := gen.ToStateAccounts()
	genesisStorage := gen.GetAllocStorage()
	genesisCode := gen.GetAllocCode()

	seed := int64(42)

	formats := []struct {
		name   string
		format generator.OutputFormat
	}{
		{"geth", generator.OutputGeth},
		{"erigon", generator.OutputErigon},
		{"nethermind", generator.OutputNethermind},
	}

	type result struct {
		stats *generator.Stats
		dir   string
	}
	results := make(map[string]result)

	for _, f := range formats {
		fDir, err := os.MkdirTemp(dir, f.name+"-*")
		if err != nil {
			t.Fatalf("Failed to create temp dir for %s: %v", f.name, err)
		}

		config := generator.Config{
			DBPath:          fDir,
			NumAccounts:     50,
			NumContracts:    25,
			MaxSlots:        100,
			MinSlots:        5,
			Distribution:    generator.PowerLaw,
			Seed:            seed,
			BatchSize:       1000,
			Workers:         1,
			CodeSize:        256,
			OutputFormat:    f.format,
			GenesisAccounts: genesisAccounts,
			GenesisStorage:  genesisStorage,
			GenesisCode:     genesisCode,
		}

		stateGen, err := generator.New(config)
		if err != nil {
			t.Fatalf("Failed to create %s generator: %v", f.name, err)
		}

		stats, err := stateGen.Generate()
		if err != nil {
			stateGen.Close()
			t.Fatalf("Failed to generate %s state: %v", f.name, err)
		}

		// Write genesis via the writer
		if err := stateGen.Writer().WriteGenesis(gen, stats.StateRoot, false); err != nil {
			stateGen.Close()
			t.Fatalf("Failed to write %s genesis: %v", f.name, err)
		}

		stateGen.Close()
		results[f.name] = result{stats: stats, dir: fDir}

		t.Logf("%s: root=%s accounts=%d contracts=%d slots=%d",
			f.name, stats.StateRoot.Hex(),
			stats.AccountsCreated, stats.ContractsCreated, stats.StorageSlotsCreated)
	}

	// All three must produce the same state root
	geth := results["geth"]
	erigon := results["erigon"]
	nethermind := results["nethermind"]

	if geth.stats.StateRoot != erigon.stats.StateRoot {
		t.Errorf("State root mismatch geth vs erigon:\n  geth:   %s\n  erigon: %s",
			geth.stats.StateRoot.Hex(), erigon.stats.StateRoot.Hex())
	}
	if geth.stats.StateRoot != nethermind.stats.StateRoot {
		t.Errorf("State root mismatch geth vs nethermind:\n  geth:       %s\n  nethermind: %s",
			geth.stats.StateRoot.Hex(), nethermind.stats.StateRoot.Hex())
	}

	// Verify stat counts match across all formats
	for _, name := range []string{"erigon", "nethermind"} {
		other := results[name]
		if geth.stats.AccountsCreated != other.stats.AccountsCreated {
			t.Errorf("AccountsCreated mismatch geth(%d) vs %s(%d)",
				geth.stats.AccountsCreated, name, other.stats.AccountsCreated)
		}
		if geth.stats.ContractsCreated != other.stats.ContractsCreated {
			t.Errorf("ContractsCreated mismatch geth(%d) vs %s(%d)",
				geth.stats.ContractsCreated, name, other.stats.ContractsCreated)
		}
		if geth.stats.StorageSlotsCreated != other.stats.StorageSlotsCreated {
			t.Errorf("StorageSlotsCreated mismatch geth(%d) vs %s(%d)",
				geth.stats.StorageSlotsCreated, name, other.stats.StorageSlotsCreated)
		}
	}

	// Verify Geth DB has the correct snapshot root and genesis metadata.
	// We use direct KV reads since pebble.Database doesn't implement
	// ethdb.Reader (no Ancient support), which rawdb functions require.
	gethDB, err := pebble.New(geth.dir, 128, 64, "verify/", true)
	if err != nil {
		t.Fatalf("Failed to reopen geth DB: %v", err)
	}
	defer gethDB.Close()

	// SnapshotRoot must match the computed state root
	snapshotRoot, err := gethDB.Get([]byte("SnapshotRoot"))
	if err != nil {
		t.Errorf("Geth: SnapshotRoot not found: %v", err)
	} else if common.BytesToHash(snapshotRoot) != geth.stats.StateRoot {
		t.Errorf("Geth: SnapshotRoot mismatch: got %x, want %s",
			snapshotRoot, geth.stats.StateRoot.Hex())
	}

	// HeadBlockHash key must be present (genesis block was written)
	headHash, err := gethDB.Get([]byte("LastBlock"))
	if err != nil {
		t.Errorf("Geth: head block hash not found: %v", err)
	} else if len(headHash) == 0 {
		t.Error("Geth: head block hash is empty")
	}

	// Verify some genesis alloc accounts are in the snapshot layer
	for _, addr := range []common.Address{
		common.HexToAddress("0x123463a4b065722e99115d6c222f267d9cabb524"),
		common.HexToAddress("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"),
	} {
		addrHash := crypto.Keccak256Hash(addr[:])
		key := append([]byte("a"), addrHash[:]...)
		data, err := gethDB.Get(key)
		if err != nil {
			t.Errorf("Geth: genesis account %s not found: %v", addr.Hex(), err)
		} else if len(data) == 0 {
			t.Errorf("Geth: genesis account %s has empty data", addr.Hex())
		}
	}

	// Verify Nethermind MDBX database has state root and chain ID in Config table
	nmEnv, err := mdbx.NewEnv()
	if err != nil {
		t.Fatalf("Failed to create mdbx env for nethermind: %v", err)
	}
	if err := nmEnv.SetOption(mdbx.OptMaxDB, 100); err != nil {
		t.Fatalf("Failed to set max dbs: %v", err)
	}
	if err := nmEnv.Open(nethermind.dir, uint(mdbx.Readonly|mdbx.NoReadahead), 0644); err != nil {
		t.Fatalf("Failed to open nethermind mdbx: %v", err)
	}
	defer nmEnv.Close()

	nmEnv.View(func(txn *mdbx.Txn) error {
		dbi, err := txn.OpenDBI("Config", 0, nil, nil)
		if err != nil {
			t.Fatalf("Nethermind: Config table not found: %v", err)
		}
		stateRootVal, err := txn.Get(dbi, []byte("StateRoot"))
		if err != nil {
			t.Errorf("Nethermind: StateRoot not found in Config: %v", err)
		} else if common.BytesToHash(stateRootVal) != nethermind.stats.StateRoot {
			t.Errorf("Nethermind: StateRoot mismatch: got %x, want %s",
				stateRootVal, nethermind.stats.StateRoot.Hex())
		}
		chainIDVal, err := txn.Get(dbi, []byte("ChainID"))
		if err != nil {
			t.Errorf("Nethermind: ChainID not found in Config: %v", err)
		} else if len(chainIDVal) != 8 {
			t.Errorf("Nethermind: ChainID wrong length: %d", len(chainIDVal))
		}
		return nil
	})

	t.Logf("All three formats produced identical state root: %s", geth.stats.StateRoot.Hex())
}
