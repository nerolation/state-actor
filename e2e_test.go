package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

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
	block, err := genesis.WriteGenesisBlock(stateGen.DB(), gen, stats.StateRoot)
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
	block, err := genesis.WriteGenesisBlock(db, &gen, stateRoot)
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
