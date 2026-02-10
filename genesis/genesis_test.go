package genesis

import (
	"encoding/json"
	"math/big"
	"os"
	"path/filepath"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/params"
	"github.com/holiman/uint256"
)

// sampleGenesis creates a minimal genesis configuration for testing.
func sampleGenesis() *Genesis {
	chainConfig := &params.ChainConfig{
		ChainID:             big.NewInt(1337),
		HomesteadBlock:      big.NewInt(0),
		EIP150Block:         big.NewInt(0),
		EIP155Block:         big.NewInt(0),
		EIP158Block:         big.NewInt(0),
		ByzantiumBlock:      big.NewInt(0),
		ConstantinopleBlock: big.NewInt(0),
		PetersburgBlock:     big.NewInt(0),
		IstanbulBlock:       big.NewInt(0),
		MuirGlacierBlock:    big.NewInt(0),
		BerlinBlock:         big.NewInt(0),
		LondonBlock:         big.NewInt(0),
		MergeNetsplitBlock:  big.NewInt(0),
		ShanghaiTime:        new(uint64),
		CancunTime:          new(uint64),
		TerminalTotalDifficulty: big.NewInt(0),
	}

	return &Genesis{
		Config:     chainConfig,
		Nonce:      0,
		Timestamp:  0,
		ExtraData:  []byte("test genesis"),
		GasLimit:   hexutil.Uint64(30000000),
		Difficulty: (*hexutil.Big)(big.NewInt(0)),
		Alloc: GenesisAlloc{
			common.HexToAddress("0x1111111111111111111111111111111111111111"): {
				Balance: (*hexutil.Big)(big.NewInt(1e18)),
				Nonce:   0,
			},
			common.HexToAddress("0x2222222222222222222222222222222222222222"): {
				Balance: (*hexutil.Big)(big.NewInt(2e18)),
				Code:    hexutil.Bytes{0x60, 0x00, 0x60, 0x00, 0xf3}, // PUSH1 0 PUSH1 0 RETURN
				Storage: map[common.Hash]common.Hash{
					common.HexToHash("0x01"): common.HexToHash("0xdeadbeef"),
				},
			},
		},
	}
}

func TestLoadGenesis(t *testing.T) {
	// Create a temporary genesis file
	dir := t.TempDir()
	genesisPath := filepath.Join(dir, "genesis.json")

	genesis := sampleGenesis()
	data, err := json.MarshalIndent(genesis, "", "  ")
	if err != nil {
		t.Fatalf("Failed to marshal genesis: %v", err)
	}

	if err := os.WriteFile(genesisPath, data, 0644); err != nil {
		t.Fatalf("Failed to write genesis file: %v", err)
	}

	// Load it back
	loaded, err := LoadGenesis(genesisPath)
	if err != nil {
		t.Fatalf("Failed to load genesis: %v", err)
	}

	// Verify
	if len(loaded.Alloc) != 2 {
		t.Errorf("Expected 2 alloc accounts, got %d", len(loaded.Alloc))
	}

	addr1 := common.HexToAddress("0x1111111111111111111111111111111111111111")
	if loaded.Alloc[addr1].Balance == nil {
		t.Error("Account 1 should have balance")
	}

	addr2 := common.HexToAddress("0x2222222222222222222222222222222222222222")
	if len(loaded.Alloc[addr2].Code) == 0 {
		t.Error("Account 2 should have code")
	}
	if len(loaded.Alloc[addr2].Storage) == 0 {
		t.Error("Account 2 should have storage")
	}
}

func TestToStateAccounts(t *testing.T) {
	genesis := sampleGenesis()
	accounts := genesis.ToStateAccounts()

	if len(accounts) != 2 {
		t.Fatalf("Expected 2 accounts, got %d", len(accounts))
	}

	addr1 := common.HexToAddress("0x1111111111111111111111111111111111111111")
	acc1, ok := accounts[addr1]
	if !ok {
		t.Fatalf("Account 1 not found")
	}

	expectedBalance := uint256.NewInt(1e18)
	if acc1.Balance.Cmp(expectedBalance) != 0 {
		t.Errorf("Account 1 balance mismatch: got %s, want %s", acc1.Balance, expectedBalance)
	}
	if acc1.CodeHash == nil {
		t.Error("Account 1 should have empty code hash")
	}

	addr2 := common.HexToAddress("0x2222222222222222222222222222222222222222")
	acc2, ok := accounts[addr2]
	if !ok {
		t.Fatalf("Account 2 not found")
	}

	// Account 2 has code, so code hash should not be empty
	if common.BytesToHash(acc2.CodeHash) == types.EmptyCodeHash {
		t.Error("Account 2 should have non-empty code hash")
	}
}

func TestGetAllocStorage(t *testing.T) {
	genesis := sampleGenesis()
	storage := genesis.GetAllocStorage()

	// Only account 2 has storage
	if len(storage) != 1 {
		t.Fatalf("Expected 1 account with storage, got %d", len(storage))
	}

	addr2 := common.HexToAddress("0x2222222222222222222222222222222222222222")
	if _, ok := storage[addr2]; !ok {
		t.Error("Account 2 storage not found")
	}

	key := common.HexToHash("0x01")
	value := common.HexToHash("0xdeadbeef")
	if storage[addr2][key] != value {
		t.Errorf("Storage mismatch: got %s, want %s", storage[addr2][key].Hex(), value.Hex())
	}
}

func TestGetAllocCode(t *testing.T) {
	genesis := sampleGenesis()
	code := genesis.GetAllocCode()

	// Only account 2 has code
	if len(code) != 1 {
		t.Fatalf("Expected 1 account with code, got %d", len(code))
	}

	addr2 := common.HexToAddress("0x2222222222222222222222222222222222222222")
	if _, ok := code[addr2]; !ok {
		t.Error("Account 2 code not found")
	}

	expectedCode := []byte{0x60, 0x00, 0x60, 0x00, 0xf3}
	if string(code[addr2]) != string(expectedCode) {
		t.Errorf("Code mismatch: got %x, want %x", code[addr2], expectedCode)
	}
}

func TestWriteGenesisBlock(t *testing.T) {
	// Use rawdb.NewMemoryDatabase which implements full ethdb.Database interface
	db := rawdb.NewMemoryDatabase()
	defer db.Close()

	genesis := sampleGenesis()

	// Use a deterministic state root for testing
	stateRoot := common.HexToHash("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")

	block, err := WriteGenesisBlock(db, genesis, stateRoot, false, "")
	if err != nil {
		t.Fatalf("Failed to write genesis block: %v", err)
	}

	// Verify block properties
	if block.NumberU64() != 0 {
		t.Errorf("Genesis block number should be 0, got %d", block.NumberU64())
	}
	if block.Root() != stateRoot {
		t.Errorf("State root mismatch: got %s, want %s", block.Root().Hex(), stateRoot.Hex())
	}

	// Verify database entries
	// 1. Canonical hash
	canonicalHash := rawdb.ReadCanonicalHash(db, 0)
	if canonicalHash != block.Hash() {
		t.Errorf("Canonical hash mismatch: got %s, want %s", canonicalHash.Hex(), block.Hash().Hex())
	}

	// 2. Head block hash
	headBlockHash := rawdb.ReadHeadBlockHash(db)
	if headBlockHash != block.Hash() {
		t.Errorf("Head block hash mismatch: got %s, want %s", headBlockHash.Hex(), block.Hash().Hex())
	}

	// 3. Head header hash
	headHeaderHash := rawdb.ReadHeadHeaderHash(db)
	if headHeaderHash != block.Hash() {
		t.Errorf("Head header hash mismatch: got %s, want %s", headHeaderHash.Hex(), block.Hash().Hex())
	}

	// 4. Chain config
	chainConfig := rawdb.ReadChainConfig(db, block.Hash())
	if chainConfig == nil {
		t.Error("Chain config not found in database")
	} else {
		if chainConfig.ChainID.Cmp(big.NewInt(1337)) != 0 {
			t.Errorf("Chain ID mismatch: got %s, want 1337", chainConfig.ChainID)
		}
		if chainConfig.EnableVerkleAtGenesis {
			t.Error("EnableVerkleAtGenesis should be false when binaryTrie=false")
		}
	}

	// 5. Block should be retrievable
	storedBlock := rawdb.ReadBlock(db, block.Hash(), 0)
	if storedBlock == nil {
		t.Error("Genesis block not found in database")
	} else if storedBlock.Hash() != block.Hash() {
		t.Errorf("Retrieved block hash mismatch: got %s, want %s", storedBlock.Hash().Hex(), block.Hash().Hex())
	}
}

func TestWriteGenesisBlockWithShanghai(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	defer db.Close()

	genesis := sampleGenesis()
	// Enable Shanghai at genesis
	zero := uint64(0)
	genesis.Config.ShanghaiTime = &zero

	stateRoot := common.HexToHash("0xabcd")
	block, err := WriteGenesisBlock(db, genesis, stateRoot, false, "")
	if err != nil {
		t.Fatalf("Failed to write genesis block: %v", err)
	}

	// Shanghai blocks should have withdrawals hash
	if block.Header().WithdrawalsHash == nil {
		t.Error("Shanghai genesis should have withdrawals hash")
	}
	if *block.Header().WithdrawalsHash != types.EmptyWithdrawalsHash {
		t.Error("Genesis withdrawals hash should be empty withdrawals hash")
	}
}

func TestWriteGenesisBlockWithCancun(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	defer db.Close()

	genesis := sampleGenesis()
	// Enable Cancun at genesis
	zero := uint64(0)
	genesis.Config.ShanghaiTime = &zero
	genesis.Config.CancunTime = &zero

	stateRoot := common.HexToHash("0xabcd")
	block, err := WriteGenesisBlock(db, genesis, stateRoot, false, "")
	if err != nil {
		t.Fatalf("Failed to write genesis block: %v", err)
	}

	header := block.Header()

	// Cancun blocks should have blob gas fields
	if header.ExcessBlobGas == nil {
		t.Error("Cancun genesis should have excess blob gas")
	}
	if header.BlobGasUsed == nil {
		t.Error("Cancun genesis should have blob gas used")
	}
	if header.ParentBeaconRoot == nil {
		t.Error("Cancun genesis should have parent beacon root")
	}
}

func TestWriteGenesisBlockBinaryTrie(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	defer db.Close()

	genesis := sampleGenesis()
	stateRoot := common.HexToHash("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890")

	block, err := WriteGenesisBlock(db, genesis, stateRoot, true, "")
	if err != nil {
		t.Fatalf("Failed to write genesis block: %v", err)
	}

	// Verify chain config was persisted with EnableVerkleAtGenesis
	chainConfig := rawdb.ReadChainConfig(db, block.Hash())
	if chainConfig == nil {
		t.Fatal("Chain config not found in database")
	}
	if !chainConfig.EnableVerkleAtGenesis {
		t.Error("EnableVerkleAtGenesis should be true for binary trie mode")
	}

	// Verify block is readable
	storedBlock := rawdb.ReadBlock(db, block.Hash(), 0)
	if storedBlock == nil {
		t.Error("Genesis block not found in database")
	}
	if storedBlock.Root() != stateRoot {
		t.Errorf("State root mismatch: got %s, want %s", storedBlock.Root().Hex(), stateRoot.Hex())
	}
}

func TestWriteGenesisBlockBinaryTrieNoMutation(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	defer db.Close()

	gen := sampleGenesis()
	origConfig := gen.Config
	origVerkle := gen.Config.EnableVerkleAtGenesis

	stateRoot := common.HexToHash("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890")

	_, err := WriteGenesisBlock(db, gen, stateRoot, true, "")
	if err != nil {
		t.Fatalf("Failed to write genesis block: %v", err)
	}

	// The caller's Genesis.Config pointer must not have been replaced
	if gen.Config != origConfig {
		t.Error("WriteGenesisBlock replaced caller's genesis.Config pointer")
	}
	// The original ChainConfig must not have been mutated
	if gen.Config.EnableVerkleAtGenesis != origVerkle {
		t.Errorf("WriteGenesisBlock mutated caller's EnableVerkleAtGenesis: got %v, want %v",
			gen.Config.EnableVerkleAtGenesis, origVerkle)
	}
}

// TestGenesisJSONRoundTrip tests that genesis JSON can be loaded and produces expected outputs.
func TestGenesisJSONRoundTrip(t *testing.T) {
	// Sample JSON similar to ethereum-package output
	genesisJSON := `{
		"config": {
			"chainId": 1337,
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
			"0x1111111111111111111111111111111111111111": {
				"balance": "0xde0b6b3a7640000"
			}
		}
	}`

	dir := t.TempDir()
	genesisPath := filepath.Join(dir, "genesis.json")
	if err := os.WriteFile(genesisPath, []byte(genesisJSON), 0644); err != nil {
		t.Fatalf("Failed to write genesis file: %v", err)
	}

	genesis, err := LoadGenesis(genesisPath)
	if err != nil {
		t.Fatalf("Failed to load genesis: %v", err)
	}

	if genesis.Config == nil {
		t.Fatal("Chain config should not be nil")
	}
	if genesis.Config.ChainID.Cmp(big.NewInt(1337)) != 0 {
		t.Errorf("Chain ID mismatch: got %s, want 1337", genesis.Config.ChainID)
	}

	accounts := genesis.ToStateAccounts()
	addr := common.HexToAddress("0x1111111111111111111111111111111111111111")
	if _, ok := accounts[addr]; !ok {
		t.Error("Expected account not found")
	}

	// Balance should be 1 ETH (0xde0b6b3a7640000 = 10^18)
	expectedBalance := uint256.NewInt(1e18)
	if accounts[addr].Balance.Cmp(expectedBalance) != 0 {
		t.Errorf("Balance mismatch: got %s, want %s", accounts[addr].Balance, expectedBalance)
	}
}
