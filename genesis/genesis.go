// Package genesis handles reading, merging, and writing genesis configurations
// with generated state.
package genesis

import (
	"encoding/json"
	"fmt"
	"math/big"
	"os"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/holiman/uint256"
)

// Genesis represents the genesis block configuration.
// This is a simplified version of go-ethereum's Genesis struct
// that handles the JSON format used by ethereum-package and devnets.
type Genesis struct {
	Config     *params.ChainConfig `json:"config"`
	Nonce      hexutil.Uint64      `json:"nonce"`
	Timestamp  hexutil.Uint64      `json:"timestamp"`
	ExtraData  hexutil.Bytes       `json:"extraData"`
	GasLimit   hexutil.Uint64      `json:"gasLimit"`
	Difficulty *hexutil.Big        `json:"difficulty"`
	Mixhash    common.Hash         `json:"mixHash"`
	Coinbase   common.Address      `json:"coinbase"`
	Alloc      GenesisAlloc        `json:"alloc"`

	// Block fields (optional in genesis.json)
	Number        hexutil.Uint64  `json:"number"`
	GasUsed       hexutil.Uint64  `json:"gasUsed"`
	ParentHash    common.Hash     `json:"parentHash"`
	BaseFee       *hexutil.Big    `json:"baseFeePerGas"`
	ExcessBlobGas *hexutil.Uint64 `json:"excessBlobGas"`
	BlobGasUsed   *hexutil.Uint64 `json:"blobGasUsed"`
}

// GenesisAlloc is the genesis allocation map.
type GenesisAlloc map[common.Address]GenesisAccount

// GenesisAccount represents an account in the genesis allocation.
type GenesisAccount struct {
	Code    hexutil.Bytes               `json:"code,omitempty"`
	Storage map[common.Hash]common.Hash `json:"storage,omitempty"`
	Balance *hexutil.Big                `json:"balance"`
	Nonce   hexutil.Uint64              `json:"nonce,omitempty"`
}

// LoadGenesis loads a genesis configuration from a JSON file.
func LoadGenesis(path string) (*Genesis, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read genesis file: %w", err)
	}

	var genesis Genesis
	if err := json.Unmarshal(data, &genesis); err != nil {
		return nil, fmt.Errorf("failed to parse genesis JSON: %w", err)
	}

	return &genesis, nil
}

// ToStateAccounts converts the genesis alloc to types.StateAccount format
// suitable for state generation.
func (g *Genesis) ToStateAccounts() map[common.Address]*types.StateAccount {
	accounts := make(map[common.Address]*types.StateAccount, len(g.Alloc))

	for addr, alloc := range g.Alloc {
		var balance *uint256.Int
		if alloc.Balance != nil {
			balance, _ = uint256.FromBig((*big.Int)(alloc.Balance))
		}
		if balance == nil {
			balance = new(uint256.Int)
		}

		// Compute code hash
		codeHash := types.EmptyCodeHash
		if len(alloc.Code) > 0 {
			codeHash = crypto.Keccak256Hash(alloc.Code)
		}

		accounts[addr] = &types.StateAccount{
			Nonce:    uint64(alloc.Nonce),
			Balance:  balance,
			Root:     types.EmptyRootHash, // Will be updated if storage exists
			CodeHash: codeHash.Bytes(),
		}
	}

	return accounts
}

// GetAllocStorage returns the storage for genesis alloc accounts.
func (g *Genesis) GetAllocStorage() map[common.Address]map[common.Hash]common.Hash {
	storage := make(map[common.Address]map[common.Hash]common.Hash)

	for addr, alloc := range g.Alloc {
		if len(alloc.Storage) > 0 {
			storage[addr] = alloc.Storage
		}
	}

	return storage
}

// GetAllocCode returns the code for genesis alloc accounts.
func (g *Genesis) GetAllocCode() map[common.Address][]byte {
	code := make(map[common.Address][]byte)

	for addr, alloc := range g.Alloc {
		if len(alloc.Code) > 0 {
			code[addr] = alloc.Code
		}
	}

	return code
}

// WriteGenesisBlock writes the genesis block and associated metadata to the database.
// This is called after state generation with the computed state root.
func WriteGenesisBlock(db ethdb.KeyValueStore, genesis *Genesis, stateRoot common.Hash) (*types.Block, error) {
	if genesis.Config == nil {
		return nil, fmt.Errorf("genesis has no chain config")
	}

	// Build the genesis block header
	header := &types.Header{
		Number:     new(big.Int).SetUint64(uint64(genesis.Number)),
		Nonce:      types.EncodeNonce(uint64(genesis.Nonce)),
		Time:       uint64(genesis.Timestamp),
		ParentHash: genesis.ParentHash,
		Extra:      genesis.ExtraData,
		GasLimit:   uint64(genesis.GasLimit),
		GasUsed:    uint64(genesis.GasUsed),
		Difficulty: (*big.Int)(genesis.Difficulty),
		MixDigest:  genesis.Mixhash,
		Coinbase:   genesis.Coinbase,
		Root:       stateRoot,
	}

	// Set defaults
	if header.GasLimit == 0 {
		header.GasLimit = params.GenesisGasLimit
	}
	if header.Difficulty == nil {
		if genesis.Config.Ethash == nil {
			header.Difficulty = big.NewInt(0)
		} else {
			header.Difficulty = params.GenesisDifficulty
		}
	}

	// Handle EIP-1559 base fee
	if genesis.Config.IsLondon(common.Big0) {
		if genesis.BaseFee != nil {
			header.BaseFee = (*big.Int)(genesis.BaseFee)
		} else {
			header.BaseFee = new(big.Int).SetUint64(params.InitialBaseFee)
		}
	}

	var withdrawals []*types.Withdrawal
	num := big.NewInt(int64(genesis.Number))
	timestamp := uint64(genesis.Timestamp)

	// Handle Shanghai
	if genesis.Config.IsShanghai(num, timestamp) {
		emptyWithdrawalsHash := types.EmptyWithdrawalsHash
		header.WithdrawalsHash = &emptyWithdrawalsHash
		withdrawals = make([]*types.Withdrawal, 0)
	}

	// Handle Cancun
	if genesis.Config.IsCancun(num, timestamp) {
		header.ParentBeaconRoot = new(common.Hash)
		if genesis.ExcessBlobGas != nil {
			excess := uint64(*genesis.ExcessBlobGas)
			header.ExcessBlobGas = &excess
		} else {
			header.ExcessBlobGas = new(uint64)
		}
		if genesis.BlobGasUsed != nil {
			used := uint64(*genesis.BlobGasUsed)
			header.BlobGasUsed = &used
		} else {
			header.BlobGasUsed = new(uint64)
		}
	}

	// Handle Prague
	if genesis.Config.IsPrague(num, timestamp) {
		emptyRequestsHash := types.EmptyRequestsHash
		header.RequestsHash = &emptyRequestsHash
	}

	// Create the block
	block := types.NewBlock(header, &types.Body{Withdrawals: withdrawals}, nil, trie.NewStackTrie(nil))

	// Write to database
	batch := db.NewBatch()

	// Marshal genesis alloc for storage (geth expects this)
	allocBlob, err := json.Marshal(genesis.Alloc)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal genesis alloc: %w", err)
	}

	// Write all the required rawdb entries
	rawdb.WriteGenesisStateSpec(batch, block.Hash(), allocBlob)
	rawdb.WriteBlock(batch, block)
	rawdb.WriteReceipts(batch, block.Hash(), block.NumberU64(), nil)
	rawdb.WriteCanonicalHash(batch, block.Hash(), block.NumberU64())
	rawdb.WriteHeadBlockHash(batch, block.Hash())
	rawdb.WriteHeadFastBlockHash(batch, block.Hash())
	rawdb.WriteHeadHeaderHash(batch, block.Hash())
	rawdb.WriteChainConfig(batch, block.Hash(), genesis.Config)

	if err := batch.Write(); err != nil {
		return nil, fmt.Errorf("failed to write genesis block: %w", err)
	}

	return block, nil
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
