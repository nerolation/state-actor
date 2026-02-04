package generator

import (
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// TrieMode represents the trie algorithm used for state root computation.
type TrieMode string

const (
	// TrieModeMPT uses the Merkle Patricia Trie (hexary, keccak256-based).
	TrieModeMPT TrieMode = "mpt"

	// TrieModeBinary uses the EIP-7864 binary trie (SHA256-based).
	// Compatible with geth's --override.verkle=0 flag.
	TrieModeBinary TrieMode = "binary"
)

// Distribution represents the storage slot distribution strategy.
type Distribution int

const (
	// PowerLaw distribution - most contracts have few slots, few have many.
	// This matches real Ethereum state where a few contracts (like Uniswap)
	// have millions of slots while most have very few.
	PowerLaw Distribution = iota

	// Uniform distribution - all contracts have similar slot counts.
	Uniform

	// Exponential distribution - exponential decay in slot counts.
	Exponential
)

// ParseDistribution parses a distribution string.
func ParseDistribution(s string) Distribution {
	switch strings.ToLower(s) {
	case "uniform":
		return Uniform
	case "exponential":
		return Exponential
	default:
		return PowerLaw
	}
}

// Config holds the configuration for state generation.
type Config struct {
	// DBPath is the path to the Pebble database directory.
	DBPath string

	// NumAccounts is the number of EOA accounts to create.
	NumAccounts int

	// NumContracts is the number of contract accounts to create.
	NumContracts int

	// MaxSlots is the maximum number of storage slots per contract.
	MaxSlots int

	// MinSlots is the minimum number of storage slots per contract.
	MinSlots int

	// Distribution is the storage slot distribution strategy.
	Distribution Distribution

	// Seed is the random seed for reproducible generation.
	Seed int64

	// BatchSize is the number of operations per database batch.
	BatchSize int

	// Workers is the number of parallel workers for generation.
	Workers int

	// CodeSize is the average contract code size in bytes.
	CodeSize int

	// Verbose enables verbose logging.
	Verbose bool

	// TrieMode selects the trie algorithm for state root computation.
	// Defaults to TrieModeMPT if empty.
	TrieMode TrieMode

	// GenesisAccounts are pre-defined accounts from genesis.json alloc.
	// These are included in state generation with their exact addresses.
	GenesisAccounts map[common.Address]*types.StateAccount

	// GenesisStorage is the storage for genesis alloc accounts.
	GenesisStorage map[common.Address]map[common.Hash]common.Hash

	// GenesisCode is the code for genesis alloc accounts.
	GenesisCode map[common.Address][]byte
}

// Stats holds statistics about the generation process.
type Stats struct {
	// AccountsCreated is the number of accounts created.
	AccountsCreated int

	// ContractsCreated is the number of contracts created.
	ContractsCreated int

	// StorageSlotsCreated is the total number of storage slots created.
	StorageSlotsCreated int

	// TotalBytes is the total number of bytes written.
	TotalBytes uint64

	// AccountBytes is the number of bytes for account data.
	AccountBytes uint64

	// StorageBytes is the number of bytes for storage data.
	StorageBytes uint64

	// CodeBytes is the number of bytes for contract code.
	CodeBytes uint64

	// StateRoot is the computed state root hash.
	StateRoot common.Hash

	// DBWriteTime is the time spent writing to the database.
	DBWriteTime time.Duration

	// GenerationTime is the time spent generating data.
	GenerationTime time.Duration
}
