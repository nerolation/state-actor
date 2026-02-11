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

	// TrieModeBinary uses the EIP-7864 binary trie.
	// Compatible with geth's --override.verkle=0 flag (=0 is the activation block number).
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

	// CommitInterval is the number of accounts between binary trie commits
	// to disk. Only used in TrieModeBinary. 0 means no intermediate commits
	// (entire trie stays in memory). When set, the trie is periodically
	// committed to a temporary Pebble database and reopened, bounding memory
	// to the working set between commits (~1-2 GB) instead of the full trie.
	CommitInterval int

	// WriteTrieNodes enables writing serialized binary trie nodes to the
	// database during Phase 2 hash computation, making the database usable
	// by geth's PathDB. Automatically set when --genesis is provided.
	WriteTrieNodes bool

	// InjectAddresses is a list of additional addresses to inject into the
	// generated state with a large balance (999999999 ETH). This is useful
	// for pre-funding test accounts (e.g. Anvil's default account).
	InjectAddresses []common.Address

	// TargetSize is the target total database size on disk in bytes.
	// When set (> 0), the generator stops creating contracts once the
	// estimated on-disk size reaches this target. NumAccounts and
	// NumContracts serve as upper bounds. 0 means no size limit.
	TargetSize uint64

	// OutputFormat specifies the database format to generate.
	// Defaults to OutputGeth if empty.
	OutputFormat OutputFormat
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

	// SampleEOAs holds a few sample EOA addresses for post-generation verification.
	SampleEOAs []common.Address

	// SampleContracts holds a few sample contract addresses for post-generation verification.
	SampleContracts []common.Address

	// DBWriteTime is the time spent writing to the database.
	DBWriteTime time.Duration

	// GenerationTime is the time spent generating data.
	GenerationTime time.Duration
}
