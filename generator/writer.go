package generator

import (
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/nerolation/state-actor/genesis"
)

// OutputFormat specifies the database format to generate.
type OutputFormat string

const (
	// OutputGeth generates a geth-compatible Pebble database (snapshot layer).
	OutputGeth OutputFormat = "geth"

	// OutputErigon generates an Erigon-compatible MDBX database (PlainState).
	OutputErigon OutputFormat = "erigon"

	// OutputNethermind generates a Nethermind-compatible MDBX database
	// with keccak256-hashed keys and RLP-encoded accounts.
	OutputNethermind OutputFormat = "nethermind"
)

// ParseOutputFormat parses an output format string.
func ParseOutputFormat(s string) OutputFormat {
	switch strings.ToLower(s) {
	case "erigon":
		return OutputErigon
	case "nethermind":
		return OutputNethermind
	default:
		return OutputGeth
	}
}

// StateWriter abstracts database writes for different client formats.
// All implementations write directly to the client's database.
type StateWriter interface {
	// WriteAccount writes an account to the output.
	// For contracts, codeHash should match the hash of the code written via WriteCode.
	WriteAccount(addr common.Address, acc *types.StateAccount, incarnation uint64) error

	// WriteStorage writes a storage slot for an account.
	WriteStorage(addr common.Address, incarnation uint64, slot, value common.Hash) error

	// WriteCode writes contract bytecode.
	WriteCode(codeHash common.Hash, code []byte) error

	// SetStateRoot sets the computed state root (for metadata/markers).
	SetStateRoot(root common.Hash) error

	// WriteGenesis writes client-specific genesis data directly into the database.
	// Called after state generation with the computed state root.
	WriteGenesis(genesisConfig *genesis.Genesis, stateRoot common.Hash, binaryTrie bool) error

	// Flush ensures all pending writes are committed.
	Flush() error

	// Close closes the writer and releases resources.
	Close() error

	// Stats returns write statistics.
	Stats() WriterStats
}

// WriterStats holds statistics about database writes.
type WriterStats struct {
	AccountBytes uint64
	StorageBytes uint64
	CodeBytes    uint64
}
