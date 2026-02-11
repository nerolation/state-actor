package generator

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/params"
)

// OutputFormat specifies the database format to generate.
type OutputFormat string

const (
	// OutputGeth generates a geth-compatible Pebble database (snapshot layer).
	OutputGeth OutputFormat = "geth"

	// OutputErigon generates an Erigon-compatible MDBX database (PlainState).
	OutputErigon OutputFormat = "erigon"

	// OutputReth generates a Reth-compatible database via the reth-import subprocess.
	OutputReth OutputFormat = "reth"
)

// ParseOutputFormat parses an output format string.
func ParseOutputFormat(s string) OutputFormat {
	switch s {
	case "erigon":
		return OutputErigon
	case "reth":
		return OutputReth
	default:
		return OutputGeth
	}
}

// StateWriter abstracts database writes for different client formats.
type StateWriter interface {
	// WriteAccount writes an account to the database.
	// For contracts, codeHash should match the hash of the code written via WriteCode.
	WriteAccount(addr common.Address, acc *types.StateAccount, incarnation uint64) error

	// WriteStorage writes a storage slot for an account.
	WriteStorage(addr common.Address, incarnation uint64, slot, value common.Hash) error

	// WriteCode writes contract bytecode.
	WriteCode(codeHash common.Hash, code []byte) error

	// SetStateRoot sets the computed state root (for metadata/markers).
	SetStateRoot(root common.Hash) error

	// WriteGenesisBlock writes the genesis block and chain config.
	// Only called when genesis integration is enabled.
	WriteGenesisBlock(config *params.ChainConfig, stateRoot common.Hash) error

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
