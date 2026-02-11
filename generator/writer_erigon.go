package generator

import (
	"encoding/binary"
	"fmt"
	"math/bits"
	"sync"
	"sync/atomic"

	"github.com/erigontech/mdbx-go/mdbx"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/params"
	"github.com/holiman/uint256"
)

// ErigonWriter writes state to an Erigon-compatible MDBX database.
// It uses the PlainState format with unhashed keys.
type ErigonWriter struct {
	env    *mdbx.Env
	dbPath string

	accountBytes atomic.Uint64
	storageBytes atomic.Uint64
	codeBytes    atomic.Uint64

	// Buffered writes
	mu       sync.Mutex
	accounts map[common.Address]*erigonAccountData
	storage  map[erigonStorageKey]common.Hash
	code     map[common.Hash][]byte
}

type erigonAccountData struct {
	account     *types.StateAccount
	incarnation uint64
}

type erigonStorageKey struct {
	addr        common.Address
	incarnation uint64
	slot        common.Hash
}

// Erigon table names
const (
	erigonPlainState = "PlainState"
	erigonCode       = "Code"
	erigonConfig     = "Config"
)

// NewErigonWriter creates a new Erigon-format state writer.
func NewErigonWriter(dbPath string) (*ErigonWriter, error) {
	env, err := mdbx.NewEnv()
	if err != nil {
		return nil, fmt.Errorf("failed to create mdbx env: %w", err)
	}

	// Set environment options
	if err := env.SetOption(mdbx.OptMaxDB, 100); err != nil {
		return nil, fmt.Errorf("failed to set max dbs: %w", err)
	}

	// Set map size to 1TB (can grow)
	if err := env.SetGeometry(-1, -1, 1<<40, -1, -1, 4096); err != nil {
		return nil, fmt.Errorf("failed to set geometry: %w", err)
	}

	// Open the environment
	flags := uint(mdbx.NoReadahead | mdbx.Coalesce | mdbx.Durable)
	if err := env.Open(dbPath, flags, 0644); err != nil {
		return nil, fmt.Errorf("failed to open mdbx: %w", err)
	}

	w := &ErigonWriter{
		env:      env,
		dbPath:   dbPath,
		accounts: make(map[common.Address]*erigonAccountData),
		storage:  make(map[erigonStorageKey]common.Hash),
		code:     make(map[common.Hash][]byte),
	}

	// Create required tables
	if err := w.createTables(); err != nil {
		env.Close()
		return nil, fmt.Errorf("failed to create tables: %w", err)
	}

	return w, nil
}

func (w *ErigonWriter) createTables() error {
	return w.env.Update(func(txn *mdbx.Txn) error {
		// PlainState with DupSort
		_, err := txn.OpenDBI(erigonPlainState, mdbx.Create, nil, nil)
		if err != nil {
			return fmt.Errorf("create PlainState: %w", err)
		}

		// Code table
		_, err = txn.OpenDBI(erigonCode, mdbx.Create, nil, nil)
		if err != nil {
			return fmt.Errorf("create Code: %w", err)
		}

		// Config table
		_, err = txn.OpenDBI(erigonConfig, mdbx.Create, nil, nil)
		if err != nil {
			return fmt.Errorf("create Config: %w", err)
		}

		return nil
	})
}

// WriteAccount buffers an account write.
func (w *ErigonWriter) WriteAccount(addr common.Address, acc *types.StateAccount, incarnation uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.accounts[addr] = &erigonAccountData{
		account:     acc,
		incarnation: incarnation,
	}

	// Estimate bytes
	encoded := erigonEncodeAccount(acc, incarnation)
	w.accountBytes.Add(uint64(len(addr) + len(encoded)))

	return nil
}

// WriteStorage buffers a storage slot write.
func (w *ErigonWriter) WriteStorage(addr common.Address, incarnation uint64, slot, value common.Hash) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	key := erigonStorageKey{
		addr:        addr,
		incarnation: incarnation,
		slot:        slot,
	}
	w.storage[key] = value

	// Estimate bytes: address(20) + incarnation(8) + slot(32) + value(32)
	w.storageBytes.Add(20 + 8 + 32 + 32)

	return nil
}

// WriteCode buffers a code write.
func (w *ErigonWriter) WriteCode(codeHash common.Hash, code []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.code[codeHash] = code
	w.codeBytes.Add(uint64(len(codeHash) + len(code)))

	return nil
}

// SetStateRoot writes the state root marker.
func (w *ErigonWriter) SetStateRoot(root common.Hash) error {
	return w.env.Update(func(txn *mdbx.Txn) error {
		dbi, err := txn.OpenDBI(erigonConfig, 0, nil, nil)
		if err != nil {
			return err
		}
		return txn.Put(dbi, []byte("StateRoot"), root[:], 0)
	})
}

// WriteGenesisBlock writes genesis metadata (simplified).
func (w *ErigonWriter) WriteGenesisBlock(config *params.ChainConfig, stateRoot common.Hash) error {
	// For now, just write the chain ID and state root
	// Full genesis writing would require more Erigon-specific logic
	return w.env.Update(func(txn *mdbx.Txn) error {
		dbi, err := txn.OpenDBI(erigonConfig, 0, nil, nil)
		if err != nil {
			return err
		}

		// Write chain ID
		chainIDBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(chainIDBytes, config.ChainID.Uint64())
		if err := txn.Put(dbi, []byte("ChainID"), chainIDBytes, 0); err != nil {
			return err
		}

		// Write state root
		return txn.Put(dbi, []byte("StateRoot"), stateRoot[:], 0)
	})
}

// Flush commits all buffered writes to the database.
func (w *ErigonWriter) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.env.Update(func(txn *mdbx.Txn) error {
		// Open tables
		plainStateDBI, err := txn.OpenDBI(erigonPlainState, 0, nil, nil)
		if err != nil {
			return fmt.Errorf("open PlainState: %w", err)
		}

		codeDBI, err := txn.OpenDBI(erigonCode, 0, nil, nil)
		if err != nil {
			return fmt.Errorf("open Code: %w", err)
		}

		// Write accounts
		for addr, data := range w.accounts {
			encoded := erigonEncodeAccount(data.account, data.incarnation)
			if err := txn.Put(plainStateDBI, addr[:], encoded, 0); err != nil {
				return fmt.Errorf("write account %s: %w", addr.Hex(), err)
			}
		}

		// Write storage
		for key, value := range w.storage {
			// PlainState storage key: address(20) + incarnation(8) + slot(32)
			storageKey := make([]byte, 20+8+32)
			copy(storageKey[:20], key.addr[:])
			binary.BigEndian.PutUint64(storageKey[20:28], key.incarnation)
			copy(storageKey[28:], key.slot[:])

			// Value is stored as-is (32 bytes) with leading zeros trimmed
			trimmedValue := erigonTrimLeftZeroes(value[:])
			if len(trimmedValue) == 0 {
				continue // Skip zero values (deletions)
			}

			if err := txn.Put(plainStateDBI, storageKey, trimmedValue, 0); err != nil {
				return fmt.Errorf("write storage %s/%s: %w", key.addr.Hex(), key.slot.Hex(), err)
			}
		}

		// Write code
		for hash, code := range w.code {
			if err := txn.Put(codeDBI, hash[:], code, 0); err != nil {
				return fmt.Errorf("write code %s: %w", hash.Hex(), err)
			}
		}

		return nil
	})
}

// Close closes the writer.
func (w *ErigonWriter) Close() error {
	w.env.Close()
	return nil
}

// Stats returns write statistics.
func (w *ErigonWriter) Stats() WriterStats {
	return WriterStats{
		AccountBytes: w.accountBytes.Load(),
		StorageBytes: w.storageBytes.Load(),
		CodeBytes:    w.codeBytes.Load(),
	}
}

// --- Erigon account encoding ---
// Based on Erigon's execution/types/accounts/account.go EncodeForStorage

func erigonEncodeAccount(acc *types.StateAccount, incarnation uint64) []byte {
	var fieldSet byte = 0
	var buf []byte

	// Field 0: Nonce
	if acc.Nonce > 0 {
		fieldSet |= 1
		nonceBytes := bitLenToByteLen(bits.Len64(acc.Nonce))
		buf = append(buf, byte(nonceBytes))
		for i := nonceBytes; i > 0; i-- {
			buf = append(buf, byte(acc.Nonce>>(8*(i-1))))
		}
	}

	// Field 1: Balance
	balance := uint256.MustFromBig(acc.Balance.ToBig())
	if !balance.IsZero() {
		fieldSet |= 2
		balanceBytes := balance.ByteLen()
		buf = append(buf, byte(balanceBytes))
		balanceSlice := make([]byte, balanceBytes)
		balance.WriteToSlice(balanceSlice)
		buf = append(buf, balanceSlice...)
	}

	// Field 2: Incarnation
	if incarnation > 0 {
		fieldSet |= 4
		incBytes := bitLenToByteLen(bits.Len64(incarnation))
		buf = append(buf, byte(incBytes))
		for i := incBytes; i > 0; i-- {
			buf = append(buf, byte(incarnation>>(8*(i-1))))
		}
	}

	// Field 3: CodeHash
	emptyCodeHash := common.HexToHash("c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470")
	codeHash := common.BytesToHash(acc.CodeHash)
	if codeHash != emptyCodeHash && codeHash != (common.Hash{}) {
		fieldSet |= 8
		buf = append(buf, 32)
		buf = append(buf, codeHash[:]...)
	}

	// Prepend fieldset byte
	result := make([]byte, 1+len(buf))
	result[0] = fieldSet
	copy(result[1:], buf)

	return result
}

func bitLenToByteLen(bitLen int) int {
	return (bitLen + 7) / 8
}

func erigonTrimLeftZeroes(s []byte) []byte {
	for i, v := range s {
		if v != 0 {
			return s[i:]
		}
	}
	return nil
}
