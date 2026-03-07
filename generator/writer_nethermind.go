package generator

import (
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/erigontech/mdbx-go/mdbx"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/nerolation/state-actor/genesis"
)

// NethermindWriter writes state to a Nethermind-compatible MDBX database.
// It stores accounts and storage using keccak256-hashed keys and RLP encoding,
// matching Nethermind's internal state representation.
type NethermindWriter struct {
	env    *mdbx.Env
	dbPath string

	accountBytes atomic.Uint64
	storageBytes atomic.Uint64
	codeBytes    atomic.Uint64

	// Buffered writes
	mu       sync.Mutex
	accounts map[common.Address]*nethermindAccountData
	storage  map[nethermindStorageKey]common.Hash
	code     map[common.Hash][]byte
}

type nethermindAccountData struct {
	account *types.StateAccount
}

type nethermindStorageKey struct {
	addr common.Address
	slot common.Hash
}

// Nethermind table names
const (
	nethermindState  = "State"
	nethermindCode   = "Code"
	nethermindConfig = "Config"
)

// NewNethermindWriter creates a new Nethermind-format state writer.
func NewNethermindWriter(dbPath string) (*NethermindWriter, error) {
	env, err := mdbx.NewEnv()
	if err != nil {
		return nil, fmt.Errorf("failed to create mdbx env: %w", err)
	}

	if err := env.SetOption(mdbx.OptMaxDB, 100); err != nil {
		return nil, fmt.Errorf("failed to set max dbs: %w", err)
	}

	if err := env.SetGeometry(-1, -1, 1<<40, -1, -1, 4096); err != nil {
		return nil, fmt.Errorf("failed to set geometry: %w", err)
	}

	flags := uint(mdbx.NoReadahead | mdbx.Coalesce | mdbx.Durable)
	if err := env.Open(dbPath, flags, 0644); err != nil {
		return nil, fmt.Errorf("failed to open mdbx: %w", err)
	}

	w := &NethermindWriter{
		env:      env,
		dbPath:   dbPath,
		accounts: make(map[common.Address]*nethermindAccountData),
		storage:  make(map[nethermindStorageKey]common.Hash),
		code:     make(map[common.Hash][]byte),
	}

	if err := w.createTables(); err != nil {
		env.Close()
		return nil, fmt.Errorf("failed to create tables: %w", err)
	}

	return w, nil
}

func (w *NethermindWriter) createTables() error {
	return w.env.Update(func(txn *mdbx.Txn) error {
		_, err := txn.OpenDBI(nethermindState, mdbx.Create, nil, nil)
		if err != nil {
			return fmt.Errorf("create State: %w", err)
		}

		_, err = txn.OpenDBI(nethermindCode, mdbx.Create, nil, nil)
		if err != nil {
			return fmt.Errorf("create Code: %w", err)
		}

		_, err = txn.OpenDBI(nethermindConfig, mdbx.Create, nil, nil)
		if err != nil {
			return fmt.Errorf("create Config: %w", err)
		}

		return nil
	})
}

// WriteAccount buffers an account write.
func (w *NethermindWriter) WriteAccount(addr common.Address, acc *types.StateAccount, incarnation uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.accounts[addr] = &nethermindAccountData{account: acc}

	encoded, _ := rlp.EncodeToBytes(acc)
	w.accountBytes.Add(uint64(common.HashLength + len(encoded)))

	return nil
}

// WriteStorage buffers a storage slot write.
func (w *NethermindWriter) WriteStorage(addr common.Address, incarnation uint64, slot, value common.Hash) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	key := nethermindStorageKey{addr: addr, slot: slot}
	w.storage[key] = value

	// keccak(addr)(32) + keccak(slot)(32) + value(32)
	w.storageBytes.Add(32 + 32 + 32)

	return nil
}

// WriteCode buffers a code write.
func (w *NethermindWriter) WriteCode(codeHash common.Hash, code []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.code[codeHash] = code
	w.codeBytes.Add(uint64(len(codeHash) + len(code)))

	return nil
}

// SetStateRoot writes the state root marker.
func (w *NethermindWriter) SetStateRoot(root common.Hash) error {
	return w.env.Update(func(txn *mdbx.Txn) error {
		dbi, err := txn.OpenDBI(nethermindConfig, 0, nil, nil)
		if err != nil {
			return err
		}
		return txn.Put(dbi, []byte("StateRoot"), root[:], 0)
	})
}

// WriteGenesis writes genesis metadata to the MDBX Config table.
func (w *NethermindWriter) WriteGenesis(genesisConfig *genesis.Genesis, stateRoot common.Hash, binaryTrie bool) error {
	if genesisConfig == nil || genesisConfig.Config == nil {
		return nil
	}
	return w.env.Update(func(txn *mdbx.Txn) error {
		dbi, err := txn.OpenDBI(nethermindConfig, 0, nil, nil)
		if err != nil {
			return err
		}

		chainIDBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(chainIDBytes, genesisConfig.Config.ChainID.Uint64())
		if err := txn.Put(dbi, []byte("ChainID"), chainIDBytes, 0); err != nil {
			return err
		}

		return txn.Put(dbi, []byte("StateRoot"), stateRoot[:], 0)
	})
}

// Flush commits all buffered writes to the database.
func (w *NethermindWriter) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.env.Update(func(txn *mdbx.Txn) error {
		stateDBI, err := txn.OpenDBI(nethermindState, 0, nil, nil)
		if err != nil {
			return fmt.Errorf("open State: %w", err)
		}

		codeDBI, err := txn.OpenDBI(nethermindCode, 0, nil, nil)
		if err != nil {
			return fmt.Errorf("open Code: %w", err)
		}

		// Write accounts: keccak256(address) -> RLP(nonce, balance, storageRoot, codeHash)
		for addr, data := range w.accounts {
			addrHash := crypto.Keccak256Hash(addr[:])
			encoded, err := rlp.EncodeToBytes(data.account)
			if err != nil {
				return fmt.Errorf("rlp encode account %s: %w", addr.Hex(), err)
			}
			if err := txn.Put(stateDBI, addrHash[:], encoded, 0); err != nil {
				return fmt.Errorf("write account %s: %w", addr.Hex(), err)
			}
		}

		// Write storage: keccak256(address) + keccak256(slot) -> trimmed value
		for key, value := range w.storage {
			addrHash := crypto.Keccak256Hash(key.addr[:])
			slotHash := crypto.Keccak256Hash(key.slot[:])

			storageKey := make([]byte, common.HashLength+common.HashLength)
			copy(storageKey[:common.HashLength], addrHash[:])
			copy(storageKey[common.HashLength:], slotHash[:])

			trimmedValue := nethermindTrimLeftZeroes(value[:])
			if len(trimmedValue) == 0 {
				continue
			}

			if err := txn.Put(stateDBI, storageKey, trimmedValue, 0); err != nil {
				return fmt.Errorf("write storage %s/%s: %w", key.addr.Hex(), key.slot.Hex(), err)
			}
		}

		// Write code: codeHash -> code
		for hash, code := range w.code {
			if err := txn.Put(codeDBI, hash[:], code, 0); err != nil {
				return fmt.Errorf("write code %s: %w", hash.Hex(), err)
			}
		}

		return nil
	})
}

// Close closes the writer.
func (w *NethermindWriter) Close() error {
	w.env.Close()
	return nil
}

// Stats returns write statistics.
func (w *NethermindWriter) Stats() WriterStats {
	return WriterStats{
		AccountBytes: w.accountBytes.Load(),
		StorageBytes: w.storageBytes.Load(),
		CodeBytes:    w.codeBytes.Load(),
	}
}

func nethermindTrimLeftZeroes(s []byte) []byte {
	for i, v := range s {
		if v != 0 {
			return s[i:]
		}
	}
	return nil
}
