package generator

import (
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/pebble"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/nerolation/state-actor/genesis"
)

// GethWriter writes state to a geth-compatible Pebble database.
// It uses the snapshot layer format with hashed keys.
type GethWriter struct {
	db        ethdb.KeyValueStore
	dbPath    string
	batchSize int
	workers   int

	bw *gethBatchWriter

	accountBytes atomic.Uint64
	storageBytes atomic.Uint64
	codeBytes    atomic.Uint64
}

// NewGethWriter creates a new geth-format state writer.
func NewGethWriter(dbPath string, batchSize, workers int) (*GethWriter, error) {
	db, err := pebble.New(dbPath, 512, 256, "stategen/", false)
	if err != nil {
		return nil, fmt.Errorf("failed to open pebble database: %w", err)
	}

	w := &GethWriter{
		db:        db,
		dbPath:    dbPath,
		batchSize: batchSize,
		workers:   workers,
	}

	w.bw = newGethBatchWriter(db, batchSize, workers)

	return w, nil
}

// DB returns the underlying database for external use (e.g., genesis writing).
func (w *GethWriter) DB() ethdb.KeyValueStore {
	return w.db
}

// WriteAccount writes an account to the snapshot layer.
func (w *GethWriter) WriteAccount(addr common.Address, acc *types.StateAccount, incarnation uint64) error {
	// Geth snapshot layer uses keccak256(address) as key
	addrHash := crypto.Keccak256Hash(addr[:])

	// SlimAccountRLP encodes the account for snapshot storage
	slimData := types.SlimAccountRLP(*acc)

	key := gethAccountSnapshotKey(addrHash)
	return w.bw.put(key, slimData, &w.accountBytes)
}

// WriteStorage writes a storage slot to the snapshot layer.
func (w *GethWriter) WriteStorage(addr common.Address, incarnation uint64, slot, value common.Hash) error {
	addrHash := crypto.Keccak256Hash(addr[:])
	slotHash := crypto.Keccak256Hash(slot[:])

	// RLP-encode the value with leading zeros trimmed
	valueRLP, err := gethEncodeStorageValue(value)
	if err != nil {
		return fmt.Errorf("encode storage value: %w", err)
	}

	key := gethStorageSnapshotKey(addrHash, slotHash)
	return w.bw.put(key, valueRLP, &w.storageBytes)
}

// WriteCode writes contract bytecode.
func (w *GethWriter) WriteCode(codeHash common.Hash, code []byte) error {
	key := gethCodeKey(codeHash)
	return w.bw.put(key, code, &w.codeBytes)
}

// SetStateRoot writes the snapshot root marker.
func (w *GethWriter) SetStateRoot(root common.Hash) error {
	return w.db.Put([]byte("SnapshotRoot"), root[:])
}

// WriteGenesisBlock writes the genesis block using the genesis package.
func (w *GethWriter) WriteGenesisBlock(config *params.ChainConfig, stateRoot common.Hash) error {
	// This requires the genesis config to be passed through
	// For now, return nil - the caller handles this via genesis.WriteGenesisBlock
	return nil
}

// Flush commits all pending writes.
func (w *GethWriter) Flush() error {
	return w.bw.finish()
}

// Close closes the writer.
func (w *GethWriter) Close() error {
	w.bw.close()
	return w.db.Close()
}

// Stats returns write statistics.
func (w *GethWriter) Stats() WriterStats {
	return WriterStats{
		AccountBytes: w.accountBytes.Load(),
		StorageBytes: w.storageBytes.Load(),
		CodeBytes:    w.codeBytes.Load(),
	}
}

// WriteGenesisBlockFull writes the genesis block with full genesis config.
func (w *GethWriter) WriteGenesisBlockFull(genesisConfig *genesis.Genesis, stateRoot common.Hash, binaryTrie bool) error {
	ancientDir := filepath.Join(w.dbPath, "ancient")
	_, err := genesis.WriteGenesisBlock(w.db, genesisConfig, stateRoot, binaryTrie, ancientDir)
	return err
}

// --- Batch writer for parallel writes ---

type gethBatchWriter struct {
	db        ethdb.KeyValueStore
	batchSize int
	batchChan chan *gethBatchWork
	errChan   chan error
	wg        sync.WaitGroup
	closeOnce sync.Once
	batch     ethdb.Batch
	count     int
}

type gethBatchWork struct {
	batch ethdb.Batch
}

func newGethBatchWriter(db ethdb.KeyValueStore, batchSize, workers int) *gethBatchWriter {
	bw := &gethBatchWriter{
		db:        db,
		batchSize: batchSize,
		batchChan: make(chan *gethBatchWork, workers*2),
		errChan:   make(chan error, 1),
		batch:     db.NewBatch(),
	}

	for i := 0; i < workers; i++ {
		bw.wg.Add(1)
		go func() {
			defer bw.wg.Done()
			for work := range bw.batchChan {
				if err := work.batch.Write(); err != nil {
					select {
					case bw.errChan <- err:
					default:
					}
					return
				}
			}
		}()
	}

	return bw
}

func (bw *gethBatchWriter) put(key, value []byte, counter *atomic.Uint64) error {
	if err := bw.batch.Put(key, value); err != nil {
		return err
	}
	counter.Add(uint64(len(key) + len(value)))
	bw.count++
	if bw.count >= bw.batchSize {
		return bw.flush()
	}
	return nil
}

func (bw *gethBatchWriter) flush() error {
	if bw.count == 0 {
		return nil
	}
	select {
	case bw.batchChan <- &gethBatchWork{batch: bw.batch}:
	case err := <-bw.errChan:
		return fmt.Errorf("batch worker failed: %w", err)
	}
	bw.batch = bw.db.NewBatch()
	bw.count = 0
	return nil
}

func (bw *gethBatchWriter) finish() error {
	if err := bw.flush(); err != nil {
		return err
	}
	bw.closeOnce.Do(func() { close(bw.batchChan) })
	bw.wg.Wait()

	select {
	case err := <-bw.errChan:
		return err
	default:
	}
	return nil
}

func (bw *gethBatchWriter) close() {
	bw.closeOnce.Do(func() { close(bw.batchChan) })
	bw.wg.Wait()
}

// --- Key encoding functions matching geth's rawdb schema ---

var (
	gethSnapshotAccountPrefix = []byte("a")
	gethSnapshotStoragePrefix = []byte("o")
	gethCodePrefix            = []byte("c")
)

func gethAccountSnapshotKey(hash common.Hash) []byte {
	return append(gethSnapshotAccountPrefix, hash.Bytes()...)
}

func gethStorageSnapshotKey(accountHash, storageHash common.Hash) []byte {
	buf := make([]byte, len(gethSnapshotStoragePrefix)+common.HashLength+common.HashLength)
	n := copy(buf, gethSnapshotStoragePrefix)
	n += copy(buf[n:], accountHash.Bytes())
	copy(buf[n:], storageHash.Bytes())
	return buf
}

func gethCodeKey(hash common.Hash) []byte {
	return append(gethCodePrefix, hash.Bytes()...)
}

func gethEncodeStorageValue(value common.Hash) ([]byte, error) {
	trimmed := gethTrimLeftZeroes(value[:])
	if len(trimmed) == 0 {
		return nil, nil
	}
	encoded, err := rlp.EncodeToBytes(trimmed)
	if err != nil {
		return nil, fmt.Errorf("failed to RLP-encode storage value %x: %w", value, err)
	}
	return encoded, nil
}

func gethTrimLeftZeroes(s []byte) []byte {
	for i, v := range s {
		if v != 0 {
			return s[i:]
		}
	}
	return nil
}
