package generator

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/params"
)

// Stream protocol constants
var rethStreamMagic = [4]byte{'S', 'A', 'R', 'B'}

const (
	rethStreamVersion = 1

	// Entry type tags
	rethTagAccount byte = 0x01
	rethTagCode    byte = 0x02
	rethTagRoot    byte = 0x03
	rethTagEnd     byte = 0xFF
)

// RethStreamWriter implements StateWriter by spawning the reth-import binary
// as a subprocess and streaming state entries to its stdin via a pipe.
type RethStreamWriter struct {
	cmd       *exec.Cmd
	stdin     *bufio.Writer
	stdinPipe interface{ Close() error }
	stderr    bytes.Buffer

	// Per-account buffer (cleared on WriteAccount)
	pendingAddr    common.Address
	pendingStorage []rethStorageSlot
	pendingCode    *rethCodeEntry
	writtenCodes   map[common.Hash]bool // dedup

	// Stats
	accountBytes atomic.Uint64
	storageBytes atomic.Uint64
	codeBytes    atomic.Uint64
}

type rethStorageSlot struct {
	Key   common.Hash
	Value common.Hash
}

type rethCodeEntry struct {
	Hash common.Hash
	Code []byte
}

// NewRethStreamWriter creates a new writer that spawns reth-import and streams to it.
func NewRethStreamWriter(datadir string, genesisJSON []byte, rethImportBin string) (*RethStreamWriter, error) {
	binPath, err := findRethImportBin(rethImportBin)
	if err != nil {
		return nil, err
	}

	cmd := exec.Command(binPath, "--datadir", datadir, "--stdin")
	stdinPipe, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to create stdin pipe: %w", err)
	}

	w := &RethStreamWriter{
		cmd:          cmd,
		stdin:        bufio.NewWriterSize(stdinPipe, 4*1024*1024), // 4 MB buffer
		stdinPipe:    stdinPipe,
		writtenCodes: make(map[common.Hash]bool),
	}

	// Tee stderr to both the buffer (for error reporting) and os.Stderr (for progress)
	cmd.Stderr = io.MultiWriter(&w.stderr, os.Stderr)
	cmd.Stdout = os.Stdout

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start reth-import: %w", err)
	}

	// Write stream header
	if err := w.writeHeader(genesisJSON); err != nil {
		// Kill the process on header write failure
		cmd.Process.Kill()
		cmd.Wait()
		return nil, fmt.Errorf("failed to write stream header: %w", err)
	}

	return w, nil
}

func (w *RethStreamWriter) writeHeader(genesisJSON []byte) error {
	// Magic
	if _, err := w.stdin.Write(rethStreamMagic[:]); err != nil {
		return err
	}

	// Version (LE u32)
	if err := binary.Write(w.stdin, binary.LittleEndian, uint32(rethStreamVersion)); err != nil {
		return err
	}

	// Genesis JSON length (LE u32) + bytes
	if err := binary.Write(w.stdin, binary.LittleEndian, uint32(len(genesisJSON))); err != nil {
		return err
	}
	if _, err := w.stdin.Write(genesisJSON); err != nil {
		return err
	}

	return nil
}

// WriteStorage buffers a storage slot for the current account.
func (w *RethStreamWriter) WriteStorage(addr common.Address, incarnation uint64, slot, value common.Hash) error {
	w.pendingAddr = addr
	w.pendingStorage = append(w.pendingStorage, rethStorageSlot{Key: slot, Value: value})
	return nil
}

// WriteCode buffers a code entry if not yet written.
func (w *RethStreamWriter) WriteCode(codeHash common.Hash, code []byte) error {
	if w.writtenCodes[codeHash] {
		return nil
	}
	w.pendingCode = &rethCodeEntry{Hash: codeHash, Code: code}
	return nil
}

// WriteAccount writes the buffered code entry (0x02) then the account entry (0x01)
// with inline storage, then clears buffers.
func (w *RethStreamWriter) WriteAccount(addr common.Address, acc *types.StateAccount, incarnation uint64) error {
	// Write pending code first
	if w.pendingCode != nil {
		if err := w.writeCodeEntry(w.pendingCode.Hash, w.pendingCode.Code); err != nil {
			return err
		}
		w.writtenCodes[w.pendingCode.Hash] = true
		w.pendingCode = nil
	}

	// Write account entry with inline storage
	if err := w.writeAccountEntry(addr, acc); err != nil {
		return err
	}

	// Clear storage buffer
	w.pendingStorage = w.pendingStorage[:0]

	return nil
}

// writeCodeEntry writes a 0x02 Code entry to the stream.
// Format: 0x02 | code_hash(32B) | code_len(4B LE) | code
func (w *RethStreamWriter) writeCodeEntry(codeHash common.Hash, code []byte) error {
	if err := w.stdin.WriteByte(rethTagCode); err != nil {
		return err
	}
	if _, err := w.stdin.Write(codeHash[:]); err != nil {
		return err
	}
	if err := binary.Write(w.stdin, binary.LittleEndian, uint32(len(code))); err != nil {
		return err
	}
	if _, err := w.stdin.Write(code); err != nil {
		return err
	}
	w.codeBytes.Add(uint64(1 + 32 + 4 + len(code)))
	return nil
}

// writeAccountEntry writes a 0x01 Account entry to the stream.
// Format: 0x01 | addr(20B) | nonce(8B LE) | balance(32B BE) | code_hash(32B)
//
//	| storage_count(4B LE) | [slot_key(32B) + slot_value(32B)]...
func (w *RethStreamWriter) writeAccountEntry(addr common.Address, acc *types.StateAccount) error {
	if err := w.stdin.WriteByte(rethTagAccount); err != nil {
		return err
	}

	// Address
	if _, err := w.stdin.Write(addr[:]); err != nil {
		return err
	}

	// Nonce (LE u64)
	if err := binary.Write(w.stdin, binary.LittleEndian, acc.Nonce); err != nil {
		return err
	}

	// Balance (BE 32 bytes)
	var balBytes [32]byte
	acc.Balance.WriteToArray32(&balBytes)
	if _, err := w.stdin.Write(balBytes[:]); err != nil {
		return err
	}

	// CodeHash (32 bytes)
	if _, err := w.stdin.Write(acc.CodeHash); err != nil {
		return err
	}

	// Storage count (LE u32)
	if err := binary.Write(w.stdin, binary.LittleEndian, uint32(len(w.pendingStorage))); err != nil {
		return err
	}

	// Storage slots
	for _, slot := range w.pendingStorage {
		if _, err := w.stdin.Write(slot.Key[:]); err != nil {
			return err
		}
		if _, err := w.stdin.Write(slot.Value[:]); err != nil {
			return err
		}
	}

	accountSize := uint64(1 + 20 + 8 + 32 + 32 + 4)
	storageSize := uint64(len(w.pendingStorage)) * 64
	w.accountBytes.Add(accountSize)
	w.storageBytes.Add(storageSize)

	return nil
}

// SetStateRoot writes a 0x03 Root entry to the stream.
func (w *RethStreamWriter) SetStateRoot(root common.Hash) error {
	if err := w.stdin.WriteByte(rethTagRoot); err != nil {
		return err
	}
	_, err := w.stdin.Write(root[:])
	return err
}

// WriteGenesisBlock is a no-op for Reth — the Rust side handles genesis.
func (w *RethStreamWriter) WriteGenesisBlock(config *params.ChainConfig, stateRoot common.Hash) error {
	return nil
}

// Flush commits pending buffered writes. For RethStreamWriter this just
// flushes the bufio writer to ensure data reaches the subprocess promptly.
// The end marker and pipe close happen in Close().
func (w *RethStreamWriter) Flush() error {
	return w.stdin.Flush()
}

// Close writes the end marker, closes the stdin pipe, and waits for the
// subprocess to exit. This must be called after SetStateRoot.
func (w *RethStreamWriter) Close() error {
	if w.cmd.Process == nil {
		return nil
	}

	// Write end marker and close pipe
	if err := w.stdin.WriteByte(rethTagEnd); err != nil {
		w.cmd.Process.Kill()
		w.cmd.Wait()
		return fmt.Errorf("failed to write end marker: %w", err)
	}
	if err := w.stdin.Flush(); err != nil {
		w.cmd.Process.Kill()
		w.cmd.Wait()
		return fmt.Errorf("failed to flush: %w", err)
	}
	if err := w.stdinPipe.Close(); err != nil {
		w.cmd.Process.Kill()
		w.cmd.Wait()
		return fmt.Errorf("failed to close pipe: %w", err)
	}

	err := w.cmd.Wait()
	if err != nil {
		stderrStr := w.stderr.String()
		if stderrStr != "" {
			return fmt.Errorf("reth-import failed: %w\nstderr: %s", err, stderrStr)
		}
		return fmt.Errorf("reth-import failed: %w", err)
	}
	return nil
}

// Stats returns write statistics.
func (w *RethStreamWriter) Stats() WriterStats {
	return WriterStats{
		AccountBytes: w.accountBytes.Load(),
		StorageBytes: w.storageBytes.Load(),
		CodeBytes:    w.codeBytes.Load(),
	}
}

// findRethImportBin resolves the reth-import binary path.
func findRethImportBin(explicit string) (string, error) {
	if explicit != "" {
		if _, err := os.Stat(explicit); err != nil {
			return "", fmt.Errorf("reth-import binary not found at %q: %w", explicit, err)
		}
		return explicit, nil
	}

	// Look in reth-import/target/release/ first (most common dev setup)
	candidate := filepath.Join("reth-import", "target", "release", "reth-import")
	if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
		return candidate, nil
	}

	// Look next to the current executable
	selfPath, err := os.Executable()
	if err == nil {
		candidate := filepath.Join(filepath.Dir(selfPath), "reth-import")
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate, nil
		}
	}

	// Look in current working directory (must be a file, not the directory)
	if info, err := os.Stat("reth-import"); err == nil && !info.IsDir() {
		return "reth-import", nil
	}

	// Fall back to PATH
	path, err := exec.LookPath("reth-import")
	if err != nil {
		return "", fmt.Errorf("reth-import binary not found: tried next to executable, CWD, reth-import/target/release/, and PATH")
	}
	return path, nil
}
