package generator

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/holiman/uint256"
)

// testRethStreamWriter is a RethStreamWriter that writes to a buffer
// instead of spawning a subprocess, for unit testing the stream protocol.
func newTestRethStreamWriter(buf *bytes.Buffer, genesisJSON []byte) *RethStreamWriter {
	w := &RethStreamWriter{
		stdin:        bufio.NewWriter(buf),
		stdinPipe:    io.NopCloser(nil),
		writtenCodes: make(map[common.Hash]bool),
	}
	// Write header manually
	w.stdin.Write(rethStreamMagic[:])
	binary.Write(w.stdin, binary.LittleEndian, uint32(rethStreamVersion))
	binary.Write(w.stdin, binary.LittleEndian, uint32(len(genesisJSON)))
	w.stdin.Write(genesisJSON)
	return w
}

func TestRethStreamHeader(t *testing.T) {
	var buf bytes.Buffer
	genesis := []byte(`{"config":{"chainId":1}}`)
	w := newTestRethStreamWriter(&buf, genesis)
	w.stdin.Flush()

	data := buf.Bytes()

	// Check magic
	if !bytes.Equal(data[:4], rethStreamMagic[:]) {
		t.Fatalf("bad magic: got %x, want %x", data[:4], rethStreamMagic[:])
	}

	// Check version
	version := binary.LittleEndian.Uint32(data[4:8])
	if version != rethStreamVersion {
		t.Fatalf("bad version: got %d, want %d", version, rethStreamVersion)
	}

	// Check genesis JSON length
	genesisLen := binary.LittleEndian.Uint32(data[8:12])
	if genesisLen != uint32(len(genesis)) {
		t.Fatalf("bad genesis len: got %d, want %d", genesisLen, len(genesis))
	}

	// Check genesis JSON bytes
	if !bytes.Equal(data[12:12+genesisLen], genesis) {
		t.Fatalf("bad genesis bytes")
	}
}

func TestRethStreamAccountEntry(t *testing.T) {
	var buf bytes.Buffer
	w := newTestRethStreamWriter(&buf, nil)

	addr := common.HexToAddress("0x1234567890abcdef1234567890abcdef12345678")
	acc := &types.StateAccount{
		Nonce:    42,
		Balance:  uint256.NewInt(1000000000000000000), // 1 ETH
		Root:     types.EmptyRootHash,
		CodeHash: types.EmptyCodeHash.Bytes(),
	}

	// Write storage first
	slot := common.HexToHash("0x01")
	value := common.HexToHash("0x2a")
	if err := w.WriteStorage(addr, 0, slot, value); err != nil {
		t.Fatal(err)
	}

	// Then write account (triggers flush)
	if err := w.WriteAccount(addr, acc, 0); err != nil {
		t.Fatal(err)
	}

	w.stdin.Flush()
	data := buf.Bytes()

	// Skip header (4 magic + 4 version + 4 genesis_len + 0 genesis = 12)
	data = data[12:]

	// Tag
	if data[0] != rethTagAccount {
		t.Fatalf("expected account tag 0x%02x, got 0x%02x", rethTagAccount, data[0])
	}
	off := 1

	// Address
	if !bytes.Equal(data[off:off+20], addr[:]) {
		t.Fatal("address mismatch")
	}
	off += 20

	// Nonce
	nonce := binary.LittleEndian.Uint64(data[off : off+8])
	if nonce != 42 {
		t.Fatalf("nonce: got %d, want 42", nonce)
	}
	off += 8

	// Balance (32B BE)
	var balBytes [32]byte
	copy(balBytes[:], data[off:off+32])
	bal := new(uint256.Int).SetBytes32(balBytes[:])
	expectedBal := uint256.NewInt(1000000000000000000)
	if bal.Cmp(expectedBal) != 0 {
		t.Fatalf("balance: got %s, want %s", bal, expectedBal)
	}
	off += 32

	// CodeHash
	if !bytes.Equal(data[off:off+32], types.EmptyCodeHash.Bytes()) {
		t.Fatal("code hash mismatch")
	}
	off += 32

	// Storage count
	storageCount := binary.LittleEndian.Uint32(data[off : off+4])
	if storageCount != 1 {
		t.Fatalf("storage count: got %d, want 1", storageCount)
	}
	off += 4

	// Storage key
	if !bytes.Equal(data[off:off+32], slot[:]) {
		t.Fatal("storage key mismatch")
	}
	off += 32

	// Storage value
	if !bytes.Equal(data[off:off+32], value[:]) {
		t.Fatal("storage value mismatch")
	}
}

func TestRethStreamCodeEntry(t *testing.T) {
	var buf bytes.Buffer
	w := newTestRethStreamWriter(&buf, nil)

	codeHash := common.HexToHash("0xdeadbeef")
	code := []byte{0x60, 0x80, 0x60, 0x40, 0x52}

	// Write code then account to trigger flush
	if err := w.WriteCode(codeHash, code); err != nil {
		t.Fatal(err)
	}

	acc := &types.StateAccount{
		Nonce:    1,
		Balance:  uint256.NewInt(0),
		Root:     types.EmptyRootHash,
		CodeHash: codeHash.Bytes(),
	}
	addr := common.HexToAddress("0xaaaa")
	if err := w.WriteAccount(addr, acc, 0); err != nil {
		t.Fatal(err)
	}

	w.stdin.Flush()
	data := buf.Bytes()

	// Skip header
	data = data[12:]

	// First entry should be code (0x02)
	if data[0] != rethTagCode {
		t.Fatalf("expected code tag 0x%02x, got 0x%02x", rethTagCode, data[0])
	}
	off := 1

	// Code hash
	if !bytes.Equal(data[off:off+32], codeHash[:]) {
		t.Fatal("code hash mismatch")
	}
	off += 32

	// Code length
	codeLen := binary.LittleEndian.Uint32(data[off : off+4])
	if codeLen != uint32(len(code)) {
		t.Fatalf("code len: got %d, want %d", codeLen, len(code))
	}
	off += 4

	// Code bytes
	if !bytes.Equal(data[off:off+int(codeLen)], code) {
		t.Fatal("code bytes mismatch")
	}
}

func TestRethStreamCodeDedup(t *testing.T) {
	var buf bytes.Buffer
	w := newTestRethStreamWriter(&buf, nil)

	codeHash := common.HexToHash("0xdeadbeef")
	code := []byte{0x60, 0x80}

	// Write same code twice
	w.WriteCode(codeHash, code)
	w.WriteAccount(common.HexToAddress("0xaa"), &types.StateAccount{
		Balance: uint256.NewInt(0), CodeHash: codeHash.Bytes(),
	}, 0)

	w.WriteCode(codeHash, code) // should be deduped
	w.WriteAccount(common.HexToAddress("0xbb"), &types.StateAccount{
		Balance: uint256.NewInt(0), CodeHash: codeHash.Bytes(),
	}, 0)

	w.stdin.Flush()
	data := buf.Bytes()[12:] // skip header

	// Parse entries and count code tags
	codeTagCount := 0
	off := 0
	for off < len(data) {
		tag := data[off]
		off++
		switch tag {
		case rethTagCode:
			codeTagCount++
			off += 32 // code hash
			codeLen := binary.LittleEndian.Uint32(data[off : off+4])
			off += 4 + int(codeLen)
		case rethTagAccount:
			off += 20 + 8 + 32 + 32 // addr + nonce + balance + code_hash
			storageCount := binary.LittleEndian.Uint32(data[off : off+4])
			off += 4 + int(storageCount)*64
		case rethTagRoot:
			off += 32
		case rethTagEnd:
			break
		default:
			t.Fatalf("unknown tag 0x%02x at offset %d", tag, off-1)
		}
	}
	if codeTagCount != 1 {
		t.Fatalf("expected 1 code entry (dedup), got %d", codeTagCount)
	}
}

func TestRethStreamRootAndEnd(t *testing.T) {
	var buf bytes.Buffer
	w := newTestRethStreamWriter(&buf, nil)

	root := common.HexToHash("0xabcdef1234567890")
	if err := w.SetStateRoot(root); err != nil {
		t.Fatal(err)
	}

	// Simulate flush (write end marker + flush buffer)
	w.stdin.WriteByte(rethTagEnd)
	w.stdin.Flush()

	data := buf.Bytes()[12:] // skip header

	// Root entry
	if data[0] != rethTagRoot {
		t.Fatalf("expected root tag, got 0x%02x", data[0])
	}
	if !bytes.Equal(data[1:33], root[:]) {
		t.Fatal("root hash mismatch")
	}

	// End marker
	if data[33] != rethTagEnd {
		t.Fatalf("expected end tag 0x%02x, got 0x%02x", rethTagEnd, data[33])
	}
}

func TestRethStreamStats(t *testing.T) {
	var buf bytes.Buffer
	w := newTestRethStreamWriter(&buf, nil)

	addr := common.HexToAddress("0x1234")
	acc := &types.StateAccount{
		Nonce:    1,
		Balance:  uint256.NewInt(100),
		Root:     types.EmptyRootHash,
		CodeHash: types.EmptyCodeHash.Bytes(),
	}

	w.WriteStorage(addr, 0, common.HexToHash("0x01"), common.HexToHash("0x02"))
	w.WriteStorage(addr, 0, common.HexToHash("0x03"), common.HexToHash("0x04"))
	w.WriteAccount(addr, acc, 0)

	stats := w.Stats()
	if stats.AccountBytes == 0 {
		t.Error("expected non-zero account bytes")
	}
	if stats.StorageBytes == 0 {
		t.Error("expected non-zero storage bytes")
	}
	// 2 slots × 64 bytes = 128
	if stats.StorageBytes != 128 {
		t.Errorf("storage bytes: got %d, want 128", stats.StorageBytes)
	}

	t.Logf("Stats: accounts=%d, storage=%d, code=%d",
		stats.AccountBytes, stats.StorageBytes, stats.CodeBytes)
}
