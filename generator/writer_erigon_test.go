package generator

import (
	"os"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/holiman/uint256"
)

func TestErigonWriterBasic(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "erigon-writer-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create writer
	w, err := NewErigonWriter(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create Erigon writer: %v", err)
	}
	defer w.Close()

	// Write an account
	addr := common.HexToAddress("0x1234567890abcdef1234567890abcdef12345678")
	acc := &types.StateAccount{
		Nonce:    42,
		Balance:  uint256.NewInt(1000000000000000000), // 1 ETH
		Root:     types.EmptyRootHash,
		CodeHash: types.EmptyCodeHash.Bytes(),
	}

	if err := w.WriteAccount(addr, acc, 0); err != nil {
		t.Fatalf("Failed to write account: %v", err)
	}

	// Write storage
	slot := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000001")
	value := common.HexToHash("0x000000000000000000000000000000000000000000000000000000000000002a")

	if err := w.WriteStorage(addr, 0, slot, value); err != nil {
		t.Fatalf("Failed to write storage: %v", err)
	}

	// Write code
	codeHash := common.HexToHash("0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470")
	code := []byte{0x60, 0x80, 0x60, 0x40, 0x52}

	if err := w.WriteCode(codeHash, code); err != nil {
		t.Fatalf("Failed to write code: %v", err)
	}

	// Flush
	if err := w.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Set state root
	root := common.HexToHash("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890")
	if err := w.SetStateRoot(root); err != nil {
		t.Fatalf("Failed to set state root: %v", err)
	}

	// Check stats
	stats := w.Stats()
	if stats.AccountBytes == 0 {
		t.Error("Expected non-zero account bytes")
	}
	if stats.StorageBytes == 0 {
		t.Error("Expected non-zero storage bytes")
	}
	if stats.CodeBytes == 0 {
		t.Error("Expected non-zero code bytes")
	}

	t.Logf("Erigon writer stats: accounts=%d, storage=%d, code=%d",
		stats.AccountBytes, stats.StorageBytes, stats.CodeBytes)
}

func TestErigonAccountEncoding(t *testing.T) {
	tests := []struct {
		name        string
		acc         *types.StateAccount
		incarnation uint64
		wantMinLen  int
	}{
		{
			name: "empty_account",
			acc: &types.StateAccount{
				Nonce:    0,
				Balance:  uint256.NewInt(0),
				Root:     types.EmptyRootHash,
				CodeHash: types.EmptyCodeHash.Bytes(),
			},
			incarnation: 0,
			wantMinLen:  1, // just the fieldset byte
		},
		{
			name: "account_with_nonce",
			acc: &types.StateAccount{
				Nonce:    42,
				Balance:  uint256.NewInt(0),
				Root:     types.EmptyRootHash,
				CodeHash: types.EmptyCodeHash.Bytes(),
			},
			incarnation: 0,
			wantMinLen:  3, // fieldset + length + nonce
		},
		{
			name: "account_with_balance",
			acc: &types.StateAccount{
				Nonce:    0,
				Balance:  uint256.NewInt(1000000000000000000),
				Root:     types.EmptyRootHash,
				CodeHash: types.EmptyCodeHash.Bytes(),
			},
			incarnation: 0,
			wantMinLen:  10, // fieldset + length + balance bytes
		},
		{
			name: "account_with_incarnation",
			acc: &types.StateAccount{
				Nonce:    0,
				Balance:  uint256.NewInt(0),
				Root:     types.EmptyRootHash,
				CodeHash: types.EmptyCodeHash.Bytes(),
			},
			incarnation: 5,
			wantMinLen:  3, // fieldset + length + incarnation
		},
		{
			name: "account_with_code",
			acc: &types.StateAccount{
				Nonce:    0,
				Balance:  uint256.NewInt(0),
				Root:     types.EmptyRootHash,
				CodeHash: common.HexToHash("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef").Bytes(),
			},
			incarnation: 0,
			wantMinLen:  34, // fieldset + length + code hash
		},
		{
			name: "full_account",
			acc: &types.StateAccount{
				Nonce:    1000,
				Balance:  uint256.NewInt(5000000000000000000),
				Root:     types.EmptyRootHash,
				CodeHash: common.HexToHash("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890").Bytes(),
			},
			incarnation: 3,
			wantMinLen:  45, // all fields
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := erigonEncodeAccount(tt.acc, tt.incarnation)
			if len(encoded) < tt.wantMinLen {
				t.Errorf("encoded length %d < expected minimum %d", len(encoded), tt.wantMinLen)
			}

			// Check fieldset byte is consistent
			fieldSet := encoded[0]
			hasNonce := tt.acc.Nonce > 0
			hasBalance := !tt.acc.Balance.IsZero()
			hasIncarnation := tt.incarnation > 0
			emptyCodeHash := common.HexToHash("c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470")
			codeHash := common.BytesToHash(tt.acc.CodeHash)
			hasCode := codeHash != emptyCodeHash && codeHash != (common.Hash{})

			if hasNonce && (fieldSet&1) == 0 {
				t.Error("expected nonce bit set")
			}
			if hasBalance && (fieldSet&2) == 0 {
				t.Error("expected balance bit set")
			}
			if hasIncarnation && (fieldSet&4) == 0 {
				t.Error("expected incarnation bit set")
			}
			if hasCode && (fieldSet&8) == 0 {
				t.Error("expected code bit set")
			}

			t.Logf("Encoded %s: %d bytes, fieldset=0x%02x", tt.name, len(encoded), fieldSet)
		})
	}
}

func TestGethWriterBasic(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "geth-writer-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create writer
	w, err := NewGethWriter(tmpDir, 1000, 4)
	if err != nil {
		t.Fatalf("Failed to create Geth writer: %v", err)
	}
	defer w.Close()

	// Write an account
	addr := common.HexToAddress("0x1234567890abcdef1234567890abcdef12345678")
	acc := &types.StateAccount{
		Nonce:    42,
		Balance:  uint256.NewInt(1000000000000000000), // 1 ETH
		Root:     types.EmptyRootHash,
		CodeHash: types.EmptyCodeHash.Bytes(),
	}

	if err := w.WriteAccount(addr, acc, 0); err != nil {
		t.Fatalf("Failed to write account: %v", err)
	}

	// Write storage
	slot := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000001")
	value := common.HexToHash("0x000000000000000000000000000000000000000000000000000000000000002a")

	if err := w.WriteStorage(addr, 0, slot, value); err != nil {
		t.Fatalf("Failed to write storage: %v", err)
	}

	// Flush
	if err := w.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Check stats
	stats := w.Stats()
	if stats.AccountBytes == 0 {
		t.Error("Expected non-zero account bytes")
	}
	if stats.StorageBytes == 0 {
		t.Error("Expected non-zero storage bytes")
	}

	t.Logf("Geth writer stats: accounts=%d, storage=%d, code=%d",
		stats.AccountBytes, stats.StorageBytes, stats.CodeBytes)
}
