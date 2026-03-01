package generator

import (
	"bytes"
	"math/big"
	mrand "math/rand"
	"path/filepath"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

func TestGetSetNibble(t *testing.T) {
	var h common.Hash
	h[0] = 0xAB
	h[1] = 0xCD

	tests := []struct {
		pos  int
		want byte
	}{
		{0, 0xA},
		{1, 0xB},
		{2, 0xC},
		{3, 0xD},
	}
	for _, tt := range tests {
		got := getNibble(h, tt.pos)
		if got != tt.want {
			t.Errorf("getNibble(pos=%d) = %x, want %x", tt.pos, got, tt.want)
		}
	}

	// Test setNibble
	setNibble(&h, 0, 0xF)
	if h[0] != 0xFB {
		t.Errorf("after setNibble(0, F): byte 0 = %x, want FB", h[0])
	}
	setNibble(&h, 3, 0x0)
	if h[1] != 0xC0 {
		t.Errorf("after setNibble(3, 0): byte 1 = %x, want C0", h[1])
	}
}

func TestConstructPhantomKeys(t *testing.T) {
	target := crypto.Keccak256Hash(common.BigToHash(big.NewInt(5)).Bytes())
	depth := 64
	phantoms := constructPhantomKeys(target, depth)

	if len(phantoms) != depth {
		t.Fatalf("expected %d phantoms, got %d", depth, len(phantoms))
	}

	for d := 0; d < depth; d++ {
		phantom := phantoms[d]

		// Verify shared prefix: nibbles [0..d-1] must match target
		for n := 0; n < d; n++ {
			tNib := getNibble(target, n)
			pNib := getNibble(phantom, n)
			if tNib != pNib {
				t.Errorf("phantom[%d]: nibble %d = %x, want %x (should match target prefix)",
					d, n, pNib, tNib)
			}
		}

		// Verify divergence: nibble d must differ from target
		tNib := getNibble(target, d)
		pNib := getNibble(phantom, d)
		if tNib == pNib {
			t.Errorf("phantom[%d]: nibble %d = %x, same as target (should differ)", d, d, pNib)
		}

		// Verify the specific nibble value: (target[d] + 1) % 16
		expectedNib := (tNib + 1) % 16
		if pNib != expectedNib {
			t.Errorf("phantom[%d]: nibble %d = %x, want %x", d, d, pNib, expectedNib)
		}
	}
}

func TestConstructPhantomKeysVariableDepth(t *testing.T) {
	target := crypto.Keccak256Hash(common.BigToHash(big.NewInt(0)).Bytes())

	tests := []int{1, 4, 8, 16, 32, 64}
	for _, depth := range tests {
		phantoms := constructPhantomKeys(target, depth)
		if len(phantoms) != depth {
			t.Errorf("depth=%d: got %d phantoms, want %d", depth, len(phantoms), depth)
		}
	}
}

func TestConstructPhantomKeysClampedAt64(t *testing.T) {
	var target common.Hash
	phantoms := constructPhantomKeys(target, 100)
	if len(phantoms) != 64 {
		t.Errorf("depth=100 should be clamped to 64, got %d", len(phantoms))
	}
}

func TestGenerateDeepBranchStorage(t *testing.T) {
	rng := mrand.New(mrand.NewSource(42))
	knownSlots := 3
	depth := 10
	entries := generateDeepBranchStorage(knownSlots, depth, rng)

	expectedCount := knownSlots * (1 + depth) // 3 * 11 = 33
	if len(entries) != expectedCount {
		t.Fatalf("expected %d entries, got %d", expectedCount, len(entries))
	}

	// Verify entries are sorted by trieKey
	for i := 1; i < len(entries); i++ {
		if bytes.Compare(entries[i-1].trieKey[:], entries[i].trieKey[:]) >= 0 {
			t.Errorf("entries not sorted: entry[%d] >= entry[%d]", i-1, i)
		}
	}

	// Count legitimate vs phantom entries
	legit, phantom := 0, 0
	for _, e := range entries {
		if e.isPhantom {
			phantom++
		} else {
			legit++
		}
	}
	if legit != knownSlots {
		t.Errorf("expected %d legitimate entries, got %d", knownSlots, legit)
	}
	if phantom != knownSlots*depth {
		t.Errorf("expected %d phantom entries, got %d", knownSlots*depth, phantom)
	}

	// Verify legitimate entries have correct rawSlotKey → trieKey mapping
	for _, e := range entries {
		if !e.isPhantom {
			expectedTrieKey := crypto.Keccak256Hash(e.rawSlotKey[:])
			if e.trieKey != expectedTrieKey {
				t.Errorf("legitimate entry: trieKey mismatch for rawSlotKey %s",
					e.rawSlotKey.Hex())
			}
		}
	}

	// Verify all values are non-zero
	for i, e := range entries {
		if e.value == (common.Hash{}) {
			t.Errorf("entry[%d] has zero value", i)
		}
	}
}

func TestGenerateDeepBranchStorageMaxDepth(t *testing.T) {
	rng := mrand.New(mrand.NewSource(99))
	entries := generateDeepBranchStorage(1, 64, rng)

	// 1 legitimate + 64 phantoms = 65 entries
	if len(entries) != 65 {
		t.Fatalf("expected 65 entries, got %d", len(entries))
	}

	// Find the legitimate entry
	var legitTrieKey common.Hash
	for _, e := range entries {
		if !e.isPhantom {
			legitTrieKey = e.trieKey
			break
		}
	}

	// Verify there's a phantom diverging at each of the 64 nibble positions
	covered := make(map[int]bool, 64)
	for _, e := range entries {
		if !e.isPhantom {
			continue
		}
		// Find the first nibble where this phantom differs from the legitimate key
		for d := 0; d < 64; d++ {
			if getNibble(e.trieKey, d) != getNibble(legitTrieKey, d) {
				// All nibbles before d must match
				for n := 0; n < d; n++ {
					if getNibble(e.trieKey, n) != getNibble(legitTrieKey, n) {
						t.Errorf("phantom diverges at nibble %d but doesn't match prefix at %d", d, n)
					}
				}
				covered[d] = true
				break
			}
		}
	}

	if len(covered) != 64 {
		t.Errorf("expected 64 depth levels covered, got %d", len(covered))
	}
}

func TestDeepBranchReproducibility(t *testing.T) {
	rng1 := mrand.New(mrand.NewSource(123))
	entries1 := generateDeepBranchStorage(2, 32, rng1)

	rng2 := mrand.New(mrand.NewSource(123))
	entries2 := generateDeepBranchStorage(2, 32, rng2)

	if len(entries1) != len(entries2) {
		t.Fatalf("entry counts differ: %d vs %d", len(entries1), len(entries2))
	}

	for i := range entries1 {
		if entries1[i].trieKey != entries2[i].trieKey {
			t.Errorf("entry[%d] trieKey differs", i)
		}
		if entries1[i].value != entries2[i].value {
			t.Errorf("entry[%d] value differs", i)
		}
		if entries1[i].isPhantom != entries2[i].isPhantom {
			t.Errorf("entry[%d] isPhantom differs", i)
		}
	}
}

func TestDeepBranchIntegration(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "testdb")

	config := Config{
		DBPath:       dbPath,
		NumAccounts:  5,
		NumContracts: 3,
		MaxSlots:     50,
		MinSlots:     5,
		Distribution: Uniform,
		Seed:         42,
		BatchSize:    100,
		Workers:      2,
		CodeSize:     128,
		DeepBranch: DeepBranchConfig{
			NumAccounts: 2,
			Depth:       16,
			KnownSlots:  3,
		},
	}

	gen, err := New(config)
	if err != nil {
		t.Fatalf("Failed to create generator: %v", err)
	}
	defer gen.Close()

	stats, err := gen.Generate()
	if err != nil {
		t.Fatalf("Failed to generate state: %v", err)
	}

	// Verify deep-branch stats
	if stats.DeepBranchAccounts != 2 {
		t.Errorf("expected 2 deep-branch accounts, got %d", stats.DeepBranchAccounts)
	}
	if stats.DeepBranchDepth != 16 {
		t.Errorf("expected depth 16, got %d", stats.DeepBranchDepth)
	}

	// Verify total contract count includes deep-branch accounts
	if stats.ContractsCreated != 5 { // 3 random + 2 deep-branch
		t.Errorf("expected 5 total contracts, got %d", stats.ContractsCreated)
	}

	// Verify state root is non-empty
	if stats.StateRoot == (common.Hash{}) {
		t.Error("state root should not be empty")
	}

	// Verify storage slots include deep-branch entries
	deepSlots := 2 * 3 * (1 + 16) // 2 accounts * 3 known_slots * (1 legit + 16 phantom)
	if stats.StorageSlotsCreated < deepSlots {
		t.Errorf("expected at least %d storage slots from deep-branch, got %d total",
			deepSlots, stats.StorageSlotsCreated)
	}
}

func TestDeepBranchWithMaxDepthIntegration(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "testdb")

	config := Config{
		DBPath:       dbPath,
		NumAccounts:  2,
		NumContracts: 0,
		MaxSlots:     10,
		MinSlots:     1,
		Distribution: Uniform,
		Seed:         99,
		BatchSize:    100,
		Workers:      1,
		CodeSize:     64,
		DeepBranch: DeepBranchConfig{
			NumAccounts: 1,
			Depth:       64,
			KnownSlots:  1,
		},
	}

	gen, err := New(config)
	if err != nil {
		t.Fatalf("Failed to create generator: %v", err)
	}
	defer gen.Close()

	stats, err := gen.Generate()
	if err != nil {
		t.Fatalf("Failed to generate state: %v", err)
	}

	if stats.StateRoot == (common.Hash{}) {
		t.Error("state root should not be empty")
	}
	if stats.DeepBranchAccounts != 1 {
		t.Errorf("expected 1 deep-branch account, got %d", stats.DeepBranchAccounts)
	}
}

func TestDeepBranchStateRootReproducibility(t *testing.T) {
	run := func() common.Hash {
		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "testdb")

		config := Config{
			DBPath:       dbPath,
			NumAccounts:  5,
			NumContracts: 3,
			MaxSlots:     50,
			MinSlots:     5,
			Distribution: Uniform,
			Seed:         777,
			BatchSize:    100,
			Workers:      2,
			CodeSize:     128,
			DeepBranch: DeepBranchConfig{
				NumAccounts: 1,
				Depth:       32,
				KnownSlots:  2,
			},
		}

		gen, err := New(config)
		if err != nil {
			t.Fatalf("Failed to create generator: %v", err)
		}
		defer gen.Close()

		stats, err := gen.Generate()
		if err != nil {
			t.Fatalf("Failed to generate state: %v", err)
		}
		return stats.StateRoot
	}

	root1 := run()
	root2 := run()

	if root1 != root2 {
		t.Errorf("state roots differ across runs with same seed:\n  run1: %s\n  run2: %s",
			root1.Hex(), root2.Hex())
	}
}
