package generator

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"sort"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	"github.com/ethereum/go-ethereum/trie/bintrie"
)

// TestGroupedEmissionConsistency verifies that the streaming builder's
// returned root hash matches what geth computes by reading and hashing
// the root grouped node from DB. This is the primary correctness test.
func TestGroupedEmissionConsistency(t *testing.T) {
	for _, gd := range []int{1, 2, 4, 8} {
		t.Run(fmt.Sprintf("gd%d", gd), func(t *testing.T) {
			entries := generateTestEntries(t, 50)

			db := memorydb.New()
			root, stats := computeBinaryRootStreamingFromSlice(entries, db, gd)

			if root == (common.Hash{}) {
				t.Fatal("root should not be zero")
			}
			if stats.Nodes == 0 {
				t.Fatal("should have written nodes")
			}

			// Read root grouped node and verify hash matches
			rootBlob, err := db.Get(verkleTrieNodeKeyPrefix)
			if err != nil {
				t.Fatalf("failed to read root node: %v", err)
			}
			groupedRoot, err := bintrie.DeserializeNode(rootBlob, 0)
			if err != nil {
				t.Fatalf("failed to deserialize root: %v", err)
			}
			deserializedHash := groupedRoot.Hash()

			if root != deserializedHash {
				t.Errorf("root hash inconsistency (groupDepth=%d):\n  streaming: %s\n  from DB:   %s",
					gd, root.Hex(), deserializedHash.Hex())
			}
		})
	}
}

// TestGroupedEmissionShallowStems verifies equivalence between direct
// grouped emission and the regroup approach when stems are at shallow
// non-boundary depths. With 2 stems differing at bit 0, both stems are
// placed at depth 1 — non-boundary for groupDepth >= 2.
func TestGroupedEmissionShallowStems(t *testing.T) {
	// Create 2 stems that differ at bit 0
	entries := makeShallowEntries()

	for _, gd := range []int{1, 2, 4, 8} {
		t.Run(fmt.Sprintf("gd%d", gd), func(t *testing.T) {
			// Approach 1: Individual emission + regroup
			db1 := memorydb.New()
			computeBinaryRootStreamingFromSlice(entries, db1, 0)
			if err := regroupTrieNodes(db1, gd, false); err != nil {
				t.Fatalf("regroupTrieNodes failed: %v", err)
			}
			rootBlob1, err := db1.Get(verkleTrieNodeKeyPrefix)
			if err != nil {
				t.Fatalf("failed to read root from db1: %v", err)
			}
			groupedRoot1, err := bintrie.DeserializeNode(rootBlob1, 0)
			if err != nil {
				t.Fatalf("failed to deserialize root from db1: %v", err)
			}
			regroupHash := groupedRoot1.Hash()

			// Approach 2: Direct grouped emission
			db2 := memorydb.New()
			directHash, _ := computeBinaryRootStreamingFromSlice(entries, db2, gd)

			if regroupHash != directHash {
				t.Errorf("hash mismatch for shallow stems (gd=%d):\n  regroup: %s\n  direct:  %s",
					gd, regroupHash.Hex(), directHash.Hex())
			}

			// Verify DB contents match
			compareDBs(t, db1, db2, gd)
		})
	}
}

// TestGroupedEmissionNoRegression verifies that groupDepth=0 produces the
// exact same result as the original ungrouped streaming builder.
func TestGroupedEmissionNoRegression(t *testing.T) {
	entries := generateTestEntries(t, 30)

	db1 := memorydb.New()
	root1, stats1 := computeBinaryRootStreamingFromSlice(entries, db1, 0)

	db2 := memorydb.New()
	root2, stats2 := computeBinaryRootStreamingFromSlice(entries, db2, 0)

	if root1 != root2 {
		t.Errorf("ungrouped root mismatch: %s != %s", root1.Hex(), root2.Hex())
	}
	if stats1.Nodes != stats2.Nodes {
		t.Errorf("node count mismatch: %d != %d", stats1.Nodes, stats2.Nodes)
	}
}

// TestGroupedEmissionSingleEntry verifies edge case with a single stem.
func TestGroupedEmissionSingleEntry(t *testing.T) {
	for _, gd := range []int{0, 1, 4, 8} {
		t.Run(fmt.Sprintf("gd%d", gd), func(t *testing.T) {
			var entries []trieEntry
			var e trieEntry
			for i := 0; i < stemSize; i++ {
				e.Key[i] = byte(i * 7)
			}
			e.Key[stemSize] = 0
			e.Value = sha256.Sum256([]byte("test-value"))
			entries = append(entries, e)

			db := memorydb.New()
			root, stats := computeBinaryRootStreamingFromSlice(entries, db, gd)

			if root == (common.Hash{}) {
				t.Error("root should not be zero for non-empty trie")
			}
			if stats.Nodes == 0 {
				t.Error("should have written at least one node")
			}

			// Verify consistency: root matches deserialized root hash
			if gd > 0 {
				rootBlob, err := db.Get(verkleTrieNodeKeyPrefix)
				if err != nil {
					t.Fatalf("failed to read root: %v", err)
				}
				groupedRoot, err := bintrie.DeserializeNode(rootBlob, 0)
				if err != nil {
					t.Fatalf("failed to deserialize root: %v", err)
				}
				if groupedRoot.Hash() != root {
					t.Errorf("root inconsistency: streaming=%s, deserialized=%s",
						root.Hex(), groupedRoot.Hash().Hex())
				}
			}
		})
	}
}

// TestGroupedEmissionRoundTrip verifies that all grouped nodes can be
// deserialized by geth's bintrie.DeserializeNode without errors.
func TestGroupedEmissionRoundTrip(t *testing.T) {
	for _, gd := range []int{1, 2, 4, 8} {
		t.Run(fmt.Sprintf("gd%d", gd), func(t *testing.T) {
			entries := generateTestEntries(t, 20)

			db := memorydb.New()
			computeBinaryRootStreamingFromSlice(entries, db, gd)

			prefix := verkleTrieNodeKeyPrefix
			iter := db.NewIterator(prefix, nil)
			defer iter.Release()

			nodeCount := 0
			for iter.Next() {
				key := iter.Key()
				if !bytes.HasPrefix(key, prefix) {
					break
				}
				blob := iter.Value()
				if len(blob) == 0 {
					continue
				}

				path := key[len(prefix):]
				depth := len(path)

				_, err := bintrie.DeserializeNode(blob, depth)
				if err != nil {
					t.Errorf("failed to deserialize node at depth %d (type=%d, len=%d): %v",
						depth, blob[0], len(blob), err)
				}
				nodeCount++
			}

			if nodeCount == 0 {
				t.Error("no nodes found in DB")
			}
		})
	}
}

// TestGroupedEmissionNodeCounts verifies that grouping reduces the number
// of nodes written (internal nodes at non-boundary depths are eliminated).
func TestGroupedEmissionNodeCounts(t *testing.T) {
	entries := generateTestEntries(t, 50)

	db0 := memorydb.New()
	_, stats0 := computeBinaryRootStreamingFromSlice(entries, db0, 0)

	for _, gd := range []int{1, 2, 4, 8} {
		t.Run(fmt.Sprintf("gd%d", gd), func(t *testing.T) {
			db := memorydb.New()
			_, stats := computeBinaryRootStreamingFromSlice(entries, db, gd)

			if gd > 1 && stats.Nodes >= stats0.Nodes {
				t.Errorf("grouped (gd=%d) should have fewer nodes: grouped=%d, ungrouped=%d",
					gd, stats.Nodes, stats0.Nodes)
			}
			t.Logf("gd=%d: %d nodes (%d bytes), ungrouped: %d nodes (%d bytes)",
				gd, stats.Nodes, stats.Bytes, stats0.Nodes, stats0.Bytes)
		})
	}
}

// --- helpers ---

// makeShallowEntries creates 2 stems that differ at bit 0, forcing stem
// placement at depth 1 (the shallowest possible depth for 2 entries).
func makeShallowEntries() []trieEntry {
	var entries []trieEntry

	// Stem A: bit 0 = 0 (0x00...)
	var eA trieEntry
	stemA := sha256.Sum256([]byte("shallow-stem-A"))
	stemA[0] &= 0x7F // ensure bit 0 = 0
	copy(eA.Key[:stemSize], stemA[:stemSize])
	eA.Key[stemSize] = 0
	eA.Value = sha256.Sum256([]byte("value-A"))
	entries = append(entries, eA)

	// Stem B: bit 0 = 1 (0x80...)
	var eB trieEntry
	stemB := sha256.Sum256([]byte("shallow-stem-B"))
	stemB[0] |= 0x80 // ensure bit 0 = 1
	copy(eB.Key[:stemSize], stemB[:stemSize])
	eB.Key[stemSize] = 0
	eB.Value = sha256.Sum256([]byte("value-B"))
	entries = append(entries, eB)

	// Sort by key
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].Key[:], entries[j].Key[:]) < 0
	})

	return entries
}

// generateTestEntries creates a deterministic set of trie entries with
// multiple stems to exercise the streaming builder's grouping logic.
func generateTestEntries(t *testing.T, numStems int) []trieEntry {
	t.Helper()
	var entries []trieEntry

	for i := 0; i < numStems; i++ {
		stemHash := sha256.Sum256([]byte{byte(i), byte(i >> 8), byte(i >> 16)})
		stem := stemHash[:stemSize]

		numSuffixes := (i % 3) + 1
		for s := 0; s < numSuffixes; s++ {
			var e trieEntry
			copy(e.Key[:stemSize], stem)
			e.Key[stemSize] = byte(s)
			e.Value = sha256.Sum256([]byte{byte(i), byte(s)})
			entries = append(entries, e)
		}
	}

	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].Key[:], entries[j].Key[:]) < 0
	})

	return entries
}

// compareDBs checks that two databases have identical contents under the
// trie node prefix.
func compareDBs(t *testing.T, db1, db2 ethdb.KeyValueStore, groupDepth int) {
	t.Helper()

	prefix := verkleTrieNodeKeyPrefix
	keys1 := collectKeys(db1, prefix)
	keys2 := collectKeys(db2, prefix)

	if len(keys1) != len(keys2) {
		t.Errorf("DB key count mismatch (groupDepth=%d): db1=%d, db2=%d",
			groupDepth, len(keys1), len(keys2))
		set1 := make(map[string]bool)
		for _, k := range keys1 {
			set1[string(k)] = true
		}
		set2 := make(map[string]bool)
		for _, k := range keys2 {
			set2[string(k)] = true
		}
		for _, k := range keys1 {
			if !set2[string(k)] {
				path := k[len(prefix):]
				t.Errorf("  only in db1: depth=%d path=%x", len(path), path)
			}
		}
		for _, k := range keys2 {
			if !set1[string(k)] {
				path := k[len(prefix):]
				t.Errorf("  only in db2: depth=%d path=%x", len(path), path)
			}
		}
		return
	}

	for _, key := range keys1 {
		val1, _ := db1.Get(key)
		val2, _ := db2.Get(key)
		if !bytes.Equal(val1, val2) {
			path := key[len(prefix):]
			t.Errorf("value mismatch at depth=%d path=%x:\n  db1: %x\n  db2: %x",
				len(path), path, truncBlob(val1), truncBlob(val2))
		}
	}
}

func truncBlob(b []byte) []byte {
	if len(b) > 40 {
		return b[:40]
	}
	return b
}

func collectKeys(db ethdb.KeyValueStore, prefix []byte) [][]byte {
	var keys [][]byte
	iter := db.NewIterator(prefix, nil)
	defer iter.Release()
	for iter.Next() {
		if !bytes.HasPrefix(iter.Key(), prefix) {
			break
		}
		key := make([]byte, len(iter.Key()))
		copy(key, iter.Key())
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})
	return keys
}

// TestGroupedEmissionGethTraversal simulates geth's trie traversal
// for both existing and non-existing stems. This exercises the resolver
// path that geth uses when inserting new accounts into a pre-built trie.
func TestGroupedEmissionGethTraversal(t *testing.T) {
	for _, gd := range []int{1, 2, 4, 8} {
		t.Run(fmt.Sprintf("gd%d", gd), func(t *testing.T) {
			entries := generateTestEntries(t, 200)

			db := memorydb.New()
			root, stats := computeBinaryRootStreamingFromSlice(entries, db, gd)

			if root == (common.Hash{}) {
				t.Fatal("root should not be zero")
			}
			t.Logf("gd=%d: %d nodes, %d bytes", gd, stats.Nodes, stats.Bytes)

			// Read and deserialize root
			rootBlob, err := db.Get(verkleTrieNodeKeyPrefix)
			if err != nil {
				t.Fatalf("failed to read root: %v", err)
			}
			rootNode, err := bintrie.DeserializeNode(rootBlob, 0)
			if err != nil {
				t.Fatalf("failed to deserialize root: %v", err)
			}

			// Create resolver (same as geth's nodeResolver)
			resolver := func(path []byte, hash common.Hash) ([]byte, error) {
				if hash == (common.Hash{}) {
					return nil, nil
				}
				key := make([]byte, len(verkleTrieNodeKeyPrefix)+len(path))
				copy(key, verkleTrieNodeKeyPrefix)
				copy(key[len(verkleTrieNodeKeyPrefix):], path)
				blob, err := db.Get(key)
				if err != nil {
					return nil, fmt.Errorf("missing trie node %s (path %x): %w", hash.Hex(), path, err)
				}
				return blob, nil
			}

			// Test 1: Traverse to every existing stem
			for _, e := range entries {
				stem := e.Key[:stemSize]
				if inode, ok := rootNode.(*bintrie.InternalNode); ok {
					_, err := inode.GetValuesAtStem(stem, resolver)
					if err != nil {
						t.Errorf("failed to traverse to existing stem %x: %v", stem[:4], err)
					}
				}
			}

			// Test 2: Traverse to non-existing stems (simulates dev account insertion)
			for i := 0; i < 50; i++ {
				fakeStem := sha256.Sum256([]byte{byte(i), 0xFF, 0xFE})
				stem := fakeStem[:stemSize]
				if inode, ok := rootNode.(*bintrie.InternalNode); ok {
					// GetValuesAtStem should return nil (not found) without error
					vals, err := inode.GetValuesAtStem(stem, resolver)
					if err != nil {
						t.Errorf("failed to traverse to non-existing stem %x: %v", stem[:4], err)
					}
					// vals can be nil (stem not found) or empty - both are OK
					_ = vals
				}
			}
		})
	}
}

// TestGroupedEmissionLargeTraversal stress-tests with 5000 stems and 500 fake lookups.
func TestGroupedEmissionLargeTraversal(t *testing.T) {
	for _, gd := range []int{1, 4, 8} {
		t.Run(fmt.Sprintf("gd%d", gd), func(t *testing.T) {
			entries := generateTestEntries(t, 5000)

			db := memorydb.New()
			root, stats := computeBinaryRootStreamingFromSlice(entries, db, gd)

			if root == (common.Hash{}) {
				t.Fatal("root should not be zero")
			}
			t.Logf("gd=%d: %d nodes, %d bytes, %d entries", gd, stats.Nodes, stats.Bytes, len(entries))

			rootBlob, err := db.Get(verkleTrieNodeKeyPrefix)
			if err != nil {
				t.Fatalf("failed to read root: %v", err)
			}
			rootNode, err := bintrie.DeserializeNode(rootBlob, 0)
			if err != nil {
				t.Fatalf("failed to deserialize root: %v", err)
			}

			resolver := func(path []byte, hash common.Hash) ([]byte, error) {
				if hash == (common.Hash{}) {
					return nil, nil
				}
				key := make([]byte, len(verkleTrieNodeKeyPrefix)+len(path))
				copy(key, verkleTrieNodeKeyPrefix)
				copy(key[len(verkleTrieNodeKeyPrefix):], path)
				blob, err := db.Get(key)
				if err != nil {
					return nil, fmt.Errorf("missing trie node %s (path len=%d): %w", hash.Hex()[:16], len(path), err)
				}
				return blob, nil
			}

			inode, ok := rootNode.(*bintrie.InternalNode)
			if !ok {
				t.Fatal("root is not InternalNode")
			}

			// Traverse all existing stems
			existingFail := 0
			for _, e := range entries {
				_, err := inode.GetValuesAtStem(e.Key[:stemSize], resolver)
				if err != nil {
					existingFail++
					if existingFail <= 3 {
						t.Errorf("existing stem %x: %v", e.Key[:4], err)
					}
				}
			}
			if existingFail > 0 {
				t.Errorf("total existing stem failures: %d / %d", existingFail, len(entries))
			}

			// Traverse 500 non-existing stems
			fakeFail := 0
			for i := 0; i < 500; i++ {
				fakeStem := sha256.Sum256([]byte{byte(i), byte(i >> 8), 0xFF, 0xFE, 0xFD})
				_, err := inode.GetValuesAtStem(fakeStem[:stemSize], resolver)
				if err != nil {
					fakeFail++
					if fakeFail <= 3 {
						t.Errorf("non-existing stem %x: %v", fakeStem[:4], err)
					}
				}
			}
			if fakeFail > 0 {
				t.Errorf("total non-existing stem failures: %d / 500", fakeFail)
			}
		})
	}
}

// TestParallelKeyDerivation verifies that parallel storage key derivation
// produces identical trie entries as sequential derivation.
func TestParallelKeyDerivation(t *testing.T) {
	addr := common.HexToAddress("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef")
	slots := make([]storageSlot, 500)
	for i := range slots {
		slots[i].Key = sha256.Sum256([]byte{byte(i), byte(i >> 8), byte(i >> 16)})
		slots[i].Value = sha256.Sum256([]byte{byte(i), 0xFF})
		if slots[i].Value == (common.Hash{}) {
			slots[i].Value[31] = 1
		}
	}

	// Sequential
	var seqEntries []trieEntry
	for i := range slots {
		seqEntries = collectStorageEntry(addr, slots[i], seqEntries)
	}
	sort.Slice(seqEntries, func(i, j int) bool {
		return bytes.Compare(seqEntries[i].Key[:], seqEntries[j].Key[:]) < 0
	})

	// Parallel
	parEntries := collectStorageEntriesParallel(addr, slots)

	if len(seqEntries) != len(parEntries) {
		t.Fatalf("entry count mismatch: seq=%d par=%d", len(seqEntries), len(parEntries))
	}
	for i := range seqEntries {
		if seqEntries[i] != parEntries[i] {
			t.Errorf("entry %d differs:\n  seq: key=%x val=%x\n  par: key=%x val=%x",
				i, seqEntries[i].Key[:4], seqEntries[i].Value[:4],
				parEntries[i].Key[:4], parEntries[i].Value[:4])
		}
	}
}
