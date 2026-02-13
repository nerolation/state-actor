package generator

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"log"
	"math/bits"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/trie/bintrie"
)

const (
	stemSize      = bintrie.StemSize     // 31
	hashSize      = bintrie.HashSize     // 32
	stemNodeWidth = bintrie.StemNodeWidth // 256

	// Node type markers matching bintrie/binary_node.go
	nodeTypeStem     = 1
	nodeTypeInternal = 2
)

// verkleTrieNodeKeyPrefix is the database key prefix for binary trie nodes.
// PathDB isolates binary trie data under a "v" namespace prefix
// (rawdb.VerklePrefix), so the full key is "v" + "A" + path.
var verkleTrieNodeKeyPrefix = []byte("vA")

// trieEntry is a single key-value pair destined for the binary trie.
// Key[0:31] is the stem (routes through InternalNode bit tree, 248 bits).
// Key[31] is the suffix (indexes into a StemNode's 256-slot Values array).
type trieEntry struct {
	Key   [hashSize]byte
	Value [hashSize]byte
}

// trieNodeWriter batches serialized trie node writes to Pebble.
// Each node is written at key "vA" + path, where path is one byte per
// tree level (0x00=left, 0x01=right). Flushes when batch exceeds 256MB.
type trieNodeWriter struct {
	batch ethdb.Batch
	db    ethdb.KeyValueStore
	nodes int
	bytes int64
}

func (w *trieNodeWriter) writeNode(path []byte, blob []byte) {
	key := make([]byte, len(verkleTrieNodeKeyPrefix)+len(path))
	copy(key, verkleTrieNodeKeyPrefix)
	copy(key[len(verkleTrieNodeKeyPrefix):], path)
	if err := w.batch.Put(key, blob); err != nil {
		log.Fatalf("failed to write trie node: %v", err)
	}
	w.nodes++
	w.bytes += int64(len(key) + len(blob))
	if w.batch.ValueSize() >= 256*1024*1024 {
		if err := w.batch.Write(); err != nil {
			log.Fatalf("failed to flush trie node batch: %v", err)
		}
		w.batch.Reset()
	}
}

func (w *trieNodeWriter) flush() {
	if w.batch.ValueSize() > 0 {
		if err := w.batch.Write(); err != nil {
			log.Fatalf("failed to flush trie node batch: %v", err)
		}
	}
}

// serializeInternalNode serializes an InternalNode to the format expected
// by bintrie.DeserializeNode: [type=2][leftHash(32)][rightHash(32)] = 65 bytes.
func serializeInternalNode(leftHash, rightHash common.Hash) []byte {
	var buf [1 + hashSize + hashSize]byte // 65 bytes
	buf[0] = nodeTypeInternal
	copy(buf[1:33], leftHash[:])
	copy(buf[33:65], rightHash[:])
	return buf[:]
}

// serializeStemNode serializes a StemNode to the format expected by
// bintrie.DeserializeNode:
//
//	[type=1][stem(31)][bitmap(32)][value₀(32)][value₁(32)]...
//
// The bitmap indicates which of the 256 suffix slots are present.
// Only present values are packed sequentially after the bitmap.
func serializeStemNode(stem []byte, entries []trieEntry) []byte {
	// Count present values and build bitmap
	var bitmap [hashSize]byte
	for _, e := range entries {
		suffix := e.Key[stemSize]
		bitmap[suffix/8] |= 1 << (7 - (suffix % 8))
	}

	// Allocate: 1 + 31 + 32 + (count * 32)
	size := 1 + stemSize + hashSize + len(entries)*hashSize
	buf := make([]byte, size)
	buf[0] = nodeTypeStem
	copy(buf[1:1+stemSize], stem)
	copy(buf[1+stemSize:1+stemSize+hashSize], bitmap[:])

	// Pack values in suffix order. Since entries are sorted by key and
	// share the same stem, they're already sorted by suffix.
	offset := 1 + stemSize + hashSize
	for _, e := range entries {
		copy(buf[offset:offset+hashSize], e.Value[:])
		offset += hashSize
	}

	return buf
}

// computeStemNodeHash computes the hash of a StemNode from its entries.
// Exactly mirrors bintrie.StemNode.Hash():
//  1. Hash each value: data[suffix] = SHA256(value)
//  2. 8-level tree reduction: data[i] = SHA256(data[2i] || data[2i+1]), skip if both zero
//  3. Final: SHA256(stem || 0x00 || data[0])
//
// Uses a [4]uint64 bitmap to skip zero pairs in the tree reduction.
// For a typical StemNode with k=2 entries, this reduces iterations from
// 255 (with 32-byte comparisons) to ~16 (with single-bit tests).
func computeStemNodeHash(stem []byte, entries []trieEntry) common.Hash {
	var data [stemNodeWidth]common.Hash
	var bm [4]uint64 // 256-bit bitmap: bit set = data[i] is non-zero

	// Step 1: Hash each value at its suffix position, mark bitmap
	for _, e := range entries {
		suffix := e.Key[stemSize] // key[31]
		data[suffix] = sha256.Sum256(e.Value[:])
		bm[suffix/64] |= 1 << (63 - uint(suffix)%64)
	}

	// Step 2: 8-level tree reduction — skip zero pairs via bitmap.
	// The reduction is in-place (writes data[i] from data[2i],data[2i+1]),
	// so we must explicitly zero data[i] when skipping to avoid stale values
	// from Step 1 or previous levels.
	var buf [64]byte
	for level := 1; level <= 8; level++ {
		count := stemNodeWidth / (1 << level)
		var newBm [4]uint64
		for i := 0; i < count; i++ {
			li, ri := i*2, i*2+1
			lSet := bm[li/64]&(1<<(63-uint(li)%64)) != 0
			rSet := bm[ri/64]&(1<<(63-uint(ri)%64)) != 0
			if !lSet && !rSet {
				data[i] = common.Hash{} // clear stale data from earlier levels
				continue
			}
			copy(buf[:32], data[li][:])
			copy(buf[32:], data[ri][:])
			data[i] = sha256.Sum256(buf[:])
			newBm[i/64] |= 1 << (63 - uint(i)%64)
		}
		bm = newBm
	}

	// Step 3: Final hash = SHA256(stem || 0x00 || data[0])
	var final [stemSize + 1 + hashSize]byte // 31 + 1 + 32 = 64 bytes
	copy(final[:stemSize], stem)
	final[stemSize] = 0x00
	copy(final[stemSize+1:], data[0][:])
	return sha256.Sum256(final[:])
}

// collectAccountEntries generates trie entries for a single account.
// This mirrors the key derivation and value encoding of:
//   - bintrie.BinaryTrie.UpdateAccount (basic data + code hash)
//   - bintrie.BinaryTrie.UpdateContractCode (code chunks)
//   - bintrie.BinaryTrie.UpdateStorage (storage slots)
func collectAccountEntries(
	addr common.Address,
	acc *types.StateAccount,
	codeLen int,
	code []byte,
	storage []storageSlot,
	entries []trieEntry,
) []trieEntry {
	// Account basic data at suffix 0 — mirrors UpdateAccount value encoding
	var basicData [hashSize]byte
	binary.BigEndian.PutUint32(basicData[bintrie.BasicDataCodeSizeOffset-1:], uint32(codeLen))
	binary.BigEndian.PutUint64(basicData[bintrie.BasicDataNonceOffset:], acc.Nonce)
	balanceBytes := acc.Balance.Bytes()
	if len(balanceBytes) > 16 {
		balanceBytes = balanceBytes[16:]
	}
	copy(basicData[hashSize-len(balanceBytes):], balanceBytes[:])

	// Stem for account header
	var zeroKey [hashSize]byte
	stem := bintrie.GetBinaryTreeKey(addr, zeroKey[:])

	// Entry for basic data (suffix = BasicDataLeafKey = 0)
	var e0 trieEntry
	copy(e0.Key[:stemSize], stem[:stemSize])
	e0.Key[stemSize] = bintrie.BasicDataLeafKey
	e0.Value = basicData
	entries = append(entries, e0)

	// Entry for code hash (suffix = CodeHashLeafKey = 1)
	var e1 trieEntry
	copy(e1.Key[:stemSize], stem[:stemSize])
	e1.Key[stemSize] = bintrie.CodeHashLeafKey
	copy(e1.Value[:], acc.CodeHash[:])
	entries = append(entries, e1)

	// Code chunk entries
	if len(code) > 0 {
		entries = collectCodeEntries(addr, code, entries)
	}

	// Storage entries
	for i := range storage {
		entries = collectStorageEntry(addr, storage[i], entries)
	}

	return entries
}

// collectCodeEntries generates trie entries for contract code chunks.
// Mirrors bintrie.BinaryTrie.UpdateContractCode: code is chunked via
// ChunkifyCode, then grouped into StemNodes of 256 values each.
// Within a group, chunks share the same stem; the suffix is the group offset.
//
// Layout:
//   - First group: chunknr 0..127 at suffixes 128..255 (starts mid-stem)
//   - Subsequent groups: 256 chunks each at suffixes 0..255
//
// The stem is recomputed only at group boundaries (groupOffset == 0 or chunknr == 0).
func collectCodeEntries(addr common.Address, code []byte, entries []trieEntry) []trieEntry {
	chunks := bintrie.ChunkifyCode(code)

	var stem []byte
	for i, chunknr := 0, uint64(0); i < len(chunks); i, chunknr = i+hashSize, chunknr+1 {
		groupOffset := (chunknr + 128) % stemNodeWidth
		if groupOffset == 0 || chunknr == 0 {
			var offset [hashSize]byte
			binary.LittleEndian.PutUint64(offset[24:], chunknr+128)
			stem = bintrie.GetBinaryTreeKey(addr, offset[:])
		}

		var e trieEntry
		copy(e.Key[:stemSize], stem[:stemSize])
		e.Key[stemSize] = byte(groupOffset)
		copy(e.Value[:], chunks[i:i+hashSize])
		entries = append(entries, e)
	}
	return entries
}

// collectStorageEntry generates a trie entry for a single storage slot.
// Mirrors bintrie.BinaryTrie.UpdateStorage: key is derived via
// GetBinaryTreeKeyStorageSlot, value is the 32-byte slot value.
func collectStorageEntry(addr common.Address, slot storageSlot, entries []trieEntry) []trieEntry {
	k := bintrie.GetBinaryTreeKeyStorageSlot(addr, slot.Key[:])

	var e trieEntry
	copy(e.Key[:], k)
	// Value encoding matches UpdateStorage: copy 32 bytes directly.
	// slot.Value is common.Hash (32 bytes), so len >= HashSize always.
	copy(e.Value[:], slot.Value[:])
	entries = append(entries, e)

	return entries
}

// --- Streaming binary trie root computation ---

const maxDepth = stemSize * 8 // 248 bits

// commonPrefixLenBits returns the number of leading bits that two stems
// share. Both stems must be exactly stemSize (31) bytes. Returns 0..248.
func commonPrefixLenBits(a, b []byte) int {
	for i := 0; i < stemSize; i++ {
		xor := a[i] ^ b[i]
		if xor != 0 {
			return i*8 + bits.LeadingZeros8(xor)
		}
	}
	return maxDepth
}

// streamingBuilder computes the binary trie root via a single forward pass
// over sorted entries, using O(depth) memory for the stack.
//
// Algorithm: maintains a stack of pending child hashes at each depth.
// Stems are processed left-to-right (sorted order). Each stem is "deferred"
// until its right-neighbor arrives, because the placement depth depends on
// max(leftCPL, rightCPL) + 1. Before placing a stem, pending hashes from
// the previous subtree are unwound (combined with empty siblings).
//
// Key correctness property: each pending hash tracks which SIDE (left or
// right) of its parent it belongs to, using the stem bits at each depth.
// This ensures H(left, right) ordering matches the recursive tree exactly,
// even when all entries go right at some depth (making left = zero).
type streamingBuilder struct {
	stack    [maxDepth]common.Hash      // pending child hash at each depth
	occupied [maxDepth]bool             // whether stack[d] is valid
	isRight  [maxDepth]bool             // true if stack[d] is a right child
	stemBits [maxDepth][stemSize]byte   // stem that placed each pending hash
	w        *trieNodeWriter            // optional: writes serialized nodes to DB
	pathBuf  [maxDepth]byte             // reusable buffer for buildPath

	// Deferred stem: waiting for right-neighbor CPL before placement.
	hasPrev     bool
	prevHash    common.Hash
	prevStem    [stemSize]byte
	prevLeftCPL int         // CPL with left neighbor (-1 if first stem)
	prevEntries []trieEntry // kept only when w != nil (for serialization)
}

// buildPath writes the bit-path from root to `depth` into sb.pathBuf and
// returns a sub-slice. The returned slice is only valid until the next
// buildPath call. Safe because Pebble batch.Put() copies key/value internally.
func (sb *streamingBuilder) buildPath(stem []byte, depth int) []byte {
	for i := 0; i < depth; i++ {
		sb.pathBuf[i] = stemBitAt(stem, i)
	}
	return sb.pathBuf[:depth]
}

// stemBitAt returns the bit value (0 or 1) at the given depth in a stem.
func stemBitAt(stem []byte, depth int) byte {
	return (stem[depth/8] >> uint(7-(depth%8))) & 1
}

// makePath builds the bit-path from root to `depth` for the given stem.
// Each byte is 0x00 (left) or 0x01 (right). Used for trie node DB keys.
func makePath(stem []byte, depth int) []byte {
	path := make([]byte, depth)
	for i := 0; i < depth; i++ {
		path[i] = stemBitAt(stem, i)
	}
	return path
}

// feedStem is called for each completed stem group (consecutive entries
// sharing the same stem). It flushes the previously deferred stem and
// defers the current one.
func (sb *streamingBuilder) feedStem(stem []byte, hash common.Hash, entries []trieEntry) {
	rightCPL := -1
	if sb.hasPrev {
		rightCPL = commonPrefixLenBits(sb.prevStem[:], stem)
		sb.flushDeferred(rightCPL)
	}

	sb.hasPrev = true
	sb.prevHash = hash
	copy(sb.prevStem[:], stem[:stemSize])
	sb.prevLeftCPL = rightCPL
	if sb.w != nil {
		sb.prevEntries = make([]trieEntry, len(entries))
		copy(sb.prevEntries, entries)
	} else {
		sb.prevEntries = nil
	}
}

// flushDeferred places the previously deferred StemNode at the correct depth.
func (sb *streamingBuilder) flushDeferred(rightCPL int) {
	targetDepth := sb.prevLeftCPL
	if rightCPL > targetDepth {
		targetDepth = rightCPL
	}
	targetDepth++ // StemNode sits one level below the divergence point

	// Resolve pending hashes from the PREVIOUS subtree. Any pending hashes
	// at depths > prevLeftCPL belong to the left neighbor's subtree.
	sb.unwindTo(sb.prevLeftCPL + 1)

	// Write the StemNode — path derived from the stem being placed.
	if sb.w != nil {
		sb.w.writeNode(sb.buildPath(sb.prevStem[:], targetDepth), serializeStemNode(sb.prevStem[:], sb.prevEntries))
	}

	sb.propagateUp(sb.prevHash, targetDepth, sb.prevStem[:])
}

// unwindTo resolves all pending child hashes at depths >= minDepth by
// combining each with an empty sibling. Uses the stored isRight flag and
// stemBits to determine the correct left/right ordering.
func (sb *streamingBuilder) unwindTo(minDepth int) {
	for d := maxDepth - 1; d >= minDepth; d-- {
		if !sb.occupied[d] {
			continue
		}
		pending := sb.stack[d]
		var left, right common.Hash
		if sb.isRight[d] {
			right = pending // H(zero, pending)
		} else {
			left = pending // H(pending, zero)
		}
		var buf [64]byte
		copy(buf[:32], left[:])
		copy(buf[32:], right[:])
		combined := sha256.Sum256(buf[:])

		if sb.w != nil {
			// Path derived from the stem that placed this pending hash.
			sb.w.writeNode(sb.buildPath(sb.stemBits[d][:], d), serializeInternalNode(left, right))
		}
		// Propagate upward using the stem that originally placed this hash.
		stem := sb.stemBits[d]
		sb.occupied[d] = false
		sb.propagateUp(combined, d, stem[:])
	}
}

// propagateUp pushes a hash from fromDepth toward the root. Uses the stem's
// bit at each depth to determine whether the hash is a left or right child.
// When the hash is right and no left exists, it combines with zero immediately.
func (sb *streamingBuilder) propagateUp(hash common.Hash, fromDepth int, stem []byte) {
	for d := fromDepth; d > 0; d-- {
		pd := d - 1
		bit := stemBitAt(stem, pd)

		if sb.occupied[pd] {
			// Combine pending + current based on their sides.
			var left, right common.Hash
			if sb.isRight[pd] {
				// pending is right, current must be left
				left = hash
				right = sb.stack[pd]
			} else {
				// pending is left, current must be right
				left = sb.stack[pd]
				right = hash
			}
			var buf [64]byte
			copy(buf[:32], left[:])
			copy(buf[32:], right[:])
			hash = sha256.Sum256(buf[:])

			if sb.w != nil {
				// Path derived from stem — both children share bits 0..pd-1.
				sb.w.writeNode(sb.buildPath(stem, pd), serializeInternalNode(left, right))
			}
			sb.occupied[pd] = false
		} else if bit == 1 {
			// Hash is on the RIGHT side, left is empty (zero).
			// Combine immediately: H(zero, hash).
			right := hash // capture before overwriting
			var buf [64]byte
			// buf[:32] is already zero (left = empty)
			copy(buf[32:], right[:])
			hash = sha256.Sum256(buf[:])

			if sb.w != nil {
				sb.w.writeNode(sb.buildPath(stem, pd), serializeInternalNode(common.Hash{}, right))
			}
			// Continue propagating upward — don't store.
		} else {
			// Hash is on the LEFT side — store and wait for right sibling.
			sb.stack[pd] = hash
			sb.occupied[pd] = true
			sb.isRight[pd] = false
			copy(sb.stemBits[pd][:], stem[:stemSize])
			return
		}
	}
	sb.stack[0] = hash
}

// finish flushes the last deferred stem and unwinds all remaining pending
// child hashes to produce the final root hash.
func (sb *streamingBuilder) finish() common.Hash {
	if !sb.hasPrev {
		return common.Hash{} // empty trie
	}
	sb.flushDeferred(-1) // last stem has no right neighbor
	sb.unwindTo(0)       // resolve everything remaining
	return sb.stack[0]
}

// trieNodeStats holds byte/node counts from Phase 2 trie node writing.
type trieNodeStats struct {
	Nodes int
	Bytes int64
}

// computeBinaryRootStreamingFromSlice is the slice-based variant used for
// testing equivalence with the recursive approach. It feeds pre-sorted
// entries directly into the streaming builder without needing a DB iterator.
func computeBinaryRootStreamingFromSlice(entries []trieEntry, db ethdb.KeyValueStore) (common.Hash, trieNodeStats) {
	if len(entries) == 0 {
		return common.Hash{}, trieNodeStats{}
	}
	sb := &streamingBuilder{}
	if db != nil {
		sb.w = &trieNodeWriter{batch: db.NewBatch(), db: db}
	}

	var currentStem [stemSize]byte
	var group []trieEntry
	copy(currentStem[:], entries[0].Key[:stemSize])

	for i := range entries {
		if !bytes.Equal(entries[i].Key[:stemSize], currentStem[:]) {
			hash := computeStemNodeHash(currentStem[:], group)
			sb.feedStem(currentStem[:], hash, group)
			group = group[:0]
			copy(currentStem[:], entries[i].Key[:stemSize])
		}
		group = append(group, entries[i])
	}
	if len(group) > 0 {
		hash := computeStemNodeHash(currentStem[:], group)
		sb.feedStem(currentStem[:], hash, group)
	}

	root := sb.finish()
	var tnStats trieNodeStats
	if sb.w != nil {
		sb.w.flush()
		tnStats = trieNodeStats{Nodes: sb.w.nodes, Bytes: sb.w.bytes}
		log.Printf("Wrote %d trie nodes (%d MB)", sb.w.nodes, sb.w.bytes/1024/1024)
	}
	return root, tnStats
}

// computeBinaryRootStreaming computes the root hash by iterating sorted
// trie entries in a single forward pass. The iterator must yield entries
// sorted by key (32 bytes each: key[0:31]=stem, key[31]=suffix).
// Values are 32 bytes. O(depth) ≈ 8 KB memory for the stack.
func computeBinaryRootStreaming(iter ethdb.Iterator, db ethdb.KeyValueStore) (common.Hash, trieNodeStats) {
	sb := &streamingBuilder{}
	if db != nil {
		sb.w = &trieNodeWriter{batch: db.NewBatch(), db: db}
	}

	var currentStem [stemSize]byte
	var currentEntries []trieEntry
	hasCurrent := false

	for iter.Next() {
		var e trieEntry
		copy(e.Key[:], iter.Key())
		copy(e.Value[:], iter.Value())

		if hasCurrent && !bytes.Equal(e.Key[:stemSize], currentStem[:]) {
			// Stem boundary — flush the completed group
			hash := computeStemNodeHash(currentStem[:], currentEntries)
			sb.feedStem(currentStem[:], hash, currentEntries)
			currentEntries = currentEntries[:0]
		}

		hasCurrent = true
		copy(currentStem[:], e.Key[:stemSize])
		currentEntries = append(currentEntries, e)
	}
	iter.Release()

	// Flush the last stem group
	if len(currentEntries) > 0 {
		hash := computeStemNodeHash(currentStem[:], currentEntries)
		sb.feedStem(currentStem[:], hash, currentEntries)
	}

	root := sb.finish()

	var tnStats trieNodeStats
	if sb.w != nil {
		sb.w.flush()
		tnStats = trieNodeStats{Nodes: sb.w.nodes, Bytes: sb.w.bytes}
		log.Printf("Wrote %d trie nodes (%d MB)", sb.w.nodes, sb.w.bytes/1024/1024)
	}

	return root, tnStats
}
