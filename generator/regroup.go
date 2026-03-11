package generator

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"log"
	"sort"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
)

// regroupTrieNodes reads all individual internal nodes written by the streaming
// builder (old format) and rewrites them in the grouped format expected by
// geth PR #33658.
//
// Old format (per node): [type=2][leftHash(32)][rightHash(32)] at full bit-path
// New format (per group): [type=2][groupDepth][bitmap][present hashes...] at group boundary
//
// Algorithm:
// 1. Read all trie nodes from DB into an in-memory map: path -> blob
// 2. For each group boundary, traverse groupDepth levels down via DFS,
//    collecting bottom-layer child hashes and building the bitmap
// 3. Serialize in new format, write at group boundary, delete old keys
func regroupTrieNodes(db ethdb.KeyValueStore, groupDepth int, verbose bool) error {
	if groupDepth < 1 || groupDepth > maxGroupDepth {
		return fmt.Errorf("groupDepth must be 1-%d, got %d", maxGroupDepth, groupDepth)
	}

	prefix := verkleTrieNodeKeyPrefix // "vA"

	// Step 1: Read all trie nodes into memory
	type nodeInfo struct {
		blob     []byte
		nodeType byte // 1=stem, 2=internal
	}
	nodes := make(map[string]nodeInfo)

	iter := db.NewIterator(prefix, nil)
	for iter.Next() {
		key := iter.Key()
		if !bytes.HasPrefix(key, prefix) {
			break
		}
		path := string(key[len(prefix):])
		blob := make([]byte, len(iter.Value()))
		copy(blob, iter.Value())
		nodes[path] = nodeInfo{
			blob:     blob,
			nodeType: blob[0],
		}
	}
	iter.Release()

	if len(nodes) == 0 {
		return nil
	}

	if verbose {
		var internalCount, stemCount int
		for _, n := range nodes {
			if n.nodeType == nodeTypeInternal {
				internalCount++
			} else {
				stemCount++
			}
		}
		log.Printf("Regrouping: %d internal nodes + %d stem nodes, groupDepth=%d",
			internalCount, stemCount, groupDepth)
	}

	batch := db.NewBatch()

	// Step 2: Find all internal node paths at group boundaries
	// (depths that are multiples of groupDepth).
	var boundaryPaths []string
	for path, n := range nodes {
		depth := len(path)
		if n.nodeType == nodeTypeInternal && depth%groupDepth == 0 {
			boundaryPaths = append(boundaryPaths, path)
		}
	}
	// Sort boundaries from deepest to shallowest for consistent processing
	sort.Slice(boundaryPaths, func(i, j int) bool {
		return len(boundaryPaths[i]) > len(boundaryPaths[j])
	})

	// Step 3: For each group boundary, collect bottom-layer children via DFS
	for _, bPath := range boundaryPaths {
		bitmapSize := bitmapSizeForDepth(groupDepth)
		bitmap := make([]byte, bitmapSize)

		type stackEntry struct {
			path     string
			relDepth int
			position int
		}
		type posHash struct {
			position int
			hash     common.Hash
		}
		var collected []posHash

		stack := []stackEntry{{path: bPath, relDepth: 0, position: 0}}

		for len(stack) > 0 {
			top := stack[len(stack)-1]
			stack = stack[:len(stack)-1]

			if top.relDepth == groupDepth {
				// Bottom layer of this group.
				n, exists := nodes[top.path]
				if exists {
					h := computeNodeHashFromBlob(n.blob)
					collected = append(collected, posHash{position: top.position, hash: h})
				}
				continue
			}

			n, exists := nodes[top.path]
			if !exists {
				continue
			}

			if n.nodeType == nodeTypeStem {
				// Stem node at intermediate depth — terminates early.
				// Extend position using the stem actual key bits so that
				// GetValuesAtStem traversal (which follows key bits) reaches it.
				remainingDepth := groupDepth - top.relDepth
				absDepth := len(bPath) + top.relDepth
				stemBytes := n.blob[1 : 1+stemSize] // stem is 31 bytes after type byte
				leafPos := top.position
				for d := 0; d < remainingDepth; d++ {
					bitIdx := absDepth + d
					bit := int(stemBytes[bitIdx/8] >> (7 - (bitIdx % 8)) & 1)
					leafPos = leafPos*2 + bit
				}
				h := computeNodeHashFromBlob(n.blob)
				collected = append(collected, posHash{position: leafPos, hash: h})
				continue
			}

			// Internal node — recurse into children.
			leftPath := top.path + string([]byte{0x00})
			stack = append(stack, stackEntry{
				path:     leftPath,
				relDepth: top.relDepth + 1,
				position: top.position * 2,
			})
			rightPath := top.path + string([]byte{0x01})
			stack = append(stack, stackEntry{
				path:     rightPath,
				relDepth: top.relDepth + 1,
				position: top.position*2 + 1,
			})
		}

		if len(collected) == 0 {
			continue
		}

		// Sort by position to ensure hashes are in bitmap order.
		sort.Slice(collected, func(i, j int) bool {
			return collected[i].position < collected[j].position
		})

		// Build bitmap and hash list.
		var hashes []common.Hash
		for _, ph := range collected {
			byteIdx := ph.position / 8
			bitIdx := 7 - (ph.position % 8)
			bitmap[byteIdx] |= 1 << uint(bitIdx)
			hashes = append(hashes, ph.hash)
		}

		// Serialize: [type=2][groupDepth][bitmap][hashes...]
		size := 1 + 1 + bitmapSize + len(hashes)*hashSize
		buf := make([]byte, size)
		buf[0] = nodeTypeInternal
		buf[1] = byte(groupDepth)
		copy(buf[2:2+bitmapSize], bitmap)
		offset := 2 + bitmapSize
		for _, h := range hashes {
			copy(buf[offset:offset+hashSize], h[:])
			offset += hashSize
		}

		// Write at group boundary path.
		key := make([]byte, len(prefix)+len(bPath))
		copy(key, prefix)
		copy(key[len(prefix):], bPath)
		if err := batch.Put(key, buf); err != nil {
			return fmt.Errorf("failed to write regrouped node: %w", err)
		}
	}

	// Step 4: Delete intermediate internal nodes (not at group boundaries).
	for path, n := range nodes {
		if n.nodeType != nodeTypeInternal {
			continue
		}
		depth := len(path)
		if depth%groupDepth != 0 {
			key := make([]byte, len(prefix)+len(path))
			copy(key, prefix)
			copy(key[len(prefix):], path)
			if err := batch.Delete(key); err != nil {
				return fmt.Errorf("failed to delete intermediate node: %w", err)
			}
		}
	}

	// Step 4b: Move stems within groups to their extended paths.
	// A stem at depth D within a group (boundary B, boundary+groupDepth) must be
	// stored at path length boundary+groupDepth, extended with the stem actual key bits.
	for path, n := range nodes {
		if n.nodeType != nodeTypeStem {
			continue
		}
		depth := len(path)
		groupBoundary := (depth / groupDepth) * groupDepth
		nextBoundary := groupBoundary + groupDepth
		if depth >= nextBoundary || depth == groupBoundary {
			// Stem is at or past group boundary — no extension needed.
			continue
		}
		stemBytes := n.blob[1 : 1+stemSize]
		extendedPath := make([]byte, nextBoundary)
		copy(extendedPath, path)
		for i := depth; i < nextBoundary; i++ {
			extendedPath[i] = stemBytes[i/8] >> (7 - (i % 8)) & 1
		}

		oldKey := make([]byte, len(prefix)+len(path))
		copy(oldKey, prefix)
		copy(oldKey[len(prefix):], path)

		newKey := make([]byte, len(prefix)+len(extendedPath))
		copy(newKey, prefix)
		copy(newKey[len(prefix):], extendedPath)

		if err := batch.Delete(oldKey); err != nil {
			return fmt.Errorf("failed to delete old stem path: %w", err)
		}
		if err := batch.Put(newKey, n.blob); err != nil {
			return fmt.Errorf("failed to write extended stem path: %w", err)
		}
	}

	if batch.ValueSize() > 0 {
		if err := batch.Write(); err != nil {
			return fmt.Errorf("failed to write regroup batch: %w", err)
		}
	}

	if verbose {
		log.Printf("Regrouping complete: %d group boundary nodes written", len(boundaryPaths))
	}

	return nil
}

// computeNodeHashFromBlob computes the hash of a trie node from its serialized blob.
// For internal nodes (old format): hash = SHA256(leftHash || rightHash)
// For stem nodes: hash = SHA256(stem || 0x00 || treeRoot) using 8-level reduction
func computeNodeHashFromBlob(blob []byte) common.Hash {
	if len(blob) == 0 {
		return common.Hash{}
	}

	switch blob[0] {
	case nodeTypeInternal:
		// Old format: [2][leftHash(32)][rightHash(32)] = 65 bytes
		if len(blob) == 65 {
			return sha256.Sum256(blob[1:65])
		}
		return sha256.Sum256(blob[1:])

	case nodeTypeStem:
		// Stem node: [1][stem(31)][bitmap(32)][values...]
		// Hash = SHA256(stem || 0x00 || treeRoot)
		if len(blob) < 1+stemSize+hashSize {
			return common.Hash{}
		}
		stem := blob[1 : 1+stemSize]
		bitmapBytes := blob[1+stemSize : 1+stemSize+hashSize]

		var data [stemNodeWidth]common.Hash
		var zeroHash common.Hash
		valueOffset := 1 + stemSize + hashSize
		for i := 0; i < stemNodeWidth; i++ {
			if bitmapBytes[i/8]&(1<<(7-(i%8))) != 0 {
				if valueOffset+hashSize <= len(blob) {
					var val [hashSize]byte
					copy(val[:], blob[valueOffset:valueOffset+hashSize])
					data[i] = sha256.Sum256(val[:])
					valueOffset += hashSize
				}
			}
		}

		// 8-level tree reduction
		var buf [64]byte
		for level := 1; level <= 8; level++ {
			count := stemNodeWidth / (1 << level)
			for i := 0; i < count; i++ {
				if data[i*2] == zeroHash && data[i*2+1] == zeroHash {
					data[i] = zeroHash
					continue
				}
				copy(buf[:32], data[i*2][:])
				copy(buf[32:], data[i*2+1][:])
				data[i] = sha256.Sum256(buf[:])
			}
		}

		// Final: SHA256(stem || 0x00 || data[0])
		var final [stemSize + 1 + hashSize]byte
		copy(final[:stemSize], stem)
		final[stemSize] = 0x00
		copy(final[stemSize+1:], data[0][:])
		return sha256.Sum256(final[:])

	default:
		return sha256.Sum256(blob)
	}
}
