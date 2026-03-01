package generator

import (
	"bytes"
	"math/big"
	mrand "math/rand"
	"sort"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

// DeepBranchConfig configures deep-branch account generation.
// Deep-branch accounts have storage tries with maximally deep chains
// of branch nodes, achieved by injecting phantom entries that force
// a branch at each nibble position along legitimate slots' trie paths.
type DeepBranchConfig struct {
	// NumAccounts is the number of additional contract accounts to create
	// with deep storage tries. Default: 0 (disabled).
	NumAccounts int

	// Depth is the branch depth in nibbles (1-64). Each legitimate slot
	// gets this many phantom entries forcing branch nodes along its
	// trie path. Default: 64 (maximum possible depth).
	Depth int

	// KnownSlots is the number of legitimate storage slots with known
	// preimages per deep-branch account. These use sequential slot
	// indices (0, 1, 2, ...) so SLOAD works normally. Default: 1.
	KnownSlots int
}

// Enabled returns true if deep-branch generation is configured.
func (c DeepBranchConfig) Enabled() bool { return c.NumAccounts > 0 }

// deepBranchEntry represents a single storage entry in a deep-branch account.
type deepBranchEntry struct {
	rawSlotKey common.Hash // Original slot key (pad32(index)) — only meaningful for legitimate entries
	trieKey    common.Hash // Trie path: keccak256(rawSlotKey) for legit, constructed for phantom
	value      common.Hash // Storage value
	isPhantom  bool        // true = write with pre-hashed key (bypass keccak256)
}

// getNibble returns the nibble at position pos (0-63) from a 32-byte hash.
// Position 0 is the highest nibble of the first byte.
func getNibble(h common.Hash, pos int) byte {
	b := h[pos/2]
	if pos%2 == 0 {
		return b >> 4
	}
	return b & 0x0F
}

// setNibble sets the nibble at position pos (0-63) in a 32-byte hash.
func setNibble(h *common.Hash, pos int, val byte) {
	if pos%2 == 0 {
		h[pos/2] = (h[pos/2] & 0x0F) | (val << 4)
	} else {
		h[pos/2] = (h[pos/2] & 0xF0) | (val & 0x0F)
	}
}

// constructPhantomKeys builds phantom trie keys from a target trie path.
// Each phantom[d] matches target on nibbles [0..d-1] and differs at nibble d,
// forcing a branch node at depth d in the MPT. Returns `depth` phantom keys.
func constructPhantomKeys(target common.Hash, depth int) []common.Hash {
	if depth > 64 {
		depth = 64
	}
	phantoms := make([]common.Hash, depth)
	for d := 0; d < depth; d++ {
		phantoms[d] = target
		currentNibble := getNibble(target, d)
		newNibble := (currentNibble + 1) % 16
		setNibble(&phantoms[d], d, newNibble)
	}
	return phantoms
}

// generateDeepBranchStorage generates all storage entries (legitimate + phantom)
// for one deep-branch account. Returns entries sorted by trieKey, ready for
// StackTrie insertion and snapshot writing.
func generateDeepBranchStorage(knownSlots, depth int, rng *mrand.Rand) []deepBranchEntry {
	totalEntries := knownSlots * (1 + depth)
	entries := make([]deepBranchEntry, 0, totalEntries)

	for i := 0; i < knownSlots; i++ {
		// Legitimate slot: pad32(i) as the raw slot key
		rawKey := common.BigToHash(big.NewInt(int64(i)))
		trieKey := crypto.Keccak256Hash(rawKey[:])

		// Random value for the legitimate entry
		var value common.Hash
		rng.Read(value[:])
		if value == (common.Hash{}) {
			value[31] = 1
		}

		entries = append(entries, deepBranchEntry{
			rawSlotKey: rawKey,
			trieKey:    trieKey,
			value:      value,
			isPhantom:  false,
		})

		// Phantom entries: one per depth level
		phantoms := constructPhantomKeys(trieKey, depth)
		for _, phantom := range phantoms {
			var phantomValue common.Hash
			phantomValue[31] = 1 // Minimal non-zero value
			entries = append(entries, deepBranchEntry{
				trieKey:   phantom,
				value:     phantomValue,
				isPhantom: true,
			})
		}
	}

	// Sort all entries by trieKey for StackTrie insertion
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].trieKey[:], entries[j].trieKey[:]) < 0
	})

	return entries
}
