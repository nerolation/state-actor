package generator

import (
	"fmt"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/params"
)

// TestBridgeStateRootMatchesDirect generates the same state via
// (a) the direct GethWriter and (b) the geth-bridge binary, then
// asserts that both produce the identical MPT state root.
//
// This is the key correctness invariant: the bridge must be a faithful
// proxy — same data in, same root out.
func TestBridgeStateRootMatchesDirect(t *testing.T) {
	bridgeBin := buildGethBridge(t)

	// Standard test config — small but exercises accounts, contracts,
	// storage, and code.
	seed := int64(77777)
	numAccounts := 20
	numContracts := 10
	maxSlots := 50
	minSlots := 5

	// --- Run 1: direct GethWriter ---
	directDir := t.TempDir()
	directDB := filepath.Join(directDir, "chaindata")

	directConfig := Config{
		DBPath:       directDB,
		NumAccounts:  numAccounts,
		NumContracts: numContracts,
		MaxSlots:     maxSlots,
		MinSlots:     minSlots,
		Distribution: Uniform,
		Seed:         seed,
		BatchSize:    1000,
		Workers:      1,
		CodeSize:     128,
		OutputFormat: OutputGeth,
	}

	gen1, err := New(directConfig)
	if err != nil {
		t.Fatalf("direct: failed to create generator: %v", err)
	}
	stats1, err := gen1.Generate()
	if err != nil {
		t.Fatalf("direct: failed to generate: %v", err)
	}
	gen1.Close()

	if stats1.StateRoot == (common.Hash{}) {
		t.Fatal("direct: state root is zero")
	}

	// --- Run 2: geth-bridge ---
	bridgeDir := t.TempDir()
	bridgeDB := filepath.Join(bridgeDir, "chaindata")

	bridgeConfig := Config{
		DBPath:       bridgeDB,
		NumAccounts:  numAccounts,
		NumContracts: numContracts,
		MaxSlots:     maxSlots,
		MinSlots:     minSlots,
		Distribution: Uniform,
		Seed:         seed,
		BatchSize:    1000,
		Workers:      1,
		CodeSize:     128,
		OutputFormat: OutputBridge,
		BridgeBin:    bridgeBin,
		BridgeArgs:   []string{"-db", bridgeDB},
	}

	gen2, err := New(bridgeConfig)
	if err != nil {
		t.Fatalf("bridge: failed to create generator: %v", err)
	}
	stats2, err := gen2.Generate()
	if err != nil {
		t.Fatalf("bridge: failed to generate: %v", err)
	}
	gen2.Close()

	if stats2.StateRoot == (common.Hash{}) {
		t.Fatal("bridge: state root is zero")
	}

	// --- Compare ---
	if stats1.StateRoot != stats2.StateRoot {
		t.Errorf("state root mismatch:\n  direct: %s\n  bridge: %s",
			stats1.StateRoot.Hex(), stats2.StateRoot.Hex())
	}

	if stats1.AccountsCreated != stats2.AccountsCreated {
		t.Errorf("accounts mismatch: direct=%d bridge=%d",
			stats1.AccountsCreated, stats2.AccountsCreated)
	}
	if stats1.ContractsCreated != stats2.ContractsCreated {
		t.Errorf("contracts mismatch: direct=%d bridge=%d",
			stats1.ContractsCreated, stats2.ContractsCreated)
	}
	if stats1.StorageSlotsCreated != stats2.StorageSlotsCreated {
		t.Errorf("storage slots mismatch: direct=%d bridge=%d",
			stats1.StorageSlotsCreated, stats2.StorageSlotsCreated)
	}

	t.Logf("State root match: %s (%d accounts, %d contracts, %d slots)",
		stats1.StateRoot.Hex(), stats1.AccountsCreated,
		stats1.ContractsCreated, stats1.StorageSlotsCreated)
}

// TestBridgeReproducibility runs the bridge twice with the same seed
// and verifies identical state roots.
func TestBridgeReproducibility(t *testing.T) {
	bridgeBin := buildGethBridge(t)

	var roots [2]common.Hash
	for i := 0; i < 2; i++ {
		dir := t.TempDir()
		dbPath := filepath.Join(dir, "chaindata")

		config := Config{
			DBPath:       dbPath,
			NumAccounts:  15,
			NumContracts: 8,
			MaxSlots:     30,
			MinSlots:     3,
			Distribution: PowerLaw,
			Seed:         99999,
			BatchSize:    1000,
			Workers:      1,
			CodeSize:     64,
			OutputFormat: OutputBridge,
			BridgeBin:    bridgeBin,
			BridgeArgs:   []string{"-db", dbPath},
		}

		gen, err := New(config)
		if err != nil {
			t.Fatalf("run %d: failed to create generator: %v", i, err)
		}
		stats, err := gen.Generate()
		if err != nil {
			t.Fatalf("run %d: failed to generate: %v", i, err)
		}
		gen.Close()
		roots[i] = stats.StateRoot
	}

	if roots[0] != roots[1] {
		t.Errorf("bridge not reproducible: %s != %s", roots[0].Hex(), roots[1].Hex())
	}
}

// TestCrossClientStateRoot generates the same state via both the geth-bridge
// and the erigon-bridge, then asserts that both produce the same MPT state root.
// This validates that Erigon's hex patricia hashed trie commitment matches
// geth's StackTrie MPT implementation.
func TestCrossClientStateRoot(t *testing.T) {
	gethBin := buildGethBridge(t)
	erigonBin := buildErigonBridge(t)
	besuBin := buildBesuBridge(t)

	seed := int64(88888)
	numAccounts := 20
	numContracts := 10
	maxSlots := 50
	minSlots := 5

	type result struct {
		name  string
		root  common.Hash
		stats Stats
	}

	bridges := []struct {
		name string
		bin  string
	}{
		{"geth", gethBin},
		{"erigon", erigonBin},
		{"besu", besuBin},
	}

	var results []result
	for _, br := range bridges {
		dir := t.TempDir()
		dbPath := filepath.Join(dir, "chaindata")

		config := Config{
			DBPath:       dbPath,
			NumAccounts:  numAccounts,
			NumContracts: numContracts,
			MaxSlots:     maxSlots,
			MinSlots:     minSlots,
			Distribution: Uniform,
			Seed:         seed,
			BatchSize:    1000,
			Workers:      1,
			CodeSize:     128,
			OutputFormat: OutputBridge,
			BridgeBin:    br.bin,
			BridgeArgs:   []string{"-db", dbPath},
		}

		gen, err := New(config)
		if err != nil {
			t.Fatalf("%s: failed to create generator: %v", br.name, err)
		}
		stats, err := gen.Generate()
		if err != nil {
			t.Fatalf("%s: failed to generate: %v", br.name, err)
		}
		gen.Close()

		if stats.StateRoot == (common.Hash{}) {
			t.Fatalf("%s: state root is zero", br.name)
		}

		results = append(results, result{br.name, stats.StateRoot, *stats})
		t.Logf("%s state root: %s (%d accounts, %d contracts, %d slots)",
			br.name, stats.StateRoot.Hex(), stats.AccountsCreated,
			stats.ContractsCreated, stats.StorageSlotsCreated)
	}

	// All bridges must produce the same state root
	for i := 1; i < len(results); i++ {
		if results[0].root != results[i].root {
			t.Errorf("state root mismatch:\n  %s: %s\n  %s: %s",
				results[0].name, results[0].root.Hex(),
				results[i].name, results[i].root.Hex())
		}
	}

	// All bridges must produce the same account/contract/slot counts
	for i := 1; i < len(results); i++ {
		if results[0].stats.AccountsCreated != results[i].stats.AccountsCreated {
			t.Errorf("accounts mismatch: %s=%d %s=%d",
				results[0].name, results[0].stats.AccountsCreated,
				results[i].name, results[i].stats.AccountsCreated)
		}
		if results[0].stats.StorageSlotsCreated != results[i].stats.StorageSlotsCreated {
			t.Errorf("storage slots mismatch: %s=%d %s=%d",
				results[0].name, results[0].stats.StorageSlotsCreated,
				results[i].name, results[i].stats.StorageSlotsCreated)
		}
	}

	t.Logf("Cross-client state root match: %s", results[0].root.Hex())
}

// TestCrossClientGenesisBlock generates state and writes genesis blocks via
// both bridges, then verifies that the genesis block hashes match.
// This ensures both clients can launch on the same genesis.
func TestCrossClientGenesisBlock(t *testing.T) {
	gethBin := buildGethBridge(t)
	erigonBin := buildErigonBridge(t)
	besuBin := buildBesuBridge(t)

	seed := int64(55555)

	bridges := []struct {
		name string
		bin  string
	}{
		{"geth", gethBin},
		{"erigon", erigonBin},
		{"besu", besuBin},
	}

	type result struct {
		name      string
		stateRoot common.Hash
		blockHash common.Hash
	}

	var results []result
	for _, br := range bridges {
		dir := t.TempDir()
		dbPath := filepath.Join(dir, "chaindata")

		config := Config{
			DBPath:       dbPath,
			NumAccounts:  15,
			NumContracts: 8,
			MaxSlots:     30,
			MinSlots:     3,
			Distribution: Uniform,
			Seed:         seed,
			BatchSize:    1000,
			Workers:      1,
			CodeSize:     64,
			OutputFormat: OutputBridge,
			BridgeBin:    br.bin,
			BridgeArgs:   []string{"-db", dbPath},
			WriteTrieNodes: true, // triggers genesis writing
		}

		gen, err := New(config)
		if err != nil {
			t.Fatalf("%s: failed to create generator: %v", br.name, err)
		}
		stats, err := gen.Generate()
		if err != nil {
			t.Fatalf("%s: failed to generate: %v", br.name, err)
		}

		// Write genesis via bridge
		bw := gen.BridgeWriter()
		if bw == nil {
			t.Fatalf("%s: not a bridge writer", br.name)
		}
		blockHash, err := bw.WriteGenesisBlockHash(testChainConfig(), stats.StateRoot)
		if err != nil {
			t.Fatalf("%s: failed to write genesis: %v", br.name, err)
		}
		gen.Close()

		results = append(results, result{br.name, stats.StateRoot, blockHash})
		t.Logf("%s: stateRoot=%s blockHash=%s", br.name, stats.StateRoot.Hex(), blockHash.Hex())
	}

	// All state roots must match
	for i := 1; i < len(results); i++ {
		if results[0].stateRoot != results[i].stateRoot {
			t.Errorf("state root mismatch:\n  %s: %s\n  %s: %s",
				results[0].name, results[0].stateRoot.Hex(),
				results[i].name, results[i].stateRoot.Hex())
		}
	}

	// All genesis block hashes must match
	for i := 1; i < len(results); i++ {
		if results[0].blockHash != results[i].blockHash {
			t.Errorf("genesis block hash mismatch:\n  %s: %s\n  %s: %s",
				results[0].name, results[0].blockHash.Hex(),
				results[i].name, results[i].blockHash.Hex())
		}
	}

	t.Logf("Cross-client genesis match: stateRoot=%s blockHash=%s",
		results[0].stateRoot.Hex(), results[0].blockHash.Hex())
}

// testChainConfig returns a standard post-merge chain config for testing.
func testChainConfig() *params.ChainConfig {
	return &params.ChainConfig{
		ChainID:                 big.NewInt(1337),
		HomesteadBlock:          big.NewInt(0),
		EIP150Block:             big.NewInt(0),
		EIP155Block:             big.NewInt(0),
		EIP158Block:             big.NewInt(0),
		ByzantiumBlock:          big.NewInt(0),
		ConstantinopleBlock:     big.NewInt(0),
		PetersburgBlock:         big.NewInt(0),
		IstanbulBlock:           big.NewInt(0),
		BerlinBlock:             big.NewInt(0),
		LondonBlock:             big.NewInt(0),
		TerminalTotalDifficulty: big.NewInt(0),
		ShanghaiTime:            new(uint64),
		CancunTime:              new(uint64),
	}
}

// buildGethBridge compiles the geth-bridge binary and returns its path.
// Skips the test if Go is not available or build fails.
func buildGethBridge(t *testing.T) string {
	t.Helper()

	// Check if pre-built binary exists
	projectRoot := findProjectRoot(t)
	bridgeDir := filepath.Join(projectRoot, "bridges", "geth")
	bridgeBin := filepath.Join(t.TempDir(), "geth-bridge")

	cmd := exec.Command("go", "build", "-o", bridgeBin, ".")
	cmd.Dir = bridgeDir
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Skipf("cannot build geth-bridge: %v", err)
	}

	return bridgeBin
}

// buildErigonBridge compiles the erigon-bridge binary and returns its path.
// Skips the test if Go is not available or build fails.
func buildErigonBridge(t *testing.T) string {
	t.Helper()

	projectRoot := findProjectRoot(t)
	bridgeDir := filepath.Join(projectRoot, "bridges", "erigon")
	bridgeBin := filepath.Join(t.TempDir(), "erigon-bridge")

	cmd := exec.Command("go", "build", "-o", bridgeBin, ".")
	cmd.Dir = bridgeDir
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Skipf("cannot build erigon-bridge: %v", err)
	}

	return bridgeBin
}

// buildBesuBridge builds the besu-bridge fat JAR and returns the path to a
// wrapper script that can be executed like a native binary.
// Skips the test if Java/Gradle is not available or build fails.
func buildBesuBridge(t *testing.T) string {
	t.Helper()

	projectRoot := findProjectRoot(t)
	bridgeDir := filepath.Join(projectRoot, "bridges", "besu")
	clientsDir := filepath.Join(projectRoot, "..", "clients")

	// Find the gradlew from the Besu project
	gradlew := filepath.Join(clientsDir, "besu", "gradlew")
	if _, err := os.Stat(gradlew); err != nil {
		t.Skipf("cannot find besu gradlew: %v", err)
	}

	// Build the fat JAR
	cmd := exec.Command(gradlew, "fatJar")
	cmd.Dir = bridgeDir
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Skipf("cannot build besu-bridge: %v", err)
	}

	// Find the fat JAR
	jarPattern := filepath.Join(bridgeDir, "build", "libs", "*-all.jar")
	jars, err := filepath.Glob(jarPattern)
	if err != nil || len(jars) == 0 {
		t.Skipf("cannot find besu-bridge fat JAR at %s", jarPattern)
	}
	fatJar := jars[0]

	// Create a wrapper script
	wrapperPath := filepath.Join(t.TempDir(), "besu-bridge")
	wrapper := fmt.Sprintf("#!/bin/sh\nexec java -jar %q \"$@\"\n", fatJar)
	if err := os.WriteFile(wrapperPath, []byte(wrapper), 0755); err != nil {
		t.Fatalf("write besu-bridge wrapper: %v", err)
	}

	return wrapperPath
}

// findProjectRoot walks up from the current directory to find go.mod.
func findProjectRoot(t *testing.T) string {
	t.Helper()

	// We're in generator/ package, project root is one level up
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}

	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			// Check this is the right go.mod (state-actor, not a sub-module)
			if _, err := os.Stat(filepath.Join(dir, "bridges", "geth")); err == nil {
				return dir
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not find project root")
		}
		dir = parent
	}
}
