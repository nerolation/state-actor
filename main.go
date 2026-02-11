// Package main provides a tool for generating realistic Ethereum state
// in a Pebble database compatible with go-ethereum.
//
// The tool writes state directly to the snapshot layer, which is the
// authoritative source for state in modern geth. The trie can be
// regenerated from snapshots on node startup.
//
// When a genesis file is provided, the tool:
// 1. Includes genesis alloc accounts in state generation
// 2. Computes the combined state root
// 3. Writes the genesis block with the correct state root
// 4. Produces a database ready to use without `geth init`
package main

import (
	"flag"
	"fmt"
	"log"
	"math/big"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/nerolation/state-actor/generator"
	"github.com/nerolation/state-actor/genesis"
)

var (
	dbPath       = flag.String("db", "", "Path to the database directory (required)")
	accounts     = flag.Int("accounts", 1000, "Number of EOA accounts to create")
	contracts    = flag.Int("contracts", 100, "Number of contracts to create")
	maxSlots     = flag.Int("max-slots", 10000, "Maximum storage slots per contract")
	minSlots     = flag.Int("min-slots", 1, "Minimum storage slots per contract")
	distribution = flag.String("distribution", "power-law", "Storage distribution: 'power-law', 'uniform', or 'exponential'")
	seed         = flag.Int64("seed", 0, "Random seed (0 = use current time)")
	batchSize    = flag.Int("batch-size", 10000, "Database batch size")
	workers      = flag.Int("workers", 0, "Number of parallel workers (0 = NumCPU)")
	codeSize     = flag.Int("code-size", 1024, "Average contract code size in bytes")
	verbose      = flag.Bool("verbose", false, "Verbose output")
	benchmark    = flag.Bool("benchmark", false, "Run in benchmark mode (print detailed stats)")
	binaryTrie     = flag.Bool("binary-trie", false, "Generate state for binary trie mode (EIP-7864)")
	commitInterval = flag.Int("commit-interval", 500000, "Binary trie: commit to disk every N trie insertions (default 500K, 0 = all in-memory)")

	// Target size
	targetSize = flag.String("target-size", "", "Target total DB size on disk (e.g. '5GB', '500MB'). Stops generating when estimated size is reached.")

	// Genesis integration
	genesisPath    = flag.String("genesis", "", "Path to genesis.json file (optional)")
	injectAccounts = flag.String("inject-accounts", "", "Comma-separated hex addresses to inject with 999999999 ETH (e.g. 0xf39F...2266)")
	chainID        = flag.Int64("chain-id", 0, "Override genesis chainId (0 = use value from genesis.json)")

	// Output format
	outputFormat = flag.String("output-format", "geth", "Output database format: 'geth' (Pebble) or 'erigon' (MDBX)")
)

func main() {
	flag.Parse()

	if *dbPath == "" {
		fmt.Fprintln(os.Stderr, "Error: -db flag is required")
		flag.Usage()
		os.Exit(1)
	}

	if *workers == 0 {
		*workers = runtime.NumCPU()
	}

	if *seed == 0 {
		*seed = time.Now().UnixNano()
	}

	trieMode := generator.TrieModeMPT
	if *binaryTrie {
		trieMode = generator.TrieModeBinary
	}

	// Parse --inject-accounts
	var injectAddrs []common.Address
	if *injectAccounts != "" {
		for _, s := range strings.Split(*injectAccounts, ",") {
			s = strings.TrimSpace(s)
			if s == "" {
				continue
			}
			if !common.IsHexAddress(s) {
				log.Fatalf("Invalid address in --inject-accounts: %q", s)
			}
			injectAddrs = append(injectAddrs, common.HexToAddress(s))
		}
	}

	// Parse --target-size
	var parsedTargetSize uint64
	if *targetSize != "" {
		var err error
		parsedTargetSize, err = parseSize(*targetSize)
		if err != nil {
			log.Fatalf("Invalid --target-size: %v", err)
		}
	}

	config := generator.Config{
		DBPath:          *dbPath,
		NumAccounts:     *accounts,
		NumContracts:    *contracts,
		MaxSlots:        *maxSlots,
		MinSlots:        *minSlots,
		Distribution:    generator.ParseDistribution(*distribution),
		Seed:            *seed,
		BatchSize:       *batchSize,
		Workers:         *workers,
		CodeSize:        *codeSize,
		Verbose:         *verbose,
		TrieMode:        trieMode,
		CommitInterval:  *commitInterval,
		WriteTrieNodes:  *genesisPath != "",
		InjectAddresses: injectAddrs,
		TargetSize:      parsedTargetSize,
		OutputFormat:    generator.ParseOutputFormat(*outputFormat),
	}

	// Load genesis if provided
	var genesisConfig *genesis.Genesis
	if *genesisPath != "" {
		var err error
		genesisConfig, err = genesis.LoadGenesis(*genesisPath)
		if err != nil {
			log.Fatalf("Failed to load genesis: %v", err)
		}

		// Extract accounts from genesis alloc
		config.GenesisAccounts = genesisConfig.ToStateAccounts()
		config.GenesisStorage = genesisConfig.GetAllocStorage()
		config.GenesisCode = genesisConfig.GetAllocCode()

		// Override chain ID if requested
		if *chainID != 0 {
			genesisConfig.Config.ChainID = big.NewInt(*chainID)
		}

		if *verbose {
			log.Printf("Loaded genesis with %d alloc accounts (chainId=%s)",
				len(config.GenesisAccounts), genesisConfig.Config.ChainID)
		}
	}

	if *verbose {
		log.Printf("Configuration:")
		log.Printf("  Database:     %s", config.DBPath)
		log.Printf("  Output Format: %s", config.OutputFormat)
		log.Printf("  Accounts:     %d", config.NumAccounts)
		log.Printf("  Contracts:    %d", config.NumContracts)
		log.Printf("  Max Slots:    %d", config.MaxSlots)
		log.Printf("  Min Slots:    %d", config.MinSlots)
		log.Printf("  Distribution: %s", *distribution)
		log.Printf("  Seed:         %d", config.Seed)
		log.Printf("  Batch Size:   %d", config.BatchSize)
		log.Printf("  Workers:      %d", config.Workers)
		log.Printf("  Code Size:    %d bytes", config.CodeSize)
		log.Printf("  Trie Mode:    %s", config.TrieMode)
		if config.CommitInterval > 0 {
			log.Printf("  Commit Interval: %d trie insertions", config.CommitInterval)
		}
		if config.TargetSize > 0 {
			log.Printf("  Target Size:  %s", formatBytes(config.TargetSize))
		}
		if *genesisPath != "" {
			log.Printf("  Genesis:      %s", *genesisPath)
		}
	}

	start := time.Now()

	gen, err := generator.New(config)
	if err != nil {
		log.Fatalf("Failed to create generator: %v", err)
	}
	defer gen.Close()

	stats, err := gen.Generate()
	if err != nil {
		log.Fatalf("Failed to generate state: %v", err)
	}

	// Write genesis block if genesis was provided
	if genesisConfig != nil {
		if *verbose {
			log.Printf("Writing genesis block with state root: %s", stats.StateRoot.Hex())
		}

		ancientDir := filepath.Join(config.DBPath, "ancient")
		block, err := genesis.WriteGenesisBlock(gen.DB(), genesisConfig, stats.StateRoot, config.TrieMode == generator.TrieModeBinary, ancientDir)
		if err != nil {
			log.Fatalf("Failed to write genesis block: %v", err)
		}

		if *verbose {
			log.Printf("Genesis block hash: %s", block.Hash().Hex())
			log.Printf("Genesis block number: %d", block.NumberU64())
		}
	}

	elapsed := time.Since(start)

	fmt.Printf("\n=== State Generation Complete ===\n")
	fmt.Printf("Total Time:        %v\n", elapsed.Round(time.Millisecond))
	fmt.Printf("Accounts Created:  %d\n", stats.AccountsCreated)
	fmt.Printf("Contracts Created: %d\n", stats.ContractsCreated)
	fmt.Printf("Storage Slots:     %d\n", stats.StorageSlotsCreated)
	fmt.Printf("Total Bytes:       %s\n", formatBytes(stats.TotalBytes))
	fmt.Printf("Throughput:        %.2f slots/sec\n", float64(stats.StorageSlotsCreated)/elapsed.Seconds())
	fmt.Printf("State Root:        %s\n", stats.StateRoot.Hex())

	if genesisConfig != nil {
		fmt.Printf("Genesis:           included (ready to use without geth init)\n")
	}

	if *benchmark {
		fmt.Printf("\n=== Detailed Stats ===\n")
		fmt.Printf("Account Bytes:     %s\n", formatBytes(stats.AccountBytes))
		fmt.Printf("Storage Bytes:     %s\n", formatBytes(stats.StorageBytes))
		fmt.Printf("Code Bytes:        %s\n", formatBytes(stats.CodeBytes))
		fmt.Printf("DB Write Time:     %v\n", stats.DBWriteTime.Round(time.Millisecond))
		fmt.Printf("Generation Time:   %v\n", stats.GenerationTime.Round(time.Millisecond))
		if len(config.GenesisAccounts) > 0 {
			fmt.Printf("Genesis Accounts:  %d\n", len(config.GenesisAccounts))
		}

		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		fmt.Printf("\n=== Memory Stats ===\n")
		fmt.Printf("Total Alloc:       %s\n", formatBytes(m.TotalAlloc))
		fmt.Printf("Current Alloc:     %s\n", formatBytes(m.Alloc))
		fmt.Printf("Sys Memory:        %s\n", formatBytes(m.Sys))
	}

	// Print sample addresses for verification
	if len(stats.SampleEOAs) > 0 {
		fmt.Printf("\n=== Sample Addresses (for verification) ===\n")
		for i, addr := range stats.SampleEOAs {
			fmt.Printf("  EOA #%d:      %s\n", i+1, addr.Hex())
		}
		for i, addr := range stats.SampleContracts {
			fmt.Printf("  Contract #%d: %s\n", i+1, addr.Hex())
		}
	}
}

func formatBytes(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

// parseSize parses a human-readable size string (e.g. "5GB", "500MB", "1TB")
// into bytes. Supports KB, MB, GB, TB suffixes (case-insensitive, base-1024).
func parseSize(s string) (uint64, error) {
	s = strings.TrimSpace(s)
	upper := strings.ToUpper(s)

	suffixes := []struct {
		suffix string
		mult   uint64
	}{
		{"TB", 1 << 40},
		{"GB", 1 << 30},
		{"MB", 1 << 20},
		{"KB", 1 << 10},
	}

	for _, sf := range suffixes {
		if strings.HasSuffix(upper, sf.suffix) {
			numStr := strings.TrimSpace(s[:len(s)-len(sf.suffix)])
			val, err := strconv.ParseFloat(numStr, 64)
			if err != nil {
				return 0, fmt.Errorf("invalid number %q in size %q", numStr, s)
			}
			if val <= 0 {
				return 0, fmt.Errorf("size must be positive: %s", s)
			}
			return uint64(val * float64(sf.mult)), nil
		}
	}

	// Plain number = bytes
	val, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size format %q (use e.g. '5GB', '500MB')", s)
	}
	return val, nil
}
