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
	"os"
	"runtime"
	"time"

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
	binaryTrie   = flag.Bool("binary-trie", false, "Generate state for binary trie mode (EIP-7864)")

	// Genesis integration
	genesisPath = flag.String("genesis", "", "Path to genesis.json file (optional)")
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

	config := generator.Config{
		DBPath:       *dbPath,
		NumAccounts:  *accounts,
		NumContracts: *contracts,
		MaxSlots:     *maxSlots,
		MinSlots:     *minSlots,
		Distribution: generator.ParseDistribution(*distribution),
		Seed:         *seed,
		BatchSize:    *batchSize,
		Workers:      *workers,
		CodeSize:     *codeSize,
		Verbose:      *verbose,
		TrieMode:     trieMode,
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

		if *verbose {
			log.Printf("Loaded genesis with %d alloc accounts", len(config.GenesisAccounts))
		}
	}

	if *verbose {
		log.Printf("Configuration:")
		log.Printf("  Database:     %s", config.DBPath)
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

		block, err := genesis.WriteGenesisBlock(gen.DB(), genesisConfig, stats.StateRoot, config.TrieMode == generator.TrieModeBinary)
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
