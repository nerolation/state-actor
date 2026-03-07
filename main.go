// Package main provides a tool for generating realistic Ethereum state
// compatible with multiple execution clients (Geth, Erigon, Nethermind).
//
// Each client has a dedicated output format:
// - Geth:       Pebble database with snapshot layer (ready to use without geth init)
// - Erigon:     MDBX database with PlainState format
// - Nethermind: MDBX database with hashed keys and RLP encoding
//
// A post-merge genesis config is built programmatically from --chain-id.
// No genesis.json or chainspec file is needed; all data is written
// directly into each client's database.
package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/nerolation/state-actor/generator"
	genesispkg "github.com/nerolation/state-actor/genesis"
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
	injectAccounts = flag.String("inject-accounts", "", "Comma-separated hex addresses to inject with 999999999 ETH (e.g. 0xf39F...2266)")
	chainID        = flag.Int64("chain-id", 1, "Chain ID for the genesis config")

	// Output format
	outputFormat = flag.String("output-format", "geth", "Output format: 'geth' (Pebble DB), 'erigon' (MDBX), or 'nethermind' (chainspec JSON)")

	// Stats server
	statsPort = flag.Int("stats-port", 0, "Port for live stats HTTP server (0 = disabled)")
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

	// Start stats server if requested
	var statsServer *generator.StatsServer
	var liveStats *generator.LiveStats
	if *statsPort > 0 {
		statsServer = generator.NewStatsServer(*statsPort)
		liveStats = statsServer.Stats()
		liveStats.SetConfig(*accounts, *contracts, *outputFormat, *distribution, *seed)
		if err := statsServer.Start(); err != nil {
			log.Fatalf("Failed to start stats server: %v", err)
		}
		log.Printf("Stats server running on http://localhost:%d", *statsPort)
		defer statsServer.Stop()
	}

	// When --target-size is set, auto-scale parameters so the user's
	// --contracts value fills the target. The contract cap is raised to
	// MaxInt32 as a safety net (dirSize() is the real stopping condition).
	if parsedTargetSize > 0 {
		userContracts := *contracts
		*contracts = math.MaxInt32

		// Auto-scale min-slots so that userContracts contracts produce
		// enough trie entries to fill the target. Each contract has
		// ~overhead fixed entries (header + code chunks) plus storage
		// slots. Power-law average slots ≈ 3 × min_slots.
		if !isFlagSet("min-slots") {
			const bytesPerEntry uint64 = 80 // empirical after Pebble compression
			numC := uint64(userContracts)
			overhead := uint64(5 + (*codeSize+30)/31) // header fields + code chunks
			targetEntries := parsedTargetSize / bytesPerEntry
			entriesPerContract := targetEntries / numC
			if entriesPerContract > overhead {
				autoMin := int((entriesPerContract - overhead) / 3)
				if autoMin > *minSlots {
					*minSlots = autoMin
				}
			}
		}
		if !isFlagSet("max-slots") && *maxSlots < *minSlots*10 {
			*maxSlots = *minSlots * 10
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
		WriteTrieNodes:  true,
		InjectAddresses: injectAddrs,
		TargetSize:      parsedTargetSize,
		OutputFormat:    generator.ParseOutputFormat(*outputFormat),
		LiveStats:       liveStats,
	}

	// Build genesis config programmatically (no file needed).
	genesisConfig := genesispkg.DefaultGenesis(uint64(*chainID))

	if *verbose {
		log.Printf("Configuration:")
		log.Printf("  Database:     %s", config.DBPath)
		log.Printf("  Output Format: %s", config.OutputFormat)
		log.Printf("  Accounts:     %d", config.NumAccounts)
		if config.NumContracts >= math.MaxInt32 {
			log.Printf("  Contracts:    unlimited (governed by --target-size)")
		} else {
			log.Printf("  Contracts:    %d", config.NumContracts)
		}
		if parsedTargetSize > 0 && !isFlagSet("max-slots") {
			log.Printf("  Max Slots:    %d (auto-scaled for target size)", config.MaxSlots)
		} else {
			log.Printf("  Max Slots:    %d", config.MaxSlots)
		}
		if parsedTargetSize > 0 && !isFlagSet("min-slots") {
			log.Printf("  Min Slots:    %d (auto-scaled for target size)", config.MinSlots)
		} else {
			log.Printf("  Min Slots:    %d", config.MinSlots)
		}
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
		log.Printf("  Chain ID:     %d", *chainID)
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

	// Update live stats with final state
	if liveStats != nil {
		liveStats.AddBytes(int64(stats.AccountBytes), int64(stats.StorageBytes), int64(stats.CodeBytes))
		liveStats.SetStateRoot(stats.StateRoot.Hex())
	}

	// Write genesis data directly into each client's database.
	if *verbose {
		log.Printf("Writing genesis data with state root: %s", stats.StateRoot.Hex())
	}
	if err := gen.Writer().WriteGenesis(genesisConfig, stats.StateRoot, config.TrieMode == generator.TrieModeBinary); err != nil {
		log.Fatalf("Failed to write genesis: %v", err)
	}

	elapsed := time.Since(start)

	fmt.Printf("\n=== State Generation Complete ===\n")
	fmt.Printf("Total Time:        %v\n", elapsed.Round(time.Millisecond))
	fmt.Printf("Accounts Created:  %d\n", stats.AccountsCreated)
	fmt.Printf("Contracts Created: %d\n", stats.ContractsCreated)
	fmt.Printf("Storage Slots:     %d\n", stats.StorageSlotsCreated)
	fmt.Printf("Total Bytes:       %s\n", formatBytes(stats.TotalBytes))
	if stats.TrieNodeBytes > 0 {
		fmt.Printf("Trie Node Bytes:   %s\n", formatBytes(stats.TrieNodeBytes))
	}
	// Report actual on-disk size (after Pebble compression).
	if dbSize, err := dirSize(config.DBPath); err == nil {
		fmt.Printf("Total DB Size:     %s\n", formatBytes(dbSize))
	}
	fmt.Printf("Throughput:        %.2f slots/sec\n", float64(stats.StorageSlotsCreated)/elapsed.Seconds())
	fmt.Printf("State Root:        %s\n", stats.StateRoot.Hex())

	switch config.OutputFormat {
	case generator.OutputGeth:
		fmt.Printf("Genesis:           written to Pebble DB (no geth init needed)\n")
	case generator.OutputErigon:
		fmt.Printf("Genesis:           written to MDBX\n")
	case generator.OutputNethermind:
		fmt.Printf("Genesis:           written to MDBX\n")
	}

	if *benchmark {
		fmt.Printf("\n=== Detailed Stats ===\n")
		fmt.Printf("Account Bytes:     %s\n", formatBytes(stats.AccountBytes))
		fmt.Printf("Storage Bytes:     %s\n", formatBytes(stats.StorageBytes))
		fmt.Printf("Code Bytes:        %s\n", formatBytes(stats.CodeBytes))
		fmt.Printf("DB Write Time:     %v\n", stats.DBWriteTime.Round(time.Millisecond))
		fmt.Printf("Generation Time:   %v\n", stats.GenerationTime.Round(time.Millisecond))
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

	// Keep stats server running after completion if enabled
	if statsServer != nil {
		fmt.Printf("\n=== Stats server still running at http://localhost:%d ===\n", *statsPort)
		fmt.Printf("Press Ctrl+C to exit...\n")
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
		<-sigCh
		fmt.Println("\nShutting down...")
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

// isFlagSet returns true if the named flag was explicitly set on the command line.
func isFlagSet(name string) bool {
	found := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}

// dirSize returns the total size of all files in a directory tree.
func dirSize(path string) (uint64, error) {
	var total uint64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			total += uint64(info.Size())
		}
		return nil
	})
	return total, err
}
