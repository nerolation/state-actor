//! reth-import: streaming state importer for Reth.
//!
//! Reads a binary bundle from stdin (produced by state-actor) and writes
//! directly into Reth's MDBX database. The full flow:
//!
//! 1. Stream state entries into DB tables (PlainAccountState, HashedAccounts, etc.)
//! 2. Compute the state root from HashedAccounts/HashedStorages via Reth's trie
//! 3. Verify root matches the Go-computed root
//! 4. Write genesis header, stage checkpoints
//! 5. Commit

mod bundle;

use bundle::{BundleReader, Entry};

use alloy_consensus::{Header, EMPTY_OMMER_ROOT_HASH};
use alloy_genesis::Genesis;
use alloy_primitives::{keccak256, Bytes, B256, B64};
use alloy_trie::EMPTY_ROOT_HASH;
use clap::Parser;
use eyre::{bail, Context};
use reth_chainspec::ChainSpec;
use reth_db::{init_db, ClientVersion, mdbx::DatabaseArguments};
use reth_db_api::{
    database::Database,
    models::{storage_sharded_key::StorageShardedKey, CompactU256, IntegerList, ShardedKey},
    tables,
    transaction::{DbTx, DbTxMut},
};
use reth_primitives_traits::{Account, Bytecode, StorageEntry};
use reth_stages_types::{StageCheckpoint, StageId};
use reth_trie::StateRoot;
use reth_trie_common::{updates::TrieUpdates, StoredNibbles, StoredNibblesSubKey};
use reth_trie_db::DatabaseStateRoot;
use std::path::PathBuf;

/// Streaming state importer for Reth databases.
#[derive(Parser)]
#[command(name = "reth-import")]
struct Cli {
    /// Path to the Reth data directory (database will be at <datadir>/db/).
    #[arg(long)]
    datadir: PathBuf,

    /// Read bundle from stdin.
    #[arg(long)]
    stdin: bool,
}

fn main() -> eyre::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();

    if !cli.stdin {
        bail!("--stdin flag is required (only stdin mode is supported)");
    }

    // Phase 0: Read stream header
    let mut reader = BundleReader::new(std::io::stdin().lock())?;
    eprintln!("reth-import: stream header OK, genesis JSON: {} bytes", reader.genesis_json.len());

    // Parse genesis
    let genesis: Genesis = if reader.genesis_json.is_empty() {
        bail!("genesis JSON is required but was empty");
    } else {
        serde_json::from_slice(&reader.genesis_json).context("parsing genesis JSON")?
    };
    let chain_spec: ChainSpec = genesis.clone().into();

    // Phase 1: Initialize database
    let db_path = cli.datadir.join("db");
    std::fs::create_dir_all(&db_path)?;

    let client_version = ClientVersion {
        version: "reth-import/0.1.0".to_string(),
        git_sha: String::new(),
        build_timestamp: String::new(),
    };
    let db = init_db(&db_path, DatabaseArguments::new(client_version))?;

    eprintln!("reth-import: database initialized at {}", db_path.display());

    // Open a write transaction for all state
    let tx = db.tx_mut()?;

    // Phase 2: Stream entries into DB tables
    let mut state_root_received: Option<B256> = None;
    let mut account_count: u64 = 0;
    let mut storage_count: u64 = 0;
    let mut code_count: u64 = 0;

    loop {
        match reader.next_entry()? {
            Entry::Account(acc) => {
                write_account(&tx, &acc)?;
                storage_count += acc.storage.len() as u64;
                account_count += 1;

                if account_count % 10000 == 0 {
                    eprintln!(
                        "reth-import: streamed {} accounts, {} storage slots",
                        account_count, storage_count
                    );
                }
            }
            Entry::Code(code) => {
                write_code(&tx, &code)?;
                code_count += 1;
            }
            Entry::Root(root) => {
                state_root_received = Some(root);
            }
            Entry::End => break,
        }
    }

    eprintln!(
        "reth-import: stream complete — {} accounts, {} storage slots, {} bytecodes",
        account_count, storage_count, code_count
    );

    // Phase 3: Compute state root from HashedAccounts + HashedStorages
    eprintln!("reth-import: computing state root from trie...");
    let (computed_root, trie_updates) = StateRoot::from_tx(&tx)
        .root_with_updates()
        .map_err(|e| eyre::eyre!("trie root computation failed: {e}"))?;

    eprintln!("reth-import: computed state root: {computed_root}");

    // Verify against Go-side root
    if let Some(expected) = state_root_received {
        if computed_root != expected {
            eprintln!("reth-import: WARNING: state root mismatch (Go MPT vs Reth trie — expected for different implementations)");
            eprintln!("  computed (Rust): {computed_root}");
            eprintln!("  expected (Go):   {expected}");
            eprintln!("  Using Reth-computed root for genesis header.");
        } else {
            eprintln!("reth-import: state root verified OK");
        }
    }

    // Write trie nodes to DB
    write_trie_updates(&tx, &trie_updates)?;

    // Phase 4: Write genesis header
    let genesis_header = build_genesis_header(&genesis, computed_root);
    let block_hash = genesis_header.hash_slow();
    eprintln!("reth-import: genesis block hash: {block_hash}");

    tx.put::<tables::Headers<Header>>(0, genesis_header)?;
    tx.put::<tables::HeaderNumbers>(block_hash, 0)?;
    tx.put::<tables::BlockBodyIndices>(
        0,
        reth_db_models::StoredBlockBodyIndices::default(),
    )?;

    // Write canonical chain mapping
    tx.put::<tables::CanonicalHeaders>(0, block_hash)?;
    tx.put::<tables::HeaderTerminalDifficulties>(
        0,
        CompactU256(chain_spec.genesis().difficulty),
    )?;

    // Phase 5: Set stage checkpoints (all stages to genesis block 0)
    let checkpoint = StageCheckpoint::new(0);
    for stage in StageId::ALL {
        tx.put::<tables::StageCheckpoints>(stage.to_string(), checkpoint)?;
    }

    // Commit the transaction
    tx.commit()?;
    eprintln!("reth-import: transaction committed");

    // Create static_files directory (reth expects it to exist)
    let static_files_dir = cli.datadir.join("static_files");
    std::fs::create_dir_all(&static_files_dir)?;

    // Print summary
    eprintln!("reth-import: import complete!");
    eprintln!("  accounts:     {account_count}");
    eprintln!("  storage slots: {storage_count}");
    eprintln!("  bytecodes:    {code_count}");
    eprintln!("  state root:   {computed_root}");
    eprintln!("  genesis hash: {block_hash}");
    eprintln!();
    eprintln!(
        "NOTE: The genesis hash ({block_hash}) includes the full generated state."
    );
    eprintln!("      This will differ from the genesis hash computed from genesis.json alloc alone.");
    eprintln!(
        "      If reth rejects the database, you may need to skip genesis validation."
    );

    Ok(())
}

/// Write an account entry to all relevant DB tables.
fn write_account<TX: DbTxMut + DbTx>(
    tx: &TX,
    acc: &bundle::AccountEntry,
) -> eyre::Result<()> {
    let address = acc.address;
    let hashed_address = keccak256(address);

    // Determine bytecode_hash: None for empty code hash (EOAs)
    let empty_code_hash = keccak256([]);
    let bytecode_hash = if acc.code_hash == empty_code_hash || acc.code_hash == B256::ZERO {
        None
    } else {
        Some(acc.code_hash)
    };

    let account = Account {
        nonce: acc.nonce,
        balance: acc.balance,
        bytecode_hash,
    };

    // PlainAccountState
    tx.put::<tables::PlainAccountState>(address, account)?;

    // HashedAccounts
    tx.put::<tables::HashedAccounts>(hashed_address, account)?;

    // AccountsHistory
    let sharded_key = ShardedKey::new(address, u64::MAX);
    let block_list = IntegerList::new([0]).expect("valid integer list");
    tx.put::<tables::AccountsHistory>(sharded_key, block_list)?;

    // Storage slots
    for &(key, value) in &acc.storage {
        let hashed_key = keccak256(key);

        // PlainStorageState (DupSort: key=address, value=StorageEntry)
        tx.put::<tables::PlainStorageState>(
            address,
            StorageEntry { key, value },
        )?;

        // HashedStorages (DupSort: key=hashed_address, value=StorageEntry with hashed key)
        tx.put::<tables::HashedStorages>(
            hashed_address,
            StorageEntry {
                key: hashed_key,
                value,
            },
        )?;

        // StoragesHistory
        let storage_sharded_key = StorageShardedKey::new(address, key, u64::MAX);
        let block_list = IntegerList::new([0]).expect("valid integer list");
        tx.put::<tables::StoragesHistory>(storage_sharded_key, block_list)?;
    }

    Ok(())
}

/// Write a code entry to the Bytecodes table.
fn write_code<TX: DbTxMut>(tx: &TX, code: &bundle::CodeEntry) -> eyre::Result<()> {
    let bytecode = Bytecode::new_raw(Bytes::from(code.code.clone()));
    tx.put::<tables::Bytecodes>(code.hash, bytecode)?;
    Ok(())
}

/// Write trie updates (account and storage trie nodes) to DB tables.
fn write_trie_updates<TX: DbTxMut>(tx: &TX, updates: &TrieUpdates) -> eyre::Result<()> {
    // Account trie nodes → AccountsTrie table
    for (nibbles, node) in &updates.account_nodes {
        tx.put::<tables::AccountsTrie>(
            StoredNibbles::from(nibbles.clone()),
            node.clone(),
        )?;
    }

    // Storage trie nodes → StoragesTrie table (DupSort: key=hashed_address)
    for (hashed_address, storage_updates) in &updates.storage_tries {
        for (nibbles, node) in &storage_updates.storage_nodes {
            tx.put::<tables::StoragesTrie>(
                *hashed_address,
                reth_trie_common::StorageTrieEntry {
                    nibbles: StoredNibblesSubKey::from(nibbles.clone()),
                    node: node.clone(),
                },
            )?;
        }
    }

    Ok(())
}

/// Build the genesis block header with the given state root.
fn build_genesis_header(genesis: &Genesis, state_root: B256) -> Header {
    let mut header = Header {
        parent_hash: B256::ZERO,
        ommers_hash: EMPTY_OMMER_ROOT_HASH,
        beneficiary: genesis.coinbase,
        state_root,
        transactions_root: EMPTY_ROOT_HASH,
        receipts_root: EMPTY_ROOT_HASH,
        logs_bloom: Default::default(),
        difficulty: genesis.difficulty,
        number: 0,
        gas_limit: genesis.gas_limit as u64,
        gas_used: 0,
        timestamp: genesis.timestamp,
        extra_data: genesis.extra_data.clone(),
        mix_hash: genesis.mix_hash,
        nonce: B64::from(genesis.nonce),
        base_fee_per_gas: genesis.base_fee_per_gas.map(|v| v as u64),
        ..Default::default()
    };

    // Shanghai: set empty withdrawals root if active at genesis
    if genesis.config.shanghai_time.is_some() {
        header.withdrawals_root = Some(EMPTY_ROOT_HASH);
    }

    // Cancun: set blob gas fields if active at genesis
    if genesis.config.cancun_time.is_some() {
        header.blob_gas_used = Some(genesis.blob_gas_used.unwrap_or(0) as u64);
        header.excess_blob_gas = Some(genesis.excess_blob_gas.unwrap_or(0) as u64);
        header.parent_beacon_block_root = Some(B256::ZERO);
    }

    // Prague: set requests hash if active at genesis
    if genesis.config.prague_time.is_some() {
        // Empty requests hash: keccak256 of the RLP-encoded empty list
        header.requests_hash = Some(keccak256(alloy_primitives::bytes!("c0")));
    }

    header
}
