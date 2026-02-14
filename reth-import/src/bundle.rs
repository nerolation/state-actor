//! Streaming reader for the state-actor binary bundle protocol.
//!
//! Protocol format (over stdin pipe):
//! ```text
//! Header:
//!   [4B magic "SARB"]
//!   [4B version (1) LE]
//!   [4B genesis_json_len LE]
//!   [genesis_json bytes]
//!
//! Entries:
//!   0x01 Account: [20B addr][8B nonce LE][32B balance BE][32B code_hash]
//!                 [4B storage_count LE][repeated: 32B key, 32B value]
//!   0x02 Code:    [32B code_hash][4B code_len LE][code bytes]
//!   0x03 Root:    [32B state_root]
//!   0xFF End
//! ```

use alloy_primitives::{Address, B256, U256};
use eyre::{bail, ensure, Context};
use std::io::{BufReader, Read};

const MAGIC: [u8; 4] = *b"SARB";
const VERSION: u32 = 1;

const TAG_ACCOUNT: u8 = 0x01;
const TAG_CODE: u8 = 0x02;
const TAG_ROOT: u8 = 0x03;
const TAG_END: u8 = 0xFF;

/// A single entry from the bundle stream.
pub enum Entry {
    Account(AccountEntry),
    Code(CodeEntry),
    Root(B256),
    End,
}

/// An account entry with inline storage slots.
pub struct AccountEntry {
    pub address: Address,
    pub nonce: u64,
    pub balance: U256,
    pub code_hash: B256,
    pub storage: Vec<(B256, U256)>,
}

/// A bytecode entry.
pub struct CodeEntry {
    pub hash: B256,
    pub code: Vec<u8>,
}

/// Streaming reader that parses the binary bundle protocol.
pub struct BundleReader<R: Read> {
    reader: BufReader<R>,
    pub genesis_json: Vec<u8>,
    done: bool,
}

impl<R: Read> BundleReader<R> {
    /// Creates a new BundleReader, reading and validating the stream header.
    pub fn new(inner: R) -> eyre::Result<Self> {
        let mut reader = BufReader::with_capacity(4 * 1024 * 1024, inner);

        // Read magic
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic).context("reading magic")?;
        ensure!(magic == MAGIC, "invalid magic: expected SARB, got {magic:?}");

        // Read version
        let mut ver_buf = [0u8; 4];
        reader.read_exact(&mut ver_buf).context("reading version")?;
        let version = u32::from_le_bytes(ver_buf);
        ensure!(version == VERSION, "unsupported version: {version}");

        // Read genesis JSON
        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf).context("reading genesis length")?;
        let genesis_len = u32::from_le_bytes(len_buf) as usize;

        let mut genesis_json = vec![0u8; genesis_len];
        if genesis_len > 0 {
            reader
                .read_exact(&mut genesis_json)
                .context("reading genesis JSON")?;
        }

        Ok(Self {
            reader,
            genesis_json,
            done: false,
        })
    }

    /// Reads the next entry from the stream.
    pub fn next_entry(&mut self) -> eyre::Result<Entry> {
        if self.done {
            return Ok(Entry::End);
        }

        let mut tag = [0u8; 1];
        self.reader.read_exact(&mut tag).context("reading tag")?;

        match tag[0] {
            TAG_ACCOUNT => self.read_account(),
            TAG_CODE => self.read_code(),
            TAG_ROOT => self.read_root(),
            TAG_END => {
                self.done = true;
                Ok(Entry::End)
            }
            other => bail!("unknown entry tag: 0x{other:02x}"),
        }
    }

    fn read_account(&mut self) -> eyre::Result<Entry> {
        // Address (20B)
        let mut addr = [0u8; 20];
        self.reader.read_exact(&mut addr)?;

        // Nonce (8B LE)
        let mut nonce_buf = [0u8; 8];
        self.reader.read_exact(&mut nonce_buf)?;
        let nonce = u64::from_le_bytes(nonce_buf);

        // Balance (32B BE)
        let mut bal_buf = [0u8; 32];
        self.reader.read_exact(&mut bal_buf)?;
        let balance = U256::from_be_bytes(bal_buf);

        // Code hash (32B)
        let mut code_hash = [0u8; 32];
        self.reader.read_exact(&mut code_hash)?;

        // Storage count (4B LE)
        let mut count_buf = [0u8; 4];
        self.reader.read_exact(&mut count_buf)?;
        let storage_count = u32::from_le_bytes(count_buf) as usize;

        // Storage slots
        let mut storage = Vec::with_capacity(storage_count);
        for _ in 0..storage_count {
            let mut key = [0u8; 32];
            self.reader.read_exact(&mut key)?;
            let mut value = [0u8; 32];
            self.reader.read_exact(&mut value)?;
            storage.push((B256::from(key), U256::from_be_bytes(value)));
        }

        Ok(Entry::Account(AccountEntry {
            address: Address::from(addr),
            nonce,
            balance,
            code_hash: B256::from(code_hash),
            storage,
        }))
    }

    fn read_code(&mut self) -> eyre::Result<Entry> {
        // Code hash (32B)
        let mut hash = [0u8; 32];
        self.reader.read_exact(&mut hash)?;

        // Code length (4B LE)
        let mut len_buf = [0u8; 4];
        self.reader.read_exact(&mut len_buf)?;
        let code_len = u32::from_le_bytes(len_buf) as usize;

        // Code bytes
        let mut code = vec![0u8; code_len];
        if code_len > 0 {
            self.reader.read_exact(&mut code)?;
        }

        Ok(Entry::Code(CodeEntry {
            hash: B256::from(hash),
            code,
        }))
    }

    fn read_root(&mut self) -> eyre::Result<Entry> {
        let mut root = [0u8; 32];
        self.reader.read_exact(&mut root)?;
        Ok(Entry::Root(B256::from(root)))
    }
}
