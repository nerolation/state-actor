package stateactor;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.core.Withdrawal;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.storage.keyvalue.WorldStatePreimageKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.forest.storage.ForestWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.forest.worldview.ForestMutableWorldState;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

/**
 * Besu bridge: reads the binary protocol from stdin, buffers state in memory,
 * uses Besu's Forest-mode Merkle Patricia Trie to compute state roots,
 * and persists everything to RocksDB.
 */
public class BesuBridge {

    // Command types (must match protocol.go)
    static final byte CMD_PUT_ACCOUNT = 0x01;
    static final byte CMD_PUT_STORAGE = 0x02;
    static final byte CMD_PUT_CODE = 0x03;
    static final byte CMD_FLUSH = 0x04;
    static final byte CMD_COMPUTE_ROOT = 0x05;
    static final byte CMD_WRITE_GENESIS = 0x06;
    static final byte CMD_CLOSE = 0x07;

    // Response status codes
    static final byte STATUS_OK = 0x00;
    static final byte STATUS_ERROR = (byte) 0xFF;

    // Empty code hash (keccak256 of empty bytes)
    static final Hash EMPTY_CODE_HASH =
            Hash.fromHexString("c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470");

    // RocksDB column family IDs matching Besu's KeyValueSegmentIdentifier
    static final byte[] CF_BLOCKCHAIN = new byte[]{1};
    static final byte[] CF_WORLD_STATE = new byte[]{2};
    static final byte[] CF_VARIABLES = new byte[]{11};

    // Blockchain storage key prefixes (from KeyValueStoragePrefixedKeyBlockchainStorage)
    static final byte PREFIX_BLOCK_HEADER = 0x02;
    static final byte PREFIX_BLOCK_BODY = 0x03;
    static final byte PREFIX_TX_RECEIPTS = 0x04;
    static final byte PREFIX_BLOCK_HASH = 0x05;
    static final byte PREFIX_TOTAL_DIFFICULTY = 0x06;

    // Variables storage key (from VariablesStorage.Keys)
    static final byte[] KEY_CHAIN_HEAD_HASH = "chainHeadHash".getBytes(StandardCharsets.UTF_8);

    // RocksDB handles
    private RocksDB db;
    private ColumnFamilyHandle hBlockchain;
    private ColumnFamilyHandle hWorldState;
    private ColumnFamilyHandle hVariables;

    // Persistent world state storage backed by RocksDB
    private ForestWorldStateKeyValueStorage worldStateStorage;

    // Buffered state
    private final Map<Address, AccountRecord> accounts = new HashMap<>();
    private final Map<Hash, byte[]> code = new HashMap<>();
    private Exception lastErr = null;

    static class AccountRecord {
        long nonce;
        Wei balance;
        Hash codeHash;
        final Map<UInt256, UInt256> storage = new HashMap<>();
    }

    public BesuBridge(String dataDir) throws RocksDBException {
        openDatabase(dataDir);
        writeDatabaseMetadata(dataDir);
        var wsKvs = new RocksDBKeyValueStorage(db, hWorldState);
        worldStateStorage = new ForestWorldStateKeyValueStorage(wsKvs);
    }

    private void writeDatabaseMetadata(String dataDir) {
        // Besu requires DATABASE_METADATA.json in the data directory.
        // Format: {"v2":{"format":"FOREST","version":3}}
        // Version 3 = FOREST_WITH_RECEIPT_COMPACTION (current default for new Forest DBs)
        Path metadataPath = Path.of(dataDir, "DATABASE_METADATA.json");
        try {
            Files.writeString(metadataPath,
                    "{\"v2\":{\"format\":\"FOREST\",\"version\":3}}\n",
                    StandardCharsets.UTF_8);
            System.err.println("[besu-bridge] wrote " + metadataPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write DATABASE_METADATA.json", e);
        }
    }

    private void openDatabase(String dataDir) throws RocksDBException {
        RocksDB.loadLibrary();

        Path dbPath = Path.of(dataDir, "database");
        try {
            Files.createDirectories(dbPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create database directory: " + dbPath, e);
        }

        var cfOpts = new ColumnFamilyOptions();
        var cfDescriptors = List.of(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts),
                new ColumnFamilyDescriptor(CF_BLOCKCHAIN, cfOpts),
                new ColumnFamilyDescriptor(CF_WORLD_STATE, cfOpts),
                new ColumnFamilyDescriptor(CF_VARIABLES, cfOpts));

        var cfHandles = new ArrayList<ColumnFamilyHandle>();
        var dbOpts = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true);

        db = RocksDB.open(dbOpts, dbPath.toString(), cfDescriptors, cfHandles);
        // cfHandles[0] is DEFAULT (unused), [1] BLOCKCHAIN, [2] WORLD_STATE, [3] VARIABLES
        hBlockchain = cfHandles.get(1);
        hWorldState = cfHandles.get(2);
        hVariables = cfHandles.get(3);

        System.err.println("[besu-bridge] opened RocksDB at " + dbPath);
    }

    public static void main(String[] args) throws Exception {
        // Redirect SLF4J simple logger to stderr
        System.setProperty("org.slf4j.simpleLogger.logFile", "System.err");
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn");

        String dataDir = null;
        for (int i = 0; i < args.length; i++) {
            if ("-db".equals(args[i]) && i + 1 < args.length) {
                dataDir = args[++i];
            }
        }
        if (dataDir == null) {
            System.err.println("[besu-bridge] -db flag is required");
            System.exit(1);
        }

        var bridge = new BesuBridge(dataDir);
        System.err.println("[besu-bridge] ready, datadir=" + dataDir);

        var in = new DataInputStream(new BufferedInputStream(System.in, 4 * 1024 * 1024));
        var out = new DataOutputStream(new BufferedOutputStream(System.out, 4 * 1024 * 1024));

        while (true) {
            byte cmd;
            byte[] payload;
            try {
                cmd = in.readByte();
                int plen = readUint32LE(in);
                payload = (plen > 0) ? in.readNBytes(plen) : new byte[0];
            } catch (java.io.EOFException e) {
                System.err.println("[besu-bridge] stdin closed, exiting");
                break;
            }

            switch (cmd) {
                case CMD_PUT_ACCOUNT:
                    try { bridge.putAccount(payload); }
                    catch (Exception e) { bridge.lastErr = e; }
                    break;

                case CMD_PUT_STORAGE:
                    try { bridge.putStorage(payload); }
                    catch (Exception e) { bridge.lastErr = e; }
                    break;

                case CMD_PUT_CODE:
                    try { bridge.putCode(payload); }
                    catch (Exception e) { bridge.lastErr = e; }
                    break;

                case CMD_FLUSH:
                    bridge.respond(out);
                    break;

                case CMD_COMPUTE_ROOT:
                    Hash root = null;
                    try { root = bridge.computeRoot(); }
                    catch (Exception e) { bridge.lastErr = e; }
                    bridge.respondWithHash(out, root);
                    break;

                case CMD_WRITE_GENESIS:
                    Hash blockHash = null;
                    try { blockHash = bridge.writeGenesis(payload); }
                    catch (Exception e) { bridge.lastErr = e; }
                    bridge.respondWithHash(out, blockHash);
                    break;

                case CMD_CLOSE:
                    bridge.respond(out);
                    out.flush();
                    bridge.close();
                    return;

                default:
                    System.err.println("[besu-bridge] unknown command 0x" +
                            String.format("%02x", cmd) + ", ignoring");
            }
        }
        bridge.close();
    }

    // --- Command handlers ---

    void putAccount(byte[] payload) {
        if (payload.length != 92) {
            throw new IllegalArgumentException("PutAccount: expected 92 bytes, got " + payload.length);
        }
        Address addr = Address.wrap(Bytes.wrap(payload, 0, 20));
        long nonce = readUint64LE(payload, 20);
        Wei balance = Wei.wrap(UInt256.fromBytes(Bytes32.wrap(payload, 28)));
        Hash codeHash = Hash.wrap(Bytes32.wrap(payload, 60));

        AccountRecord rec = accounts.computeIfAbsent(addr, k -> new AccountRecord());
        rec.nonce = nonce;
        rec.balance = balance;
        rec.codeHash = codeHash;
    }

    void putStorage(byte[] payload) {
        if (payload.length != 84) {
            throw new IllegalArgumentException("PutStorage: expected 84 bytes, got " + payload.length);
        }
        Address addr = Address.wrap(Bytes.wrap(payload, 0, 20));
        UInt256 slot = UInt256.fromBytes(Bytes32.wrap(payload, 20));
        UInt256 value = UInt256.fromBytes(Bytes32.wrap(payload, 52));

        AccountRecord rec = accounts.computeIfAbsent(addr, k -> new AccountRecord());
        rec.storage.put(slot, value);
    }

    void putCode(byte[] payload) {
        if (payload.length < 32) {
            throw new IllegalArgumentException("PutCode: payload too short (" + payload.length + " bytes)");
        }
        Hash codeHash = Hash.wrap(Bytes32.wrap(payload, 0));
        byte[] codeBytes = new byte[payload.length - 32];
        System.arraycopy(payload, 32, codeBytes, 0, codeBytes.length);
        code.put(codeHash, codeBytes);
    }

    Hash computeRoot() {
        // Create Forest world state backed by persistent RocksDB storage.
        // Preimage storage is in-memory (not needed for basic RPC queries in Forest mode).
        var preimageStorage = new WorldStatePreimageKeyValueStorage(new InMemoryKeyValueStorage());
        MutableWorldState worldState = new ForestMutableWorldState(
                worldStateStorage, preimageStorage, EvmConfiguration.DEFAULT);

        // Build a dummy block header for persist()
        BlockHeader header = BlockHeaderBuilder.createDefault()
                .parentHash(Hash.ZERO)
                .stateRoot(Hash.EMPTY_TRIE_HASH)
                .number(0)
                .gasLimit(30_000_000)
                .timestamp(0)
                .buildBlockHeader();

        // Populate world state
        WorldUpdater updater = worldState.updater();

        int count = 0;
        for (var entry : accounts.entrySet()) {
            Address addr = entry.getKey();
            AccountRecord rec = entry.getValue();

            MutableAccount account = updater.createAccount(addr);
            account.setNonce(rec.nonce);
            account.setBalance(rec.balance);

            if (rec.codeHash != null &&
                    !rec.codeHash.equals(EMPTY_CODE_HASH) &&
                    !rec.codeHash.equals(Hash.ZERO)) {
                byte[] codeBytes = code.get(rec.codeHash);
                if (codeBytes != null) {
                    account.setCode(Bytes.wrap(codeBytes));
                }
            }

            for (var storageEntry : rec.storage.entrySet()) {
                if (!storageEntry.getValue().isZero()) {
                    account.setStorageValue(storageEntry.getKey(), storageEntry.getValue());
                }
            }
            count++;
        }

        updater.commit();
        worldState.persist(header);

        Hash root = worldState.rootHash();
        System.err.println("[besu-bridge] computed state root: " + root +
                " (" + count + " accounts), persisted to RocksDB");
        return root;
    }

    Hash writeGenesis(byte[] payload) throws Exception {
        var json = new String(payload, StandardCharsets.UTF_8);
        var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        var tree = mapper.readTree(json);

        Hash stateRoot = Hash.fromHexString(tree.get("stateRoot").asText());
        long gasLimit = tree.has("gasLimit") ? tree.get("gasLimit").asLong() : 30_000_000L;
        long baseFee = tree.has("baseFee") ? tree.get("baseFee").asLong() : 1_000_000_000L;

        var chainConfig = tree.get("chainConfig");
        boolean hasShanghai = chainConfig != null && chainConfig.has("shanghaiTime");
        boolean hasCancun = chainConfig != null && chainConfig.has("cancunTime");
        boolean hasPrague = chainConfig != null && chainConfig.has("pragueTime");

        // Build genesis block header matching GenesisState.buildHeader() exactly.
        // Field values must match what Besu parses from genesis.json so that
        // --genesis-state-hash-cache-enabled produces a matching block hash.
        var builder = BlockHeaderBuilder.create()
                .parentHash(Hash.ZERO)
                .ommersHash(Hash.EMPTY_LIST_HASH)
                .coinbase(Address.ZERO)
                .stateRoot(stateRoot)
                .transactionsRoot(Hash.EMPTY_TRIE_HASH)
                .receiptsRoot(Hash.EMPTY_TRIE_HASH)
                .logsBloom(
                        org.hyperledger.besu.datatypes.LogsBloomFilter.empty())
                .difficulty(Difficulty.ZERO)
                .number(0)
                .gasLimit(gasLimit)
                .gasUsed(0)
                .timestamp(0)
                .extraData(Bytes.EMPTY)
                .mixHash(Hash.ZERO)
                .nonce(0)
                .baseFee(Wei.of(baseFee))
                .blockHeaderFunctions(new MainnetBlockHeaderFunctions());

        if (hasShanghai) {
            builder.withdrawalsRoot(Hash.EMPTY_TRIE_HASH);
        }
        if (hasCancun) {
            builder.parentBeaconBlockRoot(Bytes32.ZERO);
            builder.excessBlobGas(org.hyperledger.besu.datatypes.BlobGas.ZERO);
            builder.blobGasUsed(0L);
        }
        if (hasPrague) {
            builder.requestsHash(Hash.EMPTY_LIST_HASH);
        }

        BlockHeader header = builder.buildBlockHeader();
        Hash blockHash = header.getBlockHash();

        // Build genesis block body
        Optional<List<Withdrawal>> withdrawals = hasShanghai
                ? Optional.of(Collections.emptyList())
                : Optional.empty();
        BlockBody body = new BlockBody(
                Collections.emptyList(), Collections.emptyList(), withdrawals);

        // Write genesis block to RocksDB blockchain storage using Besu's key format
        try (var batch = new WriteBatch()) {
            // Block header: prefix 0x02 + blockHash -> RLP(header)
            byte[] rlpHeader = RLP.encode(header::writeTo).toArrayUnsafe();
            batch.put(hBlockchain, prefixedKey(PREFIX_BLOCK_HEADER, blockHash), rlpHeader);

            // Block body: prefix 0x03 + blockHash -> RLP(wrappedBody)
            byte[] rlpBody = RLP.encode(body::writeWrappedBodyTo).toArrayUnsafe();
            batch.put(hBlockchain, prefixedKey(PREFIX_BLOCK_BODY, blockHash), rlpBody);

            // Transaction receipts: prefix 0x04 + blockHash -> RLP([])
            byte[] rlpEmptyReceipts = RLP.encode(out -> out.writeEmptyList()).toArrayUnsafe();
            batch.put(hBlockchain, prefixedKey(PREFIX_TX_RECEIPTS, blockHash), rlpEmptyReceipts);

            // Block hash mapping: prefix 0x05 + UInt256(blockNumber) -> blockHash
            // UInt256(0) is 32 zero bytes, so key is prefix + 32 zero bytes = 33 bytes
            byte[] blockHashKey = new byte[33];
            blockHashKey[0] = PREFIX_BLOCK_HASH;
            // remaining 32 bytes are zero (block number 0)
            batch.put(hBlockchain, blockHashKey, blockHash.getBytes().toArrayUnsafe());

            // Total difficulty: prefix 0x06 + blockHash -> raw Difficulty bytes (32 bytes)
            batch.put(hBlockchain, prefixedKey(PREFIX_TOTAL_DIFFICULTY, blockHash),
                    Difficulty.ZERO.toArrayUnsafe());

            // Chain head in VARIABLES segment
            batch.put(hVariables, KEY_CHAIN_HEAD_HASH, blockHash.getBytes().toArrayUnsafe());

            db.write(new WriteOptions(), batch);
        }

        System.err.println("[besu-bridge] wrote genesis block " + blockHash + " to RocksDB");
        return blockHash;
    }

    void close() {
        if (db != null) {
            // Flush all memtables to SST files before closing.
            // Required because Besu reopens with TransactionDB mode which may
            // not properly replay WAL written by regular RocksDB mode.
            try {
                var flushOpts = new org.rocksdb.FlushOptions().setWaitForFlush(true);
                db.flush(flushOpts, List.of(hBlockchain, hWorldState, hVariables));
                flushOpts.close();
            } catch (RocksDBException e) {
                System.err.println("[besu-bridge] WARNING: flush failed: " + e.getMessage());
            }
            hBlockchain.close();
            hWorldState.close();
            hVariables.close();
            db.close();
            db = null;
        }
    }

    // --- Helpers ---

    static byte[] prefixedKey(byte prefix, Hash hash) {
        byte[] key = new byte[33];
        key[0] = prefix;
        System.arraycopy(hash.getBytes().toArrayUnsafe(), 0, key, 1, 32);
        return key;
    }

    // --- Protocol I/O helpers ---

    void respond(DataOutputStream out) throws IOException {
        if (lastErr != null) {
            Exception e = lastErr;
            lastErr = null;
            byte[] msg = e.getMessage().getBytes(StandardCharsets.UTF_8);
            writeResponse(out, STATUS_ERROR, msg);
            return;
        }
        writeResponse(out, STATUS_OK, null);
    }

    void respondWithHash(DataOutputStream out, Hash hash) throws IOException {
        if (lastErr != null) {
            Exception e = lastErr;
            lastErr = null;
            byte[] msg = e.getMessage().getBytes(StandardCharsets.UTF_8);
            writeResponse(out, STATUS_ERROR, msg);
            return;
        }
        writeResponse(out, STATUS_OK, hash != null ? hash.getBytes().toArrayUnsafe() : new byte[32]);
    }

    static void writeResponse(DataOutputStream out, byte status, byte[] payload) throws IOException {
        out.writeByte(status);
        int plen = (payload != null) ? payload.length : 0;
        writeUint32LE(out, plen);
        if (plen > 0) {
            out.write(payload);
        }
        out.flush();
    }

    // --- Binary encoding helpers (little-endian, matching Go's protocol) ---

    static int readUint32LE(DataInputStream in) throws IOException {
        byte[] buf = in.readNBytes(4);
        return ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }

    static long readUint64LE(byte[] data, int offset) {
        return ByteBuffer.wrap(data, offset, 8).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }

    static void writeUint32LE(DataOutputStream out, int value) throws IOException {
        byte[] buf = new byte[4];
        ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).putInt(value);
        out.write(buf);
    }

    // ========================================================================
    // Minimal KeyValueStorage adapter wrapping a RocksDB column family.
    // Only methods used by ForestWorldStateKeyValueStorage are implemented.
    // ========================================================================
    static class RocksDBKeyValueStorage implements KeyValueStorage {

        private final RocksDB db;
        private final ColumnFamilyHandle cf;
        private volatile boolean closed = false;

        RocksDBKeyValueStorage(RocksDB db, ColumnFamilyHandle cf) {
            this.db = db;
            this.cf = cf;
        }

        @Override
        public Optional<byte[]> get(byte[] key) throws StorageException {
            try {
                byte[] val = db.get(cf, key);
                return Optional.ofNullable(val);
            } catch (RocksDBException e) {
                throw new StorageException(e);
            }
        }

        @Override
        public boolean containsKey(byte[] key) throws StorageException {
            return get(key).isPresent();
        }

        @Override
        public KeyValueStorageTransaction startTransaction() throws StorageException {
            return new RocksDBTransaction(db, cf);
        }

        @Override
        public boolean isClosed() {
            return closed;
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override public void clear() { throw new UnsupportedOperationException(); }
        @Override public Stream<Pair<byte[], byte[]>> stream() { throw new UnsupportedOperationException(); }
        @Override public Stream<Pair<byte[], byte[]>> streamFromKey(byte[] startKey) { throw new UnsupportedOperationException(); }
        @Override public Stream<Pair<byte[], byte[]>> streamFromKey(byte[] startKey, byte[] endKey) { throw new UnsupportedOperationException(); }
        @Override public Stream<byte[]> streamKeys() { throw new UnsupportedOperationException(); }
        @Override public boolean tryDelete(byte[] key) { throw new UnsupportedOperationException(); }
        @Override public Set<byte[]> getAllKeysThat(Predicate<byte[]> p) { throw new UnsupportedOperationException(); }
        @Override public Set<byte[]> getAllValuesFromKeysThat(Predicate<byte[]> p) { throw new UnsupportedOperationException(); }
    }

    // ========================================================================
    // KeyValueStorageTransaction backed by RocksDB WriteBatch
    // ========================================================================
    static class RocksDBTransaction implements KeyValueStorageTransaction {

        private final RocksDB db;
        private final ColumnFamilyHandle cf;
        private final WriteBatch batch;

        RocksDBTransaction(RocksDB db, ColumnFamilyHandle cf) {
            this.db = db;
            this.cf = cf;
            this.batch = new WriteBatch();
        }

        @Override
        public void put(byte[] key, byte[] value) {
            try {
                batch.put(cf, key, value);
            } catch (RocksDBException e) {
                throw new StorageException(e);
            }
        }

        @Override
        public void remove(byte[] key) {
            try {
                batch.delete(cf, key);
            } catch (RocksDBException e) {
                throw new StorageException(e);
            }
        }

        @Override
        public void commit() throws StorageException {
            try {
                db.write(new WriteOptions(), batch);
            } catch (RocksDBException e) {
                throw new StorageException(e);
            } finally {
                batch.close();
            }
        }

        @Override
        public void rollback() {
            batch.clear();
        }

        @Override
        public void close() {
            batch.close();
        }
    }
}
