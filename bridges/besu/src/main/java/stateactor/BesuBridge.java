package stateactor;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStorageProvider;
import org.hyperledger.besu.ethereum.storage.keyvalue.WorldStatePreimageKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.forest.storage.ForestWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.forest.worldview.ForestMutableWorldState;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

/**
 * Besu bridge: reads the binary protocol from stdin, buffers state in memory,
 * uses Besu's Forest-mode Merkle Patricia Trie to compute state roots.
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

        var bridge = new BesuBridge();
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
                    return;

                default:
                    System.err.println("[besu-bridge] unknown command 0x" +
                            String.format("%02x", cmd) + ", ignoring");
            }
        }
    }

    // --- Command handlers ---

    void putAccount(byte[] payload) {
        if (payload.length != 92) {
            throw new IllegalArgumentException("PutAccount: expected 92 bytes, got " + payload.length);
        }
        Address addr = Address.wrap(Bytes.wrap(payload, 0, 20));
        long nonce = readUint64LE(payload, 20);
        // Balance is 32-byte big-endian uint256
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
        // Create an in-memory Forest world state (standard MPT)
        MutableWorldState worldState = createInMemoryForestWorldState();

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

        for (var entry : accounts.entrySet()) {
            Address addr = entry.getKey();
            AccountRecord rec = entry.getValue();

            MutableAccount account = updater.createAccount(addr);
            account.setNonce(rec.nonce);
            account.setBalance(rec.balance);

            // Set code if non-empty
            if (rec.codeHash != null &&
                    !rec.codeHash.equals(EMPTY_CODE_HASH) &&
                    !rec.codeHash.equals(Hash.ZERO)) {
                byte[] codeBytes = code.get(rec.codeHash);
                if (codeBytes != null) {
                    account.setCode(Bytes.wrap(codeBytes));
                }
            }

            // Set storage
            for (var storageEntry : rec.storage.entrySet()) {
                if (!storageEntry.getValue().isZero()) {
                    account.setStorageValue(storageEntry.getKey(), storageEntry.getValue());
                }
            }
        }

        updater.commit();
        worldState.persist(header);

        Hash root = worldState.rootHash();
        System.err.println("[besu-bridge] computed state root: " + root +
                " (" + accounts.size() + " accounts)");
        return root;
    }

    Hash writeGenesis(byte[] payload) throws Exception {
        // For now, parse the JSON and build a genesis block header.
        // The actual DB write would need RocksDB integration.
        var json = new String(payload, java.nio.charset.StandardCharsets.UTF_8);
        var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        var tree = mapper.readTree(json);

        Hash stateRoot = Hash.fromHexString(tree.get("stateRoot").asText());
        long gasLimit = tree.has("gasLimit") ? tree.get("gasLimit").asLong() : 30_000_000L;
        long baseFee = tree.has("baseFee") ? tree.get("baseFee").asLong() : 1_000_000_000L;

        // Detect fork features from chainConfig
        var chainConfig = tree.get("chainConfig");
        boolean hasShanghai = chainConfig != null && chainConfig.has("shanghaiTime");
        boolean hasCancun = chainConfig != null && chainConfig.has("cancunTime");
        boolean hasPrague = chainConfig != null && chainConfig.has("pragueTime");

        // Build genesis block header
        var builder = BlockHeaderBuilder.create()
                .parentHash(Hash.ZERO)
                .ommersHash(Hash.EMPTY_LIST_HASH)
                .coinbase(Address.ZERO)
                .stateRoot(stateRoot)
                .transactionsRoot(Hash.EMPTY_TRIE_HASH)
                .receiptsRoot(Hash.EMPTY_TRIE_HASH)
                .logsBloom(
                        org.hyperledger.besu.datatypes.LogsBloomFilter.empty())
                .difficulty(org.hyperledger.besu.ethereum.core.Difficulty.ZERO)
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

        System.err.println("[besu-bridge] wrote genesis block " + blockHash);
        return blockHash;
    }

    // --- Protocol I/O helpers ---

    void respond(DataOutputStream out) throws IOException {
        if (lastErr != null) {
            Exception e = lastErr;
            lastErr = null;
            byte[] msg = e.getMessage().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            writeResponse(out, STATUS_ERROR, msg);
            return;
        }
        writeResponse(out, STATUS_OK, null);
    }

    void respondWithHash(DataOutputStream out, Hash hash) throws IOException {
        if (lastErr != null) {
            Exception e = lastErr;
            lastErr = null;
            byte[] msg = e.getMessage().getBytes(java.nio.charset.StandardCharsets.UTF_8);
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

    // --- World state helpers ---

    static MutableWorldState createInMemoryForestWorldState() {
        var stateStorage = new ForestWorldStateKeyValueStorage(new InMemoryKeyValueStorage());
        var preimageStorage = new WorldStatePreimageKeyValueStorage(new InMemoryKeyValueStorage());
        return new ForestMutableWorldState(stateStorage, preimageStorage, EvmConfiguration.DEFAULT);
    }
}
