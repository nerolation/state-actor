using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Nethermind.Core;
using Nethermind.Core.Crypto;
using Nethermind.Db;
using Nethermind.Db.Rocks;
using Nethermind.Db.Rocks.Config;
using Nethermind.Int256;
using Nethermind.Logging;
using Nethermind.Serialization.Rlp;
using Nethermind.State;
using Nethermind.Trie;

namespace NethermindBridge;

public class Bridge
{
    // Command types (must match protocol.go)
    const byte CmdPutAccount = 0x01;
    const byte CmdPutStorage = 0x02;
    const byte CmdPutCode = 0x03;
    const byte CmdFlush = 0x04;
    const byte CmdComputeRoot = 0x05;
    const byte CmdWriteGenesis = 0x06;
    const byte CmdClose = 0x07;

    const byte StatusOK = 0x00;
    const byte StatusError = 0xFF;

    // RocksDB instances (Nethermind uses separate DBs, not column families)
    private IDb _stateDb = null!;
    private IDb _codeDb = null!;
    private IDb _headerDb = null!;
    private IDb _blockDb = null!;
    private IDb _blockNumberDb = null!;
    private IDb _blockInfoDb = null!;
    private IDb _metadataDb = null!;

    // Buffered state
    private readonly Dictionary<Address, AccountRecord> _accounts = new();
    private readonly Dictionary<Hash256, byte[]> _code = new();
    private Exception? _lastErr;

    private class AccountRecord
    {
        public long Nonce;
        public UInt256 Balance;
        public Hash256 CodeHash = Keccak.OfAnEmptyString;
        public readonly Dictionary<UInt256, byte[]> Storage = new();
    }

    public void OpenDatabases(string basePath)
    {
        Directory.CreateDirectory(basePath);

        var dbConfig = DbConfig.Default;
        var logManager = NullLogManager.Instance;

        _stateDb = OpenDb(basePath, DbNames.State, logManager, dbConfig);
        _codeDb = OpenDb(basePath, DbNames.Code, logManager, dbConfig);
        _headerDb = OpenDb(basePath, DbNames.Headers, logManager, dbConfig);
        _blockDb = OpenDb(basePath, DbNames.Blocks, logManager, dbConfig);
        _blockNumberDb = OpenDb(basePath, DbNames.BlockNumbers, logManager, dbConfig);
        _blockInfoDb = OpenDb(basePath, DbNames.BlockInfos, logManager, dbConfig);
        _metadataDb = OpenDb(basePath, DbNames.Metadata, logManager, dbConfig);

        Console.Error.WriteLine($"[nethermind-bridge] opened databases at {basePath}");
    }

    private static IDb OpenDb(string basePath, string dbName, ILogManager logManager, IDbConfig dbConfig)
    {
        var dbPath = Path.Combine(basePath, dbName);
        var settings = new DbSettings(dbName, dbPath);
        var configFactory = new SimpleRocksDbConfigFactory(dbConfig);
        return new DbOnTheRocks(basePath, settings, dbConfig, configFactory, logManager);
    }

    public void PutAccount(ReadOnlySpan<byte> payload)
    {
        if (payload.Length != 92)
            throw new ArgumentException($"PutAccount: expected 92 bytes, got {payload.Length}");

        var addr = new Address(payload[..20].ToArray());
        long nonce = BinaryPrimitives.ReadInt64LittleEndian(payload[20..28]);
        var balance = new UInt256(payload[28..60], isBigEndian: true);
        var codeHash = new Hash256(payload[60..92]);

        if (!_accounts.TryGetValue(addr, out var rec))
        {
            rec = new AccountRecord();
            _accounts[addr] = rec;
        }
        rec.Nonce = nonce;
        rec.Balance = balance;
        rec.CodeHash = codeHash;
    }

    public void PutStorage(ReadOnlySpan<byte> payload)
    {
        if (payload.Length != 84)
            throw new ArgumentException($"PutStorage: expected 84 bytes, got {payload.Length}");

        var addr = new Address(payload[..20].ToArray());
        var slot = new UInt256(payload[20..52], isBigEndian: true);
        var value = payload[52..84].ToArray();

        if (!_accounts.TryGetValue(addr, out var rec))
        {
            rec = new AccountRecord();
            _accounts[addr] = rec;
        }
        rec.Storage[slot] = value;
    }

    public void PutCode(ReadOnlySpan<byte> payload)
    {
        if (payload.Length < 32)
            throw new ArgumentException($"PutCode: payload too short ({payload.Length} bytes)");

        var codeHash = new Hash256(payload[..32]);
        var code = payload[32..].ToArray();
        _code[codeHash] = code;

        // Write code to DB immediately
        _codeDb.Set(codeHash.Bytes, code);
    }

    public Hash256 ComputeRoot()
    {
        var logManager = NullLogManager.Instance;

        // Create trie infrastructure backed by persistent RocksDB
        var nodeStorage = new NodeStorage(_stateDb);
        var trieStore = new RawTrieStore(nodeStorage);

        // First pass: build storage tries and collect storage roots
        var storageRoots = new Dictionary<Address, Hash256>();
        foreach (var (addr, rec) in _accounts)
        {
            if (rec.Storage.Count == 0) continue;

            var addrHash = Keccak.Compute(addr.Bytes);
            var storageTrieStore = trieStore.GetTrieStore(addrHash);
            var storageTree = new StorageTree(storageTrieStore, logManager);

            foreach (var (slot, value) in rec.Storage)
            {
                // Skip zero values
                bool allZero = true;
                for (int i = 0; i < value.Length; i++)
                {
                    if (value[i] != 0) { allZero = false; break; }
                }
                if (allZero) continue;

                storageTree.Set(slot, value);
            }

            storageTree.UpdateRootHash();
            storageTree.Commit();
            storageRoots[addr] = storageTree.RootHash;
        }

        // Second pass: build state trie
        var stateTrieStore = trieStore.GetTrieStore(null);
        var stateTree = new StateTree(stateTrieStore, logManager);

        int count = 0;
        foreach (var (addr, rec) in _accounts)
        {
            var storageRoot = storageRoots.GetValueOrDefault(addr, Keccak.EmptyTreeHash);
            var account = new Account(
                (UInt256)rec.Nonce,
                rec.Balance,
                storageRoot,
                rec.CodeHash);
            stateTree.Set(addr, account);
            count++;
        }

        stateTree.UpdateRootHash();
        stateTree.Commit();

        var root = stateTree.RootHash;
        Console.Error.WriteLine($"[nethermind-bridge] computed state root: {root} ({count} accounts), persisted to RocksDB");
        return root;
    }

    public Hash256 WriteGenesis(ReadOnlySpan<byte> payload)
    {
        var json = Encoding.UTF8.GetString(payload);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        var stateRootHex = root.GetProperty("stateRoot").GetString()!;
        var stateRoot = new Hash256(stateRootHex);
        long gasLimit = root.TryGetProperty("gasLimit", out var gl) ? gl.GetInt64() : 30_000_000L;
        long baseFee = root.TryGetProperty("baseFee", out var bf) ? bf.GetInt64() : 1_000_000_000L;

        var chainConfig = root.TryGetProperty("chainConfig", out var cc) ? cc : default;
        bool hasShanghai = chainConfig.ValueKind == JsonValueKind.Object && chainConfig.TryGetProperty("shanghaiTime", out _);
        bool hasCancun = chainConfig.ValueKind == JsonValueKind.Object && chainConfig.TryGetProperty("cancunTime", out _);
        bool hasPrague = chainConfig.ValueKind == JsonValueKind.Object && chainConfig.TryGetProperty("pragueTime", out _);

        // Build genesis block header matching other clients
        var header = new BlockHeader(
            parentHash: Keccak.Zero,
            unclesHash: Keccak.OfAnEmptySequenceRlp,
            beneficiary: Address.Zero,
            difficulty: UInt256.Zero,
            number: 0,
            gasLimit: gasLimit,
            timestamp: 0,
            extraData: [])
        {
            StateRoot = stateRoot,
            TxRoot = Keccak.EmptyTreeHash,
            ReceiptsRoot = Keccak.EmptyTreeHash,
            Bloom = Bloom.Empty,
            MixHash = Keccak.Zero,
            Nonce = 0,
            BaseFeePerGas = (UInt256)baseFee,
            TotalDifficulty = UInt256.Zero,
        };

        if (hasShanghai)
        {
            header.WithdrawalsRoot = Keccak.EmptyTreeHash;
        }
        if (hasCancun)
        {
            header.ParentBeaconBlockRoot = Keccak.Zero;
            header.BlobGasUsed = 0;
            header.ExcessBlobGas = 0;
        }
        if (hasPrague)
        {
            header.RequestsHash = Keccak.EmptyTreeHash;
        }

        // Compute block hash via RLP encoding + Keccak
        header.Hash = Keccak.Compute(new HeaderDecoder().Encode(header).Bytes);
        var blockHash = header.Hash;

        // Build genesis block body
        var body = hasCancun || hasShanghai
            ? new BlockBody([], [], [])
            : new BlockBody();

        var block = new Block(header, body);

        // --- Write to DBs ---

        // 1. Header DB: key = [8B blockNumber BE][32B hash]
        var headerRlp = new HeaderDecoder().Encode(header);
        WriteBlockNumPrefixed(_headerDb, 0, blockHash, headerRlp.Bytes);

        // 2. BlockNumbers DB: hash -> 8B blockNumber BE
        var blockNumBytes = new byte[8];
        BinaryPrimitives.WriteInt64BigEndian(blockNumBytes, 0);
        _blockNumberDb.Set(blockHash.Bytes, blockNumBytes);

        // 3. Blocks DB: key = [8B blockNumber BE][32B hash]
        var blockRlp = new BlockDecoder(new HeaderDecoder()).Encode(block);
        WriteBlockNumPrefixed(_blockDb, 0, blockHash, blockRlp.Bytes);

        // 4. BlockInfos DB: chain level info for block 0
        var blockInfo = new BlockInfo(blockHash, UInt256.Zero)
        {
            WasProcessed = true,
        };
        var chainLevelInfo = new ChainLevelInfo(true, blockInfo);
        var chainLevelRlp = Rlp.Encode(chainLevelInfo).Bytes;
        // Block number 0: ToBigEndianSpanWithoutLeadingZeros(0) = [0x00]
        _blockInfoDb.Set(new byte[] { 0x00 }, chainLevelRlp);

        // 5. Head pointer in blockInfos: key = Keccak.Zero (32 zero bytes)
        _blockInfoDb.Set(Keccak.Zero.Bytes, blockHash.Bytes.ToArray());

        // 6. State head in blockInfos: key = 16 zero bytes, value = RLP(0L)
        var stateHeadKey = new byte[16]; // all zeros
        _blockInfoDb.Set(stateHeadKey, Rlp.Encode(0L).Bytes);

        Console.Error.WriteLine($"[nethermind-bridge] wrote genesis block {blockHash} to RocksDB");
        return blockHash;
    }

    public void Close()
    {
        _stateDb?.Flush(false);
        _codeDb?.Flush(false);
        _headerDb?.Flush(false);
        _blockDb?.Flush(false);
        _blockNumberDb?.Flush(false);
        _blockInfoDb?.Flush(false);
        _metadataDb?.Flush(false);

        _stateDb?.Dispose();
        _codeDb?.Dispose();
        _headerDb?.Dispose();
        _blockDb?.Dispose();
        _blockNumberDb?.Dispose();
        _blockInfoDb?.Dispose();
        _metadataDb?.Dispose();

        Console.Error.WriteLine("[nethermind-bridge] databases closed");
    }

    // --- Helpers ---

    private static void WriteBlockNumPrefixed(IDb db, long blockNumber, Hash256 hash, byte[] value)
    {
        var key = new byte[40];
        BinaryPrimitives.WriteInt64BigEndian(key, blockNumber);
        hash.Bytes.CopyTo(key.AsSpan(8));
        db.Set(key, value);
    }

    // --- Protocol I/O ---

    public static async Task Main(string[] args)
    {
        string? dataDir = null;
        for (int i = 0; i < args.Length; i++)
        {
            if (args[i] == "-db" && i + 1 < args.Length)
            {
                dataDir = args[++i];
            }
        }
        if (dataDir == null)
        {
            Console.Error.WriteLine("[nethermind-bridge] -db flag is required");
            Environment.Exit(1);
        }

        var bridge = new Bridge();
        bridge.OpenDatabases(dataDir);
        Console.Error.WriteLine($"[nethermind-bridge] ready, datadir={dataDir}");

        using var stdin = Console.OpenStandardInput();
        using var stdout = Console.OpenStandardOutput();
        using var reader = new BinaryReader(new BufferedStream(stdin, 4 * 1024 * 1024));
        using var writer = new BinaryWriter(new BufferedStream(stdout, 4 * 1024 * 1024));

        while (true)
        {
            byte cmd;
            byte[] payload;
            try
            {
                cmd = reader.ReadByte();
                int plen = ReadUint32LE(reader);
                payload = plen > 0 ? reader.ReadBytes(plen) : [];
            }
            catch (EndOfStreamException)
            {
                Console.Error.WriteLine("[nethermind-bridge] stdin closed, exiting");
                break;
            }

            switch (cmd)
            {
                case CmdPutAccount:
                    try { bridge.PutAccount(payload); }
                    catch (Exception e) { bridge._lastErr = e; }
                    break;

                case CmdPutStorage:
                    try { bridge.PutStorage(payload); }
                    catch (Exception e) { bridge._lastErr = e; }
                    break;

                case CmdPutCode:
                    try { bridge.PutCode(payload); }
                    catch (Exception e) { bridge._lastErr = e; }
                    break;

                case CmdFlush:
                    Respond(writer, bridge);
                    break;

                case CmdComputeRoot:
                    Hash256? root = null;
                    try { root = bridge.ComputeRoot(); }
                    catch (Exception e) { bridge._lastErr = e; }
                    RespondWithHash(writer, bridge, root);
                    break;

                case CmdWriteGenesis:
                    Hash256? blockHash = null;
                    try { blockHash = bridge.WriteGenesis(payload); }
                    catch (Exception e) { bridge._lastErr = e; }
                    RespondWithHash(writer, bridge, blockHash);
                    break;

                case CmdClose:
                    Respond(writer, bridge);
                    writer.Flush();
                    bridge.Close();
                    return;

                default:
                    Console.Error.WriteLine($"[nethermind-bridge] unknown command 0x{cmd:x2}, ignoring");
                    break;
            }
        }
        bridge.Close();
    }

    private static void Respond(BinaryWriter writer, Bridge bridge)
    {
        if (bridge._lastErr != null)
        {
            var e = bridge._lastErr;
            bridge._lastErr = null;
            var msg = Encoding.UTF8.GetBytes(e.Message);
            WriteResponse(writer, StatusError, msg);
            return;
        }
        WriteResponse(writer, StatusOK, null);
    }

    private static void RespondWithHash(BinaryWriter writer, Bridge bridge, Hash256? hash)
    {
        if (bridge._lastErr != null)
        {
            var e = bridge._lastErr;
            bridge._lastErr = null;
            var msg = Encoding.UTF8.GetBytes(e.Message);
            WriteResponse(writer, StatusError, msg);
            return;
        }
        WriteResponse(writer, StatusOK, hash != null ? hash.Bytes.ToArray() : new byte[32]);
    }

    private static void WriteResponse(BinaryWriter writer, byte status, byte[]? payload)
    {
        writer.Write(status);
        int plen = payload?.Length ?? 0;
        WriteUint32LE(writer, plen);
        if (plen > 0)
            writer.Write(payload!);
        writer.Flush();
    }

    private static int ReadUint32LE(BinaryReader reader)
    {
        Span<byte> buf = stackalloc byte[4];
        reader.Read(buf);
        return BinaryPrimitives.ReadInt32LittleEndian(buf);
    }

    private static void WriteUint32LE(BinaryWriter writer, int value)
    {
        Span<byte> buf = stackalloc byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(buf, value);
        writer.Write(buf);
    }
}

// Minimal IRocksDbConfigFactory implementation
internal class SimpleRocksDbConfigFactory(IDbConfig dbConfig) : IRocksDbConfigFactory
{
    public IRocksDbConfig GetForDatabase(string databaseName, string? columnName)
    {
        return new PerTableDbConfig(dbConfig, databaseName, columnName);
    }
}
