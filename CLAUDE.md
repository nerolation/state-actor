This tool is used to quickly bootstrap clients with big state. Target state sizes are 1 Tb or larger.
We do NOT want to first write to genesis.json and then import this file. This leads to numerous problems and also assumptions like the file can be read in memory, the trie is kept in memory, etc. etc.
We included clients in the clients/ subfolder as git submodules. These are the clients we target. For each client we create a bridge in their respective language which are called from a main program. These write to the client-specific database directly into it, such that this can thus be streamed.

## Client submodules

The client submodules (clients/) should NOT be modified. All bridge code lives in our repo (bridges/). The submodules are reference source code only.

## Besu bridge

The Besu bridge uses Forest-mode (MPT) and writes directly to RocksDB with column families matching Besu's KeyValueSegmentIdentifier: BLOCKCHAIN({1}), WORLD_STATE({2}), VARIABLES({11}).
Besu requires `--data-storage-format=FOREST` and `--genesis-state-hash-cache-enabled=true` at startup so it reads the state root from the stored genesis header rather than recomputing from genesis.json alloc (which is empty). This avoids a genesis block hash mismatch.
The genesis.json must include `"ethash": {}` in the config section for Besu to recognize the consensus mechanism.
