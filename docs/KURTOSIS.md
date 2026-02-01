# Kurtosis / ethereum-package Integration

This guide explains how to integrate State Actor with [ethereum-package](https://github.com/ethpandaops/ethereum-package) for Kurtosis-based devnets.

## Overview

State Actor can pre-generate bloated state that's immediately usable by geth nodes in your devnet, without requiring `geth init`.

## Methods

### Method 1: Pre-generate Before Kurtosis

Generate state locally, then mount it into your Kurtosis package:

```bash
# 1. Get genesis from ethereum-package or create your own
# ethereum-package generates genesis during EL/CL genesis data generation

# 2. Generate state
state-actor \
    --db ./bloated-chaindata \
    --genesis genesis.json \
    --accounts 100000 \
    --contracts 50000 \
    --max-slots 10000 \
    --seed 42

# 3. Package for Kurtosis
tar -czf bloated-state.tar.gz -C ./bloated-chaindata .
```

Then in your Kurtosis config, mount the state:

```yaml
participants:
  - el_type: geth
    el_extra_env_vars:
      SKIP_GETH_INIT: "true"
    el_extra_mounts:
      /pregenerated-state: "./bloated-state"
```

### Method 2: Starlark Module Integration

Use the provided Starlark module to generate state as part of your Kurtosis package:

```python
# Import the module
load("github.com/nerolation/state-actor/integration/stategen_launcher.star", 
     "generate_bloated_state")

def run(plan, args):
    # First, get genesis from your genesis generator
    genesis_artifact = generate_genesis(plan, ...)
    
    # Generate bloated state
    chaindata = generate_bloated_state(
        plan,
        output_artifact_name="bloated-chaindata",
        genesis_artifact=genesis_artifact,
        num_accounts=100000,
        num_contracts=50000,
        max_slots=10000,
        seed=42,
    )
    
    # Use chaindata artifact in geth config
    # ...
```

### Method 3: Custom Docker Image

Build a geth image with the wrapper script:

```dockerfile
FROM ethereum/client-go:stable

COPY geth-wrapper.sh /usr/local/bin/
COPY bloated-chaindata /pregenerated-state/chaindata

ENTRYPOINT ["/usr/local/bin/geth-wrapper.sh"]
```

## Starlark Module Reference

### `generate_bloated_state`

```python
generate_bloated_state(
    plan,
    output_artifact_name,
    genesis_artifact=None,      # Optional: genesis.json artifact
    num_accounts=10000,         # EOA accounts to generate
    num_contracts=5000,         # Contracts to generate
    max_slots=10000,            # Max storage slots per contract
    min_slots=100,              # Min storage slots per contract
    distribution="power-law",   # Distribution type
    seed=0,                     # Random seed (0 = random)
    tolerations=[],             # K8s tolerations
    node_selectors={},          # K8s node selectors
)
```

Returns: Files artifact containing the generated chaindata.

## geth-wrapper.sh

The wrapper script handles pre-generated state:

```bash
#!/bin/bash
# Checks for pre-generated state at $PREGENERATED_STATE_PATH
# If found, copies to geth data directory
# If genesis block is present, skips geth init
# Otherwise, runs geth init
# Finally, starts geth with provided arguments
```

Environment variables:
- `PREGENERATED_STATE_PATH`: Path to pre-generated chaindata (default: `/pregenerated-state/chaindata`)
- `GENESIS_PATH`: Path to genesis.json (default: `/genesis/genesis.json`)
- `GETH_DATADIR`: Geth data directory (default: `/data/geth/execution-data`)

## Example: Full ethereum-package Config

```yaml
# bloated-devnet.yaml
participants:
  - el_type: geth
    el_image: ethereum/client-go:stable
    el_extra_params:
      - --db.engine=pebble
    el_extra_env_vars:
      PREGENERATED_STATE_PATH: /pregenerated-state/chaindata
    el_extra_mounts:
      /pregenerated-state: "{{.StateArtifact}}"
    cl_type: lighthouse
    count: 2

network_params:
  network_id: "32382"
  
additional_services:
  - prometheus_grafana
```

## Timing Considerations

State generation timing in Kurtosis:

1. **EL/CL Genesis Generation** → produces genesis.json
2. **State Actor** → generates state with genesis
3. **Node Launch** → nodes start with pre-generated state

Ensure state generation completes before node launch. The Starlark module handles this ordering automatically.

## Large State (Terabyte Scale)

For very large state:

### Pre-generate and Host

```bash
# Generate large state (may take hours)
state-actor --db ./mainnet-scale \
    --genesis genesis.json \
    --accounts 10000000 \
    --contracts 5000000 \
    --max-slots 100000 \
    --seed 12345

# Compress
tar -czf mainnet-scale.tar.gz -C ./mainnet-scale .

# Upload to storage
aws s3 cp mainnet-scale.tar.gz s3://your-bucket/
```

### Download at Launch

```python
def download_state(plan, url):
    return plan.run_sh(
        name="download-state",
        run=f"wget -O /state.tar.gz {url} && tar -xzf /state.tar.gz -C /output",
        store=[StoreSpec(src="/output", name="chaindata")],
    )
```

## Troubleshooting

### State Root Mismatch

If geth reports state root mismatch:
- Ensure `--genesis` flag is used
- Verify genesis.json matches the one used by other nodes

### Genesis Block Not Found

If geth can't find genesis block:
- Check that State Actor completed successfully
- Verify database files were copied correctly

### Performance Issues

For faster generation:
- Increase `--batch-size` (e.g., 100000)
- Increase `--workers` (up to 2x CPU cores)
- Use SSD storage
