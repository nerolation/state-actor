# Starlark module for integrating stategen with ethereum-package
# This generates pre-populated state before launching geth nodes

STATEGEN_IMAGE = "stategen:latest"

def generate_bloated_state(
    plan,
    output_artifact_name,
    genesis_artifact=None,
    num_accounts=10000,
    num_contracts=5000,
    max_slots=10000,
    min_slots=100,
    distribution="power-law",
    seed=0,
    binary_trie=False,
    tolerations=[],
    node_selectors={},
):
    """
    Generates pre-populated Ethereum state using stategen.
    
    Args:
        plan: Kurtosis plan
        output_artifact_name: Name for the output artifact containing chaindata
        genesis_artifact: Optional artifact containing genesis.json (if provided,
                         genesis block will be written with correct state root)
        num_accounts: Number of EOA accounts to create
        num_contracts: Number of contracts with storage
        max_slots: Maximum storage slots per contract
        min_slots: Minimum storage slots per contract
        distribution: Storage distribution (power-law, uniform, exponential)
        seed: Random seed for reproducibility (0 = random)
        binary_trie: Enable binary trie mode (EIP-7864, requires geth --override.verkle=0)
        tolerations: Kubernetes tolerations
        node_selectors: Kubernetes node selectors
    
    Returns:
        Artifact containing the generated chaindata (ready to use without geth init)
    """
    
    seed_arg = ""
    if seed != 0:
        seed_arg = "--seed {0}".format(seed)

    binary_trie_arg = ""
    if binary_trie:
        binary_trie_arg = "--binary-trie"

    genesis_arg = ""
    files = {}
    if genesis_artifact != None:
        genesis_arg = "--genesis /genesis/genesis.json"
        files["/genesis"] = genesis_artifact
    
    cmd = """
    stategen \
        --db /output/chaindata \
        {genesis_arg} \
        --accounts {accounts} \
        --contracts {contracts} \
        --max-slots {max_slots} \
        --min-slots {min_slots} \
        --distribution {distribution} \
        {seed_arg} \
        {binary_trie_arg} \
        --batch-size 50000 \
        --verbose \
        --benchmark
    """.format(
        genesis_arg=genesis_arg,
        accounts=num_accounts,
        contracts=num_contracts,
        max_slots=max_slots,
        min_slots=min_slots,
        distribution=distribution,
        seed_arg=seed_arg,
        binary_trie_arg=binary_trie_arg,
    )
    
    result = plan.run_sh(
        name="generate-bloated-state",
        description="Generating pre-populated Ethereum state with genesis integration",
        run=cmd,
        image=STATEGEN_IMAGE,
        files=files,
        store=[
            StoreSpec(src="/output/chaindata", name=output_artifact_name),
        ],
        wait="30m",  # Large state can take a while
        tolerations=tolerations,
        node_selectors=node_selectors,
    )
    
    return result.files_artifacts[0]


def get_geth_init_command(chaindata_mount_path):
    """
    Returns a command prefix that initializes geth with pre-generated state.
    
    Args:
        chaindata_mount_path: Path where pre-generated chaindata is mounted
    
    Returns:
        Command string to prepend to geth startup
    """
    return """
    if [ -d "{mount}/chaindata" ] && [ "$(ls -A {mount}/chaindata 2>/dev/null)" ]; then
        echo "Installing pre-generated state..."
        mkdir -p /data/geth/execution-data/geth
        cp -r {mount}/chaindata /data/geth/execution-data/geth/
        echo "Pre-generated state installed: $(du -sh /data/geth/execution-data/geth/chaindata | cut -f1)"
    fi &&
    """.format(mount=chaindata_mount_path)


def new_bloated_state_config(
    num_accounts=10000,
    num_contracts=5000,
    max_slots=10000,
    min_slots=100,
    distribution="power-law",
    seed=0,
    binary_trie=False,
):
    """
    Creates a configuration struct for bloated state generation.
    """
    return struct(
        num_accounts=num_accounts,
        num_contracts=num_contracts,
        max_slots=max_slots,
        min_slots=min_slots,
        distribution=distribution,
        seed=seed,
        binary_trie=binary_trie,
    )


def get_binary_trie_params():
    """
    Returns the geth extra params needed for binary trie mode.
    Add these to el_extra_params when using binary trie state.
    """
    # Legacy geth flag name; --override.verkle=0 enables EIP-7864 binary trie mode
    return ["--override.verkle=0"]
