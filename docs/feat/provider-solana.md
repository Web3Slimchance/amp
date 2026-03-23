---
name: "provider-solana"
description: "Solana blockchain provider with Old Faithful archive support. Load when asking about Solana providers, Old Faithful, or CAR files"
type: feature
status: experimental
components: "crate:solana"
---

# Solana Provider

## Summary

The Solana provider enables data access from the Solana blockchain using a two-stage approach: historical data from Old Faithful CAR archive files and real-time data from JSON-RPC endpoints. It handles Solana's slot-based architecture and supports configurable rate limiting.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Configuration](#configuration)
3. [Architecture](#architecture)
4. [Usage](#usage)
5. [Implementation](#implementation)
6. [Limitations](#limitations)
7. [References](#references)

## Key Concepts

- **Slot**: Solana's time interval unit (~400ms); not every slot produces a block
- **Old Faithful**: Archive of historical Solana data in CAR file format
- **CAR File**: Content-addressable archive file containing epoch data
- **Epoch**: ~432,000 slots (~2 days) of Solana data

## Configuration

For the complete field reference, see the [config schema](../schemas/providers/solana.spec.json).

### Example Configuration

```toml
kind = "solana"
network = "mainnet"
rpc_provider_url = "${SOLANA_MAINNET_RPC_URL}"

# Archive mode: "always" (default), "auto", or "never"
# - "always": Always use archive, even for recent data
# - "auto": RPC for recent slots, archive for historical
# - "never": Never use archive, RPC-only mode
use_archive = "always"

# Archive dir: Optional local directory for pre-downloaded
# CAR files (if not using Old Faithful directly)
# archive_dir = "path/to/pre-downloaded/car/files"

max_rpc_calls_per_second = 50
```

## Architecture

### Two-Stage Data Extraction

The provider supports three archive modes controlled by the `use_archive` configuration:

- **`"always"`** (default): Always use archive mode, even for recent slots. Downloads epoch CAR files (~745GB each).
- **`"auto"`**: Smart selection based on slot age. Uses RPC-only mode when `start_slot > current_slot - 10,000` (recent slots within ~83 minutes on mainnet), and archive mode for historical data.
- **`"never"`**: Always use RPC-only mode. Best for demos and recent data access.

```
Historical Data                    Real-time Data
     ↓                                  ↓
Old Faithful Archive → CAR files →  RPC endpoint
     ↓                                  ↓
  Stream epoch CAR                 getBlock calls
     ↓                                  ↓
  Process in-memory                Stream blocks
                                        ↓
                                  Continuous sync
```

### Data Sources

| Stage      | Source                                  | Data                              |
| ---------- | --------------------------------------- | --------------------------------- |
| Historical | Old Faithful (`files.old-faithful.net`) | Archived epoch CAR files (~745GB) |
| Real-time  | Solana RPC                              | Live blocks via JSON-RPC          |

## Usage

### Required Environment Variables

```bash
export SOLANA_MAINNET_RPC_URL="https://api.mainnet-beta.solana.com"
```

### CAR File Management

Solana extractor supports reading pre-downloaded CAR files from a local directory.
To use this feature, set the `archive_dir` configuration field to the path where your CAR files are stored:

```toml
archive_dir = "path/to/pre-downloaded/car/files"
```

### Rate Limiting

For public RPC endpoints, configure rate limiting:

```toml
# 50 requests per second
max_rpc_calls_per_second = 50
```

## Implementation

### Extracted Tables

| Table           | Key Fields                                              |
| --------------- | ------------------------------------------------------- |
| `block_headers` | slot, parent_slot, block_hash, block_height, block_time |
| `transactions`  | slot, tx_index, signatures, status, fee, balances       |
| `messages`      | slot, tx_index, message fields                          |
| `instructions`  | slot, tx_index, program_id_index, accounts, data        |

### Slot Handling

- Solana uses slots rather than sequential block numbers
- Skipped slots (no block produced) do not produce any rows, creating gaps in the block number sequence
- Chain integrity is maintained through hash-based validation where each block's `prev_hash` must match the previous block's hash

### Source Files

- `crates/extractors/solana/src/lib.rs` - ProviderConfig and factory
- `crates/extractors/solana/src/extractor.rs` - SolanaExtractor implementation
- `crates/extractors/solana/src/rpc_client.rs` - RPC client with rate limiting
- `crates/extractors/solana/src/of1_client.rs` - Old Faithful CAR client

## Limitations

- CAR files are ~745GB per epoch; download takes 10+ hours on typical connections
- Archive mode can be controlled via `use_archive` config (`"auto"`, `"always"`, `"never"`)
- Only HTTP/HTTPS RPC URLs supported (no WebSocket)
- Uses finalized commitment level (not configurable)

## References

- [provider](provider.md) - Base: Provider system overview
- [provider-config](provider-config.md) - Related: Configuration format
