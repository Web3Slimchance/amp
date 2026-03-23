# Solana Dataset Extractor

A high-performance extractor for Solana blockchain data, designed to work with the [Old Faithful](https://docs.old-faithful.net/) archive and Solana JSON-RPC providers.

## Overview

The Solana extractor implements a two-stage data extraction pipeline:

1. **Historical Data**: Downloads and processes epoch-based CAR files from the Old Faithful archive
2. **Recent Data**: Fetches the latest slots via JSON-RPC once the historical data is exhausted

This hybrid approach ensures efficient historical backfills while maintaining low-latency access to new blocks. The `use_archive` configuration controls how these two stages interact (see [Provider Config](#provider-config)).

## Architecture

### Components

- **`Client`**: Main client implementing the `BlockStreamer` trait. Supports a primary and optional fallback RPC client for filling truncated log messages.
- **`SolanaRpcClient`**: Handles HTTP requests to a Solana RPC endpoint, with optional rate limiting and metrics
- **`of1_client`**: Manages Old Faithful CAR file downloads with resume support, retry logic, and lifecycle tracking

### Data Flow

```
Old Faithful Archive (CAR files) ──┐
                                   ├──> Client ──> Block Processing ──> Parquet Tables
Main Solana JSON-RPC ──────────────┘
                                         │
Fallback Solana JSON-RPC ────────────────┘ (fills truncated log messages)
```

### Archive Usage Modes

The `use_archive` setting controls how the extractor sources block data:

- **`always`** (default): Always use Old Faithful CAR files for block data
- **`never`**: RPC-only mode, no archive downloads
- **`auto`**: Use RPC for recent slots (last ~10k), archive for historical data

### Tables

The extractor produces the following tables:

| Table           | Description                                                                 |
| --------------- | --------------------------------------------------------------------------- |
| `block_headers` | Block-level metadata (slot, parent slot, blockhash, timestamp, etc.)        |
| `block_rewards` | Per-block rewards (pubkey, lamports, post balance, reward type, commission) |
| `transactions`  | Transaction data (signatures, status, fees, compute units, etc.)            |
| `messages`      | Transaction messages (accounts, recent blockhash, address table lookups)    |
| `instructions`  | Both top-level and inner instructions with program IDs and data             |

### Slot vs. Block Number Handling

**Important**: Solana uses "slots" as time intervals for block production, but not every slot produces a block. Some slots may be skipped due to network issues or validator performance.

This extractor treats Solana slots as block numbers for compatibility with the `BlockStreamer` infrastructure.

## Provider Config

```toml
kind = "solana"
network = "mainnet"

# Archive mode: "always" (default), "auto", or "never"
# - "always": Always use archive, even for recent data
# - "auto": RPC for recent slots, archive for historical
# - "never": Never use archive, RPC-only mode
use_archive = "always"

# Archive dir: Optional local directory for pre-downloaded
# CAR files (if not using Old Faithful directly)
# archive_dir = "path/to/pre-downloaded/car/files"

[rpc_provider_info]
url = "https://api.mainnet-beta.solana.com"
# auth_header = "X-Api-Key"   # Optional: custom auth header name
# auth_token = "your-token"   # Optional: auth token (Bearer by default)

# [fallback_rpc_provider_info]   # Optional: used to fill truncated log messages
# url = "https://another-rpc.example.com"
# auth_token = "your-token"

max_rpc_calls_per_second = 50
```

**Configuration Options**:

| Field                           | Required | Description                                                                                   |
| ------------------------------- | -------- | --------------------------------------------------------------------------------------------- |
| `rpc_provider_info`             | Yes      | Primary RPC endpoint connection info (table)                                                  |
| `rpc_provider_info.url`         | Yes      | Solana RPC HTTP endpoint URL                                                                  |
| `rpc_provider_info.auth_header` | No       | Custom header name for auth (default: `Authorization: Bearer`)                                |
| `rpc_provider_info.auth_token`  | No       | Authentication token for RPC requests                                                         |
| `fallback_rpc_provider_info`    | No       | Fallback RPC endpoint for filling truncated log messages (same fields as `rpc_provider_info`) |
| `max_rpc_calls_per_second`      | No       | Rate limit for RPC calls (applies to main RPC only)                                           |
| `use_archive`                   | No       | Archive usage mode: `always` (default), `never`, or `auto`                                    |
| `archive_dir`                   | No       | Directory for pre-downloaded CAR files (if not using Old Faithful downloads)                  |
| `start_block`                   | No       | Starting slot number for extraction (set in the manifest)                                     |
| `finalized_blocks_only`         | No       | Whether to only extract finalized blocks (set in the manifest)                                |
| `commitment`                    | No       | Commitment level for Solana RPC requests: `finalized` (default), `processed` or `confirmed`   |

## Old Faithful Archive

The extractor downloads epoch-based CAR (Content Addressable aRchive) files from the Old Faithful service:

- **Archive URL**: `https://files.old-faithful.net`
- **File Format**: `epoch-<epoch_number>.car`
- **Epoch Size**: 432,000 slots per epoch (~2 days at 400ms slot time)

CAR files are streamed from the archive and processed in memory. The extractor also supports using pre-downloaded CAR files from a local directory specified by `archive_dir`. This allows users to manage their own archive downloads and avoid redundant network usage.

### Warning

Due to the large size of Solana CAR files, ensure sufficient disk space is available in the specified `archive_dir`, if choosing pre-downloaded CAR files route.

## Utilities

### `solana-compare`

A companion example (`examples/solana_compare.rs`) that compares block data from Old Faithful CAR files against the RPC endpoint for the same epoch. Useful for validating data consistency between the two sources.

### solana-car-download

A utility for downloading Solana epoch CAR files directly from Old Faithful, with support for resuming interrupted downloads and retrying on failures. This can be used to pre-populate the `archive_dir` with CAR files before running the extractor.

## JSON Schema Generation

JSON schemas for Solana dataset manifests can be generated using the companion `solana-gen` crate:

```bash
just gen-solana-dataset-manifest-schema
```

This generates a JSON schema from the `Manifest` struct. The unified raw manifest schema is output to `docs/schemas/manifest/raw.spec.json` via `just gen-raw-dataset-manifest-schema`.

## Related Resources

- [Old Faithful Documentation](https://docs.old-faithful.net/)
- [Solana RPC API](https://docs.solana.com/api)
- [Yellowstone Faithful CAR Parser](https://github.com/lamports-dev/yellowstone-faithful-car-parser)
