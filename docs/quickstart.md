# Getting Started: Build and Deploy Datasets Locally

This guide walks you through the complete process of setting up Amp locally, defining blockchain datasets, deploying them, and querying the results -- from zero to running SQL queries over derived data.

## Prerequisites

- PostgreSQL installed locally (the `initdb` and `postgres` binaries must be in your PATH)
- Node.js 18+ and pnpm (for the TypeScript CLI)
- An Ethereum RPC endpoint (or a local node like Anvil for testing)

## Step 1: Install Amp

Install ampup, the Amp version manager, which provides the `ampd` server and `ampctl` admin CLI:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://ampup.sh/install | sh
```

Restart your terminal or run `source ~/.zshenv` to update your PATH.

Verify the installation:

```bash
ampd --version
ampctl --version
```

**Alternative (Nix users):**

```bash
nix run github:edgeandnode/amp -- --version
```

## Step 2: Install the Amp TypeScript CLI

Install the `@edgeandnode/amp` package, which provides the `amp` CLI for building and managing datasets.

First, install pnpm if you don't have it:

```bash
# install via npm
npm install -g pnpm
```

Then create your project and add the package:

```bash
mkdir my-amp-project && cd my-amp-project
pnpm init
pnpm add @edgeandnode/amp
```

Verify:

```bash
pnpm amp --version
```

## Step 3: Start the Amp Daemon

Start `ampd` in solo mode, which runs the query server, admin API, and extraction worker in a single process -- ideal for local development:

```bash
ampd solo
```

That's it. Solo mode automatically:

- Creates the `.amp/` workspace directory structure
- Starts and manages an embedded PostgreSQL instance (data persists in `.amp/metadb/` across restarts)
- Auto-discovers configuration at `.amp/config.toml` if present (no config file required for defaults)

**Advanced: Using an external database**

If you prefer to manage your own PostgreSQL instance (e.g., via Docker or a system installation), create a config file pointing to it:

```bash
mkdir -p .amp
cat > .amp/config.toml << 'EOF'
[metadata_db]
url = "postgresql://postgres:postgres@localhost:5432/amp"
EOF
```

When a `[metadata_db]` section is present, solo mode skips the embedded PostgreSQL and connects to the provided URL instead.

Once running, you will have three services:

| Service          | Port | Protocol          |
|------------------|------|-------------------|
| Arrow Flight     | 1602 | gRPC              |
| JSON Lines       | 1603 | HTTP              |
| Admin API        | 1610 | HTTP              |

## Step 4: Define a Dataset

Create an `amp.config.ts` file that defines your dataset. This example creates a derived dataset that decodes ERC-20 Transfer events:

```typescript
// amp.config.ts
import { defineDataset } from "@edgeandnode/amp"

export default defineDataset(() => ({
  name: "my_tokens",
  namespace: "_",

  // Depend on a raw EVM RPC dataset for blockchain data
  dependencies: {
    eth_mainnet: "_/eth_mainnet@0.0.1",
  },

  tables: {
    // Select raw blocks
    blocks: {
      sql: `
        SELECT hash, block_num, timestamp
        FROM eth_mainnet.blocks
      `,
    },

    // Decode ERC-20 Transfer events from logs
    transfers: {
      sql: `
        SELECT
          e.block_hash, e.tx_hash, e.address, e.block_num, e.timestamp,
          e.event['from'] AS transfer_from,
          e.event['to'] AS transfer_to,
          e.event['value'] AS value
        FROM (
          SELECT block_hash, tx_hash, address, block_num, timestamp,
                 evm_decode_log(topic1, topic2, topic3, data,
                   'Transfer(address indexed from, address indexed to, uint256 value)')
                 AS event
          FROM eth_mainnet.logs
          WHERE topic0 = evm_topic('Transfer(address indexed from, address indexed to, uint256 value)')
        ) AS e
      `,
    },
  },
}))
```

**Tip:** For cleaner configs, use the `eventQuery` helper to generate SQL from ABI events:

```typescript
import { defineDataset, eventQuery } from "@edgeandnode/amp"
import { parseAbiItem } from "viem"

const transfer = parseAbiItem("event Transfer(address indexed from, address indexed to, uint256 value)")

export default defineDataset(() => ({
  name: "my_tokens",
  namespace: "_",
  dependencies: { eth_mainnet: "_/eth_mainnet@0.0.1" },
  tables: {
    transfers: {
      sql: eventQuery(transfer, "eth_mainnet"),
    },
  },
}))
```

## Step 5: Build the Manifest

Build the dataset config into a validated JSON manifest:

```bash
pnpm amp build
```

This compiles `amp.config.ts`, validates the SQL schemas against the admin server, and outputs the manifest JSON. To save it to a file:

```bash
pnpm amp build --output manifest.json
```

## Step 6: Register the Dataset

Register the dataset with the Amp admin server:

```bash
# Register with the dev tag
pnpm amp register

# Or register with a specific version
pnpm amp register --tag 1.0.0
```

This sends the manifest to the admin API (default: `http://localhost:1610`) and creates a dataset reference like `_/my_tokens@dev`.

## Step 7: Deploy the Dataset

Deploy the registered dataset to start extraction:

```bash
# Deploy the dev revision
pnpm amp deploy

# Or deploy a specific version
pnpm amp deploy --reference _/my_tokens@1.0.0

# Optionally set a block limit for testing
pnpm amp deploy --end-block 1000
```

Once deployed, the worker begins extracting and transforming data. You can monitor progress through the admin API.

### Shortcut: Use Dev Mode

For rapid iteration, use `amp dev` which watches your config file and automatically re-registers and re-deploys on every save:

```bash
pnpm amp dev
```

This is the recommended workflow during development.

## Step 8: Query Your Data

Once data is being extracted, you can query it immediately.

### Using the amp CLI

```bash
# Table-formatted output
pnpm amp query "SELECT * FROM \"_/my_tokens\".transfers LIMIT 10"

# JSON output
pnpm amp query "SELECT * FROM \"_/my_tokens\".transfers LIMIT 10" --format json

# JSONL output
pnpm amp query "SELECT * FROM \"_/my_tokens\".blocks LIMIT 5" --format jsonl
```

### Using curl (JSON Lines endpoint)

```bash
curl -X POST http://localhost:1603 \
  --data 'SELECT * FROM "_/my_tokens".transfers LIMIT 10'
```

### Using the TypeScript Client

```typescript
import { createClient } from "@edgeandnode/amp"
import { createGrpcTransport } from "@connectrpc/connect-node"

const transport = createGrpcTransport({
  baseUrl: "http://localhost:1602",
})

const client = createClient(transport)

for await (const batch of client.query('SELECT * FROM "_/my_tokens".transfers LIMIT 10')) {
  for (const row of batch) {
    console.log(row)
  }
}
```

## Step 9: Build Derived Datasets

Derived datasets transform data from other datasets using SQL. You can chain transformations by depending on your own datasets.

Create a second config that depends on the token dataset:

```typescript
// amp.config.analytics.ts
import { defineDataset } from "@edgeandnode/amp"

export default defineDataset(() => ({
  name: "token_analytics",
  namespace: "_",

  dependencies: {
    tokens: "_/my_tokens@dev",
  },

  tables: {
    // Per-address transfer volume
    transfer_volume: {
      sql: `
        SELECT address, transfer_to, transfer_from, value, block_num, timestamp
        FROM tokens.transfers
      `,
    },
  },
}))
```

Register and deploy it:

```bash
pnpm amp register dev --config amp.config.analytics.ts
pnpm amp deploy --config amp.config.analytics.ts
```

Then query the derived data:

```bash
pnpm amp query "SELECT * FROM \"_/token_analytics\".transfer_volume LIMIT 10"
```

## Summary

| Step | Action | Command |
|------|--------|---------|
| 1 | Install Amp binaries | `curl ... \| sh` |
| 2 | Install TypeScript CLI | `pnpm add @edgeandnode/amp` |
| 3 | Start Amp daemon | `ampd solo` |
| 4 | Define dataset | Create `amp.config.ts` |
| 5 | Build manifest | `pnpm amp build` |
| 6 | Register | `pnpm amp register dev` |
| 7 | Deploy | `pnpm amp deploy` or `pnpm amp dev` |
| 8 | Query | `pnpm amp query "SELECT ..."` |
| 9 | Derive more datasets | Chain configs with dependencies |

## Next Steps

- Explore [configuration options](config.md) for tuning performance and storage
- Learn about [operational modes](modes.md) for production deployments
- Set up [telemetry](telemetry.md) for monitoring with Grafana
- Browse the [architecture overview](architecture.md) for deeper understanding
- See the [Python client](https://github.com/edgeandnode/amp-python) for notebook-based exploration
