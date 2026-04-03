---
name: "app-amp-typescript"
description: "TypeScript CLI and library for building, registering, deploying, and querying blockchain datasets. Load when asking about the amp CLI, amp.config.ts, defineDataset, eventQuery, or the @edgeandnode/amp package"
type: feature
status: "experimental"
components: "app:amp-typescript"
---

# Amp TypeScript CLI & Library

## Summary

The `@edgeandnode/amp` package provides a TypeScript CLI (`amp`) and programmatic library for building, registering, deploying, and querying blockchain datasets. It transforms smart contract events and blockchain data into SQL-queryable datasets using a configuration-as-code approach via `amp.config.ts` files. The package also exports a client library for querying Amp data via Arrow Flight.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Usage](#usage)
3. [Configuration](#configuration)
4. [API Reference](#api-reference)
5. [References](#references)

## Key Concepts

- **Dataset Config**: A TypeScript file (`amp.config.ts`) that defines dataset metadata, dependencies, tables (as SQL queries), and custom functions using `defineDataset()`
- **Manifest**: The compiled JSON output of a dataset config, containing validated table schemas and SQL definitions. Sent to the admin server for registration
- **Revision**: A versioned snapshot of a dataset, identified by semantic version, git hash, or tags (`dev`, `latest`)
- **Dataset Reference**: A string in the format `namespace/name@revision` (e.g., `edgeandnode/mainnet@1.0.0`) that uniquely identifies a dataset version

## Usage

### Installation

```bash
npm install @edgeandnode/amp
# or
pnpm add @edgeandnode/amp
```

The package provides the `amp` CLI binary.

### CLI Commands

#### `amp build` - Build a Manifest

Compiles a dataset config into a JSON manifest, optionally validating schemas against the admin server.

```bash
# Build from default config (amp.config.ts in current directory)
amp build

# Build from a specific config file
amp build --config ./path/to/amp.config.ts

# Write manifest to a file
amp build --output manifest.json

# Validate against a specific admin server
amp build --admin-url http://localhost:1610
```

#### `amp register` - Register a Dataset

Sends the built manifest to the admin server for registration.

```bash
# Register with implicit dev tag
amp register

# Register with a specific version
amp register 1.0.0

# Register with explicit dev tag
amp register dev

# Register from a specific config
amp register --config ./amp.config.ts --admin-url http://localhost:1610
```

#### `amp deploy` - Deploy a Dataset

Deploys a registered dataset version for extraction and transformation.

```bash
# Deploy from config (uses namespace/name@dev)
amp deploy

# Deploy a specific version
amp deploy edgeandnode/mainnet@1.0.0

# Deploy with a block cutoff
amp deploy --end-block 17000000

# Force deployment (skip confirmation for base datasets in terminal states)
amp deploy --force
```

#### `amp dev` - Development Server

Watches `amp.config.ts` for changes and automatically registers and deploys on save. Ideal for rapid iteration during development.

```bash
# Start dev server (watches amp.config.ts)
amp dev

# With custom admin URL
amp dev --admin-url http://localhost:1610
```

#### `amp query` - Execute SQL Queries

Runs SQL queries against the Amp server via Arrow Flight.

```bash
# Basic query with table output
amp query "SELECT * FROM \"my_namespace/dataset\".blocks LIMIT 10"

# JSON output format
amp query "SELECT * FROM ..." --format json

# JSONL output format
amp query "SELECT * FROM ..." --format jsonl

# Pretty-printed output
amp query "SELECT * FROM ..." --format pretty

# Limit rows
amp query "SELECT * FROM ..." --limit 100

# Custom Flight server URL
amp query "SELECT * FROM ..." --flight-url http://localhost:1602

# With authentication
amp query "SELECT * FROM ..." --bearer-token <token>
```

#### `amp publish` - Publish to Registry

Publishes a dataset to the public Amp registry (requires authentication).

```bash
# Publish with version tag
amp publish 1.0.0

# Publish with changelog
amp publish 1.0.0 --changelog "Added new swap tracking tables"
```

This runs the full workflow: register, deploy, and publish.

#### `amp proxy` - Arrow Flight Proxy

Starts a Connect proxy for the Arrow Flight server, useful for development and testing.

```bash
# Start proxy on default port (8080)
amp proxy

# Custom port
amp proxy --port 3000

# Custom backend
amp proxy --flight-url http://localhost:1602
```


#### Global Options

| Option   | Env Var         | Default | Description            |
|----------|-----------------|---------|------------------------|
| `--logs` | `AMP_LOG_LEVEL` | `info`  | Log level (trace, debug, info, warn, error, fatal) |

### Programmatic API

The package exports a client library for querying Amp data in TypeScript applications.

#### Creating a Query Client

```typescript
import { createClient, createAuthInterceptor } from "@edgeandnode/amp"
import { createConnectTransport } from "@connectrpc/connect-node"

const transport = createConnectTransport({
  baseUrl: "http://localhost:1602",
  httpVersion: "2",
})

const client = createClient(transport)
```

#### Streaming Queries

For large or infinite result sets:

```typescript
for await (const batch of client.stream("SELECT * FROM \"ns/dataset\".logs")) {
  console.log(`Received ${batch.length} rows`)
  for (const row of batch) {
    console.log(row)
  }
}
```

#### Batch Queries

For finite result sets:

```typescript
for await (const batch of client.query("SELECT * FROM \"ns/dataset\".blocks LIMIT 10")) {
  console.log(batch)
}
```

## Configuration

### Dataset Config File (`amp.config.ts`)

The dataset config file defines what data to extract and how to transform it:

```typescript
import { defineDataset } from "@edgeandnode/amp"

export default defineDataset((context) => ({
  // Required
  name: "my_dataset",

  // Optional metadata
  namespace: "my_namespace",       // defaults to "_"
  version: "1.0.0",
  description: "My blockchain dataset",
  readme: "# My Dataset\nDetailed description...",
  repository: new URL("https://github.com/myorg/myrepo"),
  keywords: ["DeFi", "DEX"],
  sources: ["0x1F98431c8aD98523631AE4a59f267346ea31F984"],
  license: "MIT",
  private: false,

  // Dependencies on other datasets
  dependencies: {
    anvil: "_/anvil@0.0.1",
    mainnet: "edgeandnode/mainnet@1.0.0",
  },

  // Table definitions as SQL queries
  tables: {
    my_table: {
      sql: `
        SELECT block_hash, tx_hash, block_num, timestamp, address,
               evm_decode_log(topic1, topic2, topic3, data,
                 'Transfer(address indexed from, address indexed to, uint256 value)')
               AS event
        FROM anvil.logs
        WHERE topic0 = evm_topic('Transfer(address indexed from, address indexed to, uint256 value)')
      `,
    },
  },

  // Custom SQL functions (optional)
  functions: {
    my_function: {
      source: context.functionSource("./sql/my_function.sql"),
      inputTypes: ["VARCHAR", "INTEGER"],
      outputType: "DECIMAL(18,6)",
    },
  },
}))
```

### Naming Rules

| Field       | Pattern               | Example        |
|-------------|-----------------------|----------------|
| `name`      | `[a-z_][a-z0-9_]*`   | `uniswap_v3`  |
| `namespace` | `[a-z0-9_]+`          | `edgeandnode`  |
| Table names | `[a-z0-9_]+`          | `swap_events`  |

### Helper Functions

The package provides helpers for generating table definitions from contract ABIs:

```typescript
import { eventQuery, eventTable, eventTables, eventTableName } from "@edgeandnode/amp"

// Generate a SQL query for a single ABI event
const query = eventQuery(transferEvent, "mainnet")  // rpcSource defaults to "anvil"

// Generate a table definition { sql: "..." } for an event
const table = eventTable(transferEvent)

// Generate all event tables from a full ABI
const tables = eventTables(abi)

// Get snake_case table name from an ABI event
const name = eventTableName(transferEvent)  // e.g., "transfer" from Transfer event
```

### Config File Discovery

The CLI auto-discovers config files in the current directory:

| Extension                   | Loader              |
|-----------------------------|---------------------|
| `.ts`, `.mts`, `.cts`      | jiti (TypeScript)   |
| `.js`, `.mjs`, `.cjs`      | Dynamic import      |
| `.json`                     | JSON parse          |

The default lookup is `amp.config.*` in the current directory. Override with `--config`.

### Environment Variables

| Variable              | Default                    | Description                     |
|-----------------------|----------------------------|---------------------------------|
| `AMP_ADMIN_URL`       | `http://localhost:1610`    | Admin API server URL            |
| `AMP_ARROW_FLIGHT_URL`| `http://localhost:1602`    | Arrow Flight server URL         |
| `AMP_LOG_LEVEL`       | `info`                     | Log level                       |

## API Reference

### Exported Functions

| Export                   | Description                                      |
|--------------------------|--------------------------------------------------|
| `defineDataset(fn)`      | Type-safe dataset config factory                 |
| `eventQuery(abi, src?)`  | Generate SQL query from an ABI event             |
| `eventTable(abi, src?)`  | Generate table definition from an ABI event      |
| `eventTables(abi, src?)` | Generate all event tables from a full ABI        |
| `eventTableName(abi)`    | Get snake_case name from an ABI event            |
| `camelToSnake(str)`      | Convert camelCase string to snake_case           |
| `createClient(transport)`| Create an Arrow Flight query client              |
| `createAuthInterceptor(token)` | Create a bearer token interceptor for gRPC |

## References

- [app-ampd](app-ampd.md) - Related: The Amp daemon that the TypeScript CLI communicates with
- [datasets](datasets.md) - Related: Dataset concepts and types
- [datasets-derived](datasets-derived.md) - Related: Derived dataset SQL transformations
- [datasets-manifest](datasets-manifest.md) - Related: Manifest format specification
- [query-transport-flight](query-transport-flight.md) - Related: Arrow Flight protocol used by the query client
