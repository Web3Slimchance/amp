# Table Schema Generation

A generation crate for Bitcoin RPC dataset table schema documentation. This crate generates Markdown table schema documentation for Bitcoin RPC dataset tables.

## Table Schema Markdown Generation

The library uses a build configuration flag `gen_schema_tables` that enables table schema markdown generation during the build process. When enabled, the build script will generate Markdown documentation from the Bitcoin RPC table definitions.

To generate table schema markdown, run:

```bash
just gen-bitcoin-rpc-tables-schema
```

Or using the full `cargo` command:

```bash
RUSTFLAGS="--cfg gen_schema_tables" cargo check -p datasets-bitcoin-rpc-gen

mkdir -p docs/schemas/tables
cp target/debug/build/datasets-bitcoin-rpc-gen-*/out/tables.md docs/schemas/tables/bitcoin-rpc.md
```

This will generate table schema markdown from the Bitcoin RPC table definitions and copy it to `docs/schemas/tables/bitcoin-rpc.md`.
