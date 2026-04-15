//! Integration tests for the Bitcoin RPC `BlockStreamer` implementation.
//!
//! These tests require a live Bitcoin Core node. Set the following environment variables
//! before running:
//!
//! - `BITCOIN_RPC_URL`      — required, e.g. `http://localhost:8332`
//! - `BITCOIN_RPC_USER`     — optional HTTP Basic Auth username
//! - `BITCOIN_RPC_PASSWORD` — optional HTTP Basic Auth password
//! - `BITCOIN_RPC_NETWORK`  — optional network id (default: `bitcoin-mainnet`)

use std::time::Duration;

use arrow::array::Array as _;
use datasets_raw::{
    arrow::FixedSizeBinaryArray,
    client::{BlockStreamError, BlockStreamer},
};
use futures::TryStreamExt as _;

use crate::Client;

// Expected column counts for each table — mirror the schema definitions.
const BLOCKS_COLUMNS: usize = 16;
const TRANSACTIONS_COLUMNS: usize = 14;
const INPUTS_COLUMNS: usize = 14;
const OUTPUTS_COLUMNS: usize = 12;

// Index of `parent_hash` within the blocks RecordBatch schema:
// 0=_block_num, 1=_ts, 2=block_num, 3=timestamp, 4=hash, 5=parent_hash
const PARENT_HASH_COL: usize = 5;

fn build_client() -> Client {
    let url: url::Url = std::env::var("BITCOIN_RPC_URL")
        .expect("BITCOIN_RPC_URL must be set for Bitcoin RPC integration tests")
        .parse()
        .expect("BITCOIN_RPC_URL must be a valid URL");

    let user = std::env::var("BITCOIN_RPC_USER").ok();
    let password = std::env::var("BITCOIN_RPC_PASSWORD").ok();
    let auth = match (user, password) {
        (Some(u), Some(p)) => Some((u, p)),
        _ => None,
    };

    let network: amp_providers_common::network_id::NetworkId = std::env::var("BITCOIN_RPC_NETWORK")
        .unwrap_or_else(|_| "bitcoin-mainnet".to_string())
        .parse()
        .expect("BITCOIN_RPC_NETWORK must be a valid network id");

    let provider_name = "it_bitcoin_rpc".parse().expect("valid provider name");

    Client::new(
        url,
        network,
        provider_name,
        auth,
        None,
        Some(4),
        6,
        Duration::from_secs(30),
        None,
    )
    .expect("failed to build Bitcoin RPC client")
}

async fn collect_stream(
    client: Client,
    start: u64,
    end: u64,
) -> Result<Vec<datasets_raw::rows::Rows>, BlockStreamError> {
    let stream = client.block_stream(start, end).await;
    std::pin::pin!(stream).try_collect().await
}

/// `latest_block(finalized=true)` returns a block height above zero.
#[tokio::test]
async fn latest_block_returns_finalized_height() {
    //* Given
    let mut client = build_client();

    //* When
    let result = client.latest_block(true).await;

    //* Then
    let height = result
        .expect("latest_block should not error")
        .expect("latest_block should return Some");
    assert!(height > 0, "finalized height should be > 0, got {height}");
}

/// `latest_block(finalized=false)` returns a height at least as high as the finalized tip.
#[tokio::test]
async fn latest_unfinalized_is_at_least_finalized() {
    //* Given
    let mut client = build_client();

    //* When
    let finalized = client
        .latest_block(true)
        .await
        .expect("no error")
        .expect("some value");
    let unfinalized = client
        .latest_block(false)
        .await
        .expect("no error")
        .expect("some value");

    //* Then
    assert!(
        unfinalized >= finalized,
        "unfinalized ({unfinalized}) should be >= finalized ({finalized})"
    );
}

/// Streaming the genesis block (0) produces exactly 4 tables with `parent_hash` null.
#[tokio::test]
async fn stream_genesis_block_has_null_parent_hash() {
    //* Given
    let client = build_client();

    //* When
    let rows_vec = collect_stream(client, 0, 0)
        .await
        .expect("stream should not error");

    //* Then
    assert_eq!(
        rows_vec.len(),
        1,
        "should yield exactly one Rows for block 0"
    );

    let tables: Vec<_> = rows_vec
        .into_iter()
        .next()
        .expect("rows_vec has one element")
        .into_iter()
        .collect();
    assert_eq!(
        tables.len(),
        4,
        "should have 4 tables (blocks/txs/inputs/outputs)"
    );

    let blocks = &tables[0];
    assert_eq!(blocks.rows.num_rows(), 1, "blocks table should have 1 row");
    assert_eq!(
        blocks.rows.num_columns(),
        BLOCKS_COLUMNS,
        "blocks column count"
    );

    let parent_hash_col = blocks
        .rows
        .column(PARENT_HASH_COL)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("parent_hash should be FixedSizeBinaryArray");

    assert!(
        parent_hash_col.is_null(0),
        "genesis block parent_hash must be null"
    );
}

/// Streaming blocks 1-3 yields sequential block numbers, correct column counts,
/// non-null parent hashes, and at least one transaction per block (coinbase).
#[tokio::test]
async fn stream_blocks_schema_and_row_counts_are_correct() {
    //* Given
    let client = build_client();
    let start = 1u64;
    let end = 3u64;

    //* When
    let rows_vec = collect_stream(client, start, end)
        .await
        .expect("stream should not error");

    //* Then
    assert_eq!(
        rows_vec.len() as u64,
        end - start + 1,
        "should yield one Rows per block"
    );

    for (i, rows) in rows_vec.iter().enumerate() {
        let expected_block = start + i as u64;

        assert_eq!(
            rows.block_num(),
            expected_block,
            "block_num should be sequential"
        );

        let tables: Vec<_> = rows.into_iter().collect();
        assert_eq!(tables.len(), 4, "each Rows should contain 4 tables");

        let [blocks, transactions, inputs, outputs] = tables.as_slice() else {
            panic!("expected exactly 4 tables");
        };

        // Schema checks
        assert_eq!(
            blocks.rows.num_columns(),
            BLOCKS_COLUMNS,
            "block {expected_block}: blocks column count"
        );
        assert_eq!(
            transactions.rows.num_columns(),
            TRANSACTIONS_COLUMNS,
            "block {expected_block}: transactions column count"
        );
        assert_eq!(
            inputs.rows.num_columns(),
            INPUTS_COLUMNS,
            "block {expected_block}: inputs column count"
        );
        assert_eq!(
            outputs.rows.num_columns(),
            OUTPUTS_COLUMNS,
            "block {expected_block}: outputs column count"
        );

        // Each block has exactly one block header row
        assert_eq!(
            blocks.rows.num_rows(),
            1,
            "block {expected_block}: blocks should have exactly 1 row"
        );

        // Non-genesis blocks must have a non-null parent_hash
        let parent_hash_col = blocks
            .rows
            .column(PARENT_HASH_COL)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("parent_hash should be FixedSizeBinaryArray");
        assert!(
            parent_hash_col.is_valid(0),
            "block {expected_block}: parent_hash should not be null"
        );

        // Every Bitcoin block contains at least a coinbase transaction
        assert!(
            transactions.rows.num_rows() >= 1,
            "block {expected_block}: expected at least 1 transaction"
        );
        assert!(
            inputs.rows.num_rows() >= 1,
            "block {expected_block}: expected at least 1 input (coinbase)"
        );
        assert!(
            outputs.rows.num_rows() >= 1,
            "block {expected_block}: expected at least 1 output"
        );
    }
}
