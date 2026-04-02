//! Integration tests for ts() / _ts watermark column with Anvil.
//!
//! Mines fresh blocks via Anvil, dumps the raw dataset, then verifies that
//! `ts()` and `_ts` return the correct block timestamp by cross-checking
//! against the Anvil RPC.

use alloy::eips::BlockNumberOrTag;
use monitoring::logging;

use crate::testlib::{
    ctx::TestCtxBuilder,
    helpers::{self as test_helpers, create_test_metrics_context},
};

#[derive(Debug, serde::Deserialize)]
struct TsRow {
    block_num: u64,
    #[serde(rename = "ts()")]
    ts_udf: String,
    #[serde(rename = "_ts")]
    ts_col: String,
    timestamp: String,
}

#[tokio::test(flavor = "multi_thread")]
async fn ts_matches_block_timestamp() {
    logging::init();

    let test_ctx = TestCtxBuilder::new("ts_matches_block_timestamp")
        .with_anvil_ipc()
        .with_dataset_manifest("anvil_rpc_ts")
        .with_meter(create_test_metrics_context().create_meter())
        .build()
        .await
        .expect("Failed to create test environment");

    // Mine a few blocks so we have data with real timestamps.
    test_ctx
        .anvil()
        .mine(3)
        .await
        .expect("Failed to mine blocks");

    // Dump the raw dataset.
    let ampctl = test_ctx.new_ampctl();
    let dataset_ref = "_/anvil_rpc_ts@0.0.0".parse().unwrap();
    test_helpers::deploy_and_wait(
        &ampctl,
        &dataset_ref,
        Some(3),
        std::time::Duration::from_secs(30),
        false,
    )
    .await
    .expect("Failed to dump dataset");

    // Query ts(), _ts, and timestamp from the dumped data.
    let jsonl = test_ctx.new_jsonl_client();
    let rows: Vec<TsRow> = jsonl
        .query(
            "SELECT block_num, ts(), _ts, timestamp \
             FROM anvil_rpc_ts.blocks \
             ORDER BY block_num",
        )
        .await
        .expect("Failed to query ts columns");

    assert!(!rows.is_empty(), "should have at least one row");

    for row in &rows {
        // ts() and _ts must return the same value.
        assert_eq!(
            row.ts_udf, row.ts_col,
            "ts() and _ts should be equal for block {}",
            row.block_num
        );
        // Both must match the regular timestamp column.
        assert_eq!(
            row.ts_udf, row.timestamp,
            "ts() should equal timestamp for block {}",
            row.block_num
        );
    }

    // Cross-check against Anvil RPC: fetch each block's timestamp directly.
    use alloy::providers::Provider as _;
    let provider = test_ctx.anvil().provider();
    for row in &rows {
        let block = provider
            .get_block_by_number(BlockNumberOrTag::Number(row.block_num))
            .await
            .unwrap_or_else(|e| panic!("Failed to get block {} from Anvil: {e}", row.block_num))
            .unwrap_or_else(|| panic!("Block {} not found in Anvil", row.block_num));
        let anvil_ts = block.header.timestamp;

        // Parse the SQL timestamp string to a unix epoch for comparison.
        // The JSONL output format is like "2026-03-31T11:30:48Z".
        let parsed = chrono::DateTime::parse_from_rfc3339(&row.ts_udf)
            .unwrap_or_else(|e| panic!("Failed to parse ts() value {:?}: {e}", row.ts_udf));
        let sql_epoch = parsed.timestamp() as u64;

        assert_eq!(
            sql_epoch, anvil_ts,
            "ts() epoch ({sql_epoch}) should match Anvil block timestamp ({anvil_ts}) for block {}",
            row.block_num
        );
    }
}
