use std::{collections::BTreeSet, time::Duration};

use alloy::{eips::BlockNumberOrTag, providers::Provider};
use chrono::DateTime;
use datasets_common::reference::Reference;
use monitoring::logging;
use serde::Deserialize;

use crate::testlib::{
    ctx::{TestCtx, TestCtxBuilder},
    fixtures::{Anvil, DaemonConfigBuilder, FlightClient},
    helpers as test_helpers,
};

/// Helper to create a provider TOML config for a given Anvil instance and network name.
fn provider_config(anvil: &Anvil, network: &str) -> String {
    indoc::formatdoc! {r#"
        kind = "evm-rpc"
        url = "{url}"
        network = "{network}"
    "#,
        url = anvil.connection_url(),
    }
}

/// Set up a test context with two Anvil instances registered as separate networks.
///
/// Both Anvil instances are configured with deterministic 1-second block timestamp
/// intervals starting from the same aligned timestamp, so mined block timestamps
/// are identical across networks and the common watermark includes all blocks.
///
/// Returns `(TestCtx, Anvil, aligned_start)` where `aligned_start` is the unix
/// timestamp of block 1 on both networks.
async fn setup(test_name: &str) -> (TestCtx, Anvil, u64) {
    logging::init();

    // Use a small microbatch interval (3 seconds) to force multiple batches
    // across the ~10-second block range, exercising batch-boundary correctness.
    let config = DaemonConfigBuilder::new()
        .microbatch_max_interval(3)
        .build();
    let ctx = TestCtxBuilder::new(test_name)
        .with_config(config)
        .with_dataset_manifests(["anvil_rpc_a", "anvil_rpc_b"])
        .with_anvil_ipc()
        .build()
        .await
        .expect("Failed to create test context");

    // Create a second Anvil instance for network B.
    let anvil_b = Anvil::new_ipc()
        .await
        .expect("Failed to create second Anvil");
    anvil_b
        .wait_for_ready(Duration::from_secs(30))
        .await
        .expect("Second Anvil not ready");

    // Align both Anvils to deterministic 1-second block intervals starting from
    // the same timestamp. Read both genesis timestamps, pick the later one, and
    // set block 1 on both networks to start 1 second after that.
    let anvil_a = ctx.anvil();
    let genesis_ts_a = anvil_a
        .provider()
        .get_block_by_number(BlockNumberOrTag::Earliest)
        .await
        .expect("get genesis A")
        .expect("genesis A exists")
        .header
        .timestamp;
    let genesis_ts_b = anvil_b
        .provider()
        .get_block_by_number(BlockNumberOrTag::Earliest)
        .await
        .expect("get genesis B")
        .expect("genesis B exists")
        .header
        .timestamp;
    let aligned_start = genesis_ts_a.max(genesis_ts_b) + 1;
    for anvil in [anvil_a, &anvil_b] {
        anvil
            .set_block_timestamp_interval(1)
            .await
            .expect("Failed to set block timestamp interval");
        anvil
            .set_next_block_timestamp(aligned_start)
            .await
            .expect("Failed to set next block timestamp");
    }

    // Register both Anvil instances as providers with distinct network names.
    let ampctl = ctx.new_ampctl();
    ampctl
        .register_provider("anvil_a", &provider_config(anvil_a, "anvil-a"))
        .await
        .expect("Failed to register anvil_a provider");
    ampctl
        .register_provider("anvil_b", &provider_config(&anvil_b, "anvil-b"))
        .await
        .expect("Failed to register anvil_b provider");

    (ctx, anvil_b, aligned_start)
}

/// Dump a dataset up to a given block number.
async fn dump(ctx: &TestCtx, dataset: &str, end_block: Option<u64>) {
    let ampctl = ctx.new_ampctl();
    let dataset_ref: Reference = format!("_/{}@0.0.0", dataset)
        .parse()
        .expect("valid dataset reference");
    test_helpers::deploy_and_wait(
        &ampctl,
        &dataset_ref,
        end_block,
        Duration::from_secs(30),
        false,
    )
    .await
    .unwrap_or_else(|e| panic!("Failed to dump {}: {}", dataset, e));
}

/// Take rows from a named stream, deserialized into `T`.
async fn take_rows<T: serde::de::DeserializeOwned>(
    client: &mut FlightClient,
    name: &str,
    n: usize,
) -> Vec<T> {
    let (json, _batch_count) = client
        .take_from_stream(name, n)
        .await
        .expect("Failed to take from stream");
    serde_json::from_value(json).expect("Failed to deserialize rows")
}

/// Parse an RFC3339 `_ts` string into a unix timestamp in seconds.
fn ts_secs(ts: &str) -> u64 {
    DateTime::parse_from_rfc3339(ts)
        .unwrap_or_else(|e| panic!("invalid _ts {ts:?}: {e}"))
        .timestamp() as u64
}

#[derive(Debug, Deserialize)]
struct UnionRow {
    network: String,
    block_num: u64,
    #[serde(rename = "_ts")]
    ts: String,
}

#[derive(Debug, Deserialize)]
struct JoinRow {
    block_a: u64,
    block_b: u64,
}

/// Verify that a streaming UNION ALL across two networks produces rows from both
/// with correct block numbers and timestamps.
///
/// With deterministic timestamps (both networks sharing the same base and 1-second
/// intervals), the common watermark includes all mined blocks, so we can assert
/// exact row content.
#[tokio::test(flavor = "multi_thread")]
async fn streaming_multi_network_union() {
    let (ctx, anvil_b, aligned_start) = setup("streaming_multi_network_union").await;
    let anvil_a = ctx.anvil();

    anvil_a.mine(10).await.expect("mine A");
    anvil_b.mine(10).await.expect("mine B");

    dump(&ctx, "anvil_rpc_a", Some(10)).await;
    dump(&ctx, "anvil_rpc_b", Some(10)).await;

    let query = r#"
        SELECT 'a' as network, block_num, _ts FROM anvil_rpc_a.blocks
        UNION ALL
        SELECT 'b' as network, block_num, _ts FROM anvil_rpc_b.blocks
        SETTINGS stream = true
    "#;
    let mut client = ctx
        .new_flight_client()
        .await
        .expect("Failed to create flight client");
    client
        .register_stream("stream", query)
        .await
        .expect("Failed to register stream");

    // Both networks have blocks 0-10 (11 blocks each) with identical timestamps.
    let rows: Vec<UnionRow> = take_rows(&mut client, "stream", 22).await;
    assert_eq!(rows.len(), 22, "expected 22 rows, got {}", rows.len());

    // Rows must be in non-decreasing _ts order (streaming watermark guarantee).
    for pair in rows.windows(2) {
        assert!(
            ts_secs(&pair[0].ts) <= ts_secs(&pair[1].ts),
            "rows not in _ts order: {:?} then {:?}",
            pair[0],
            pair[1]
        );
    }

    // Verify no duplicates: raw row count per network must match expected.
    let a_count = rows.iter().filter(|r| r.network == "a").count();
    let b_count = rows.iter().filter(|r| r.network == "b").count();
    assert_eq!(a_count, 11, "network A has duplicate rows: got {a_count}");
    assert_eq!(b_count, 11, "network B has duplicate rows: got {b_count}");

    // Verify exact block sets per network.
    let a_blocks: BTreeSet<u64> = rows
        .iter()
        .filter(|r| r.network == "a")
        .map(|r| r.block_num)
        .collect();
    let b_blocks: BTreeSet<u64> = rows
        .iter()
        .filter(|r| r.network == "b")
        .map(|r| r.block_num)
        .collect();

    let expected: BTreeSet<u64> = (0..=10).collect();
    assert_eq!(a_blocks, expected, "network A blocks: got {:?}", a_blocks);
    assert_eq!(b_blocks, expected, "network B blocks: got {:?}", b_blocks);

    // Verify timestamps: blocks 1-10 have aligned_start + (block_num - 1).
    // Block 0 is the genesis block whose timestamp predates aligned_start.
    for row in &rows {
        let ts = ts_secs(&row.ts);
        if row.block_num == 0 {
            assert!(
                ts < aligned_start,
                "{}: block 0 ts {} should be before aligned_start {}",
                row.network,
                ts,
                aligned_start
            );
        } else {
            let expected_ts = aligned_start + row.block_num - 1;
            assert_eq!(
                ts, expected_ts,
                "{}: block {} ts {} != expected {}",
                row.network, row.block_num, ts, expected_ts
            );
        }
    }
}

/// Verify that a streaming cross-network JOIN produces matched rows with correct
/// block numbers.
#[tokio::test(flavor = "multi_thread")]
async fn streaming_multi_network_join() {
    let (ctx, anvil_b, _aligned_start) = setup("streaming_multi_network_join").await;
    let anvil_a = ctx.anvil();

    anvil_a.mine(10).await.expect("mine A");
    anvil_b.mine(10).await.expect("mine B");

    dump(&ctx, "anvil_rpc_a", Some(10)).await;
    dump(&ctx, "anvil_rpc_b", Some(10)).await;

    let query = r#"
        SELECT a.block_num as block_a, b.block_num as block_b
        FROM anvil_rpc_a.blocks a
        CROSS JOIN anvil_rpc_b.blocks b
        WHERE a.block_num = b.block_num
        SETTINGS stream = true
    "#;
    let mut client = ctx
        .new_flight_client()
        .await
        .expect("Failed to create flight client");
    client
        .register_stream("stream", query)
        .await
        .expect("Failed to register stream");

    // With identical timestamps, all 11 blocks match (0-10).
    let rows: Vec<JoinRow> = take_rows(&mut client, "stream", 11).await;
    assert_eq!(rows.len(), 11, "expected 11 join rows, got {}", rows.len());

    // Every row must have block_a == block_b.
    for row in &rows {
        assert_eq!(row.block_a, row.block_b, "mismatched join: {:?}", row);
    }

    // Verify the exact set of joined block numbers.
    let joined_blocks: BTreeSet<u64> = rows.iter().map(|r| r.block_a).collect();
    let expected: BTreeSet<u64> = (0..=10).collect();
    assert_eq!(
        joined_blocks, expected,
        "expected blocks 0-10, got {:?}",
        joined_blocks
    );
}

/// Verify that a reorg on one network rewinds ALL networks in the stream output.
#[tokio::test(flavor = "multi_thread")]
async fn streaming_multi_network_reorg() {
    let (ctx, anvil_b, _aligned_start) = setup("streaming_multi_network_reorg").await;
    let anvil_a = ctx.anvil();

    anvil_a.mine(10).await.expect("mine A");
    anvil_b.mine(10).await.expect("mine B");

    dump(&ctx, "anvil_rpc_a", Some(10)).await;
    dump(&ctx, "anvil_rpc_b", Some(10)).await;

    let query = r#"
        SELECT 'a' as network, block_num, _ts FROM anvil_rpc_a.blocks
        UNION ALL
        SELECT 'b' as network, block_num, _ts FROM anvil_rpc_b.blocks
        SETTINGS stream = true
    "#;
    let mut client = ctx
        .new_flight_client()
        .await
        .expect("Failed to create flight client");
    client
        .register_stream("stream", query)
        .await
        .expect("Failed to register stream");

    // Take all 22 initial rows (11 per network, blocks 0-10).
    let initial: Vec<UnionRow> = take_rows(&mut client, "stream", 22).await;
    assert_eq!(initial.len(), 22, "expected 22 initial rows");

    // Verify initial rows are _ts-ordered.
    for pair in initial.windows(2) {
        assert!(
            ts_secs(&pair[0].ts) <= ts_secs(&pair[1].ts),
            "initial rows not in _ts order: {:?} then {:?}",
            pair[0],
            pair[1]
        );
    }

    // Verify exact initial content.
    let initial_a: BTreeSet<u64> = initial
        .iter()
        .filter(|r| r.network == "a")
        .map(|r| r.block_num)
        .collect();
    let initial_b: BTreeSet<u64> = initial
        .iter()
        .filter(|r| r.network == "b")
        .map(|r| r.block_num)
        .collect();
    let expected_initial: BTreeSet<u64> = (0..=10).collect();
    assert_eq!(initial_a, expected_initial, "initial A blocks");
    assert_eq!(initial_b, expected_initial, "initial B blocks");

    // Trigger reorg on network A only (depth 2: blocks 9-10 replaced), then mine
    // new blocks on both.
    anvil_a.reorg(2).await.expect("reorg A");
    anvil_a.mine(5).await.expect("mine A after reorg");
    anvil_b.mine(5).await.expect("mine B after reorg");

    // Dump A twice to ensure reorg detection, then dump B.
    dump(&ctx, "anvil_rpc_a", Some(15)).await;
    dump(&ctx, "anvil_rpc_a", Some(15)).await;
    dump(&ctx, "anvil_rpc_b", Some(15)).await;

    // After reorg on A, the multi-network streaming logic rewinds ALL networks
    // to the min reorg timestamp. A reorged at depth 2 (blocks 9-10 replaced)
    // then mined 5 new (blocks 11-15). B had no reorg but gets re-emitted from
    // the rewind point onward.
    //
    // The reorg replaces A's blocks 9-10 with wall-clock timestamps, so the
    // deterministic timestamp alignment is broken for A after the reorg point.
    // The common watermark may therefore exclude 1-2 trailing blocks. We verify
    // structural properties: both networks contribute, the rewind point is at
    // block 9, and rows are _ts-ordered.
    let rewound: Vec<UnionRow> = take_rows(&mut client, "stream", 12).await;
    assert_eq!(rewound.len(), 12, "expected 12 rewound rows");

    // Rewound rows must be _ts-ordered.
    for pair in rewound.windows(2) {
        assert!(
            ts_secs(&pair[0].ts) <= ts_secs(&pair[1].ts),
            "rewound rows not in _ts order: {:?} then {:?}",
            pair[0],
            pair[1]
        );
    }

    // Both networks must be present in the rewound output.
    let rewound_a: BTreeSet<u64> = rewound
        .iter()
        .filter(|r| r.network == "a")
        .map(|r| r.block_num)
        .collect();
    let rewound_b: BTreeSet<u64> = rewound
        .iter()
        .filter(|r| r.network == "b")
        .map(|r| r.block_num)
        .collect();
    assert!(
        !rewound_a.is_empty() && !rewound_b.is_empty(),
        "expected rows from both networks, got A={:?} B={:?}",
        rewound_a,
        rewound_b
    );

    // The rewind point must start at block 9 (the first reorged block) for both.
    assert_eq!(
        rewound_a.iter().next().copied(),
        Some(9),
        "A rewind should start at block 9, got {:?}",
        rewound_a
    );
    assert_eq!(
        rewound_b.iter().next().copied(),
        Some(9),
        "B rewind should start at block 9, got {:?}",
        rewound_b
    );

    // Both networks must include the reorged blocks (9-10) and at least some
    // new blocks beyond 10.
    assert!(
        rewound_a.contains(&9) && rewound_a.contains(&10),
        "A must include reorged blocks 9-10, got {:?}",
        rewound_a
    );
    assert!(
        *rewound_a.iter().last().expect("A non-empty") > 10,
        "A must include blocks beyond 10, got {:?}",
        rewound_a
    );
    assert!(
        rewound_b.contains(&9) && rewound_b.contains(&10),
        "B must include re-emitted blocks 9-10, got {:?}",
        rewound_b
    );
    assert!(
        *rewound_b.iter().last().expect("B non-empty") > 10,
        "B must include blocks beyond 10, got {:?}",
        rewound_b
    );

    // Both networks' rewound block ranges must be contiguous.
    for (name, blocks) in [("A", &rewound_a), ("B", &rewound_b)] {
        let min = *blocks.iter().next().expect("blocks non-empty");
        let max = *blocks.iter().last().expect("blocks non-empty");
        let expected_contiguous: BTreeSet<u64> = (min..=max).collect();
        assert_eq!(
            blocks, &expected_contiguous,
            "{name} rewound blocks must be contiguous, got {:?}",
            blocks
        );
    }
}
