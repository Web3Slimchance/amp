//! Integration tests for filter pushthrough on `evm_decode` output fields.
//!
//! Verifies that the `FilterPushthroughDecodeRule` optimizer rule derives
//! topic-column predicates from filters on decoded indexed parameters, and
//! that those derived predicates appear in the physical plan.
//!
//! Reuses the `sql-tests` spec to set up a synced `eth_rpc` dataset, then
//! runs EXPLAIN queries to inspect the physical plan.

use monitoring::logging;

use crate::{steps::run_spec, testlib::ctx::TestCtxBuilder};

/// Sets up the eth_rpc dataset via the sql-tests spec, then verifies that
/// EXPLAIN plans for evm_decode filter queries contain derived topic predicates.
#[tokio::test(flavor = "multi_thread")]
async fn filter_pushthrough_decode() {
    logging::init();

    let test_ctx = TestCtxBuilder::new("filter_pushthrough")
        .with_dataset_manifests(["eth_rpc", "eth_firehose"])
        .with_dataset_snapshots(["eth_rpc", "eth_firehose"])
        .with_provider_configs(["rpc_eth_mainnet", "firehose_eth_mainnet"])
        .build()
        .await
        .expect("Failed to create test environment");

    let mut client = test_ctx
        .new_flight_client()
        .await
        .expect("Failed to connect FlightClient");

    // Run the sql-tests spec to sync data into the dataset.
    run_spec("sql-tests", &test_ctx, &mut client, None)
        .await
        .expect("Failed to run sql-tests spec");

    // --- Verify derived topic2 predicate for indexed 'to' field ---
    let plan = explain(
        &mut client,
        "\
        SELECT pc.dec['to'] AS to_address \
        FROM ( \
            SELECT \
                evm_decode(l.topic1, l.topic2, l.topic3, l.data, \
                    'Transfer(address indexed from, address indexed to, uint256 value)') AS dec \
            FROM eth_rpc.logs l \
            WHERE l.topic0 = evm_topic('Transfer(address indexed from, address indexed to, uint256 value)') \
                AND l.topic3 IS NULL \
        ) pc \
        WHERE pc.dec['to'] = X'beefbabeea323f07c59926295205d3b7a17e8638'",
    )
    .await;

    assert!(
        plan.contains("topic2"),
        "expected derived topic2 predicate in physical plan:\n{plan}"
    );
    // The derived predicate should be a topic2 equality (not just any
    // reference to topic2 from e.g. evm_decode arguments).
    assert!(
        plan.contains("topic2 =") || plan.contains("topic2@"),
        "expected derived topic2 equality predicate in physical plan:\n{plan}"
    );

    // --- Verify derived topic1 predicate for indexed 'from' field ---
    let plan = explain(
        &mut client,
        "\
        SELECT pc.dec['from'] AS from_address \
        FROM ( \
            SELECT \
                evm_decode(l.topic1, l.topic2, l.topic3, l.data, \
                    'Transfer(address indexed from, address indexed to, uint256 value)') AS dec \
            FROM eth_rpc.logs l \
            WHERE l.topic0 = evm_topic('Transfer(address indexed from, address indexed to, uint256 value)') \
                AND l.topic3 IS NULL \
        ) pc \
        WHERE pc.dec['from'] = X'06729eb2424da47898f935267bd4a62940de5105'",
    )
    .await;

    assert!(
        plan.contains("topic1"),
        "expected derived topic1 predicate in physical plan:\n{plan}"
    );

    // --- Verify non-indexed field does NOT produce derived topic predicate ---
    let plan = explain(
        &mut client,
        "\
        SELECT pc.dec['value'] AS value \
        FROM ( \
            SELECT \
                evm_decode(l.topic1, l.topic2, l.topic3, l.data, \
                    'Transfer(address indexed from, address indexed to, uint256 value)') AS dec \
            FROM eth_rpc.logs l \
            WHERE l.topic0 = evm_topic('Transfer(address indexed from, address indexed to, uint256 value)') \
                AND l.topic3 IS NULL \
        ) pc \
        WHERE pc.dec['value'] > 0",
    )
    .await;

    // topic0 may appear from the WHERE clause, but topic1/topic2 should
    // not appear as derived equality predicates.
    let has_derived_topic = plan.contains("topic1 =") || plan.contains("topic2 =");
    assert!(
        !has_derived_topic,
        "non-indexed field should not produce derived topic predicate:\n{plan}"
    );
}

/// Runs EXPLAIN on a query and returns the plan text.
async fn explain(client: &mut crate::testlib::fixtures::FlightClient, query: &str) -> String {
    let explain_query = format!("EXPLAIN {query}");
    let (json, _) = client
        .run_query(&explain_query, None)
        .await
        .unwrap_or_else(|e| panic!("EXPLAIN query failed: {e}\nquery: {explain_query}"));

    match json {
        serde_json::Value::Array(rows) => rows
            .iter()
            .filter_map(|row| row.get("plan").and_then(|v| v.as_str()))
            .collect::<Vec<_>>()
            .join("\n"),
        other => panic!("Unexpected EXPLAIN output format: {other:?}"),
    }
}
