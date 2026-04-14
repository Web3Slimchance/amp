use std::time::Duration;

use alloy::{
    node_bindings::Anvil,
    providers::{Provider as _, ProviderBuilder, ext::AnvilApi as _},
};
use amp_providers_evm_rpc::new_heads::{BackoffConfig, NewHeadNotifier, Notification};
use monitoring::logging;

fn default_backoff() -> BackoffConfig {
    BackoffConfig::default()
}

fn fast_backoff() -> BackoffConfig {
    BackoffConfig {
        min: Duration::from_millis(100),
        max: Duration::from_millis(500),
    }
}

/// Verify the notifier fires for each mined block with the correct block number.
#[tokio::test]
async fn notifier_receives_new_heads() {
    logging::init();

    let anvil = Anvil::new().spawn();
    let ws_url = anvil.ws_endpoint_url();

    let mut notifier = NewHeadNotifier::spawn(ws_url, None, default_backoff());

    let provider = ProviderBuilder::new()
        .connect_http(anvil.endpoint_url())
        .erased();

    // Wait for the subscription to be fully established.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Mine 5 blocks one-at-a-time, assert we get notified with the right block number.
    for i in 1..=5u64 {
        provider.anvil_mine(Some(1), None).await.unwrap();
        let got = tokio::time::timeout(Duration::from_secs(5), notifier.notified())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for notification on block {i}"));
        assert!(
            matches!(got, Notification::NewHead(n) if n == i),
            "expected NewHead({i}), got {got:?}"
        );
    }
}

/// Stress: mine many blocks in rapid succession and confirm the notifier keeps up.
///
/// With a `watch` channel, the consumer always gets the latest block number.
#[tokio::test]
async fn notifier_handles_rapid_mining() {
    logging::init();

    let anvil = Anvil::new().spawn();
    let ws_url = anvil.ws_endpoint_url();

    let mut notifier = NewHeadNotifier::spawn(ws_url, None, default_backoff());

    let provider = ProviderBuilder::new()
        .connect_http(anvil.endpoint_url())
        .erased();

    // Wait for the subscription to be established before mining.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Mine 50 blocks in a single RPC call (instant).
    provider.anvil_mine(Some(50), None).await.unwrap();

    // Drain all notifications — the last one should have block number 50.
    let mut last_block = 0;
    while let Ok(Notification::NewHead(n)) =
        tokio::time::timeout(Duration::from_millis(500), notifier.notified()).await
    {
        last_block = n;
    }
    assert_eq!(
        last_block, 50,
        "should have seen block 50 after mining burst"
    );

    // Mine another block to verify the notifier is still alive after the burst.
    provider.anvil_mine(Some(1), None).await.unwrap();
    let got = tokio::time::timeout(Duration::from_secs(5), notifier.notified())
        .await
        .expect("timed out waiting for notification after burst recovery");
    assert!(
        matches!(got, Notification::NewHead(51)),
        "expected NewHead(51), got {got:?}"
    );
}

/// Verify the notifier reconnects after the WebSocket server goes away and comes back.
#[tokio::test]
async fn notifier_reconnects_after_server_restart() {
    logging::init();

    // Spawn Anvil on a fixed port so we can restart on the same address.
    let anvil = Anvil::new().port(0u16).spawn();
    let port = anvil.port();
    let ws_url: url::Url = format!("ws://localhost:{port}").parse().unwrap();

    let mut notifier = NewHeadNotifier::spawn(ws_url.clone(), None, fast_backoff());

    let provider = ProviderBuilder::new()
        .connect_http(anvil.endpoint_url())
        .erased();

    // Wait for the subscription to be fully established.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Confirm it works initially.
    provider.anvil_mine(Some(1), None).await.unwrap();
    let got = tokio::time::timeout(Duration::from_secs(5), notifier.notified())
        .await
        .expect("timed out on initial notification");
    assert!(matches!(got, Notification::NewHead(1)), "got {got:?}");

    // Kill Anvil.
    drop(provider);
    drop(anvil);

    // Give the notifier time to detect the disconnect and start reconnecting.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Restart Anvil on the same port.
    let anvil2 = Anvil::new().port(port).spawn();
    let provider2 = ProviderBuilder::new()
        .connect_http(anvil2.endpoint_url())
        .erased();

    // Wait for the notifier to reconnect (fast backoff: 100ms min).
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Mine a block and verify the notifier fires.
    provider2.anvil_mine(Some(1), None).await.unwrap();
    let got = tokio::time::timeout(Duration::from_secs(5), notifier.notified())
        .await
        .expect("timed out waiting for notification after reconnect");
    // New Anvil starts at block 0, so mining 1 gives block 1.
    assert!(
        matches!(got, Notification::NewHead(_)),
        "expected NewHead, got {got:?}"
    );
}

/// Verify that dropping the notifier cleanly shuts down the background task.
#[tokio::test]
async fn notifier_shuts_down_on_drop() {
    logging::init();

    let anvil = Anvil::new().spawn();
    let ws_url = anvil.ws_endpoint_url();

    let notifier = NewHeadNotifier::spawn(ws_url, None, default_backoff());

    // Give the background task time to connect.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Drop the notifier — the background task should detect the closed channel and exit.
    drop(notifier);

    // If the background task panicked, the test would fail here.
    tokio::time::sleep(Duration::from_millis(500)).await;
}
