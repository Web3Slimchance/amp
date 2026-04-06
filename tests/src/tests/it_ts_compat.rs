//! Tests that ts() works on old datasets via the timestamp→_ts compat layer.

use monitoring::logging;

use crate::{steps::run_spec, testlib::ctx::TestCtxBuilder};

#[tokio::test(flavor = "multi_thread")]
async fn ts_compat_with_timestamp_column() {
    logging::init();

    let test_ctx = TestCtxBuilder::new("ts_compat")
        .with_anvil_ipc()
        .with_dataset_manifest("anvil_rpc")
        .build()
        .await
        .expect("Failed to create test environment");

    let mut client = test_ctx
        .new_flight_client()
        .await
        .expect("Failed to connect FlightClient");

    run_spec("batch-ts-compat-anvil", &test_ctx, &mut client, None)
        .await
        .expect("Failed to run batch ts compat spec");
}
