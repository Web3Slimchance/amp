//! Integration tests for block_num() in batch queries with Anvil.

use monitoring::logging;

use crate::{steps::run_spec, testlib::ctx::TestCtxBuilder};

#[tokio::test(flavor = "multi_thread")]
async fn batch_block_num() {
    logging::init();

    let test_ctx = TestCtxBuilder::new("batch_block_num")
        .with_anvil_ipc()
        .with_dataset_manifest("anvil_rpc")
        .build()
        .await
        .expect("Failed to create test environment");

    let mut client = test_ctx
        .new_flight_client()
        .await
        .expect("Failed to connect FlightClient");

    run_spec("batch-block-num-anvil", &test_ctx, &mut client, None)
        .await
        .expect("Failed to run batch block_num spec");
}
