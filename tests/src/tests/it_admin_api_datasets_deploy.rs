//! Integration tests for dataset deployment via the admin API.
//!
//! Verifies redeployment idempotency and retry_strategy round-tripping through
//! the admin API: the field is persisted in the job descriptor when provided
//! and absent when omitted.

use amp_client_admin::{self as client, end_block::EndBlock};
use amp_job_core::{
    job_id::JobId,
    retry_strategy::{Backoff, BaseDelaySecs, ExponentialParams, MaxAttempts, RetryStrategy},
};
use datasets_common::reference::Reference;

use crate::testlib::ctx::TestCtxBuilder;

#[tokio::test]
async fn redeploy_with_different_end_block_returns_same_job_id() {
    //* Given
    let ctx = TestCtx::setup("test_redeploy_diff_endblock").await;

    // Mine some blocks so the dataset has data to sync
    ctx.mine_blocks(10).await;

    // First deploy with no end block (continuous)
    let _job_id_1 = ctx
        .deploy_dataset(None)
        .await
        .expect("first deployment should succeed");

    // Wait for the job to be fully scheduled
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    //* When — redeploy with a different end_block while job is still active
    let job_id_2 = ctx.deploy_dataset(None).await;

    //* Then — 409 conflict because an active job already exists with different options
    let error = job_id_2.expect_err("second deploy should fail with conflict");
    assert!(
        matches!(error, client::datasets::DeployError::ActiveJobConflict(_)),
        "expected ActiveJobConflict, got: {error:?}"
    );
}

// --- Retry strategy tests ---

#[tokio::test]
async fn deploy_with_retry_strategy_persists_in_descriptor() {
    //* Given
    let ctx = TestCtx::setup("deploy_retry_bounded").await;

    let retry_strategy = RetryStrategy::Bounded {
        max_attempts: MaxAttempts::new(10).unwrap(),
        backoff: Backoff::Exponential {
            params: ExponentialParams::new(1, 3.0, None).unwrap(),
        },
    };

    //* When
    let job_id = ctx
        .deploy_dataset_with_retry(Some(EndBlock::Latest), Some(retry_strategy.clone()))
        .await
        .expect("deployment should succeed");

    //* Then
    let job = ctx
        .ampctl_client
        .jobs()
        .get(&job_id)
        .await
        .expect("get job should succeed")
        .expect("job should exist");

    let rs: RetryStrategy = serde_json::from_value(job.descriptor["retry_strategy"].clone())
        .expect("retry_strategy should deserialize");
    assert_eq!(rs, retry_strategy, "persisted strategy should match input");
}

#[tokio::test]
async fn deploy_without_retry_strategy_defaults_to_unless_stopped() {
    //* Given
    let ctx = TestCtx::setup("deploy_retry_omitted").await;

    //* When
    let job_id = ctx
        .deploy_dataset(Some(EndBlock::Latest))
        .await
        .expect("deployment should succeed");

    //* Then
    let job = ctx
        .ampctl_client
        .jobs()
        .get(&job_id)
        .await
        .expect("get job should succeed")
        .expect("job should exist");

    let rs: RetryStrategy = serde_json::from_value(job.descriptor["retry_strategy"].clone())
        .expect("retry_strategy should deserialize");
    assert_eq!(
        rs,
        RetryStrategy::default(),
        "default retry_strategy should be UnlessStopped with exponential backoff"
    );
}

#[tokio::test]
async fn deploy_with_none_retry_strategy_persists_in_descriptor() {
    //* Given
    let ctx = TestCtx::setup("deploy_retry_none").await;

    //* When
    let job_id = ctx
        .deploy_dataset_with_retry(Some(EndBlock::Latest), Some(RetryStrategy::None))
        .await
        .expect("deployment should succeed");

    //* Then
    let job = ctx
        .ampctl_client
        .jobs()
        .get(&job_id)
        .await
        .expect("get job should succeed")
        .expect("job should exist");

    let rs: RetryStrategy = serde_json::from_value(job.descriptor["retry_strategy"].clone())
        .expect("retry_strategy should deserialize");
    assert_eq!(rs, RetryStrategy::None, "persisted strategy should be None");
}

#[tokio::test]
async fn deploy_with_unless_stopped_retry_strategy_persists_in_descriptor() {
    //* Given
    let ctx = TestCtx::setup("deploy_retry_unless_stopped").await;

    let retry_strategy = RetryStrategy::UnlessStopped {
        backoff: Backoff::Fixed {
            base_delay_secs: BaseDelaySecs::new(10).unwrap(),
        },
    };

    //* When
    let job_id = ctx
        .deploy_dataset_with_retry(Some(EndBlock::Latest), Some(retry_strategy.clone()))
        .await
        .expect("deployment should succeed");

    //* Then
    let job = ctx
        .ampctl_client
        .jobs()
        .get(&job_id)
        .await
        .expect("get job should succeed")
        .expect("job should exist");

    let rs: RetryStrategy = serde_json::from_value(job.descriptor["retry_strategy"].clone())
        .expect("retry_strategy should deserialize");
    assert_eq!(rs, retry_strategy, "persisted strategy should match input");
}

#[tokio::test]
async fn deploy_with_exponential_jitter_backoff_persists_in_descriptor() {
    //* Given
    let ctx = TestCtx::setup("deploy_retry_exp_jitter").await;

    let retry_strategy = RetryStrategy::Bounded {
        max_attempts: MaxAttempts::new(5).unwrap(),
        backoff: Backoff::ExponentialWithJitter {
            params: ExponentialParams::new(2, 3.0, Some(120)).unwrap(),
        },
    };

    //* When
    let job_id = ctx
        .deploy_dataset_with_retry(Some(EndBlock::Latest), Some(retry_strategy.clone()))
        .await
        .expect("deployment should succeed");

    //* Then
    let job = ctx
        .ampctl_client
        .jobs()
        .get(&job_id)
        .await
        .expect("get job should succeed")
        .expect("job should exist");

    let rs: RetryStrategy = serde_json::from_value(job.descriptor["retry_strategy"].clone())
        .expect("retry_strategy should deserialize");
    assert_eq!(rs, retry_strategy, "persisted strategy should match input");
}

// --- Test helpers ---

struct TestCtx {
    ctx: crate::testlib::ctx::TestCtx,
    dataset_ref: Reference,
    ampctl_client: client::Client,
}

impl TestCtx {
    async fn setup(test_name: &str) -> Self {
        let dataset_ref: Reference = "_/anvil_rpc@0.0.0"
            .parse()
            .expect("Failed to parse dataset reference");

        let ctx = TestCtxBuilder::new(test_name)
            .with_anvil_http()
            .with_dataset_manifest("anvil_rpc")
            .build()
            .await
            .expect("failed to build test context");

        let admin_api_url = ctx.daemon_controller().admin_api_url();
        let base_url = admin_api_url
            .parse()
            .expect("failed to parse admin API URL");

        let ampctl_client = client::Client::new(base_url);

        Self {
            ctx,
            dataset_ref,
            ampctl_client,
        }
    }

    async fn mine_blocks(&self, count: u64) {
        self.ctx
            .anvil()
            .mine(count)
            .await
            .expect("failed to mine blocks");
    }

    async fn deploy_dataset(
        &self,
        end_block: Option<EndBlock>,
    ) -> Result<JobId, client::datasets::DeployError> {
        self.deploy_dataset_with_retry(end_block, None).await
    }

    async fn deploy_dataset_with_retry(
        &self,
        end_block: Option<EndBlock>,
        retry_strategy: Option<RetryStrategy>,
    ) -> Result<JobId, client::datasets::DeployError> {
        self.ampctl_client
            .datasets()
            .deploy(&self.dataset_ref, end_block, 1, None, false, retry_strategy)
            .await
    }
}
