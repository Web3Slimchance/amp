//! Internal job implementation for the worker service.

use std::sync::Arc;

use amp_worker_core::{
    ProgressReporter,
    error_detail::ErrorDetailsProvider,
    jobs::job_id::JobId,
    metrics::MetricsRegistry,
    retryable::{JobErrorExt, RetryableErrorExt},
};
use datasets_common::hash_reference::HashReference;
use tracing::{Instrument, info_span};

use crate::{
    events::WorkerProgressReporter, job::JobDescriptor, kafka::proto, service::WorkerJobCtx,
};

/// Create and run a worker job.
///
/// This function returns a future that executes the job operation.
/// Raw datasets are handled by `amp_worker_datasets_raw`, derived
/// datasets by `amp_worker_datasets_derived`, and garbage collection
/// by `amp_worker_gc`.
pub(super) async fn new(
    job_ctx: WorkerJobCtx,
    job_desc: JobDescriptor,
    job_id: JobId,
) -> Result<(), JobError> {
    match job_desc {
        JobDescriptor::Gc(desc) => {
            let ctx = amp_worker_gc::job_ctx::Context {
                metadata_db: job_ctx.metadata_db.clone(),
                data_store: job_ctx.data_store.clone(),
                meter: job_ctx.meter.clone(),
            };

            amp_worker_gc::job_impl::execute(ctx, desc)
                .instrument(info_span!("gc_job", %job_id))
                .await
                .map_err(JobError::Gc)?;
        }
        JobDescriptor::MaterializeRaw(desc) => {
            let reference = HashReference::new(
                desc.dataset_namespace.clone(),
                desc.dataset_name.clone(),
                desc.manifest_hash.clone(),
            );
            let metrics = job_ctx
                .meter
                .as_ref()
                .map(|m| Arc::new(MetricsRegistry::new(m, reference.clone(), *job_id)));
            let progress_reporter: Option<Arc<dyn ProgressReporter>> = {
                let dataset_info = proto::DatasetInfo {
                    namespace: reference.namespace().to_string(),
                    name: reference.name().to_string(),
                    manifest_hash: reference.hash().to_string(),
                };
                Some(Arc::new(WorkerProgressReporter::new(
                    job_id,
                    dataset_info,
                    job_ctx.event_emitter.clone(),
                )))
            };
            let writer: metadata_db::jobs::JobId = job_id.into();

            let ctx = amp_worker_datasets_raw::job_ctx::Context {
                job_id: Some(writer),
                config: amp_worker_datasets_raw::job_ctx::Config {
                    poll_interval: job_ctx.config.poll_interval,
                    progress_interval: job_ctx
                        .config
                        .events_config
                        .progress_interval
                        .clone()
                        .into(),
                    parquet_writer: (&job_ctx.config.parquet).into(),
                },
                metadata_db: job_ctx.metadata_db.clone(),
                datasets_cache: job_ctx.datasets_cache.clone(),
                ethcall_udfs_cache: job_ctx.ethcall_udfs_cache.clone(),
                providers_registry: job_ctx.ethcall_udfs_cache.providers_registry().clone(),
                data_store: job_ctx.data_store.clone(),
                notification_multiplexer: job_ctx.notification_multiplexer.clone(),
                metrics,
                progress_reporter,
            };

            amp_worker_datasets_raw::job_impl::execute(ctx, desc, writer)
                .instrument(
                    info_span!("materialize_raw_job", %job_id, dataset = %format!("{reference:#}")),
                )
                .await
                .map_err(JobError::MaterializeRaw)?;
        }
        JobDescriptor::MaterializeDerived(desc) => {
            let reference = HashReference::new(
                desc.dataset_namespace.clone(),
                desc.dataset_name.clone(),
                desc.manifest_hash.clone(),
            );
            let metrics = job_ctx
                .meter
                .as_ref()
                .map(|m| Arc::new(MetricsRegistry::new(m, reference.clone(), *job_id)));
            let progress_reporter: Option<Arc<dyn ProgressReporter>> = {
                let dataset_info = proto::DatasetInfo {
                    namespace: reference.namespace().to_string(),
                    name: reference.name().to_string(),
                    manifest_hash: reference.hash().to_string(),
                };
                Some(Arc::new(WorkerProgressReporter::new(
                    job_id,
                    dataset_info,
                    job_ctx.event_emitter.clone(),
                )))
            };
            let writer: metadata_db::jobs::JobId = job_id.into();

            let ctx = amp_worker_datasets_derived::job_ctx::Context {
                job_id: Some(writer),
                config: amp_worker_datasets_derived::job_ctx::Config {
                    keep_alive_interval: job_ctx.config.keep_alive_interval,
                    max_mem_mb: job_ctx.config.max_mem_mb,
                    query_max_mem_mb: job_ctx.config.query_max_mem_mb,
                    spill_location: job_ctx.config.spill_location.clone(),
                    metadata_fetch_concurrency: job_ctx.config.metadata_fetch_concurrency,
                    progress_interval: job_ctx
                        .config
                        .events_config
                        .progress_interval
                        .clone()
                        .into(),
                    parquet_writer: (&job_ctx.config.parquet).into(),
                    microbatch_max_interval: job_ctx.config.microbatch_max_interval,
                },
                metadata_db: job_ctx.metadata_db.clone(),
                datasets_cache: job_ctx.datasets_cache.clone(),
                ethcall_udfs_cache: job_ctx.ethcall_udfs_cache.clone(),
                data_store: job_ctx.data_store.clone(),
                isolate_pool: job_ctx.isolate_pool.clone(),
                notification_multiplexer: job_ctx.notification_multiplexer.clone(),
                metrics,
                progress_reporter,
            };

            amp_worker_datasets_derived::job_impl::execute(ctx, desc, writer)
                .instrument(info_span!("materialize_derived_job", %job_id, dataset = %format!("{reference:#}")))
                .await
                .map_err(JobError::MaterializeDerived)?;
        }
    }

    Ok(())
}

/// Errors from worker job execution.
///
/// Wraps the specific error types from each job type to provide a unified
/// error type for the worker job system.
#[derive(Debug, thiserror::Error)]
pub(crate) enum JobError {
    /// Raw dataset materialization operation failed.
    #[error("Failed to materialize raw dataset")]
    MaterializeRaw(#[source] amp_worker_datasets_raw::job_impl::Error),

    /// Derived dataset materialization operation failed.
    #[error("Failed to materialize derived dataset")]
    MaterializeDerived(#[source] amp_worker_datasets_derived::job_impl::Error),

    /// Garbage collection job failed.
    #[error("Failed to run garbage collection")]
    Gc(#[source] amp_worker_gc::job_impl::Error),
}

impl JobErrorExt for JobError {
    fn error_code(&self) -> &'static str {
        match self {
            Self::MaterializeRaw(err) => err.error_code(),
            Self::MaterializeDerived(err) => err.error_code(),
            Self::Gc(err) => err.error_code(),
        }
    }
}

impl ErrorDetailsProvider for JobError {
    fn detail_source(&self) -> Option<&dyn ErrorDetailsProvider> {
        match self {
            Self::MaterializeRaw(err) => Some(err),
            Self::MaterializeDerived(err) => Some(err),
            Self::Gc(err) => Some(err),
        }
    }
}

impl RetryableErrorExt for JobError {
    fn is_retryable(&self) -> bool {
        match self {
            Self::MaterializeRaw(err) => err.is_retryable(),
            Self::MaterializeDerived(err) => err.is_retryable(),
            Self::Gc(err) => err.is_retryable(),
        }
    }
}
