//! Worker job context and [`FromCtx`](amp_job_core::ctx::FromCtx) implementations
//! for extracting job-specific contexts from [`WorkerJobCtx`].

use std::sync::Arc;

use amp_data_store::DataStore;
use amp_job_core::{ctx::FromCtx, events::EventEmitter};
use common::{datasets_cache::DatasetsCache, udfs::eth_call::EthCallUdfsCache};
use js_runtime::isolate_pool::IsolatePool;
use metadata_db::{MetadataDb, NotificationMultiplexerHandle};
use monitoring::telemetry::metrics::Meter;

use crate::config::Config;

/// Worker job context parameters.
#[derive(Clone)]
pub(crate) struct WorkerJobCtx {
    pub config: Config,
    pub metadata_db: MetadataDb,
    pub data_store: DataStore,
    pub datasets_cache: DatasetsCache,
    pub ethcall_udfs_cache: EthCallUdfsCache,
    pub isolate_pool: IsolatePool,
    pub notification_multiplexer: Arc<NotificationMultiplexerHandle>,
    pub meter: Option<Meter>,
    pub event_emitter: Arc<dyn EventEmitter>,
}

impl FromCtx<WorkerJobCtx> for amp_job_gc::job_ctx::Context {
    fn from_ctx(ctx: &WorkerJobCtx) -> Self {
        Self {
            data_store: ctx.data_store.clone(),
            meter: ctx.meter.clone(),
        }
    }
}

impl FromCtx<WorkerJobCtx> for amp_job_materialize_datasets_raw::job_ctx::Context {
    fn from_ctx(ctx: &WorkerJobCtx) -> Self {
        Self {
            config: amp_job_materialize_datasets_raw::job_ctx::Config {
                poll_interval: ctx.config.poll_interval,
                progress_interval: ctx.config.events_config.progress_interval.clone().into(),
                parquet_writer: (&ctx.config.parquet).into(),
            },
            metadata_db: ctx.metadata_db.clone(),
            datasets_cache: ctx.datasets_cache.clone(),
            ethcall_udfs_cache: ctx.ethcall_udfs_cache.clone(),
            providers_registry: ctx.ethcall_udfs_cache.providers_registry().clone(),
            data_store: ctx.data_store.clone(),
            notification_multiplexer: ctx.notification_multiplexer.clone(),
            meter: ctx.meter.clone(),
            event_emitter: ctx.event_emitter.clone(),
        }
    }
}

impl FromCtx<WorkerJobCtx> for amp_job_materialize_datasets_derived::job_ctx::Context {
    fn from_ctx(ctx: &WorkerJobCtx) -> Self {
        Self {
            config: amp_job_materialize_datasets_derived::job_ctx::Config {
                keep_alive_interval: ctx.config.keep_alive_interval,
                max_mem_mb: ctx.config.max_mem_mb,
                query_max_mem_mb: ctx.config.query_max_mem_mb,
                spill_location: ctx.config.spill_location.clone(),
                metadata_fetch_concurrency: ctx.config.metadata_fetch_concurrency,
                progress_interval: ctx.config.events_config.progress_interval.clone().into(),
                parquet_writer: (&ctx.config.parquet).into(),
                microbatch_max_interval: ctx.config.microbatch_max_interval,
            },
            metadata_db: ctx.metadata_db.clone(),
            datasets_cache: ctx.datasets_cache.clone(),
            ethcall_udfs_cache: ctx.ethcall_udfs_cache.clone(),
            data_store: ctx.data_store.clone(),
            isolate_pool: ctx.isolate_pool.clone(),
            notification_multiplexer: ctx.notification_multiplexer.clone(),
            meter: ctx.meter.clone(),
            event_emitter: ctx.event_emitter.clone(),
        }
    }
}
