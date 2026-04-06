use std::{sync::Arc, time::Duration};

use amp_data_store::DataStore;
use amp_job_core::{events::EventEmitter, materialize::config::ParquetConfig};
use amp_providers_registry::ProvidersRegistry;
use common::{datasets_cache::DatasetsCache, udfs::eth_call::EthCallUdfsCache};
use metadata_db::{MetadataDb, NotificationMultiplexerHandle};
use monitoring::telemetry::metrics::Meter;

/// Job context for raw dataset materialization.
///
/// Contains configuration and runtime dependencies needed by the raw dataset crate,
/// making dependencies explicit. Constructed by the worker service before
/// dispatching a raw dataset job.
#[derive(Clone)]
pub struct Context {
    /// Job configuration parameters.
    pub config: Config,
    /// Connection pool for the metadata database.
    pub metadata_db: MetadataDb,
    /// In-memory cache of resolved dataset definitions.
    pub datasets_cache: DatasetsCache,
    /// Cache of compiled ethcall UDF definitions.
    pub ethcall_udfs_cache: EthCallUdfsCache,
    /// Registry of external data provider configurations.
    /// Duplicated from `ethcall_udfs_cache` for direct access during provider selection.
    pub providers_registry: ProvidersRegistry,
    /// Object store abstraction for reading/writing Parquet files.
    pub data_store: DataStore,
    /// Shared notification multiplexer for streaming queries.
    pub notification_multiplexer: Arc<NotificationMultiplexerHandle>,
    /// Optional meter for creating job-specific metrics registries.
    pub meter: Option<Meter>,
    /// Event emitter for creating job-specific progress reporters.
    pub event_emitter: Arc<dyn EventEmitter>,
}

/// Configuration parameters for raw dataset materialization.
///
/// Groups tunable settings that control how the raw dataset job operates,
/// separate from runtime service dependencies.
#[derive(Clone)]
pub struct Config {
    /// Poll interval for raw datasets.
    pub poll_interval: Duration,
    /// Progress event emission interval.
    pub progress_interval: Duration,
    /// Parquet file configuration.
    pub parquet_writer: ParquetConfig,
}
