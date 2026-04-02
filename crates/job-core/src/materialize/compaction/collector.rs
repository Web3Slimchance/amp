use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::{Debug, Display, Formatter},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering::SeqCst},
    },
    time::Duration,
};

use amp_data_store::{DataStore, DeleteFilesStreamError};
use amp_parquet::timestamp::Timestamp;
use common::physical_table::PhysicalTable;
use futures::{StreamExt as _, TryStreamExt as _, stream};
use metadata_db::{MetadataDb, files::FileId, gc::GcManifestRow};
use object_store::{Error as ObjectStoreError, path::Path};
use tokio::task::JoinHandle;

use super::{
    compactor::{AmpCompactorTaskError, Compactor, TaskResult},
    config::ParquetConfig,
    error::{CollectionResult, CollectorError, CompactionResult, CompactorError},
};
use crate::materialize::{metrics::MetricsRegistry, writer::WriterProperties};

#[derive(Debug, Clone)]
pub struct CollectorProperties {
    pub active: Arc<AtomicBool>,
    pub interval: Duration,
    pub file_lock_duration: Duration,
}

impl<'a> From<&'a ParquetConfig> for CollectorProperties {
    fn from(config: &'a ParquetConfig) -> Self {
        CollectorProperties {
            active: Arc::new(AtomicBool::new(config.collector.active)),
            interval: config.collector.min_interval.clone().into(),
            file_lock_duration: config.collector.deletion_lock_duration.clone().into(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct AmpCollectorInnerTask {
    pub compactor: Compactor,
    pub collector: Collector,
    pub props: Arc<WriterProperties>,
    pub previous_collection: Timestamp,
    pub previous_compaction: Timestamp,
}

impl AmpCollectorInnerTask {
    pub fn new(
        metadata_db: MetadataDb,
        store: DataStore,
        props: Arc<WriterProperties>,
        table: Arc<PhysicalTable>,
        metrics: Option<Arc<MetricsRegistry>>,
    ) -> Self {
        let compactor = Compactor::new(
            metadata_db.clone(),
            store.clone(),
            props.clone(),
            table.clone(),
            metrics.clone(),
        );
        let collector = Collector::new(
            metadata_db,
            store,
            props.clone(),
            table.clone(),
            metrics.clone(),
        );
        let previous_collection = Timestamp::now();
        let previous_compaction = Timestamp::now();

        AmpCollectorInnerTask {
            compactor,
            collector,
            props,
            previous_collection,
            previous_compaction,
        }
    }

    pub fn start(
        metadata_db: MetadataDb,
        store: DataStore,
        props: Arc<WriterProperties>,
        table: Arc<PhysicalTable>,
        metrics: Option<Arc<MetricsRegistry>>,
    ) -> JoinHandle<Result<Self, AmpCompactorTaskError>> {
        let task = AmpCollectorInnerTask::new(metadata_db, store, props, table, metrics);
        tokio::spawn(futures::future::ok(task))
    }

    /// Try to run compaction followed by collection
    ///
    /// This will only run compaction and/or collection if the
    /// configured intervals have elapsed for both or either
    /// tasks and if they are enabled. If neither is enabled,
    /// and/or neither respective interval has elapsed this is
    /// a no-op
    pub async fn try_run(self) -> TaskResult<Self> {
        match self.clone().try_compact().await {
            Ok(task) => match task.try_collect().await {
                Ok(task) => Ok(task),
                Err(err) if Self::is_cancellation_task_error(&err) => {
                    tracing::warn!("Collection task cancelled: {}", err);
                    Ok(self)
                }
                Err(err) => Err(err),
            },
            Err(err) if Self::is_cancellation_task_error(&err) => {
                tracing::warn!("Compaction task cancelled: {}", err);
                Ok(self)
            }
            Err(err) => Err(err),
        }
    }

    async fn collect(mut self) -> CollectionResult<Self> {
        self.collector = self.collector.collect().await?;
        Ok(self)
    }

    async fn compact(mut self) -> CompactionResult<Self> {
        self.compactor = self.compactor.compact().await?;
        Ok(self)
    }

    async fn try_collect(mut self) -> TaskResult<Self> {
        // If collection is active and the interval has elapsed, run collection
        let is_active = self.props.collector.active.load(SeqCst);
        let has_elapsed = Timestamp::now()
            .0
            .saturating_sub(self.previous_collection.0)
            > self.props.collector.interval;
        if is_active && has_elapsed {
            self.previous_collection = Timestamp::now();
            Ok(self.collect().await?)
        // Otherwise, return self without doing anything
        } else {
            Ok(self)
        }
    }

    async fn try_compact(mut self) -> TaskResult<Self> {
        // If compaction is active and the interval has elapsed, run compaction
        let is_active = self.props.compactor.active.load(SeqCst);
        let has_elapsed = Timestamp::now()
            .0
            .saturating_sub(self.previous_compaction.0)
            > self.props.compactor.interval;

        if is_active && has_elapsed {
            self.previous_compaction = Timestamp::now();
            Ok(self.compact().await?)
        // Otherwise, return self without doing anything
        } else {
            Ok(self)
        }
    }

    fn is_compactor_cancellation(err: &CompactorError) -> bool {
        matches!(err, CompactorError::Join(join_err) if join_err.is_cancelled())
    }

    fn is_collector_cancellation(err: &CollectorError) -> bool {
        matches!(err, CollectorError::Join(join_err) if join_err.is_cancelled())
    }

    fn is_cancellation_task_error(err: &AmpCompactorTaskError) -> bool {
        match err {
            AmpCompactorTaskError::Join(join_err) => join_err.is_cancelled(),
            AmpCompactorTaskError::Compaction(compactor_err) => {
                Self::is_compactor_cancellation(compactor_err)
            }
            AmpCompactorTaskError::Collection(collector_err) => {
                Self::is_collector_cancellation(collector_err)
            }
        }
    }
}

#[derive(Clone)]
pub struct Collector {
    metadata_db: MetadataDb,
    store: DataStore,
    table: Arc<PhysicalTable>,
    props: Arc<WriterProperties>,
    metrics: Option<Arc<MetricsRegistry>>,
}

impl Debug for Collector {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Garbage Collector {{ table: {} }}",
            self.table.table_ref_compact()
        )
    }
}

impl Display for Collector {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Garbage Collector {{ table: {}, opts: {:?} }}",
            self.table.table_ref_compact(),
            self.props
        )
    }
}

impl Collector {
    pub fn new(
        metadata_db: MetadataDb,
        store: DataStore,
        props: Arc<WriterProperties>,
        table: Arc<PhysicalTable>,
        metrics: Option<Arc<MetricsRegistry>>,
    ) -> Self {
        Collector {
            metadata_db,
            store,
            table,
            props,
            metrics,
        }
    }

    #[tracing::instrument(skip_all, err, fields(location_id=%self.table.location_id(), table=self.table.table_ref_compact()))]
    pub async fn collect(self) -> CollectionResult<Self> {
        let table_name: Arc<str> = Arc::from(self.table.table_name().as_str());

        let location_id = self.table.location_id();

        let found_file_ids_to_paths: BTreeMap<FileId, Path> = self
            .store
            .get_expired_gc_files(location_id)
            .map_err(CollectorError::file_stream_error)
            .map(|manifest_row| {
                let GcManifestRow {
                    file_id,
                    file_path: file_name,
                    ..
                } = manifest_row?;

                // Use relative path (table path + filename), not the absolute URL path
                let path = self.table.path().child(file_name.as_str());
                Ok::<_, CollectorError>((file_id, path))
            })
            .try_collect()
            .await?;

        tracing::debug!("Expired files found: {}", found_file_ids_to_paths.len());

        if found_file_ids_to_paths.is_empty() {
            return Ok(self);
        }

        if let Some(metrics) = &self.metrics {
            metrics.inc_expired_files_found(
                found_file_ids_to_paths.len(),
                self.table.table_name().to_string(),
            );
        }

        let file_ids_to_delete = found_file_ids_to_paths.keys().copied().collect::<Vec<_>>();
        let paths_to_remove =
            metadata_db::files::delete_by_ids(&self.metadata_db, &file_ids_to_delete)
                .await
                .map_err(CollectorError::file_metadata_delete(
                    found_file_ids_to_paths.keys(),
                ))?
                .into_iter()
                .filter_map(|file_id| found_file_ids_to_paths.get(&file_id).cloned())
                .collect::<BTreeSet<_>>();

        tracing::debug!("Metadata entries deleted: {}", paths_to_remove.len());

        if let Some(metrics) = &self.metrics {
            metrics.inc_expired_entries_deleted(
                paths_to_remove.len(),
                self.table.table_name().to_string(),
            );
        }

        let store = self.store.clone();
        let mut delete_stream =
            store.delete_files_stream(stream::iter(paths_to_remove).map(Ok).boxed());

        let mut files_deleted = 0;
        let mut files_not_found = 0;

        while let Some(path) = delete_stream.next().await {
            match path {
                Ok(path) => {
                    tracing::debug!("Deleted expired file: {}", path);
                    files_deleted += 1;
                    if let Some(metrics) = &self.metrics {
                        metrics.inc_files_deleted(1, table_name.clone().to_string());
                    }
                }
                Err(DeleteFilesStreamError(ObjectStoreError::NotFound { path, .. })) => {
                    tracing::debug!("Expired file not found: {}", path);
                    files_not_found += 1;
                    if let Some(metrics) = &self.metrics {
                        metrics.inc_files_not_found(1, table_name.to_string());
                    }
                }
                Err(DeleteFilesStreamError(err)) => {
                    tracing::debug!("Expired files deleted: {}", files_deleted);
                    tracing::debug!("Expired files not found: {}", files_not_found);
                    if let Some(metrics) = &self.metrics {
                        metrics.inc_failed_collections(table_name.to_string());
                    }

                    return Err(err.into());
                }
            }
        }

        tracing::debug!("Expired files deleted: {}", files_deleted);

        if files_not_found > 0 {
            tracing::debug!("Expired files not found: {}", files_not_found);
        }

        if let Some(metrics) = self.metrics.as_ref() {
            metrics.inc_successful_collections(table_name.to_string());
        }

        return Ok(self);
    }
}
