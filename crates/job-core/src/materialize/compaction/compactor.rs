use std::{
    fmt::{Debug, Display, Formatter},
    ops::RangeInclusive,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use amp_data_store::{DataStore, file_name::FileName};
use amp_parquet::{
    reader::AmpReaderFactory,
    writer::{ParquetFileWriter, ParquetFileWriterOutput},
};
use common::{metadata::SegmentSize, physical_table::PhysicalTable};
use datasets_common::{block_num::BlockNum, block_range::BlockRange};
use futures::{FutureExt as _, StreamExt as _, TryStreamExt as _, stream};
use metadata_db::MetadataDb;
use monitoring::logging;
use parking_lot::RwLock;
use tokio::task::JoinError;

use super::{
    AmpCompactorTask,
    algorithm::CompactionAlgorithm,
    config::ParquetConfig,
    error::{CollectorError, CompactionResult, CompactorError},
    plan::{CompactionFile, CompactionPlan},
};
use crate::{
    error::RetryableErrorExt,
    materialize::{metrics::MetricsRegistry, writer::WriterProperties},
};

pub type TaskResult<T> = Result<T, AmpCompactorTaskError>;

/// Errors that can occur during compaction or garbage-collection task execution.
///
/// The custom [`From`] impls flatten inner `Join` variants so that all
/// task-cancellation / join failures surface uniformly as [`Self::Join`].
#[derive(Debug, thiserror::Error)]
pub enum AmpCompactorTaskError {
    /// A compaction operation failed (e.g. I/O, metadata, or merge error).
    #[error(transparent)]
    Compaction(CompactorError),
    /// A garbage-collection (file deletion) operation failed.
    #[error(transparent)]
    Collection(CollectorError),
    /// The spawned Tokio task panicked or was cancelled.
    #[error(transparent)]
    Join(JoinError),
}

impl RetryableErrorExt for AmpCompactorTaskError {
    fn is_retryable(&self) -> bool {
        match self {
            Self::Compaction(err) => err.is_retryable(),
            Self::Collection(err) => err.is_retryable(),
            Self::Join(_) => false,
        }
    }
}

impl From<CollectorError> for AmpCompactorTaskError {
    fn from(err: CollectorError) -> Self {
        match err {
            CollectorError::Join(err) => AmpCompactorTaskError::Join(err),
            other => AmpCompactorTaskError::Collection(other),
        }
    }
}

impl From<CompactorError> for AmpCompactorTaskError {
    fn from(err: CompactorError) -> Self {
        match err {
            CompactorError::Join(err) => AmpCompactorTaskError::Join(err),
            other => AmpCompactorTaskError::Compaction(other),
        }
    }
}

impl From<JoinError> for AmpCompactorTaskError {
    fn from(err: JoinError) -> Self {
        AmpCompactorTaskError::Join(err)
    }
}

#[derive(Debug, Clone)]
pub struct CompactorProperties {
    pub active: Arc<AtomicBool>,
    pub algorithm: CompactionAlgorithm,
    pub interval: Duration,
    pub metadata_concurrency: usize,
    pub write_concurrency: usize,
}

impl<'a> From<&'a ParquetConfig> for CompactorProperties {
    fn from(config: &'a ParquetConfig) -> Self {
        CompactorProperties {
            active: Arc::new(AtomicBool::new(config.compactor.active)),
            algorithm: CompactionAlgorithm::from(config),
            interval: config.compactor.min_interval.clone().into(),
            metadata_concurrency: config.compactor.metadata_concurrency,
            write_concurrency: config.compactor.write_concurrency,
        }
    }
}

pub struct AmpCompactor {
    task: RwLock<AmpCompactorTask>,
}

impl AmpCompactor {
    pub fn start(
        metadata_db: MetadataDb,
        store: DataStore,
        props: Arc<WriterProperties>,
        table: Arc<PhysicalTable>,
        metrics: Option<Arc<MetricsRegistry>>,
    ) -> Self {
        let task = AmpCompactorTask::start(metadata_db, store, props, table, metrics).into();
        Self { task }
    }

    pub fn is_finished(&self) -> bool {
        self.task
            .try_read()
            .map(|t| t.is_finished())
            .unwrap_or_default()
    }

    pub fn try_run(&self) -> TaskResult<()> {
        if let Some(mut task) = self.task.try_write()
            && task.is_finished()
        {
            task.join_current_then_spawn_new().now_or_never().unwrap()?;
        }
        Ok(())
    }

    #[allow(clippy::await_holding_lock)]
    pub async fn join_current_then_spawn_new(&mut self) -> TaskResult<()> {
        // Clippy: This is a test utility, so holding the lock while awaiting
        // is acceptable here because we know there won't be contention and we
        // always ensure the task is finished before acquiring the lock.
        if let Some(mut task) = self.task.try_write() {
            task.join_current_then_spawn_new().await?;
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct Compactor {
    metadata_db: MetadataDb,
    store: DataStore,
    table: Arc<PhysicalTable>,
    props: Arc<WriterProperties>,
    metrics: Option<Arc<MetricsRegistry>>,
}

impl Debug for Compactor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Compactor {{ table: {}, algorithm: {:?} }}",
            self.table.table_ref_compact(),
            self.props.compactor.algorithm,
        )
    }
}

impl Display for Compactor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Compactor {{ opts: {:?}, table: {} }}",
            self.props,
            self.table.table_ref_compact()
        )
    }
}

impl Compactor {
    pub fn new(
        metadata_db: MetadataDb,
        store: DataStore,
        props: Arc<WriterProperties>,
        table: Arc<PhysicalTable>,
        metrics: Option<Arc<MetricsRegistry>>,
    ) -> Self {
        Compactor {
            metadata_db,
            store,
            table,
            props,
            metrics,
        }
    }

    #[tracing::instrument(skip_all, fields(table = self.table.table_ref_compact()))]
    pub async fn compact(self) -> CompactionResult<Self> {
        if !self.props.compactor.active.load(Ordering::SeqCst) {
            return Ok(self);
        }

        let snapshot = self
            .table
            .snapshot(false)
            .await
            .map_err(CompactorError::chain_error)?;
        let reader_factory = Arc::new(AmpReaderFactory {
            location_id: self.table.location_id(),
            store: self.store.clone(),
            schema: self.table.schema(),
        });
        let props = self.props.clone();
        let table_name = self.table.table_name();

        let mut compaction_futures_stream = if let Some(plan) = CompactionPlan::from_snapshot(
            self.metadata_db.clone(),
            self.store.clone(),
            props,
            &snapshot,
            reader_factory,
            &self.metrics,
        )? {
            let groups = plan.collect::<Vec<_>>().await;
            tracing::trace!(
                table = %table_name,
                group_count = groups.len(),
                "Compaction Groups: {:?}",
                groups
            );

            stream::iter(groups)
                .map(|group| group.compact())
                .buffered(self.props.compactor.write_concurrency)
        } else {
            return Ok(self);
        };

        while let Some(res) = compaction_futures_stream.next().await {
            match res {
                Ok(block_num) => {
                    tracing::info!(
                        table = %table_name,
                        block_num,
                        "compaction group completed successfully"
                    );
                    if let Some(metrics) = &self.metrics {
                        metrics.inc_successful_compactions(table_name.to_string());
                    }
                }
                Err(err) => {
                    tracing::error!(
                        error = %err,
                        error_source = logging::error_source(&err),
                        table = %table_name,
                        "compaction failed"
                    );
                    if let Some(metrics) = &self.metrics {
                        metrics.inc_failed_compactions(table_name.to_string());
                    }
                }
            }
        }

        Ok(self)
    }
}

pub struct CompactionGroup {
    metadata_db: MetadataDb,
    store: DataStore,
    props: Arc<WriterProperties>,
    metrics: Option<Arc<MetricsRegistry>>,
    pub(super) size: SegmentSize,
    streams: Vec<CompactionFile>,
    table: Arc<PhysicalTable>,
}

impl Debug for CompactionGroup {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ file_count: {}, range: {:?} }}",
            self.streams.len(),
            self.range()
        )
    }
}

impl CompactionGroup {
    pub fn new_empty(
        metadata_db: MetadataDb,
        store: DataStore,
        props: Arc<WriterProperties>,
        table: Arc<PhysicalTable>,
        metrics: Option<Arc<MetricsRegistry>>,
    ) -> Self {
        CompactionGroup {
            metadata_db,
            store,
            props,
            metrics,
            size: SegmentSize::default(),
            streams: Vec::new(),
            table,
        }
    }

    pub fn push(&mut self, file: CompactionFile) {
        self.size += file.size;
        self.streams.push(file);
    }

    #[tracing::instrument(skip_all, fields(files = self.len(), start = self.range().start(), end = self.range().end()))]
    pub async fn compact(self) -> CompactionResult<BlockNum> {
        let start = *self.range().start();
        let start_time = std::time::Instant::now();
        let metadata_db = self.metadata_db.clone();
        let data_store = self.store.clone();
        let duration = self.props.collector.file_lock_duration;

        // Calculate input metrics before compaction
        let input_file_count = self.streams.len() as u64;
        let input_bytes: u64 = self.streams.iter().map(|s| s.size.bytes).sum();

        // Extract values before move
        let metrics = self.metrics.clone();
        let table_name = self.table.table_name().to_string();
        let location_id = *self.table.location_id();

        let output = self.write_and_finish().await?;

        // Calculate output metrics
        let output_bytes = output.object_meta.size;
        let duration_millis = start_time.elapsed().as_millis() as f64;

        // Record metrics if available
        if let Some(ref metrics) = metrics {
            metrics.record_compaction(
                table_name,
                location_id,
                input_file_count,
                input_bytes,
                output_bytes,
                duration_millis,
            );
        }

        commit_compaction_metadata(&output, &metadata_db)
            .await
            .map_err(CompactorError::metadata_commit_error)?;

        upsert_gc_manifest(&output, &data_store, duration)
            .await
            .map_err(CompactorError::manifest_update_error(&output.parent_ids))?;

        tracing::info!("Compaction Success: {}", output.object_meta.location,);

        Ok(start)
    }

    pub fn len(&self) -> usize {
        self.streams.len()
    }

    pub fn is_empty(&self) -> bool {
        self.streams.is_empty()
    }

    pub fn range(&self) -> RangeInclusive<BlockNum> {
        let start = self
            .streams
            .first()
            .expect("At least one file in group")
            .range
            .start();
        let end = self
            .streams
            .last()
            .expect("At least one file in group")
            .range
            .end();
        start..=end
    }

    async fn write_and_finish(self) -> CompactionResult<ParquetFileWriterOutput> {
        let range = {
            let start_range = &self
                .streams
                .first()
                .expect("At least one stream in group")
                .range;

            let end_range = &self
                .streams
                .last()
                .expect("At least one stream in group")
                .range;

            let network = start_range.network.to_owned();
            let numbers = start_range.start()..=end_range.end();

            BlockRange {
                network,
                numbers,
                hash: end_range.hash,
                prev_hash: start_range.prev_hash,
                timestamp: end_range.timestamp,
            }
        };

        let filename = FileName::new_with_random_suffix(range.start());
        let buf_writer = self
            .store
            .create_revision_file_writer(self.table.revision(), &filename);
        let mut writer = ParquetFileWriter::new(
            self.store,
            buf_writer,
            filename,
            self.table.schema(),
            self.table.table_ref_compact(),
            &*self.table,
            self.props.max_row_group_bytes,
            self.props.parquet.clone(),
        )
        .map_err(CompactorError::create_writer_error(&self.props))?;

        let mut parent_ids = Vec::with_capacity(self.streams.len());
        for mut file in self.streams {
            while let Some(ref batch) = file.sendable_stream.try_next().await? {
                writer.write(batch).await?;
            }
            parent_ids.push(file.file_id);
        }
        // Increment generation for new file
        let generation = self.size.generation + 1;

        writer
            .close(range, parent_ids, generation)
            .await
            .map_err(CompactorError::FileWrite)
    }
}

/// Registers the compacted file's metadata in the database.
async fn commit_compaction_metadata(
    output: &ParquetFileWriterOutput,
    metadata_db: &MetadataDb,
) -> Result<(), metadata_db::Error> {
    let location_id = output.location_id;
    let file_name = output.object_meta.location.filename().unwrap().to_string();
    let file_name = FileName::new_unchecked(file_name);
    let object_size = output.object_meta.size;
    let object_e_tag = output.object_meta.e_tag.clone();
    let object_version = output.object_meta.version.clone();
    let parquet_meta = serde_json::to_value(&output.parquet_meta).unwrap();
    let footer = &output.footer;

    metadata_db::files::register(
        metadata_db,
        location_id,
        &output.url,
        file_name,
        object_size,
        object_e_tag,
        object_version,
        parquet_meta,
        footer,
    )
    .await
}

/// Records the parent file IDs in the GC manifest so they can be collected after compaction.
async fn upsert_gc_manifest(
    output: &ParquetFileWriterOutput,
    data_store: &DataStore,
    duration: Duration,
) -> Result<(), amp_data_store::ScheduleFilesForGcError> {
    data_store
        .schedule_files_for_gc(output.location_id, &output.parent_ids, duration)
        .await
}
