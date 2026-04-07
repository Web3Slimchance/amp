pub mod message_stream_with_block_complete;

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use alloy::{hex::ToHexExt as _, primitives::BlockHash};
use amp_data_store::DataStore;
use datafusion::{
    common::cast::{as_fixed_size_binary_array, as_string_array},
    error::DataFusionError,
    prelude::lit,
};
use datasets_common::{
    dataset::{Dataset, Table},
    dataset_kind_str::DatasetKindStr,
    hash_reference::HashReference,
    network_id::NetworkId,
    table_name::TableName,
};
use datasets_derived::deps::SELF_REF_KEYWORD;
use datasets_raw::dataset::Dataset as RawDataset;
use futures::stream::{self, BoxStream, StreamExt};
use js_runtime::isolate_pool::IsolatePool;
use message_stream_with_block_complete::MessageStreamWithWatermark;
use metadata_db::{NotificationMultiplexerHandle, physical_table_revision::LocationId};
use tokio::{
    sync::{mpsc, watch},
    task::JoinError,
    time::MissedTickBehavior,
};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::task::AbortOnDropHandle;
use tracing::{Instrument, instrument};

use self::message_stream_with_block_complete::MessageStreamError;
use crate::{
    BlockNum, BlockRange,
    amp_catalog_provider::{AMP_CATALOG_NAME, AmpCatalogProvider, AsyncSchemaProvider},
    arrow::{
        array::{Array, Int64Array, RecordBatch, TimestampNanosecondArray},
        datatypes::SchemaRef,
    },
    catalog::{logical::LogicalTable, physical::Catalog},
    context::{
        exec::{ExecContext, ExecContextBuilder},
        plan::PlanContextBuilder,
    },
    cursor::{Cursor, CursorNetworkNotFoundError, NetworkCursor, Watermark},
    datasets_cache::DatasetsCache,
    detached_logical_plan::DetachedLogicalPlan,
    exec_env::ExecEnv,
    incrementalizer::incrementalize_plan,
    physical_table::{CanonicalChainError, PhysicalTable, segments::Segment},
    plan_visitors::{
        WatermarkColumn, find_cross_network_join, order_by_watermark, unproject_columns,
    },
    retryable::RetryableErrorExt,
    rpc_catalog_provider::{RPC_CATALOG_NAME, RpcCatalogProvider},
    self_schema_provider::SelfSchemaProvider,
    sql::TableReference,
    sql_str::SqlStr,
};

/// Errors that occur when spawning a streaming query
///
/// Streaming queries execute SQL continuously over blockchain data, processing
/// new blocks as they arrive. This error type covers all initialization phases:
/// query planning, network validation, and blocks table resolution.
#[derive(thiserror::Error, Debug)]
pub enum SpawnError {
    /// Failed to propagate `_block_num` column through query plan
    ///
    /// The `_block_num` column is a special column added to all tables to enable
    /// incremental query processing. This error occurs when the logical plan
    /// transformation to propagate this column through the query fails.
    ///
    /// Common causes:
    /// - Invalid query structure that cannot support block number propagation
    /// - Incompatible aggregations or window functions
    /// - Schema conflicts when adding the special column
    /// - DataFusion internal errors during plan transformation
    ///
    /// This prevents the streaming query from being initialized as incremental
    /// processing requires watermark columns.
    #[error("failed to propagate watermark columns in query plan")]
    PropagateBlockNum(#[source] DataFusionError),

    /// Failed to optimize query plan
    ///
    /// This occurs when DataFusion's logical optimizer fails to process the query
    /// plan during initialization. The optimizer applies transformations like
    /// predicate pushdown, projection pruning, and constant folding.
    ///
    /// Common causes:
    /// - Invalid logical plan structure
    /// - Optimizer rule failures
    /// - Type inference errors
    /// - Schema inconsistencies
    ///
    /// Optimization failures prevent the streaming query from starting with an
    /// efficient execution plan.
    #[error("failed to optimize query plan")]
    OptimizePlan(#[source] DataFusionError),

    /// Query contains a join across tables from different blockchain networks
    ///
    /// Common causes:
    /// - User query explicitly joins tables across networks (e.g., Ethereum + Base)
    /// - Query references datasets with dependencies across multiple networks
    /// - Catalog construction includes tables from different networks
    ///
    /// The error shows which tables are involved in the cross-network join and
    /// their respective networks.
    ///
    /// **Note**: Batch (non-streaming) queries CAN join across networks because
    /// they process complete, static ranges rather than incremental updates.
    #[error("streaming query contains a cross-network join: {info}")]
    CrossNetworkJoin {
        info: crate::plan_visitors::CrossNetworkJoinInfo,
    },

    /// Source tables do not share a common watermark column (`_block_num` or `_ts`).
    #[error("all source tables must have at least one watermark column (_block_num or _ts)")]
    NoWatermarkColumn,

    /// Failed to resolve raw dataset from dependencies
    ///
    /// This occurs when traversing dataset dependencies fails to find a raw
    /// (non-derived) dataset whose network can be used for the streaming query.
    #[error("failed to resolve raw dataset from dependencies")]
    ResolveRawDataset(#[source] ResolveRawDatasetError),

    /// Failed to resolve blocks table for network
    ///
    /// Every streaming query requires access to a `blocks` table containing the
    /// canonical blockchain data for the network. This error occurs when finding
    /// or loading the blocks table fails.
    ///
    /// Common causes:
    /// - No raw dataset with blocks table exists for the network
    /// - Dataset dependency tree doesn't include a blocks table
    /// - Blocks table exists but hasn't been synced (no active revision)
    /// - Data store errors when loading table metadata
    /// - Dataset manifest not found or corrupted
    ///
    /// Without a blocks table, the streaming query cannot determine block ranges
    /// or detect chain reorganizations.
    #[error("failed to resolve blocks table for network")]
    ResolveBlocksTable(#[source] ResolveBlocksTableError),

    /// Failed to convert cursor to target network
    ///
    /// When resuming a streaming query from a previous cursor, the cursor
    /// must be converted to the target network's format. This error occurs when
    /// the cursor doesn't contain an entry for the expected network.
    ///
    /// Common causes:
    /// - Cursor from a different network than current query
    /// - Corrupted or invalid cursor state
    /// - Network name mismatch (e.g., "ethereum" vs "mainnet")
    /// - Resume state out of sync with current catalog
    ///
    /// This prevents the query from resuming at the correct position and may
    /// require starting from scratch or using a different resume point.
    #[error("failed to convert cursor")]
    ConvertCursor(#[source] CursorNetworkNotFoundError),
}

/// Awaits any update for tables in a query context catalog.
struct TableUpdates {
    subscriptions: BTreeMap<LocationId, watch::Receiver<()>>,
    ready: bool,
}

impl TableUpdates {
    async fn new(catalog: &Catalog, multiplexer_handle: &NotificationMultiplexerHandle) -> Self {
        let mut subscriptions: BTreeMap<LocationId, watch::Receiver<()>> = BTreeMap::new();
        for table in catalog.physical_tables() {
            let location = table.location_id();
            subscriptions.insert(location, multiplexer_handle.subscribe(location).await);
        }
        Self {
            subscriptions,
            ready: true,
        }
    }

    fn set_ready(&mut self) {
        self.ready = true
    }

    async fn changed(&mut self) {
        if self.ready {
            self.ready = false;
            return;
        }

        // Never return if there are no remaining subscriptions.
        if self.subscriptions.is_empty() {
            std::future::pending::<()>().await;
        }

        let notifications = self
            .subscriptions
            .iter_mut()
            .map(|(location, rx)| {
                Box::pin(async move { rx.changed().await.map_err(|_| *location) })
            })
            .collect::<Vec<_>>();
        let result = futures::future::select_all(notifications).await.0;
        if let Err(location) = result {
            tracing::warn!("notifications ended for location {location}");
            self.subscriptions.remove(&location);
        }
    }
}

/// Represents a message from the streaming query, which can be either data or a completion signal.
///
/// Completion points do not necessarily follow increments of 1, as the query progresses in batches.
pub enum QueryMessage {
    MicrobatchStart {
        range: BlockRange,
        is_reorg: bool,
    },
    Data(RecordBatch),
    MicrobatchEnd(BlockRange),

    /// Watermark indicating the query has emitted all outputs up to the given block number.
    /// This represents a monotonically increasing checkpoint in the stream.
    Watermark(Watermark),
}

struct MicrobatchRange {
    range: BlockRange,
    direction: StreamDirection,
}

struct SegmentStart {
    number: BlockNum,
    prev_hash: BlockHash,
    timestamp: Option<u64>,
}

impl From<BlockRow> for SegmentStart {
    fn from(row: BlockRow) -> Self {
        Self {
            number: row.number,
            prev_hash: row.prev_hash,
            timestamp: row.timestamp,
        }
    }
}

/// Watermark with timestamp for the end block of a segment.
struct BlockWatermark {
    number: BlockNum,
    hash: BlockHash,
    timestamp: Option<u64>,
}

/// Direction of the stream. Helpful to distinguish reorgs, with the payload being the first block
/// after the base of the fork.
enum StreamDirection {
    ForwardFrom(SegmentStart),
    ReorgFrom(SegmentStart),
}

impl StreamDirection {
    fn segment_start(&self) -> &SegmentStart {
        match self {
            StreamDirection::ForwardFrom(block) => block,
            StreamDirection::ReorgFrom(block) => block,
        }
    }

    fn is_reorg(&self) -> bool {
        matches!(self, StreamDirection::ReorgFrom(_))
    }
}
/// A handle to a streaming query that can be used to retrieve results as a stream.
///
/// Aborts the query task when dropped.
pub struct StreamingQueryHandle {
    rx: mpsc::Receiver<QueryMessage>,
    join_handle: AbortOnDropHandle<Result<(), StreamingQueryExecutionError>>,
    watermark_column: WatermarkColumn,
}

impl StreamingQueryHandle {
    pub fn into_stream(self) -> BoxStream<'static, Result<QueryMessage, MessageStreamError>> {
        let data_stream = MessageStreamWithWatermark::new(
            ReceiverStream::new(self.rx).map(Ok),
            self.watermark_column,
        );

        let join = self.join_handle;

        // If `tx` has been dropped then the query task has terminated. So we check if it has
        // terminated with errors, and if so send the error as the final item of the stream.
        let get_task_result = async move {
            match tokio::time::timeout(Duration::from_secs(1), join).await {
                Ok(Ok(Ok(()))) => None,
                Ok(Ok(Err(e))) => Some(Err(e.into())),
                Ok(Err(join_err)) => Some(Err(
                    StreamingQueryExecutionError::StreamingTaskFailedToJoin(join_err).into(),
                )),

                // This would only happen under extreme CPU or tokio scheduler contention.
                // Or blocking `Drop` implementations.
                Err(_) => Some(Err(StreamingQueryExecutionError::TaskTimeout.into())),
            }
        };

        data_stream
            .chain(stream::once(get_task_result).filter_map(|x| async { x }))
            .boxed()
    }
}

/// A streaming query that continuously listens for new blocks and emits incremental results.
///
/// This follows a 'microbatch' model where it processes data in chunks based on a block range
/// stream.
pub struct StreamingQuery {
    exec_env: ExecEnv,
    isolate_pool: IsolatePool,
    catalog: Catalog,
    plan: DetachedLogicalPlan,
    start_block: BlockNum,
    end_block: Option<BlockNum>,
    table_updates: TableUpdates,
    tx: mpsc::Sender<QueryMessage>,
    microbatch_max_interval: u64,
    keep_alive_interval: u64,
    destination: Option<Arc<PhysicalTable>>,
    /// The watermark column used for incrementalization, ordering, and output splitting.
    watermark_column: WatermarkColumn,
    /// Watermark columns to remove from the output (those not explicitly selected).
    watermark_columns_to_unproject: Vec<&'static str>,
    network: NetworkId,
    /// `blocks` table for the network associated with the catalog.
    blocks_table: Arc<ResolvedBlocksTable>,
    /// The single-network cursor for the previously processed range. This may be provided by the
    /// consumer (as a multi-network cursor) and converted to this single-network cursor.
    prev_cursor: Option<NetworkCursor>,
    /// Job ID for tracing. Recorded on execute_microbatch spans so it's
    /// searchable in Jaeger even when parent spans are still open.
    job_id: Option<metadata_db::jobs::JobId>,
}

impl StreamingQuery {
    /// Creates a new streaming query. It is assumed that the `ctx` was built such that it contains
    /// only the tables relevant for the query.
    ///
    /// The query execution loop will run in its own task.
    #[instrument(skip_all, err)]
    #[expect(clippy::too_many_arguments)]
    pub async fn spawn(
        exec_env: ExecEnv,
        isolate_pool: IsolatePool,
        catalog: Catalog,
        plan: DetachedLogicalPlan,
        start_block: BlockNum,
        end_block: Option<BlockNum>,
        cursor: Option<Cursor>,
        multiplexer_handle: &NotificationMultiplexerHandle,
        destination: Option<Arc<PhysicalTable>>,
        microbatch_max_interval: u64,
        keep_alive_interval: u64,
        job_id: Option<metadata_db::jobs::JobId>,
    ) -> Result<StreamingQueryHandle, SpawnError> {
        let (tx, rx) = mpsc::channel(10);

        // Determine which watermark columns to remove from the output.
        // Keep all if materializing to a destination table; otherwise only keep
        // those the user explicitly selected.
        let watermark_columns_to_unproject = if destination.is_some() {
            vec![]
        } else {
            let schema = plan.schema();
            datasets_common::watermark_columns::WATERMARK_COLUMN_NAMES
                .iter()
                .copied()
                .filter(|name| !schema.fields().iter().any(|f| f.name() == *name))
                .collect()
        };

        // Prevent streaming cross-network joins (check runs before plan optimization).
        if let Some(info) =
            find_cross_network_join(&plan, &catalog).map_err(SpawnError::OptimizePlan)?
        {
            return Err(SpawnError::CrossNetworkJoin { info });
        }

        // Only propagate watermark columns that all source tables support.
        // When materializing to a destination, also restrict to columns the
        // destination schema includes (the manifest may predate `_ts`).
        let mut watermark_columns_to_propagate =
            WatermarkColumn::supported_by_all(catalog.entries().iter().map(|(pt, _)| pt.schema()));
        if let Some(dest) = &destination {
            watermark_columns_to_propagate.retain(|wm| {
                dest.schema()
                    .fields()
                    .iter()
                    .any(|f| f.name() == wm.column_name())
            });
        }
        let watermark_column =
            if watermark_columns_to_propagate.contains(&WatermarkColumn::BlockNum) {
                WatermarkColumn::BlockNum
            } else if watermark_columns_to_propagate.contains(&WatermarkColumn::Ts) {
                WatermarkColumn::Ts
            } else {
                return Err(SpawnError::NoWatermarkColumn);
            };

        // This plan is the starting point of each microbatch execution. Transformations applied to it:
        // - Propagate watermark columns.
        // - Run logical optimizations ahead of execution.
        let plan = {
            let plan = plan
                .propagate_watermark_columns(&watermark_columns_to_propagate)
                .map_err(SpawnError::PropagateBlockNum)?;

            // Use dep alias map from the catalog so AmpCatalogProvider
            // resolves dep aliases to pinned hash references.
            let dep_alias_map = catalog.dep_aliases().clone();

            let self_schema: Arc<dyn AsyncSchemaProvider> = Arc::new(SelfSchemaProvider::new(
                SELF_REF_KEYWORD.to_string(),
                catalog.tables().to_vec(),
                catalog.udfs().to_vec(),
            ));
            let amp_catalog = Arc::new(
                AmpCatalogProvider::new(exec_env.datasets_cache.clone())
                    .with_dep_aliases(dep_alias_map)
                    .with_self_schema(self_schema),
            );
            let rpc_catalog =
                Arc::new(RpcCatalogProvider::new(exec_env.ethcall_udfs_cache.clone()));
            let ctx = PlanContextBuilder::new(exec_env.session_config.clone())
                .with_table_catalog(AMP_CATALOG_NAME, amp_catalog.clone())
                .with_func_catalog(AMP_CATALOG_NAME, amp_catalog)
                .with_func_catalog(RPC_CATALOG_NAME, rpc_catalog)
                .build();
            ctx.optimize(&plan).map_err(SpawnError::OptimizePlan)?
        };

        // Resolve the network by walking dataset dependencies to find a raw dataset,
        // then resolve the blocks table for that network.
        let (network, blocks_table) = {
            let unique_refs: BTreeSet<HashReference> = catalog
                .physical_tables()
                .map(|t| t.dataset_reference().clone())
                .collect();

            let raw_dataset =
                resolve_raw_dataset_from_dependencies(&exec_env.datasets_cache, unique_refs.iter())
                    .await
                    .map_err(SpawnError::ResolveRawDataset)?;

            let network = raw_dataset.network().clone();
            let blocks_table = ResolvedBlocksTable::new(raw_dataset, exec_env.store.clone())
                .await
                .map_err(SpawnError::ResolveBlocksTable)?;

            (network, Arc::new(blocks_table))
        };

        let table_updates = TableUpdates::new(&catalog, multiplexer_handle).await;
        let prev_cursor = cursor
            .map(|c| c.to_single_network(&network))
            .transpose()
            .map_err(SpawnError::ConvertCursor)?;
        let streaming_query = Self {
            exec_env,
            isolate_pool,
            catalog,
            plan,
            tx,
            start_block,
            end_block,
            prev_cursor,
            table_updates,
            microbatch_max_interval,
            keep_alive_interval,
            destination,
            watermark_column,
            watermark_columns_to_unproject,
            network,
            blocks_table,
            job_id,
        };

        let execute_span = tracing::info_span!("streaming_query_execute");
        execute_span.follows_from(tracing::Span::current());
        let join_handle = AbortOnDropHandle::new(tokio::spawn(
            streaming_query.execute().instrument(execute_span),
        ));

        Ok(StreamingQueryHandle {
            rx,
            join_handle,
            watermark_column,
        })
    }

    /// The loop:
    /// 1. Get new input range
    /// 2. Start executing microbatch for the range
    /// 3. Stream out time-ordered results
    /// 4. Once execution of batch is exhausted, send completion trigger
    #[instrument(skip_all, err)]
    async fn execute(mut self) -> Result<(), StreamingQueryExecutionError> {
        loop {
            self.table_updates.changed().await;

            // The table snapshots to execute the microbatch against.
            let ctx = ExecContextBuilder::new(self.exec_env.clone())
                .with_isolate_pool(self.isolate_pool.clone())
                .for_catalog(self.catalog.clone(), false)
                .await
                .map_err(StreamingQueryExecutionError::CreateExecContext)?;

            // Get the next execution range
            let Some(MicrobatchRange { range, direction }) = self
                .next_microbatch_range(&ctx)
                .await
                .map_err(StreamingQueryExecutionError::NextMicrobatchRange)?
            else {
                continue;
            };

            if self.execute_microbatch(&ctx, &range, &direction).await? {
                return Ok(());
            }
            self.prev_cursor = Some((&range).into());
        }
    }

    fn watermark_bounds(
        &self,
        range: &BlockRange,
        direction: &StreamDirection,
    ) -> (datafusion::prelude::Expr, datafusion::prelude::Expr) {
        use datafusion::common::ScalarValue;

        match self.watermark_column {
            WatermarkColumn::BlockNum => (lit(range.start()), lit(range.end())),
            WatermarkColumn::Ts => {
                // Block timestamps are stored in seconds (see `get_timestamp_value`).
                // Convert to nanoseconds to match the `_ts` column type.
                let start_secs = direction
                    .segment_start()
                    .timestamp
                    .expect("start block timestamp required when _ts is the watermark column");
                let end_secs = range
                    .timestamp()
                    .expect("end block timestamp required when _ts is the watermark column");
                let tz: Arc<str> = Arc::from("+00:00");
                (
                    lit(ScalarValue::TimestampNanosecond(
                        Some(start_secs as i64 * 1_000_000_000),
                        Some(tz.clone()),
                    )),
                    lit(ScalarValue::TimestampNanosecond(
                        Some(end_secs as i64 * 1_000_000_000),
                        Some(tz),
                    )),
                )
            }
        }
    }

    /// Execute a single microbatch for the given range. Returns `true` if this was the final
    /// batch (i.e. `range.end() == self.end_block`).
    #[instrument(skip_all, err, fields(start_block = %range.start(), end_block = %range.end(), job_id = self.job_id.map(tracing::field::display)))]
    async fn execute_microbatch(
        &mut self,
        ctx: &ExecContext,
        range: &BlockRange,
        direction: &StreamDirection,
    ) -> Result<bool, StreamingQueryExecutionError> {
        let plan = {
            // Incrementalize the plan
            let plan = self
                .plan
                .clone()
                .attach_to(ctx)
                .map_err(StreamingQueryExecutionError::AttachPlan)?;
            let (start, end) = self.watermark_bounds(range, direction);
            let mut plan = incrementalize_plan(plan, start, end, self.watermark_column)
                .map_err(StreamingQueryExecutionError::IncrementalizePlan)?;

            // Enforce ordering by the watermark column.
            plan = order_by_watermark(plan, self.watermark_column);

            // Remove watermark columns the user didn't explicitly select.
            if !self.watermark_columns_to_unproject.is_empty() {
                plan = unproject_columns(plan, &self.watermark_columns_to_unproject)
                    .map_err(StreamingQueryExecutionError::UnprojectWatermarkColumn)?
            }
            plan
        };

        let keep_alive_interval = self.keep_alive_interval.max(30);
        let schema = Arc::new(plan.schema().as_arrow().clone());
        let mut stream = keep_alive_stream(
            ctx.execute_plan(plan, false)
                .await
                .map_err(StreamingQueryExecutionError::ExecutePlan)?,
            schema,
            keep_alive_interval,
        );

        // Send start message for this microbatch
        let _ = self
            .tx
            .send(QueryMessage::MicrobatchStart {
                range: range.clone(),
                is_reorg: direction.is_reorg(),
            })
            .await;

        // Drain the microbatch completely
        while let Some(item) = stream.next().await {
            let item = item.map_err(StreamingQueryExecutionError::StreamItem)?;

            // If the receiver in `StreamingQueryHandle` is dropped, then this task has been
            // aborted, so we don't bother checking for errors when sending a message.
            let _ = self.tx.send(QueryMessage::Data(item)).await;
        }

        // Send end message for this microbatch
        let _ = self
            .tx
            .send(QueryMessage::MicrobatchEnd(range.clone()))
            .await;

        Ok(Some(range.end()) == self.end_block)
    }

    #[instrument(skip_all, err)]
    async fn next_microbatch_range(
        &mut self,
        ctx: &ExecContext,
    ) -> Result<Option<MicrobatchRange>, NextMicrobatchRangeError> {
        // Gather the chains for each source table.
        let chains = ctx
            .physical_table()
            .table_snapshots()
            .map(|(s, _)| s.canonical_segments());

        // Use a single context for all queries against the blocks table. This is to keep a
        // consistent reference chain within the scope of this function.
        let blocks_ctx = {
            // Construct a catalog for the single `blocks_table`.
            let catalog = {
                let physical_table = self.blocks_table.physical_table.clone();
                let sql_schema_name = self.blocks_table.sql_schema_name.clone();
                let sql_schema_name_arc = Arc::from(sql_schema_name.as_str());
                let resolved_table = LogicalTable::new(
                    sql_schema_name,
                    physical_table.dataset_reference().clone(),
                    physical_table.table().clone(),
                );
                Catalog::new(
                    vec![resolved_table],
                    vec![],
                    vec![(Arc::new(physical_table), sql_schema_name_arc)],
                    Default::default(),
                )
            };
            ExecContextBuilder::new(self.exec_env.clone())
                .with_isolate_pool(self.isolate_pool.clone())
                .for_catalog(catalog, false)
                .await
                .map_err(NextMicrobatchRangeError::CreateExecContext)?
        };

        // The latest common watermark across the source tables.
        let Some(common_watermark) = self
            .latest_src_watermark(&blocks_ctx, chains)
            .await
            .map_err(NextMicrobatchRangeError::LatestSrcWatermark)?
        else {
            // No common watermark across source tables.
            tracing::debug!("no common watermark found");
            return Ok(None);
        };

        if common_watermark.number < self.start_block {
            // Common watermark hasn't reached the requested start block yet.
            return Ok(None);
        }

        let Some(direction) = self
            .next_microbatch_start(&blocks_ctx)
            .await
            .map_err(NextMicrobatchRangeError::NextMicrobatchStart)?
        else {
            tracing::debug!("no next microbatch start found");
            return Ok(None);
        };
        let start = direction.segment_start();
        let Some(end) = self
            .next_microbatch_end(&blocks_ctx, start, common_watermark)
            .await
            .map_err(NextMicrobatchRangeError::NextMicrobatchEnd)?
        else {
            tracing::debug!("no next microbatch end found");
            return Ok(None);
        };
        Ok(Some(MicrobatchRange {
            range: BlockRange {
                numbers: start.number..=end.number,
                network: self.network.clone(),
                hash: end.hash,
                prev_hash: start.prev_hash,
                timestamp: end.timestamp,
            },
            direction,
        }))
    }

    #[instrument(skip_all, err)]
    async fn next_microbatch_start(
        &self,
        ctx: &ExecContext,
    ) -> Result<Option<StreamDirection>, NextMicrobatchStartError> {
        match &self.prev_cursor {
            // start stream
            None => {
                let block = self
                    .blocks_table_fetch(ctx, self.start_block, None)
                    .await
                    .map_err(NextMicrobatchStartError::BlocksTableFetch)?;
                Ok(block.map(|b| StreamDirection::ForwardFrom(b.into())))
            }
            // continue stream
            Some(prev)
                if self
                    .blocks_table_contains(ctx, prev)
                    .await
                    .map_err(NextMicrobatchStartError::BlocksTableContains)?
                    .is_some() =>
            {
                let segment_start = SegmentStart {
                    number: prev.number + 1,
                    prev_hash: prev.hash,
                    timestamp: None,
                };
                Ok(Some(StreamDirection::ForwardFrom(segment_start)))
            }
            // rewind stream due to reorg
            Some(prev) => {
                let block = self
                    .reorg_base(ctx, prev)
                    .await
                    .map_err(NextMicrobatchStartError::ReorgBase)?;
                Ok(block.map(|b| StreamDirection::ReorgFrom(b.into())))
            }
        }
    }

    #[instrument(skip_all, err)]
    async fn next_microbatch_end(
        &mut self,
        ctx: &ExecContext,
        start: &SegmentStart,
        common_watermark: BlockWatermark,
    ) -> Result<Option<BlockWatermark>, NextMicrobatchEndError> {
        let number = {
            let end = self
                .end_block
                .map(|end_block| BlockNum::min(common_watermark.number, end_block))
                .unwrap_or(common_watermark.number);
            let limit = start.number + self.microbatch_max_interval - 1;
            if end > limit {
                // We're limiting this batch, so make sure we can immediately continue to the next
                // range regardless of source table updates.
                self.table_updates.set_ready();
                limit
            } else {
                end
            }
        };
        if number < start.number {
            // Invalid range: end block is before start block.
            return Ok(None);
        }
        if number == common_watermark.number {
            Ok(Some(common_watermark))
        } else {
            self.blocks_table_fetch(ctx, number, None)
                .await
                .map(|r| r.map(|r| r.block_watermark()))
                .map_err(NextMicrobatchEndError)
        }
    }

    #[instrument(skip_all, err)]
    async fn latest_src_watermark(
        &self,
        ctx: &ExecContext,
        chains: impl Iterator<Item = &[Segment]>,
    ) -> Result<Option<BlockWatermark>, LatestSrcWatermarkError> {
        // For each chain, collect the latest segment
        let mut latest_src_watermarks: Vec<BlockWatermark> = Default::default();
        'chain_loop: for chain in chains {
            for segment in chain.iter().rev() {
                let cursor = segment.single_range().into();
                if let Some(block_watermark) = self
                    .blocks_table_contains(ctx, &cursor)
                    .await
                    .map_err(LatestSrcWatermarkError)?
                {
                    latest_src_watermarks.push(block_watermark);
                    continue 'chain_loop;
                }
            }
            return Ok(None);
        }
        // Select the minimum table watermark as the end.
        Ok(latest_src_watermarks.into_iter().min_by_key(|w| w.number))
    }

    /// Find the block to resume streaming from after detecting a reorg.
    ///
    /// When a streaming query detects that the previous block range is no longer on the canonical
    /// chain (indicating a reorg), this method walks backwards from the end of the previous block
    /// range to find the latest adjacent block that exists on the canonical chain.
    #[instrument(skip_all, err)]
    async fn reorg_base(
        &self,
        ctx: &ExecContext,
        prev_cursor: &NetworkCursor,
    ) -> Result<Option<BlockRow>, ReorgBaseError> {
        // context for querying forked blocks
        let fork_ctx = {
            let catalog = Catalog::new(
                ctx.physical_table().tables().to_vec(),
                ctx.physical_table().udfs().to_vec(),
                ctx.physical_table().catalog_entries(),
                Default::default(),
            );
            ExecContextBuilder::new(ctx.env.clone())
                .with_isolate_pool(ctx.isolate_pool().clone())
                .for_catalog(catalog, true)
                .await
                .map_err(ReorgBaseError::CreateExecContext)?
        };

        let mut min_fork_block_num = prev_cursor.number;
        let mut fork: Option<BlockRow> = self
            .blocks_table_fetch(&fork_ctx, prev_cursor.number, Some(&prev_cursor.hash))
            .await
            .map_err(ReorgBaseError::BlocksTableFetch)?;
        while let Some(block) = fork.take() {
            if self
                .blocks_table_contains(ctx, &block.cursor())
                .await
                .map_err(ReorgBaseError::BlocksTableContains)?
                .is_some()
            {
                break;
            }
            min_fork_block_num = block.number;
            fork = self
                .blocks_table_fetch(
                    &fork_ctx,
                    block.number.saturating_sub(1),
                    Some(&block.prev_hash),
                )
                .await
                .map_err(ReorgBaseError::BlocksTableFetch)?;
        }

        // If we're dumping a derived dataset, we must rewind to the start of the canonical segment
        // boudary. Otherwise, the new segments may not form a canonical chain.
        if let Some(destination) = self.destination.as_ref()
            && let Some(destination_chain) = destination
                .canonical_chain()
                .await
                .map_err(ReorgBaseError::CanonicalChain)?
        {
            min_fork_block_num = *destination_chain
                .0
                .iter()
                .rev()
                .map(|s| &s.single_range().numbers)
                .find(|r| r.contains(&min_fork_block_num))
                .unwrap_or(&(0..=0))
                .start();
        }

        self.blocks_table_fetch(ctx, min_fork_block_num, None)
            .await
            .map_err(ReorgBaseError::BlocksTableFetch)
    }

    #[instrument(skip_all, err)]
    async fn blocks_table_contains(
        &self,
        ctx: &ExecContext,
        cursor: &NetworkCursor,
    ) -> Result<Option<BlockWatermark>, BlocksTableContainsError> {
        // Panic safety: The `blocks_ctx` always has a single table.
        let (blocks_segments, _) = ctx.physical_table().table_snapshots().next().unwrap();

        // Optimization: Check segment metadata first to avoid expensive query,
        // Walk segments in reverse to find one that covers this block number.
        for segment in blocks_segments.canonical_segments().iter().rev() {
            if *segment.single_range().numbers.start() <= cursor.number {
                // Found segment that could contain this block
                if *segment.single_range().numbers.end() == cursor.number {
                    // Exact match on segment end - use segment data directly
                    let range = segment.single_range();
                    if range.hash == cursor.hash {
                        return Ok(Some(BlockWatermark {
                            number: cursor.number,
                            hash: range.hash,
                            timestamp: range.timestamp,
                        }));
                    } else {
                        return Ok(None);
                    }
                }
                // Block is inside segment but not at end.
                // So we will need to query the data file to find the hash.
                break;
            }
        }

        self.blocks_table_fetch(ctx, cursor.number, Some(&cursor.hash))
            .await
            .map(|row| row.map(|r| r.block_watermark()))
            .map_err(BlocksTableContainsError)
    }

    #[instrument(skip(self, ctx), err)]
    async fn blocks_table_fetch(
        &self,
        ctx: &ExecContext,
        number: BlockNum,
        hash: Option<&BlockHash>,
    ) -> Result<Option<BlockRow>, BlocksTableFetchError> {
        let hash_column = self.blocks_table.hash_column;
        let parent_hash_column = self.blocks_table.parent_hash_column;
        let timestamp_column = self.blocks_table.timestamp_column;
        let blocks_sql_schema_name = self.blocks_table.sql_schema_name.clone();
        let blocks_physical_table_name = self.blocks_table.physical_table.table_name().clone();
        let hash_constraint = self.blocks_table.hash_constraint_sql(hash);

        let sql = format!(
            "SELECT {}, {}, {} FROM {} WHERE _block_num = {} {} LIMIT 1",
            hash_column,
            parent_hash_column,
            timestamp_column,
            TableReference::Partial {
                schema: Arc::new(blocks_sql_schema_name.to_string()),
                table: Arc::new(blocks_physical_table_name),
            }
            .to_quoted_string(),
            number,
            hash_constraint,
        );

        // SAFETY: Validation is deferred to the SQL parser which will return appropriate errors
        // for empty or invalid SQL. The format! macro ensures non-empty output.
        let sql_str = SqlStr::new_unchecked(sql);
        let query = crate::sql::parse(&sql_str).map_err(BlocksTableFetchError::ParseSql)?;
        let plan = ctx
            .statement_to_plan(query)
            .await
            .map_err(BlocksTableFetchError::PlanSql)?;
        let results = ctx
            .execute_and_concat(plan)
            .await
            .map_err(BlocksTableFetchError::ExecuteSql)?;
        if results.num_rows() == 0 {
            tracing::debug!("blocks table missing block {} {:?}", number, hash);
            return Ok(None);
        }

        let hash = self
            .blocks_table
            .get_hash_value(&results)
            .map_err(BlocksTableFetchError::ExtractHash)?;
        let prev_hash = self
            .blocks_table
            .get_parent_hash_value(&results)
            .map_err(BlocksTableFetchError::ExtractHash)?;
        let timestamp = self.blocks_table.get_timestamp_value(&results);
        Ok(Some(BlockRow {
            number,
            hash,
            prev_hash,
            timestamp,
        }))
    }
}

/// Errors that occur during streaming query execution
///
/// This error type is used by `StreamingQuery::execute()`.
#[derive(Debug, thiserror::Error)]
pub enum StreamingQueryExecutionError {
    /// Streaming task failed to join
    ///
    /// This occurs when the streaming query task panics or is cancelled unexpectedly.
    /// The JoinError contains information about why the task failed to complete.
    #[error("streaming task failed to join: {0}")]
    StreamingTaskFailedToJoin(#[source] JoinError),

    /// Streaming task join timed out
    ///
    /// This occurs when the streaming query task does not complete within the expected
    /// timeout period (1 second) after being signaled to stop. This indicates extreme
    /// CPU or tokio scheduler contention, or blocking `Drop` implementations.
    #[error("streaming task join timed out")]
    TaskTimeout,

    /// Failed to create an exec context
    ///
    /// This occurs when the exec context cannot be created.
    #[error("failed to create exec context: {0}")]
    CreateExecContext(#[source] crate::context::exec::CreateContextError),

    /// Failed to get the next microbatch range
    ///
    /// This occurs when the next microbatch range cannot be found.
    #[error("failed to get next microbatch range: {0}")]
    NextMicrobatchRange(#[source] NextMicrobatchRangeError),

    /// Failed to attach the plan to the query context
    ///
    /// This occurs when the plan cannot be attached to the query context.
    #[error("failed to attach the plan to the query context: {0}")]
    AttachPlan(#[source] crate::detached_logical_plan::AttachPlanError),

    /// Failed to incrementalize the plan
    ///
    /// This occurs when the plan cannot be incrementalized.
    #[error("failed to incrementalize the plan: {0}")]
    IncrementalizePlan(#[source] DataFusionError),

    /// Failed to unproject the special block num column
    ///
    /// This occurs when the special block num column cannot be unprojected.
    #[error("failed to unproject the special block num column: {0}")]
    UnprojectWatermarkColumn(#[source] DataFusionError),

    /// Failed to execute the plan
    ///
    /// This occurs when the plan cannot be executed.
    #[error("failed to execute the plan: {0}")]
    ExecutePlan(#[source] crate::context::exec::ExecutePlanError),

    /// Failed to stream item
    ///
    /// This occurs when the item cannot be streamed.
    #[error("failed to stream item: {0}")]
    StreamItem(#[source] DataFusionError),
}

impl RetryableErrorExt for StreamingQueryExecutionError {
    fn is_retryable(&self) -> bool {
        match self {
            // Plan-level failures are permanent — the query structure won't change on retry.
            Self::IncrementalizePlan(_)
            | Self::AttachPlan(_)
            | Self::UnprojectWatermarkColumn(_) => false,

            // Task-level failures are transient.
            Self::StreamingTaskFailedToJoin(_) | Self::TaskTimeout => true,

            // Execution failures are generally transient (I/O, timeouts, etc.).
            Self::CreateExecContext(_)
            | Self::NextMicrobatchRange(_)
            | Self::ExecutePlan(_)
            | Self::StreamItem(_) => true,
        }
    }
}

/// Errors that occur when determining the next microbatch range
///
/// This error type is used by `StreamingQuery::next_microbatch_range()`.
#[derive(Debug, thiserror::Error)]
pub enum NextMicrobatchRangeError {
    /// Failed to create an exec context
    ///
    /// This occurs when the exec context cannot be created.
    #[error("failed to create exec context: {0}")]
    CreateExecContext(#[source] crate::context::exec::CreateContextError),

    /// Failed to get the latest source watermark
    ///
    /// This occurs when the latest source watermark cannot be found.
    #[error("failed to get latest source watermark: {0}")]
    LatestSrcWatermark(#[source] LatestSrcWatermarkError),

    /// Failed to get the next microbatch start
    ///
    /// This occurs when the next microbatch start cannot be found.
    #[error("failed to get next microbatch start: {0}")]
    NextMicrobatchStart(#[source] NextMicrobatchStartError),

    /// Failed to get the next microbatch end
    ///
    /// This occurs when the next microbatch end cannot be found.
    #[error("failed to get next microbatch end: {0}")]
    NextMicrobatchEnd(#[source] NextMicrobatchEndError),
}

/// Errors that occur when determining the next microbatch start position
///
/// This error type is used by `StreamingQuery::next_microbatch_start()`.
#[derive(Debug, thiserror::Error)]
pub enum NextMicrobatchStartError {
    /// Failed to fetch the blocks table
    ///
    /// This occurs when the blocks table cannot be fetched.
    #[error("failed to fetch the blocks table: {0}")]
    BlocksTableFetch(#[source] BlocksTableFetchError),

    /// Failed to check if the blocks table contains the watermark
    ///
    /// This occurs when the blocks table cannot be checked if it contains the watermark.
    #[error("failed to check if the blocks table contains the watermark: {0}")]
    BlocksTableContains(#[source] BlocksTableContainsError),

    /// Failed to get the reorg base
    ///
    /// This occurs when the reorg base cannot be found.
    #[error("failed to get the reorg base: {0}")]
    ReorgBase(#[source] ReorgBaseError),
}

/// Failed to fetch the blocks table
///
/// This occurs when the blocks table cannot be fetched.
#[derive(Debug, thiserror::Error)]
#[error("failed to fetch the blocks table: {0}")]
pub struct NextMicrobatchEndError(#[source] BlocksTableFetchError);

/// Failed to get the latest source watermark
///
/// This error is returned by `latest_src_watermark()` when checking if blocks
/// are present in the blocks table fails.
#[derive(Debug, thiserror::Error)]
#[error("failed to get latest source watermark: {0}")]
pub struct LatestSrcWatermarkError(#[source] BlocksTableContainsError);

/// Errors that occur when finding the reorg base block
///
/// This error type is used by `StreamingQuery::reorg_base()`.
#[derive(Debug, thiserror::Error)]
pub enum ReorgBaseError {
    /// Failed to create an exec context
    ///
    /// This occurs when the exec context cannot be created.
    #[error("failed to create exec context: {0}")]
    CreateExecContext(#[source] crate::context::exec::CreateContextError),

    /// Failed to fetch the blocks table
    ///
    /// This occurs when the blocks table cannot be fetched.
    #[error("failed to fetch the blocks table: {0}")]
    BlocksTableFetch(#[source] BlocksTableFetchError),

    /// Failed to check if the blocks table contains the watermark
    ///
    /// This occurs when the blocks table cannot be checked if it contains the watermark.
    #[error("failed to check if the blocks table contains the watermark: {0}")]
    BlocksTableContains(#[source] BlocksTableContainsError),

    /// Failed to get the canonical chain
    ///
    /// This occurs when the canonical chain cannot be found.
    #[error("failed to get the canonical chain: {0}")]
    CanonicalChain(#[source] CanonicalChainError),
}

/// Failed to check if the blocks table contains a watermark
///
/// This error type is used by `StreamingQuery::blocks_table_contains()`.
#[derive(Debug, thiserror::Error)]
#[error("failed to fetch the blocks table: {0}")]
pub struct BlocksTableContainsError(#[source] BlocksTableFetchError);

/// Errors that occur when fetching block data from the blocks table
///
/// This error type is used by `StreamingQuery::blocks_table_fetch()`.
#[derive(Debug, thiserror::Error)]
pub enum BlocksTableFetchError {
    /// Failed to parse the SQL
    ///
    /// This occurs when the SQL cannot be parsed.
    #[error("failed to parse the SQL: {0}")]
    ParseSql(#[source] crate::sql::ParseSqlError),

    /// Failed to plan the SQL
    ///
    /// This occurs when the SQL cannot be planned.
    #[error("failed to plan the SQL: {0}")]
    PlanSql(#[source] crate::context::exec::SqlError),

    /// Failed to execute the SQL
    ///
    /// This occurs when the SQL cannot be executed.
    #[error("failed to execute the SQL: {0}")]
    ExecuteSql(#[source] crate::context::exec::ExecuteAndConcatError),

    /// Failed to extract a hash value from query results
    ///
    /// This occurs when the hash value cannot be found.
    #[error("failed to extract hash value: {0}")]
    ExtractHash(#[source] GetHashValueError),
}

/// Errors that occur when extracting hash values from block query results
///
/// This error type is used internally by `blocks_table_fetch()` when extracting
/// block hash and parent hash values from query results.
#[derive(Debug, thiserror::Error)]
pub enum GetHashValueError {
    /// Blocks table missing expected column
    ///
    /// This occurs when the blocks table does not contain the expected column
    /// (e.g., 'hash' or 'parent_hash').
    #[error("blocks table missing column: {0}")]
    MissingColumn(String),

    /// Failed to downcast the column
    ///
    /// This occurs when the column cannot be downcast.
    #[error("failed to downcast the column: {0}")]
    Downcast(#[source] DataFusionError),

    /// Blocks table missing block hash value
    ///
    /// This occurs when the blocks table column exists but does not contain
    /// a valid block hash value (either null or not convertible to BlockHash).
    #[error("blocks table missing block hash value for column {0}")]
    MissingBlockHashValue(String),

    /// Failed to parse the block hash value
    ///
    /// This occurs when the block hash value cannot be parsed into a BlockHash (e.g., due to invalid length).
    #[error("failed to parse block hash value for column {0}")]
    BlockHashParsing(String),

    /// Invalid Base58 hash value
    ///
    /// This occurs when a hash value in the blocks table is expected to be Base58-encoded
    /// (e.g., for Solana) but fails to decode properly.
    #[error("invalid Base58 hash value: {0}")]
    InvalidBase58HashValue(#[source] bs58::decode::Error),
}

struct BlockRow {
    number: BlockNum,
    hash: BlockHash,
    prev_hash: BlockHash,
    timestamp: Option<u64>,
}

impl BlockRow {
    fn cursor(&self) -> NetworkCursor {
        NetworkCursor {
            number: self.number,
            hash: self.hash,
        }
    }

    fn block_watermark(&self) -> BlockWatermark {
        BlockWatermark {
            number: self.number,
            hash: self.hash,
            timestamp: self.timestamp,
        }
    }
}

/// Wraps a record batch stream to periodically emit empty record batches as keep-alive signals.
/// These empty batches have the same schema as the original stream.
///
/// The keep-alive batches are emitted at the specified interval (in seconds) until the original
/// stream is exhausted.
pub fn keep_alive_stream<'a>(
    record_batch_stream: BoxStream<'a, Result<RecordBatch, DataFusionError>>,
    schema: SchemaRef,
    keep_alive_interval: u64,
) -> BoxStream<'a, Result<RecordBatch, DataFusionError>> {
    let period = Duration::from_secs(keep_alive_interval);
    let mut keep_alive_interval = tokio::time::interval(period);

    let missed_tick_behavior = MissedTickBehavior::Delay;
    keep_alive_interval.set_missed_tick_behavior(missed_tick_behavior);

    let mut record_batch_stream = record_batch_stream.fuse();

    Box::pin(async_stream::stream! {
        loop {
            tokio::select! {
                biased;

                maybe_batch = record_batch_stream.next() => {
                    match maybe_batch {
                        Some(batch) => {
                            yield batch;
                        }
                        None => {
                            break;
                        }
                    }
                }

                _ = keep_alive_interval.tick() => {
                    let empty_batch = RecordBatch::new_empty(schema.clone());
                    yield Ok(empty_batch);
                }
            }
        }
    })
}

/// Resolved blocks table information for a given network.
struct ResolvedBlocksTable {
    dataset_kind: DatasetKindStr,
    physical_table: PhysicalTable,
    sql_schema_name: String,
    hash_column: &'static str,
    parent_hash_column: &'static str,
    timestamp_column: &'static str,
}

impl ResolvedBlocksTable {
    /// Resolves the blocks table for the given dataset, selecting the correct
    /// table name and column names based on the dataset kind.
    async fn new(
        dataset: Arc<RawDataset>,
        data_store: DataStore,
    ) -> Result<Self, ResolveBlocksTableError> {
        let dataset_kind = dataset.kind();
        let table_info = BlocksTableInfo::new(&dataset_kind);
        let table = dataset.get_table(&table_info.table_name).ok_or_else(|| {
            ResolveBlocksTableError::BlocksTableNotFound(table_info.table_name.to_string())
        })?;

        let revision = data_store
            .get_table_active_revision(dataset.reference(), table.name())
            .await
            .map_err(ResolveBlocksTableError::GetActiveRevision)?
            .ok_or_else(|| {
                ResolveBlocksTableError::TableNotSynced(
                    dataset.reference().to_string(),
                    table.name().to_string(),
                )
            })?;

        let sql_schema_name = dataset.reference().to_reference().to_string();
        let networks = table.network().into_iter().cloned().collect();
        let physical_table = PhysicalTable::from_revision(
            data_store,
            dataset.reference().clone(),
            dataset.start_block(),
            Arc::clone(table) as Arc<dyn Table>,
            networks,
            revision,
        );

        Ok(Self {
            dataset_kind,
            physical_table,
            sql_schema_name,
            hash_column: table_info.hash_column,
            parent_hash_column: table_info.parent_hash_column,
            timestamp_column: table_info.timestamp_column,
        })
    }

    /// Returns the SQL fragment for constraining a query to a specific block hash,
    /// or an empty string if no hash is provided.
    ///
    /// Solana stores hashes as base58 strings (`Utf8`), so the constraint uses a
    /// string literal. All other chains store hashes as binary, so the constraint
    /// uses a hex literal (`x'...'`).
    fn hash_constraint_sql(&self, hash: Option<&BlockHash>) -> String {
        let Some(h) = hash else {
            return String::new();
        };
        let hash_column = self.hash_column;
        if self.dataset_kind.as_str() == "solana" {
            let base58_value = bs58::encode(h.as_slice()).into_string();
            format!("AND {hash_column} = '{base58_value}'")
        } else {
            let hex_value = h.encode_hex();
            format!("AND {hash_column} = x'{hex_value}'")
        }
    }

    /// Returns the block timestamp in seconds. Providers populate block
    /// timestamps with second precision.
    fn get_timestamp_value(&self, results: &RecordBatch) -> Option<u64> {
        let col = results.column_by_name(self.timestamp_column)?;
        if self.dataset_kind.as_str() == "solana" {
            // Solana's `block_time` is (nullable) DataType::Int64 (Unix timestamp in seconds).
            col.as_any().downcast_ref::<Int64Array>().and_then(|arr| {
                if arr.is_null(0) {
                    None
                } else {
                    arr.value(0).try_into().ok()
                }
            })
        } else {
            // Timestamp column is DataType::Timestamp(Nanosecond, _). Extract the
            // raw i64 nanos and convert to seconds.
            col.as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .and_then(|arr| {
                    if arr.is_null(0) {
                        None
                    } else {
                        Some(arr.value(0) as u64 / 1_000_000_000)
                    }
                })
        }
    }

    fn get_hash_value(&self, results: &RecordBatch) -> Result<BlockHash, GetHashValueError> {
        self.get_hash_inner(self.hash_column, results)
    }

    fn get_parent_hash_value(&self, results: &RecordBatch) -> Result<BlockHash, GetHashValueError> {
        self.get_hash_inner(self.parent_hash_column, results)
    }

    fn get_hash_inner(
        &self,
        column_name: &str,
        results: &RecordBatch,
    ) -> Result<BlockHash, GetHashValueError> {
        let column = results
            .column_by_name(column_name)
            .ok_or_else(|| GetHashValueError::MissingColumn(column_name.to_string()))?;

        if self.dataset_kind.as_str() == "solana" {
            let column = as_string_array(column).map_err(GetHashValueError::Downcast)?;
            column
                .iter()
                .flatten()
                .next()
                .map(|bs58_hash| {
                    bs58::decode(bs58_hash)
                        .into_vec()
                        .map_err(GetHashValueError::InvalidBase58HashValue)
                        .and_then(|decoded| {
                            BlockHash::try_from(decoded.as_slice()).map_err(|_| {
                                GetHashValueError::BlockHashParsing(column_name.to_string())
                            })
                        })
                })
                .ok_or_else(|| GetHashValueError::MissingBlockHashValue(column_name.to_string()))?
        } else {
            let column = as_fixed_size_binary_array(column).map_err(GetHashValueError::Downcast)?;
            column
                .iter()
                .flatten()
                .next()
                .map(|b| {
                    BlockHash::try_from(b)
                        .map_err(|_| GetHashValueError::BlockHashParsing(column_name.to_string()))
                })
                .ok_or_else(|| GetHashValueError::MissingBlockHashValue(column_name.to_string()))?
        }
    }
}

struct BlocksTableInfo {
    table_name: TableName,
    hash_column: &'static str,
    parent_hash_column: &'static str,
    timestamp_column: &'static str,
}

impl BlocksTableInfo {
    fn new(dataset_kind: &DatasetKindStr) -> Self {
        if dataset_kind.as_str() == "solana" {
            Self {
                table_name: "block_headers".parse().expect("valid table name"),
                hash_column: "block_hash",
                parent_hash_column: "previous_block_hash",
                timestamp_column: "block_time",
            }
        } else {
            Self {
                table_name: "blocks".parse().expect("valid table name"),
                hash_column: "hash",
                parent_hash_column: "parent_hash",
                timestamp_column: "timestamp",
            }
        }
    }
}

/// Errors that occur when resolving the blocks table for a network
///
/// This error type is used by `resolve_blocks_table()`.
#[derive(Debug, thiserror::Error)]
pub enum ResolveBlocksTableError {
    /// Blocks table not found
    ///
    /// This occurs when the blocks table is not found in the dataset.
    #[error("'blocks' table not found in dataset '{0}'")]
    BlocksTableNotFound(String),

    /// Failed to get active table revision
    ///
    /// This occurs when querying for an active physical table revision fails.
    #[error("Failed to get active table revision")]
    GetActiveRevision(#[source] amp_data_store::GetTableActiveRevisionError),

    /// Table not synced
    ///
    /// This occurs when the table has not been synced.
    #[error("table '{0}.{1}' has not been synced")]
    TableNotSynced(String, String),
}

/// Resolve the raw dataset from a set of root dataset references.
///
/// Traverses transitive dependencies from each root, collecting raw datasets.
/// Returns the first raw dataset found, validating that all raw datasets in
/// the dependency tree belong to the same network.
async fn resolve_raw_dataset_from_dependencies(
    datasets_cache: &DatasetsCache,
    root_dataset_refs: impl Iterator<Item = &HashReference>,
) -> Result<Arc<RawDataset>, ResolveRawDatasetError> {
    let mut found: Option<Arc<RawDataset>> = None;

    for hash_ref in root_dataset_refs {
        let dataset = datasets_cache
            .get_dataset(hash_ref)
            .await
            .map_err(ResolveRawDatasetError::GetDataset)?;

        let raw_datasets = crate::datasets_cache::collect_raw_datasets(datasets_cache, dataset)
            .await
            .map_err(ResolveRawDatasetError::Traversal)?;

        for raw in raw_datasets {
            match &found {
                None => found = Some(raw),
                Some(first) if *first.network() != *raw.network() => {
                    return Err(ResolveRawDatasetError::MultipleNetworks {
                        first: first.network().clone(),
                        second: raw.network().clone(),
                    });
                }
                Some(_) => {} // same network
            }
        }
    }

    found.ok_or(ResolveRawDatasetError::NoRawDatasetFound)
}

/// Errors that occur when resolving the raw dataset from dependencies.
#[derive(Debug, thiserror::Error)]
pub enum ResolveRawDatasetError {
    /// Failed to get root dataset from dataset store.
    #[error("failed to get dataset")]
    GetDataset(#[source] crate::datasets_cache::GetDatasetError),

    /// Failed to traverse dataset dependencies.
    #[error("failed to traverse dependencies")]
    Traversal(#[source] crate::datasets_cache::DependencyTraversalError),

    /// Multiple networks found in the dependency tree.
    #[error("multiple networks in dependency tree: {first} and {second}")]
    MultipleNetworks { first: NetworkId, second: NetworkId },

    /// No raw dataset found in the dependency tree.
    #[error("no raw dataset found in dependency tree")]
    NoRawDatasetFound,
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use datafusion::error::DataFusionError;
    use futures::stream;

    use super::keep_alive_stream;
    use crate::arrow::{
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };

    #[tokio::test]
    async fn test_keep_alive_stream() {
        use tokio_stream::StreamExt;
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]));

        let record_batches = vec![
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(crate::arrow::array::Int32Array::from(vec![1, 2, 3])),
                    Arc::new(crate::arrow::array::StringArray::from(vec!["x", "y", "z"])),
                ],
            )
            .unwrap(),
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(crate::arrow::array::Int32Array::from(vec![4, 5, 6])),
                    Arc::new(crate::arrow::array::StringArray::from(vec!["u", "v", "w"])),
                ],
            )
            .unwrap(),
        ];
        let original_batches_count = record_batches.len();

        let record_batch_stream = Box::pin(
            stream::iter(
                record_batches
                    .into_iter()
                    .map(Ok)
                    .collect::<Vec<Result<RecordBatch, DataFusionError>>>(),
            )
            .throttle(Duration::from_secs(2)),
        );

        let keep_alive_interval = 1; // 1 second for testing
        let mut stream =
            keep_alive_stream(record_batch_stream, schema.clone(), keep_alive_interval);

        let mut received_batches = Vec::new();
        let mut ticks = 0;

        while let Some(Ok(batch)) = stream.next().await {
            if batch.num_rows() == 0 {
                ticks += 1;
            }
            received_batches.push(batch);
        }

        assert!(
            received_batches.len() == original_batches_count + ticks,
            "Expected total batches to be {}, got {}",
            original_batches_count + ticks,
            received_batches.len()
        );
    }
}
