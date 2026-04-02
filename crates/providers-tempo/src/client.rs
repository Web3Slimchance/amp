use std::{
    num::{NonZeroU32, NonZeroU64},
    sync::Arc,
    time::{Duration, Instant},
};

use alloy::{
    eips::{BlockId, BlockNumberOrTag},
    hex,
    providers::Provider as _,
    rpc::{
        client::BatchRequest,
        json_rpc::{RpcRecv, RpcSend},
    },
    transports::http::reqwest::Url,
};
use amp_providers_common::{network_id::NetworkId, provider_name::ProviderName};
use async_stream::stream;
use datasets_common::block_num::BlockNum;
use datasets_raw::{
    client::{BlockStreamError, BlockStreamResultExt, BlockStreamer, LatestBlockError},
    rows::Rows,
};
use futures::{Stream, future::try_join_all};
use tempo_alloy::{TempoNetwork, rpc::TempoTransactionReceipt};

use crate::{
    convert::{TempoRpcBlock, rpc_to_rows},
    error::{BatchRequestError, BatchingError, ClientError, FetchReceiptsError},
    provider::Auth,
};

struct BatchingRpcWrapper {
    client: RootProviderWithMetrics,
    batch_size: usize,
    limiter: Arc<tokio::sync::Semaphore>,
}

impl BatchingRpcWrapper {
    fn new(
        client: RootProviderWithMetrics,
        batch_size: usize,
        limiter: Arc<tokio::sync::Semaphore>,
    ) -> Self {
        assert!(batch_size > 0, "batch_size must be > 0");
        Self {
            client,
            batch_size,
            limiter,
        }
    }

    async fn execute<T: RpcRecv, Params: RpcSend>(
        &self,
        calls: Vec<(&'static str, Params)>,
    ) -> Result<Vec<T>, BatchingError> {
        if calls.is_empty() {
            tracing::debug!("skipped batch execution, no calls provided");
            return Ok(Vec::new());
        }
        let mut results = Vec::new();
        let mut remaining_calls = calls;

        while !remaining_calls.is_empty() {
            let chunk: Vec<_> = remaining_calls
                .drain(..self.batch_size.min(remaining_calls.len()))
                .collect();

            let _permit = self
                .limiter
                .acquire()
                .await
                .map_err(BatchingError::RateLimitAcquire)?;

            let batch_responses = self
                .client
                .batch_request(&chunk)
                .await
                .map_err(BatchingError::Request)?;
            results.extend(batch_responses);
        }
        Ok(results)
    }
}

/// Tempo RPC block-streaming client.
///
/// Connects to a Tempo-compatible JSON-RPC endpoint and streams blocks with
/// their transactions, receipts, and logs as dataset rows.
#[derive(Clone)]
pub struct Client {
    client: RootProviderWithMetrics,
    network: NetworkId,
    provider_name: ProviderName,
    limiter: Arc<tokio::sync::Semaphore>,
    batch_size: usize,
    fetch_receipts_per_tx: bool,
}

impl Client {
    /// Create a new HTTP/HTTPS Tempo RPC client.
    ///
    /// # Panics
    ///
    /// Panics if `request_limit` is zero.
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        url: Url,
        network: NetworkId,
        provider_name: ProviderName,
        request_limit: u16,
        batch_size: usize,
        rate_limit: Option<NonZeroU32>,
        fetch_receipts_per_tx: bool,
        timeout: Duration,
        auth: Option<Auth>,
        meter: Option<&monitoring::telemetry::metrics::Meter>,
    ) -> Result<Self, ClientError> {
        assert!(request_limit >= 1);
        let client = crate::provider::new_http(url, auth, rate_limit, timeout)?;
        let client =
            RootProviderWithMetrics::new(client, meter, provider_name.to_string(), network.clone());
        let limiter = tokio::sync::Semaphore::new(request_limit as usize).into();
        Ok(Self {
            client,
            network,
            provider_name,
            limiter,
            batch_size,
            fetch_receipts_per_tx,
        })
    }

    /// Create a new IPC Tempo RPC client.
    ///
    /// # Panics
    ///
    /// Panics if `request_limit` is zero.
    #[expect(clippy::too_many_arguments)]
    pub async fn new_ipc(
        path: std::path::PathBuf,
        network: NetworkId,
        provider_name: ProviderName,
        request_limit: u16,
        batch_size: usize,
        rate_limit: Option<NonZeroU32>,
        fetch_receipts_per_tx: bool,
        meter: Option<&monitoring::telemetry::metrics::Meter>,
    ) -> Result<Self, ClientError> {
        assert!(request_limit >= 1);
        let client = crate::provider::new_ipc(path, rate_limit)
            .await
            .map_err(ClientError::Transport)
            .map(|c| {
                RootProviderWithMetrics::new(c, meter, provider_name.to_string(), network.clone())
            })?;
        let limiter = tokio::sync::Semaphore::new(request_limit as usize).into();
        Ok(Self {
            client,
            network,
            provider_name,
            limiter,
            batch_size,
            fetch_receipts_per_tx,
        })
    }

    /// Create a new WebSocket Tempo RPC client.
    ///
    /// # Panics
    ///
    /// Panics if `request_limit` is zero.
    #[expect(clippy::too_many_arguments)]
    pub async fn new_ws(
        url: Url,
        network: NetworkId,
        provider_name: ProviderName,
        request_limit: u16,
        batch_size: usize,
        rate_limit: Option<NonZeroU32>,
        fetch_receipts_per_tx: bool,
        auth: Option<Auth>,
        meter: Option<&monitoring::telemetry::metrics::Meter>,
    ) -> Result<Self, ClientError> {
        assert!(request_limit >= 1);
        let client = crate::provider::new_ws(url, auth, rate_limit)
            .await
            .map_err(ClientError::Transport)
            .map(|c| {
                RootProviderWithMetrics::new(c, meter, provider_name.to_string(), network.clone())
            })?;
        let limiter = tokio::sync::Semaphore::new(request_limit as usize).into();
        Ok(Self {
            client,
            network,
            provider_name,
            limiter,
            batch_size,
            fetch_receipts_per_tx,
        })
    }

    /// Returns the configured provider name.
    pub fn provider_name(&self) -> &str {
        &self.provider_name
    }

    /// Create a stream that fetches blocks one at a time.
    fn unbatched_block_stream(
        self,
        start_block: u64,
        end_block: u64,
    ) -> impl Stream<Item = Result<Rows, BlockStreamError>> + Send {
        assert!(end_block >= start_block);
        let total_blocks_to_stream = end_block - start_block + 1;

        tracing::info!(
            %start_block,
            %end_block,
            total = %total_blocks_to_stream,
            "started unbatched block fetch"
        );

        let mut last_progress_report = Instant::now();

        stream! {
            'outer: for block_num in start_block..=end_block {
                let elapsed = last_progress_report.elapsed();
                if elapsed >= Duration::from_secs(15) {
                    let blocks_streamed = block_num - start_block;
                    let percentage_streamed = (blocks_streamed as f32 / total_blocks_to_stream as f32) * 100.0;
                    tracing::info!(
                        current = %block_num,
                        start = %start_block,
                        end = %end_block,
                        completed = blocks_streamed,
                        total = total_blocks_to_stream,
                        percent = format_args!("{:.2}", percentage_streamed),
                        "block fetch progress"
                    );
                    last_progress_report = Instant::now();
                }

                let Ok(_permit) = self.limiter.acquire().await else {
                    yield Err("rate limiter semaphore closed").recoverable();
                    return;
                };

                let block_num = BlockNumberOrTag::Number(block_num);
                let block = self.client.with_metrics("eth_getBlockByNumber", async |c| {
                    c.get_block_by_number(block_num).full().await
                }).await;
                let block = match block {
                    Ok(Some(block)) => block,
                    Ok(None) => {
                        yield Err(format!("block {} not found", block_num)).recoverable();
                        continue;
                    }
                    Err(err) => {
                        yield Err(err).recoverable();
                        continue;
                    }
                };

                if block.transactions.is_empty() {
                    yield rpc_to_rows(block, Vec::new(), &self.network).recoverable();
                    continue;
                }

                let receipts = if self.fetch_receipts_per_tx {
                    let calls = block
                        .transactions
                        .hashes()
                        .map(|hash| {
                            let client = &self.client;
                            async move {
                                client.with_metrics("eth_getTransactionReceipt", |c| async move {
                                    c.get_transaction_receipt(hash).await.map(|r| (hash, r))
                                }).await
                            }
                        });
                    let Ok(receipts) = try_join_all(calls).await else {
                        yield Err(format!("error fetching receipts for block {}", block.number())).recoverable();
                        continue;
                    };
                    let mut received_receipts = Vec::new();
                    for (hash, receipt) in receipts {
                        match receipt {
                            Some(receipt) => received_receipts.push(receipt),
                            None => {
                                yield Err(format!("missing receipt for transaction: {}", hex::encode(hash))).recoverable();
                                continue 'outer;
                            }
                        }
                    }
                    received_receipts
                } else {
                    let rpc_result = self.client
                        .with_metrics("eth_getBlockReceipts", async |c| {
                            c.get_block_receipts(BlockId::Number(block_num)).await
                        })
                        .await;

                    match rpc_result {
                        Ok(Some(mut receipts)) => {
                            receipts.sort_unstable_by_key(|r| r.transaction_index);
                            receipts
                        }
                        Ok(None) => {
                            yield Err(FetchReceiptsError::Empty { block_num: block.number() }).recoverable();
                            continue;
                        }
                        Err(err) => {
                            yield Err(FetchReceiptsError::Rpc { block_num: block.number(), err: ClientError::Transport(err) }).recoverable();
                            continue;
                        }
                    }
                };

                yield rpc_to_rows(block, receipts, &self.network).recoverable();
            }
        }
    }

    /// Create a stream that fetches blocks in batches.
    fn batched_block_stream(
        self,
        start_block: u64,
        end_block: u64,
    ) -> impl Stream<Item = Result<Rows, BlockStreamError>> + Send {
        tracing::info!(
            %start_block,
            %end_block,
            "started batched block fetch"
        );
        let batching_client =
            BatchingRpcWrapper::new(self.client.clone(), self.batch_size, self.limiter.clone());

        let mut blocks_completed = 0;
        let mut txns_completed = 0;

        stream! {
            let stream_start = Instant::now();
            let block_calls: Vec<_> = (start_block..=end_block)
                .map(|block_num| (
                    "eth_getBlockByNumber",
                    (BlockNumberOrTag::Number(block_num), true),
                ))
                .collect::<Vec<_>>()
                .chunks(self.batch_size * 10)
                .map(<[_]>::to_vec)
                .collect();

            for batch_calls in block_calls {
                let start = Instant::now();
                let blocks_result: Result<Vec<TempoRpcBlock>, BatchingError> = batching_client.execute(batch_calls).await;
                let blocks = match blocks_result {
                    Ok(blocks) => blocks,
                    Err(err) => {
                        yield Err(err).recoverable();
                        return;
                    }
                };

                let total_tx_count: usize = blocks.iter().map(|b| b.transactions.len()).sum();

                if total_tx_count == 0 {
                    for block in blocks.into_iter() {
                        blocks_completed += 1;
                        yield rpc_to_rows(block, Vec::new(), &self.network).recoverable();
                    }
                } else {
                    let all_receipts_result: Result<Vec<_>, BatchingError> = if self.fetch_receipts_per_tx {
                        let receipt_calls: Vec<_> = blocks
                            .iter()
                            .flat_map(|block| {
                                block.transactions.hashes().map(|tx_hash| {
                                    let tx_hash = format!("0x{}",hex::encode(tx_hash));
                                    ("eth_getTransactionReceipt", [tx_hash])
                                })
                            })
                            .collect();
                        batching_client.execute(receipt_calls).await
                    } else {
                        let receipt_calls: Vec<_> = blocks
                            .iter()
                            .map(|block| (
                                "eth_getBlockReceipts",
                                [BlockNumberOrTag::Number(block.number())]
                            ))
                            .collect();
                        let receipts_result: Result<Vec<Vec<TempoTransactionReceipt>>, BatchingError> =
                            batching_client.execute(receipt_calls).await;
                        receipts_result.map(|receipts| receipts.into_iter().flatten().collect())
                    };

                    let all_receipts = match all_receipts_result {
                        Ok(receipts) => receipts,
                        Err(err) => {
                            yield Err(err).recoverable();
                            return;
                        }
                    };

                    if total_tx_count != all_receipts.len() {
                        let err = format!(
                            "mismatched tx and receipt count in batch: {} txs, {} receipts",
                            total_tx_count,
                            all_receipts.len()
                        );
                        yield Err(err).recoverable();
                        return;
                    }

                    let mut all_receipts = all_receipts.into_iter();

                    for block in blocks {
                        let mut block_receipts: Vec<_> =
                            all_receipts.by_ref().take(block.transactions.len()).collect();
                        block_receipts.sort_unstable_by_key(|r| r.transaction_index);
                        blocks_completed += 1;
                        txns_completed += block.transactions.len();
                        yield rpc_to_rows(block, block_receipts, &self.network).recoverable();
                    }
                }

                let total_blocks_to_stream = end_block - start_block + 1;
                tracing::info!(
                    completed = blocks_completed,
                    total = total_blocks_to_stream,
                    percent = format_args!("{:.2}", (blocks_completed as f32 / total_blocks_to_stream as f32) * 100.0),
                    txns = txns_completed,
                    elapsed_ms = start.elapsed().as_millis() as u64,
                    "batch fetch progress"
                );
            }
            tracing::info!(
                %start_block,
                %end_block,
                elapsed_ms = stream_start.elapsed().as_millis() as u64,
                blocks = blocks_completed,
                txns = txns_completed,
                "completed batched block fetch"
            );
        }
    }
}

impl AsRef<alloy::providers::RootProvider<TempoNetwork>> for Client {
    fn as_ref(&self) -> &alloy::providers::RootProvider<TempoNetwork> {
        &self.client.inner
    }
}

impl BlockStreamer for Client {
    async fn block_stream(
        self,
        start: BlockNum,
        end: BlockNum,
    ) -> impl Stream<Item = Result<Rows, BlockStreamError>> + Send {
        stream! {
            if self.batch_size > 1 {
                for await item in self.batched_block_stream(start, end) {
                    yield item;
                }
            } else {
                for await item in self.unbatched_block_stream(start, end) {
                    yield item;
                }
            }
        }
    }

    #[tracing::instrument(skip(self), err)]
    async fn latest_block(
        &mut self,
        finalized: bool,
    ) -> Result<Option<BlockNum>, LatestBlockError> {
        let number = match finalized {
            true => BlockNumberOrTag::Finalized,
            false => BlockNumberOrTag::Latest,
        };
        let _permit = self
            .limiter
            .acquire()
            .await
            .map_err(|err| LatestBlockError::from(BatchingError::RateLimitAcquire(err)))?;
        let block = self
            .client
            .with_metrics("eth_getBlockByNumber", async |c| {
                c.get_block_by_number(number).await
            })
            .await?;
        Ok(block.map(|b| b.number()))
    }

    fn bucket_size(&self) -> Option<NonZeroU64> {
        None
    }

    fn provider_name(&self) -> &str {
        &self.provider_name
    }
}

#[derive(Debug, Clone)]
struct RootProviderWithMetrics {
    inner: alloy::providers::RootProvider<TempoNetwork>,
    metrics: Option<crate::metrics::MetricsRegistry>,
    provider: String,
    network: NetworkId,
}

impl RootProviderWithMetrics {
    fn new(
        inner: alloy::providers::RootProvider<TempoNetwork>,
        meter: Option<&monitoring::telemetry::metrics::Meter>,
        provider: String,
        network: NetworkId,
    ) -> Self {
        let metrics = meter.map(crate::metrics::MetricsRegistry::new);
        Self {
            inner,
            metrics,
            provider,
            network,
        }
    }

    async fn with_metrics<T, E, F, Fut>(&self, method: &str, func: F) -> Fut::Output
    where
        F: FnOnce(alloy::providers::RootProvider<TempoNetwork>) -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        let Some(metrics) = self.metrics.as_ref() else {
            return func(self.inner.clone()).await;
        };

        let start = Instant::now();
        let resp = func(self.inner.clone()).await;
        let duration = start.elapsed().as_millis() as f64;
        metrics.record_single_request(duration, &self.provider, &self.network, method);
        if resp.is_err() {
            metrics.record_error(&self.provider, &self.network);
        }
        resp
    }

    async fn batch_request<Params, Resp>(
        &self,
        requests: &[(&'static str, Params)],
    ) -> Result<Vec<Resp>, BatchRequestError>
    where
        Params: RpcSend,
        Resp: RpcRecv,
    {
        let mut batch = BatchRequest::new(self.inner.client());
        let mut waiters = Vec::new();

        for (method, params) in requests.iter() {
            waiters.push(
                batch
                    .add_call(*method, &params)
                    .map_err(BatchRequestError)?,
            );
        }

        let Some(metrics) = self.metrics.as_ref() else {
            batch.send().await.map_err(BatchRequestError)?;
            let resp = try_join_all(waiters).await.map_err(BatchRequestError)?;
            return Ok(resp);
        };

        let start = Instant::now();

        batch
            .send()
            .await
            .inspect_err(|_| {
                let duration = start.elapsed().as_millis() as f64;
                metrics.record_batch_request(
                    duration,
                    requests.len() as u64,
                    &self.provider,
                    &self.network,
                );
                metrics.record_error(&self.provider, &self.network);
            })
            .map_err(BatchRequestError)?;

        let resp = try_join_all(waiters).await;
        let duration = start.elapsed().as_millis() as f64;
        metrics.record_batch_request(
            duration,
            requests.len() as u64,
            &self.provider,
            &self.network,
        );

        if resp.is_err() {
            metrics.record_error(&self.provider, &self.network);
        }

        let resp = resp.map_err(BatchRequestError)?;
        Ok(resp)
    }
}
