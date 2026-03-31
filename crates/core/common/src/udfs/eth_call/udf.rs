use std::{any::Any, str::FromStr, sync::Arc, time::Instant};

use alloy::{
    eips::BlockNumberOrTag,
    hex,
    network::{AnyNetwork, Ethereum},
    primitives::{Address, Bytes, TxKind},
    providers::Provider,
    rpc::{json_rpc::ErrorPayload, types::TransactionInput},
    transports::{RpcError, TransportErrorKind},
};
use amp_providers_common::{network_id::NetworkId, provider_name::ProviderName};
use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{
            Array, ArrayBuilder, BinaryArray, BinaryBuilder, FixedSizeBinaryArray, Int64Array,
            StringArray, StringBuilder, StructBuilder, UInt64Array,
        },
        datatypes::{DataType, Field, Fields},
    },
    common::{internal_err, plan_err},
    error::DataFusionError,
    logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
        async_udf::AsyncScalarUDFImpl,
    },
};
use itertools::izip;

use super::metrics::EthCallMetrics;
use crate::plan;

pub(crate) const MAX_RETRY_ATTEMPTS: u32 = 3;

type TransactionRequest = <Ethereum as alloy::network::Network>::TransactionRequest;

/// DataFusion UDF that executes an `eth_call` against an Ethereum JSON-RPC endpoint.
///
/// This async UDF performs read-only contract calls at a specified block, returning
/// the call result or error message. It's commonly used for querying contract state,
/// simulating transactions, or reading view functions without sending a transaction.
///
/// # SQL Usage
///
/// ```ignore
/// // Call a contract function at the latest block
/// eth_call(NULL, 0x1234...address, 0xabcd...calldata, 'latest')
///
/// // Call with a specific sender address at block 19000000
/// eth_call(0xsender...addr, 0xcontract...addr, 0xcalldata, '19000000')
///
/// // Query token balance (balanceOf call)
/// eth_call(NULL, token_address, balanceOf_calldata, 'latest')
/// ```
///
/// # Arguments
///
/// * `from` - `FixedSizeBinary(20)` sender address (optional, can be NULL)
/// * `to` - `FixedSizeBinary(20)` target contract address (required)
/// * `input` - `Binary` encoded function call data (optional)
/// * `block` - `Utf8` or `UInt64` or `Int64` block number or tag ("latest", "pending", "earliest")
///
/// # Returns
///
/// A struct with two fields:
/// * `data` - `Binary` the return data from the call (NULL on error)
/// * `message` - `Utf8` error message if the call reverted (NULL on success)
///
/// # Errors
///
/// Returns a planning error if:
/// - `to` address is NULL
/// - `block` is NULL
/// - `block` is not a valid integer or block tag
/// - Address conversion fails
#[derive(Debug, Clone)]
pub struct EthCall {
    name: String,
    client: alloy::providers::RootProvider<AnyNetwork>,
    provider_name: ProviderName,
    network: NetworkId,
    metrics: Option<Arc<EthCallMetrics>>,
    signature: Signature,
    fields: Fields,
}

impl PartialEq for EthCall {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.provider_name == other.provider_name
            && self.network == other.network
            && self.signature == other.signature
            && self.fields == other.fields
    }
}

impl Eq for EthCall {}

impl std::hash::Hash for EthCall {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.provider_name.hash(state);
        self.network.hash(state);
        self.signature.hash(state);
        self.fields.hash(state);
    }
}

impl EthCall {
    /// Creates an `EthCall` UDF with the given name, RPC client, and observability context.
    ///
    /// The name must match the flat lookup key that DataFusion's planner constructs
    /// for the function reference, e.g., `rpc.mainnet.eth_call`.
    pub fn new(
        name: String,
        client: alloy::providers::RootProvider<AnyNetwork>,
        provider_name: ProviderName,
        network: NetworkId,
        metrics: Option<Arc<EthCallMetrics>>,
    ) -> Self {
        EthCall {
            name,
            client,
            provider_name,
            network,
            metrics,
            signature: Signature {
                type_signature: TypeSignature::Any(4),
                volatility: Volatility::Volatile,
                parameter_names: Some(vec![
                    "from".to_string(),
                    "to".to_string(),
                    "input_data".to_string(),
                    "block".to_string(),
                ]),
            },
            fields: Fields::from_iter([
                Field::new("data", DataType::Binary, true),
                Field::new("message", DataType::Utf8, true),
            ]),
        }
    }
}

impl ScalarUDFImpl for EthCall {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(DataType::Struct(self.fields.clone()))
    }

    /// Since this is an async UDF, the `invoke_with_args` method will not be called.
    fn invoke_with_args(
        &self,
        _args: ScalarFunctionArgs,
    ) -> Result<ColumnarValue, DataFusionError> {
        unreachable!("is only called as async UDF");
    }
}

#[async_trait]
impl AsyncScalarUDFImpl for EthCall {
    async fn invoke_async_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> Result<ColumnarValue, DataFusionError> {
        let name = self.name().to_string();
        let client = self.client.clone();
        let fields = self.fields.clone();
        let network_label = self.network.as_str();
        let provider_label = self.provider_name.as_str();

        // Decode the arguments.
        let args: Vec<_> = ColumnarValue::values_to_arrays(&args.args)?;
        let [from, to, input_data, block] = args.as_slice() else {
            return internal_err!("{}: expected 4 arguments, but got {}", name, args.len());
        };

        // from: Optional, only accepts address
        let from = match from.data_type() {
            DataType::Null => {
                let from_len = from.len();
                &FixedSizeBinaryArray::new_null(20, from_len)
            }
            DataType::FixedSizeBinary(20) => from
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap(),
            _ => return plan_err!("{}: 'from' address is not a valid address", name),
        };
        // to: Required, only accepts address
        let to = match to.data_type() {
            DataType::FixedSizeBinary(20) => {
                to.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap()
            }
            _ => return plan_err!("{}: 'to' address is not a valid address", name),
        };
        // input_data: Optional, only accepts binary
        let input_data = match input_data.data_type() {
            DataType::Null => {
                let input_data_len = input_data.len();
                &BinaryArray::new_null(input_data_len)
            }
            DataType::Binary => input_data.as_any().downcast_ref::<BinaryArray>().unwrap(),
            _ => return plan_err!("{}: input data is not a valid data", name),
        };
        // block: Required, accepts block number (UInt64/Int64) or tag (string)
        let parse_block_str = |s: &str| -> Result<BlockNumberOrTag, DataFusionError> {
            u64::from_str(s)
                .map(BlockNumberOrTag::Number)
                .or_else(|_| BlockNumberOrTag::from_str(s))
                .map_err(|_| {
                    plan!("block is not a valid integer, \"0x\" prefixed hex integer, or tag")
                })
        };

        let block_null_err = || plan!("'block' is NULL");
        let downcast_err = |expected| plan!("Failed to downcast block to {}", expected);

        let blocks: Vec<BlockNumberOrTag> = match block.data_type() {
            DataType::Utf8 => block
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| downcast_err("StringArray"))?
                .iter()
                .map(|b| parse_block_str(b.ok_or_else(block_null_err)?))
                .collect::<Result<_, _>>()?,
            DataType::UInt64 => block
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| downcast_err("UInt64Array"))?
                .iter()
                .map(|b| b.ok_or_else(block_null_err).map(BlockNumberOrTag::Number))
                .collect::<Result<_, _>>()?,
            DataType::Int64 => block
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| downcast_err("Int64Array"))?
                .iter()
                .map(|b| {
                    b.ok_or_else(block_null_err).and_then(|n| {
                        if n < 0 {
                            plan_err!("block number cannot be negative: {}", n)
                        } else {
                            Ok(BlockNumberOrTag::Number(n as u64))
                        }
                    })
                })
                .collect::<Result<_, _>>()?,
            _ => {
                return plan_err!(
                    "{}: 'block' is not a valid block number or tag: {}",
                    name,
                    block.data_type()
                );
            }
        };

        // Make the eth_call requests.
        let mut result_builder = StructBuilder::from_fields(fields, from.len());
        for (from, to, input_data, block) in izip!(from, to, input_data, blocks) {
            let Some(to) = to else {
                return plan_err!("to address is NULL");
            };

            let block_selector = block.to_string();
            let calldata_len = input_data.map(|d| d.len()).unwrap_or(0);
            let to_hex = hex::encode(to);
            let from_hex = from.map(hex::encode);

            let span = tracing::info_span!(
                "eth_call",
                network = %network_label,
                provider = %provider_label,
                udf_name = %name,
                block_selector_type = block_selector_type(&block),
                block_selector_value = %block_selector,
                to = %to_hex,
                calldata_len,
                attempts_total = tracing::field::Empty,
                outcome = tracing::field::Empty,
                error_class = tracing::field::Empty,
            );

            if let Some(metrics) = &self.metrics {
                metrics.record_request(network_label, provider_label);
            }

            let invocation_start = Instant::now();

            let retry_result = eth_call_with_retry(
                |req, blk| {
                    let c = client.clone();
                    async move { classify_rpc_result(c.call(req.into()).block(blk.into()).await) }
                },
                block,
                TransactionRequest {
                    // `eth_call` only requires the following fields
                    // (https://ethereum.org/en/developers/docs/apis/json-rpc/#eth_call)
                    from: match from {
                        Some(from) => Some(Address::new(from.try_into().map_err(|_| {
                            DataFusionError::Execution(format!(
                                "invalid from address: {}",
                                hex::encode(from)
                            ))
                        })?)),
                        None => None,
                    },
                    to: Some(TxKind::Call(Address::new(to.try_into().map_err(|_| {
                        DataFusionError::Execution(format!(
                            "invalid to address: {}",
                            hex::encode(to)
                        ))
                    })?))),
                    gas: None,
                    gas_price: None,
                    value: None,
                    input: TransactionInput {
                        input: input_data.map(Bytes::copy_from_slice),
                        data: None,
                    },
                    ..Default::default()
                },
                network_label,
                provider_label,
                self.metrics.as_deref(),
            )
            .await;

            let latency_ms = invocation_start.elapsed().as_secs_f64() * 1000.0;

            if let Some(metrics) = &self.metrics {
                metrics.record_latency(latency_ms, network_label, provider_label);
            }

            span.record("attempts_total", retry_result.attempts);

            // Build the response row, record metrics, and log — single match.
            match retry_result.result {
                Ok(bytes) => {
                    span.record("outcome", "ok");
                    tracing::debug!(
                        network = %network_label,
                        provider = %provider_label,
                        udf_name = %name,
                        block_selector = %block_selector,
                        to = %to_hex,
                        from = ?from_hex,
                        calldata_len_bytes = calldata_len,
                        success = true,
                        attempts_total = retry_result.attempts,
                        latency_ms,
                        "eth_call invocation completed"
                    );
                    result_builder
                        .field_builder::<BinaryBuilder>(0)
                        .unwrap()
                        .append_value(&bytes);
                    result_builder
                        .field_builder::<StringBuilder>(1)
                        .unwrap()
                        .append_null();
                    result_builder.append(true);
                }
                Err(err) => {
                    let error_class = err.error_class();

                    if let Some(metrics) = &self.metrics {
                        metrics.record_retry_error(network_label, provider_label, &err);
                    }

                    span.record("outcome", "error");
                    span.record("error_class", error_class);

                    let (rpc_error_code, message) = match &err {
                        EthCallRetryError::RpcError(resp) => {
                            (Some(resp.code), resp.message.to_string())
                        }
                        EthCallRetryError::RetriesFailed => {
                            (None, "unexpected rpc error".to_string())
                        }
                    };
                    let truncated_message = message.get(..500).unwrap_or(&message);
                    tracing::debug!(
                        network = %network_label,
                        provider = %provider_label,
                        udf_name = %name,
                        block_selector = %block_selector,
                        to = %to_hex,
                        from = ?from_hex,
                        calldata_len_bytes = calldata_len,
                        success = false,
                        attempts_total = retry_result.attempts,
                        latency_ms,
                        error_class,
                        rpc_error_code,
                        error_message = truncated_message,
                        "eth_call invocation completed"
                    );

                    match err {
                        EthCallRetryError::RpcError(resp) => {
                            match resp.data {
                                Some(data) => {
                                    match hex::decode(
                                        data.get().trim_start_matches('"').trim_end_matches('"'),
                                    ) {
                                        Ok(data) => result_builder
                                            .field_builder::<BinaryBuilder>(0)
                                            .unwrap()
                                            .append_value(data),
                                        Err(_) => {
                                            result_builder
                                                .field_builder::<BinaryBuilder>(0)
                                                .unwrap()
                                                .append_null();
                                        }
                                    }
                                }
                                None => result_builder
                                    .field_builder::<BinaryBuilder>(0)
                                    .unwrap()
                                    .append_null(),
                            }
                            if !resp.message.is_empty() {
                                result_builder
                                    .field_builder::<StringBuilder>(1)
                                    .unwrap()
                                    .append_value(resp.message)
                            } else {
                                result_builder
                                    .field_builder::<StringBuilder>(1)
                                    .unwrap()
                                    .append_null()
                            }
                        }
                        EthCallRetryError::RetriesFailed => {
                            result_builder
                                .field_builder::<BinaryBuilder>(0)
                                .unwrap()
                                .append_null();
                            result_builder
                                .field_builder::<StringBuilder>(1)
                                .unwrap()
                                .append_value("unexpected rpc error");
                        }
                    }
                    result_builder.append(true);
                }
            }
        }
        Ok(ColumnarValue::Array(ArrayBuilder::finish(
            &mut result_builder,
        )))
    }
}

// ---------------------------------------------------------------------------
// Retry logic
// ---------------------------------------------------------------------------

pub(crate) struct EthCallRetryResult {
    pub(crate) result: Result<Bytes, EthCallRetryError>,
    pub(crate) attempts: u32,
}

/// Terminal error from an eth_call invocation after the retry loop completes.
#[derive(Debug, thiserror::Error)]
pub(crate) enum EthCallRetryError {
    /// Non-retryable JSON-RPC error (codes 3 or -32000).
    #[error("non-retryable RPC error")]
    RpcError(ErrorPayload),
    /// All retry attempts exhausted without a successful response.
    #[error("eth_call retries exhausted after {MAX_RETRY_ATTEMPTS} attempts")]
    RetriesFailed,
}

impl EthCallRetryError {
    /// Returns the metric/log label for this error category.
    pub(crate) fn error_class(&self) -> &'static str {
        match self {
            Self::RpcError(_) => "rpc_error_non_retryable",
            Self::RetriesFailed => "rpc_error_retry_exhausted",
        }
    }
}

/// Outcome of a single RPC call attempt, abstracting over transport details.
///
/// Used to decouple retry logic from the concrete alloy transport so that
/// unit tests can supply scripted responses.
pub(crate) enum RpcCallOutcome {
    Ok(Bytes),
    NonRetryableError(ErrorPayload),
    RetryableError(RpcError<TransportErrorKind>),
}

/// Execute an eth_call with retries, recording per-attempt metrics and logs.
///
/// `call_fn` is invoked for each attempt. In production it wraps
/// `client.call(...)`. In tests it returns scripted [`RpcCallOutcome`]s.
pub(crate) async fn eth_call_with_retry<F, Fut>(
    call_fn: F,
    block: BlockNumberOrTag,
    req: TransactionRequest,
    network: &str,
    provider: &str,
    metrics: Option<&EthCallMetrics>,
) -> EthCallRetryResult
where
    F: Fn(TransactionRequest, BlockNumberOrTag) -> Fut,
    Fut: std::future::Future<Output = RpcCallOutcome>,
{
    for attempt in 1..=MAX_RETRY_ATTEMPTS {
        let attempt_start = Instant::now();
        let outcome = call_fn(req.clone(), block).await;
        let attempt_latency_ms = attempt_start.elapsed().as_secs_f64() * 1000.0;

        match outcome {
            RpcCallOutcome::Ok(bytes) => {
                return EthCallRetryResult {
                    result: Ok(bytes),
                    attempts: attempt,
                };
            }
            RpcCallOutcome::NonRetryableError(resp) => {
                tracing::debug!(
                    attempt,
                    network,
                    provider,
                    latency_ms_attempt = attempt_latency_ms,
                    error_class_attempt = "rpc_error_non_retryable",
                    rpc_error_code_attempt = resp.code,
                    will_retry = false,
                    error_message = %resp.message,
                    "eth_call attempt returned non-retryable error"
                );
                return EthCallRetryResult {
                    result: Err(EthCallRetryError::RpcError(resp)),
                    attempts: attempt,
                };
            }
            RpcCallOutcome::RetryableError(source) => {
                let will_retry = attempt < MAX_RETRY_ATTEMPTS;

                tracing::debug!(
                    attempt,
                    network,
                    provider,
                    latency_ms_attempt = attempt_latency_ms,
                    error_class_attempt = "rpc_error_retryable",
                    will_retry,
                    error_message = %source,
                    "eth_call attempt failed with retryable error"
                );

                if let Some(metrics) = metrics {
                    metrics.record_retry(network, provider);
                }

                if !will_retry {
                    tracing::warn!(
                        attempts = MAX_RETRY_ATTEMPTS,
                        network,
                        provider,
                        error_message = %source,
                        "eth_call retries exhausted"
                    );
                }
            }
        }
    }
    EthCallRetryResult {
        result: Err(EthCallRetryError::RetriesFailed),
        attempts: MAX_RETRY_ATTEMPTS,
    }
}

/// Map an alloy `RpcError` to our transport-agnostic [`RpcCallOutcome`].
fn classify_rpc_result(result: Result<Bytes, RpcError<TransportErrorKind>>) -> RpcCallOutcome {
    match result {
        Ok(bytes) => RpcCallOutcome::Ok(bytes),
        Err(RpcError::ErrorResp(resp)) if [3, -32000].contains(&resp.code) => {
            RpcCallOutcome::NonRetryableError(resp)
        }
        Err(err) => RpcCallOutcome::RetryableError(err),
    }
}

fn block_selector_type(block: &BlockNumberOrTag) -> &'static str {
    match block {
        BlockNumberOrTag::Number(_) => "number",
        _ => "tag",
    }
}
