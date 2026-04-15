//! Bitcoin Core JSON-RPC response types and HTTP client.

use std::{num::NonZeroU32, sync::Arc, time::Instant};

use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};
use serde::Deserialize;

use crate::metrics::MetricsRegistry;

fn de_number_as_string<'de, D>(d: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    serde_json::Number::deserialize(d).map(|n| n.to_string())
}

/// Generic JSON-RPC response envelope.
#[derive(Debug, Deserialize)]
pub struct JsonRpcResponse<T> {
    pub result: Option<T>,
    pub error: Option<JsonRpcError>,
}

/// JSON-RPC error object.
#[derive(Debug, Deserialize)]
pub struct JsonRpcError {
    pub code: i64,
    pub message: String,
}

/// Full Bitcoin block with verbosity=2 (includes full transaction data).
#[derive(Debug, Deserialize)]
pub struct RpcBlock {
    pub hash: String,
    pub height: u64,
    pub version: i32,
    pub merkleroot: String,
    pub time: u32,
    pub mediantime: u32,
    pub nonce: u32,
    pub bits: String,
    pub difficulty: f64,
    #[serde(rename = "nTx")]
    pub n_tx: u32,
    #[serde(default)]
    pub previousblockhash: Option<String>,
    pub strippedsize: u32,
    pub size: u32,
    pub weight: u32,
    pub tx: Vec<RpcTransaction>,
}

/// Bitcoin transaction.
#[derive(Debug, Deserialize)]
pub struct RpcTransaction {
    pub txid: String,
    pub hash: String,
    pub version: i32,
    pub size: u32,
    pub vsize: u32,
    pub weight: u32,
    pub locktime: u32,
    pub vin: Vec<RpcInput>,
    pub vout: Vec<RpcOutput>,
}

/// Transaction input (either coinbase or spending a previous output).
#[derive(Debug, Deserialize)]
pub struct RpcInput {
    /// Coinbase data - present only for coinbase transactions.
    #[serde(default)]
    pub coinbase: Option<String>,
    /// Transaction being spent - None for coinbase.
    #[serde(default)]
    pub txid: Option<String>,
    /// Output index being spent - None for coinbase.
    #[serde(default)]
    pub vout: Option<u32>,
    /// Signature script.
    #[serde(rename = "scriptSig")]
    pub script_sig: Option<RpcScriptSig>,
    /// Segregated witness stack items (hex-encoded).
    #[serde(default)]
    pub txinwitness: Option<Vec<String>>,
    pub sequence: u32,
}

/// Script signature.
#[derive(Debug, Deserialize)]
pub struct RpcScriptSig {
    pub hex: String,
}

/// Transaction output.
#[derive(Debug, Deserialize)]
pub struct RpcOutput {
    #[serde(deserialize_with = "de_number_as_string")]
    pub value: String,
    pub n: u32,
    #[serde(rename = "scriptPubKey")]
    pub script_pubkey: RpcScriptPubKey,
}

/// Output script.
#[derive(Debug, Deserialize)]
pub struct RpcScriptPubKey {
    pub hex: String,
    #[serde(rename = "type")]
    pub type_: String,
    #[serde(default)]
    pub address: Option<String>,
}

/// Simple HTTP JSON-RPC client for Bitcoin Core.
#[derive(Clone, Debug)]
pub struct BitcoinRpcClient {
    http: reqwest::Client,
    url: url::Url,
    auth: Option<(String, String)>,
    limiter: Option<Arc<DefaultDirectRateLimiter>>,
    metrics: Option<Arc<MetricsRegistry>>,
    provider: String,
    network: String,
}

impl BitcoinRpcClient {
    pub fn new(
        url: url::Url,
        auth: Option<(String, String)>,
        rate_limit: Option<NonZeroU32>,
        timeout: std::time::Duration,
        provider: String,
        network: String,
        meter: Option<&monitoring::telemetry::metrics::Meter>,
    ) -> Result<Self, reqwest::Error> {
        let http = reqwest::Client::builder().timeout(timeout).build()?;
        let limiter = rate_limit.map(|rl| {
            let quota = Quota::per_minute(rl);
            Arc::new(RateLimiter::direct(quota))
        });
        let metrics = meter.map(MetricsRegistry::new).map(Arc::new);
        Ok(Self {
            http,
            url,
            auth,
            limiter,
            metrics,
            provider,
            network,
        })
    }

    /// Execute a JSON-RPC call, applying rate limiting and recording metrics.
    async fn call_raw<T: for<'de> Deserialize<'de>>(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<T, crate::error::RpcError> {
        if let Some(limiter) = &self.limiter {
            limiter.until_ready().await;
        }

        let start = Instant::now();
        let result = self.send(method, params).await;
        let duration = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);

        if let Some(metrics) = &self.metrics {
            metrics.record_rpc_request(duration, &self.provider, &self.network, method);
            if result.is_err() {
                metrics.record_rpc_error(&self.provider, &self.network);
            }
        }

        result
    }

    /// Send a single JSON-RPC request and deserialise the result.
    async fn send<T: for<'de> Deserialize<'de>>(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<T, crate::error::RpcError> {
        let body = serde_json::json!({
            "jsonrpc": "1.0",
            "id": 1,
            "method": method,
            "params": params,
        });

        let mut req = self.http.post(self.url.clone()).json(&body);

        if let Some((user, pass)) = &self.auth {
            req = req.basic_auth(user, Some(pass));
        }

        let resp = req.send().await.map_err(crate::error::RpcError::Http)?;
        let json: JsonRpcResponse<T> = resp.json().await.map_err(crate::error::RpcError::Http)?;

        if let Some(err) = json.error {
            return Err(crate::error::RpcError::Rpc {
                code: err.code,
                message: err.message,
            });
        }

        json.result
            .ok_or(crate::error::RpcError::MissingField("result"))
    }

    /// Returns the height of the best block.
    pub async fn get_block_count(&self) -> Result<u64, crate::error::RpcError> {
        self.call_raw("getblockcount", serde_json::json!([])).await
    }

    /// Returns the block hash for the given height.
    pub async fn get_block_hash(&self, height: u64) -> Result<String, crate::error::RpcError> {
        self.call_raw("getblockhash", serde_json::json!([height]))
            .await
    }

    /// Returns full block data with verbosity=2 (full transaction objects).
    pub async fn get_block(&self, hash: &str) -> Result<RpcBlock, crate::error::RpcError> {
        self.call_raw("getblock", serde_json::json!([hash, 2]))
            .await
    }
}
