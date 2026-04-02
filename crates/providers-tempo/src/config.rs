use std::{num::NonZeroU32, time::Duration};

use amp_providers_common::{network_id::NetworkId, redacted::Redacted};
use headers::{HeaderName, HeaderValue};
use serde_with::{DurationSeconds, serde_as};
use url::Url;

use crate::kind::TempoProviderKind;

/// Tempo RPC provider configuration for parsing TOML config.
#[serde_as]
#[derive(Debug, Clone, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct TempoProviderConfig {
    /// The provider kind, must be `"tempo"`.
    pub kind: TempoProviderKind,

    /// The network this provider serves.
    pub network: NetworkId,

    /// The URL of the Tempo RPC endpoint (HTTP, HTTPS, WebSocket, or IPC).
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub url: Redacted<Url>,

    /// Custom header name for authentication.
    #[serde(default)]
    #[cfg_attr(feature = "schemars", schemars(with = "Option<String>"))]
    pub auth_header: Option<AuthHeaderName>,

    /// Authentication token for RPC requests.
    #[serde(default)]
    #[cfg_attr(feature = "schemars", schemars(with = "Option<String>"))]
    pub auth_token: Option<AuthToken>,

    /// Optional rate limit for requests per minute.
    #[cfg_attr(feature = "schemars", schemars(default))]
    pub rate_limit_per_minute: Option<NonZeroU32>,

    /// Optional limit on the number of concurrent requests.
    #[cfg_attr(
        feature = "schemars",
        schemars(default = "default_concurrent_request_limit")
    )]
    pub concurrent_request_limit: Option<u16>,

    /// Maximum number of JSON-RPC requests to batch together.
    ///
    /// Set to 0 to disable batching.
    #[serde(default)]
    pub rpc_batch_size: usize,

    /// Whether to use `eth_getTransactionReceipt` to fetch receipts for each transaction
    /// or `eth_getBlockReceipts` to fetch all receipts for a block in one call.
    #[serde(default)]
    pub fetch_receipts_per_tx: bool,

    /// Request timeout in seconds.
    #[serde_as(as = "DurationSeconds<u64>")]
    #[serde(default = "default_timeout", rename = "timeout_secs")]
    #[cfg_attr(feature = "schemars", schemars(with = "u64"))]
    pub timeout: Duration,
}

/// Validated HTTP header name for custom authentication.
#[derive(Debug, Clone)]
pub struct AuthHeaderName(String);

impl AuthHeaderName {
    /// Consumes the wrapper and returns the inner string.
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl<'de> serde::Deserialize<'de> for AuthHeaderName {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        HeaderName::try_from(&s).map_err(serde::de::Error::custom)?;
        Ok(AuthHeaderName(s))
    }
}

/// Validated authentication token for RPC requests.
#[derive(Debug, Clone)]
pub struct AuthToken(Redacted<String>);

impl AuthToken {
    /// Consumes the wrapper and returns the inner string.
    pub fn into_inner(self) -> String {
        self.0.into_inner()
    }
}

impl<'de> serde::Deserialize<'de> for AuthToken {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        HeaderValue::try_from(&s).map_err(serde::de::Error::custom)?;
        Ok(AuthToken(Redacted::from(s)))
    }
}

fn default_timeout() -> Duration {
    Duration::from_secs(30)
}

#[cfg(feature = "schemars")]
fn default_concurrent_request_limit() -> Option<u16> {
    Some(1024)
}
