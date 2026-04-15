use std::{num::NonZeroU32, time::Duration};

use amp_providers_common::{network_id::NetworkId, redacted::Redacted};
use serde_with::{DurationSeconds, serde_as};
use url::Url;

use crate::kind::BitcoinRpcProviderKind;

/// HTTP Basic Auth credentials for the Bitcoin RPC endpoint.
#[derive(Debug, Clone, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct BasicAuth {
    /// RPC username.
    pub user: String,
    /// RPC password.
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub password: Redacted<String>,
}

/// Bitcoin RPC provider configuration for parsing TOML config.
#[serde_as]
#[derive(Debug, Clone, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct BitcoinRpcProviderConfig {
    /// The provider kind, must be `"bitcoin-rpc"`.
    pub kind: BitcoinRpcProviderKind,

    /// The network this provider serves (e.g., "mainnet", "testnet", "signet").
    pub network: NetworkId,

    /// The URL of the Bitcoin RPC endpoint (HTTP or HTTPS).
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub url: Redacted<Url>,

    /// Optional HTTP Basic Auth credentials.
    #[serde(default)]
    pub auth: Option<BasicAuth>,

    /// Optional rate limit for requests per minute.
    #[cfg_attr(feature = "schemars", schemars(default))]
    pub rate_limit_per_minute: Option<NonZeroU32>,

    /// Optional limit on the number of concurrent requests.
    pub concurrent_request_limit: Option<u16>,

    /// Number of confirmations required for "finalized" blocks.
    /// Defaults to 6.
    #[serde(default = "default_confirmations")]
    pub confirmations: u32,

    /// Request timeout in seconds.
    #[serde_as(as = "DurationSeconds<u64>")]
    #[serde(default = "default_timeout", rename = "timeout_secs")]
    #[cfg_attr(feature = "schemars", schemars(with = "u64"))]
    pub timeout: Duration,
}

fn default_timeout() -> Duration {
    Duration::from_secs(30)
}

fn default_confirmations() -> u32 {
    6
}
