//! Bitcoin RPC provider types, configuration, and extraction client.

use amp_providers_common::provider_name::ProviderName;

use crate::{config::BitcoinRpcProviderConfig, error::ClientError};

pub mod client;
pub mod config;
pub mod error;
pub mod kind;
pub mod metrics;
pub mod rpc;
pub mod tables;

pub use self::client::Client;

/// Create a Bitcoin RPC block-streaming client from provider configuration.
pub async fn client(
    name: ProviderName,
    config: BitcoinRpcProviderConfig,
    meter: Option<&monitoring::telemetry::metrics::Meter>,
) -> Result<Client, ClientError> {
    let url = config.url.into_inner();
    let auth = config.auth.map(|a| (a.user, a.password.into_inner()));
    Client::new(
        url,
        config.network,
        name,
        auth,
        config.rate_limit_per_minute,
        config.concurrent_request_limit,
        config.confirmations,
        config.timeout,
        meter,
    )
}

#[cfg(test)]
mod tests {
    mod it_block_stream;
}
