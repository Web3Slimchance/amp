//! EthCall UDF cache for EVM RPC providers.
//!
//! This module provides the `EthCallUdfsCache` struct which manages creation and caching
//! of `eth_call` scalar UDFs keyed by network through the providers registry.

use std::sync::Arc;

use amp_providers_common::network_id::NetworkId;
use amp_providers_registry::{EvmRpcProviderKind, ProvidersRegistry};
use datafusion::{
    common::HashMap,
    logical_expr::{ScalarUDF, async_udf::AsyncScalarUDF},
};
use monitoring::telemetry::metrics::Meter;
use parking_lot::RwLock;

use super::{metrics::EthCallMetrics, udf::EthCall};

/// Manages creation and caching of `eth_call` scalar UDFs keyed by network.
///
/// Orchestrates UDF creation through the providers registry with in-memory caching.
#[derive(Clone)]
pub struct EthCallUdfsCache {
    registry: ProvidersRegistry,
    cache: Arc<RwLock<HashMap<NetworkId, ScalarUDF>>>,
    metrics: Option<Arc<EthCallMetrics>>,
}

impl std::fmt::Debug for EthCallUdfsCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EthCallUdfsCache").finish_non_exhaustive()
    }
}

impl EthCallUdfsCache {
    /// Creates a new EthCall UDFs cache.
    ///
    /// If a `meter` is provided, metrics will be recorded for every eth_call invocation.
    pub fn new(registry: ProvidersRegistry, meter: Option<&Meter>) -> Self {
        let metrics = meter.map(|m| Arc::new(EthCallMetrics::new(m)));
        Self {
            registry,
            cache: Default::default(),
            metrics,
        }
    }

    /// Returns a reference to the underlying providers registry.
    pub fn providers_registry(&self) -> &ProvidersRegistry {
        &self.registry
    }

    /// Returns cached eth_call scalar UDF for a network, creating one if not cached.
    ///
    /// The `udf_name` is the name DataFusion's planner uses to look up the function
    /// (e.g., `rpc.mainnet.eth_call`). The caller controls the naming convention.
    pub async fn eth_call_for_network(
        &self,
        udf_name: &str,
        network: &NetworkId,
    ) -> Result<ScalarUDF, EthCallForNetworkError> {
        // Check cache first.
        if let Some(udf) = self.cache.read().get(network) {
            return Ok(udf.clone());
        }

        // TODO: Always selects the first provider. Rotation across retries would
        // require rethinking the cache key (currently NetworkId only) since the
        // cached UDF holds a reference to a specific provider.
        let Some((provider_name, config)) = self
            .registry
            .find_providers(EvmRpcProviderKind, network)
            .await
            .into_iter()
            .next()
        else {
            tracing::warn!(
                provider_network = %network,
                "no EVM RPC provider found for network"
            );
            let err = EthCallForNetworkError::ProviderNotFound {
                network: network.clone(),
            };
            if let Some(metrics) = &self.metrics {
                metrics.record_network_error(network.as_str(), &err);
            }
            return Err(err);
        };
        let provider =
            amp_providers_registry::create_evm_rpc_client(provider_name.to_string(), config)
                .await
                .map_err(EthCallForNetworkError::ProviderCreation)?;

        let udf = AsyncScalarUDF::new(Arc::new(EthCall::new(
            udf_name.to_string(),
            provider,
            provider_name,
            network.clone(),
            self.metrics.clone(),
        )))
        .into_scalar_udf();

        self.cache.write().insert(network.clone(), udf.clone());

        Ok(udf)
    }
}

/// Errors that occur when creating eth_call UDFs for a network.
#[derive(Debug, thiserror::Error)]
pub enum EthCallForNetworkError {
    /// No provider configuration found for the network.
    #[error("No EVM RPC provider found for network '{network}'")]
    ProviderNotFound { network: NetworkId },

    /// Failed to create the EVM RPC provider.
    #[error("Failed to create EVM RPC provider")]
    ProviderCreation(#[source] amp_providers_registry::CreateEvmRpcClientError),
}
