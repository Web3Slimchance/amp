//! Provider configuration management for dataset stores.
//!
//! This module provides functionality to manage external data source provider configurations
//! (such as EVM RPC endpoints, Firehose endpoints, etc.) that datasets connect to.
//! Provider configurations are defined as TOML files.
//!
//! ## Provider Configuration Structure
//!
//! Each provider configuration is a TOML file whose filename determines the provider name
//! (e.g., `my_provider.toml` → provider name `my_provider`). The file must define at least:
//! - `kind`: The type of provider (e.g., "evm-rpc", "firehose")
//! - `network`: The blockchain network (e.g., "mainnet", "goerli", "polygon")
//!
//! Additional fields depend on the provider type.
use std::{collections::BTreeMap, ops::Deref};

pub use amp_providers_common::{config::ProviderResolvedConfigRaw, provider_name::ProviderName};
use amp_providers_common::{
    config::{ConfigHeaderWithNetwork, ProviderConfigRaw, TryIntoConfig},
    network_id::NetworkId,
};
use monitoring::logging;
use object_store::ObjectStore;

mod client;
pub mod retryable;
mod store;

pub use amp_providers_evm_rpc::{
    kind::EvmRpcProviderKind,
    provider::{
        CreateEvmRpcClientError, EvmRpcAlloyProvider as EvmRpcProvider,
        create as create_evm_rpc_client,
    },
};

pub use self::{
    client::block_stream::{
        BlockStreamClient, CreateClientError, create as create_block_stream_client,
    },
    store::{ConfigDeleteError, ConfigStoreError, ProviderConfigsStore},
};

/// Manages provider configurations with caching
///
/// ## Object Store Agnostic Design
///
/// The store logic is agnostic to rate-limiting and location prefixing. API users must provide
/// the appropriate object store implementation to the struct constructor:
///
/// - **Rate Limiting**: Use `object_store::limit::LimitStore` to wrap the underlying store
/// - **Path Prefixing**: Use `object_store::prefix::PrefixStore` or equivalent for path isolation
/// - **Composition**: These can be combined as needed (e.g., `LimitStore<PrefixStore<LocalFileSystem>>`)
///
/// ## Caching
///
/// Caching is handled by the underlying [`ProviderConfigsStore`]. See its documentation for
/// details on the lazy-loaded, write-through caching strategy.
#[derive(Debug, Clone)]
pub struct ProvidersRegistry<S: ObjectStore = std::sync::Arc<dyn ObjectStore>> {
    store: ProviderConfigsStore<S>,
}

impl<S> ProvidersRegistry<S>
where
    S: ObjectStore + Clone,
{
    /// Creates a new registry wrapping the given provider configs store.
    pub fn new(store: ProviderConfigsStore<S>) -> Self {
        Self { store }
    }

    /// Get all provider configurations, using cache if available
    ///
    /// Returns a read guard that dereferences to the cached `BTreeMap<ProviderName, ProviderConfigRaw>`.
    /// This provides efficient access to all provider configurations without cloning.
    ///
    /// # Deadlock Warning
    ///
    /// The returned guard holds a read lock on the internal cache. Holding this guard
    /// for extended periods can cause deadlocks with operations that require write access
    /// (such as `register` and `delete`). Extract the needed data immediately and drop
    /// the guard as soon as possible.
    #[must_use]
    pub async fn get_all(
        &self,
    ) -> impl Deref<Target = BTreeMap<ProviderName, ProviderConfigRaw>> + '_ {
        self.store.get_all().await
    }

    /// Get a provider configuration by `name`, using cache if available. Returns None if not found.
    pub async fn get_by_name(&self, name: &str) -> Option<ProviderConfigRaw> {
        self.store.get(name).await
    }

    /// Register a provider configuration in both cache and store
    ///
    /// If a provider configuration with the same name already exists, it will be overwritten.
    pub async fn register(
        &self,
        name: ProviderName,
        config: ProviderConfigRaw,
    ) -> Result<(), RegisterError> {
        self.store.store(name, config).await.map_err(RegisterError)
    }

    /// Delete a provider configuration by name from both the store and cache
    ///
    /// This operation is idempotent: deleting a non-existent provider succeeds silently.
    ///
    /// # Cache Management
    /// - If file not found: removes stale cache entry and returns `Ok(())`
    /// - If other store errors: preserves cache (file may still exist) and propagates error
    /// - If deletion succeeds: removes from both store and cache
    pub async fn delete(&self, name: &str) -> Result<(), DeleteError> {
        self.store.delete(name).await.map_err(DeleteError)
    }

    /// Find all providers matching a kind and network, applying environment variable substitution.
    ///
    /// Returns all matching providers in deterministic order by provider name (lexicographic).
    /// This ordering is guaranteed by the underlying `BTreeMap` storage.
    /// Providers whose environment variable substitution fails are skipped with a warning.
    pub async fn find_providers(
        &self,
        kind: impl AsRef<str>,
        network: &NetworkId,
    ) -> Vec<(ProviderName, ProviderResolvedConfigRaw)> {
        let kind = kind.as_ref();

        // Collect matching (name, config) pairs while holding the read lock,
        // then drop the lock before performing env substitution.
        let candidates: Vec<_> = self
            .get_all()
            .await
            .iter()
            .filter_map(|(name, config)| {
                let header = match config.try_into_config::<ConfigHeaderWithNetwork>() {
                    Ok(h) => h,
                    Err(err) => {
                        tracing::warn!(
                            provider_name = %name,
                            error = %err,
                            error_source = logging::error_source(&err),
                            "failed to parse provider config header, skipping"
                        );
                        return None;
                    }
                };
                if header.kind == kind && header.network == network {
                    Some((name.clone(), config.clone()))
                } else {
                    None
                }
            })
            .collect();

        let mut providers = Vec::new();
        for (name, config) in candidates {
            match config.with_env_substitution() {
                Ok(resolved) => {
                    tracing::debug!(
                        provider_name = %name,
                        provider_kind = %kind,
                        provider_network = %network,
                        "resolved provider with environment substitution"
                    );
                    providers.push((name, resolved));
                }
                Err(err) => {
                    tracing::warn!(
                        provider_name = %name,
                        provider_kind = %kind,
                        provider_network = %network,
                        error = %err,
                        error_source = logging::error_source(&err),
                        "environment variable substitution failed for provider, skipping"
                    );
                }
            }
        }

        providers
    }
}

/// Error that can occur when registering a provider configuration.
#[derive(Debug, thiserror::Error)]
#[error("failed to store provider configuration")]
pub struct RegisterError(#[source] ConfigStoreError);

/// Error that can occur when deleting a provider configuration.
#[derive(Debug, thiserror::Error)]
#[error("Failed to delete provider configuration")]
pub struct DeleteError(#[source] pub ConfigDeleteError);

#[cfg(test)]
mod tests {
    mod cache;
    mod crud;
    mod it_find_providers;
}
