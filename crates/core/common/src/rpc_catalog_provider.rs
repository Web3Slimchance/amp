//! Catalog provider for RPC functions.
//!
//! Resolves `rpc.<network>.eth_call(...)` function references by looking up
//! providers directly from the [`ProvidersRegistry`] without requiring a dataset.
//!
//! SQL uses `snake_case` network names (e.g., `base_sepolia`) while provider
//! configs use `kebab-case` (e.g., `base-sepolia`). The [`SqlNetworkId`] type
//! handles this conversion.

use std::sync::Arc;

use amp_providers_common::network_id::NetworkId;
use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datasets_derived::func_name::ETH_CALL_FUNCTION_NAME;

use crate::{
    func_catalog::{
        function_provider::{FunctionProvider, ScalarFunctionProvider},
        schema_provider::AsyncSchemaProvider as FuncAsyncSchemaProvider,
    },
    udfs::eth_call::EthCallUdfsCache,
};

/// The catalog name used to register the RPC function provider.
pub const RPC_CATALOG_NAME: &str = "rpc";

/// Catalog provider for RPC functions.
///
/// Resolves network names as schemas (e.g., `rpc.mainnet`) and provides
/// `eth_call` as the only function within each network schema.
#[derive(Clone)]
pub struct RpcCatalogProvider {
    cache: EthCallUdfsCache,
}

impl RpcCatalogProvider {
    /// Creates a new RPC catalog provider.
    pub fn new(cache: EthCallUdfsCache) -> Self {
        Self { cache }
    }
}

impl std::fmt::Debug for RpcCatalogProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcCatalogProvider").finish_non_exhaustive()
    }
}

#[async_trait]
impl crate::func_catalog::catalog_provider::AsyncCatalogProvider for RpcCatalogProvider {
    async fn schema(
        &self,
        name: &str,
    ) -> Result<Option<Arc<dyn FuncAsyncSchemaProvider>>, DataFusionError> {
        let sql_network: SqlNetworkId = name
            .parse()
            .map_err(|err| DataFusionError::Plan(format!("invalid network '{name}': {err}")))?;
        Ok(Some(Arc::new(RpcSchemaProvider {
            sql_network,
            cache: self.cache.clone(),
        })))
    }
}

/// Schema provider for a single network within the `rpc` catalog.
///
/// Resolves the `eth_call` function by creating an EVM RPC client from
/// the providers registry for the given network.
#[derive(Debug)]
struct RpcSchemaProvider {
    sql_network: SqlNetworkId,
    cache: EthCallUdfsCache,
}

#[async_trait]
impl FuncAsyncSchemaProvider for RpcSchemaProvider {
    async fn function(
        &self,
        name: &str,
    ) -> Result<Option<Arc<dyn FunctionProvider>>, DataFusionError> {
        if name != ETH_CALL_FUNCTION_NAME {
            return Ok(None);
        }

        let udf_name = self.eth_call_udf_name();
        let network: NetworkId = self.sql_network.clone().into();
        let udf = self
            .cache
            .eth_call_for_network(&udf_name, &network)
            .await
            .map_err(|err| DataFusionError::External(Box::new(err)))?;

        Ok(Some(Arc::new(ScalarFunctionProvider::from(Arc::new(udf)))))
    }
}

impl RpcSchemaProvider {
    /// Returns the UDF name for DataFusion's flat function registry lookup.
    ///
    /// Uses the SQL-facing `snake_case` network name so the UDF name matches
    /// what DataFusion's planner constructs (e.g., `rpc.base_sepolia.eth_call`).
    fn eth_call_udf_name(&self) -> String {
        format!(
            "{}.{}.{}",
            RPC_CATALOG_NAME, self.sql_network, ETH_CALL_FUNCTION_NAME
        )
    }
}

/// A SQL-compatible network identifier using `snake_case` format.
///
/// SQL identifiers cannot contain hyphens, so network names in SQL use
/// `snake_case` (e.g., `base_sepolia`). This type validates the `snake_case`
/// format and converts to [`NetworkId`] (`kebab-case`) for provider lookups.
#[derive(Debug, Clone)]
pub struct SqlNetworkId(String);

impl std::fmt::Display for SqlNetworkId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::str::FromStr for SqlNetworkId {
    type Err = InvalidSqlNetworkIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(InvalidSqlNetworkIdError::Empty);
        }
        // SQL network names must be snake_case: lowercase alphanumeric + underscores
        if !s
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        {
            return Err(InvalidSqlNetworkIdError::InvalidFormat(s.to_string()));
        }
        Ok(Self(s.to_string()))
    }
}

impl From<SqlNetworkId> for NetworkId {
    fn from(sql: SqlNetworkId) -> Self {
        let kebab = sql.0.replace('_', "-");
        // Safety: we validated non-empty in FromStr, and replacing underscores
        // with hyphens preserves non-emptiness.
        NetworkId::new_unchecked(kebab)
    }
}

/// Error for invalid SQL network identifiers.
#[derive(Debug, thiserror::Error)]
pub enum InvalidSqlNetworkIdError {
    /// Network identifier is empty.
    #[error("SQL network identifier cannot be empty")]
    Empty,
    /// Network identifier contains invalid characters (must be snake_case).
    #[error(
        "SQL network identifier must be snake_case (lowercase alphanumeric and underscores): '{0}'"
    )]
    InvalidFormat(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_network_id_simple_name_converts_to_network_id() {
        //* Given
        let sql_id: SqlNetworkId = "mainnet".parse().unwrap();

        //* When
        let network: NetworkId = sql_id.into();

        //* Then
        assert_eq!(network.as_str(), "mainnet");
    }

    #[test]
    fn sql_network_id_snake_case_converts_underscores_to_hyphens() {
        //* Given
        let sql_id: SqlNetworkId = "base_mainnet".parse().unwrap();

        //* When
        let network: NetworkId = sql_id.into();

        //* Then
        assert_eq!(network.as_str(), "base-mainnet");
    }

    #[test]
    fn sql_network_id_multiple_underscores_all_convert() {
        //* Given
        let sql_id: SqlNetworkId = "arbitrum_one_nova".parse().unwrap();

        //* When
        let network: NetworkId = sql_id.into();

        //* Then
        assert_eq!(network.as_str(), "arbitrum-one-nova");
    }

    #[test]
    fn sql_network_id_empty_fails() {
        //* Given/When
        let result = "".parse::<SqlNetworkId>();

        //* Then
        assert!(result.is_err());
    }

    #[test]
    fn sql_network_id_with_hyphens_fails() {
        //* Given/When
        let result = "base-mainnet".parse::<SqlNetworkId>();

        //* Then
        assert!(result.is_err());
    }

    #[test]
    fn sql_network_id_with_uppercase_fails() {
        //* Given/When
        let result = "Base_Mainnet".parse::<SqlNetworkId>();

        //* Then
        assert!(result.is_err());
    }

    #[test]
    fn sql_network_id_display_shows_snake_case() {
        //* Given
        let sql_id: SqlNetworkId = "base_mainnet".parse().unwrap();

        //* When
        let display = sql_id.to_string();

        //* Then
        assert_eq!(display, "base_mainnet");
    }
}
