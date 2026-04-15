//! Bitcoin RPC provider kind type and parsing utilities.

use amp_providers_common::kind::ProviderKindStr;

/// The canonical string identifier for Bitcoin RPC providers.
const PROVIDER_KIND: &str = "bitcoin-rpc";

/// Type-safe representation of the Bitcoin RPC provider kind.
///
/// This zero-sized type represents the "bitcoin-rpc" provider kind, which interacts
/// with Bitcoin Core JSON-RPC endpoints for blockchain data extraction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(inline))]
#[cfg_attr(
    feature = "schemars",
    schemars(schema_with = "bitcoin_rpc_provider_kind_schema")
)]
pub struct BitcoinRpcProviderKind;

impl BitcoinRpcProviderKind {
    /// Returns the canonical string identifier for this provider kind.
    #[inline]
    pub const fn as_str(self) -> &'static str {
        PROVIDER_KIND
    }
}

impl AsRef<str> for BitcoinRpcProviderKind {
    fn as_ref(&self) -> &str {
        PROVIDER_KIND
    }
}

impl From<BitcoinRpcProviderKind> for ProviderKindStr {
    fn from(value: BitcoinRpcProviderKind) -> Self {
        // SAFETY: The constant PROVIDER_KIND is "bitcoin-rpc", which is non-empty
        ProviderKindStr::new_unchecked(value.to_string())
    }
}

#[cfg(feature = "schemars")]
fn bitcoin_rpc_provider_kind_schema(_gen: &mut schemars::SchemaGenerator) -> schemars::Schema {
    let schema_obj = serde_json::json!({
        "const": PROVIDER_KIND
    });
    serde_json::from_value(schema_obj).unwrap()
}

impl std::str::FromStr for BitcoinRpcProviderKind {
    type Err = BitcoinRpcProviderKindError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s != PROVIDER_KIND {
            return Err(BitcoinRpcProviderKindError(s.to_string()));
        }

        Ok(BitcoinRpcProviderKind)
    }
}

impl std::fmt::Display for BitcoinRpcProviderKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        PROVIDER_KIND.fmt(f)
    }
}

impl serde::Serialize for BitcoinRpcProviderKind {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(PROVIDER_KIND)
    }
}

impl<'de> serde::Deserialize<'de> for BitcoinRpcProviderKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

impl PartialEq<str> for BitcoinRpcProviderKind {
    fn eq(&self, other: &str) -> bool {
        PROVIDER_KIND == other
    }
}

impl PartialEq<BitcoinRpcProviderKind> for str {
    fn eq(&self, _other: &BitcoinRpcProviderKind) -> bool {
        self == PROVIDER_KIND
    }
}

impl PartialEq<&str> for BitcoinRpcProviderKind {
    fn eq(&self, other: &&str) -> bool {
        PROVIDER_KIND == *other
    }
}

impl PartialEq<BitcoinRpcProviderKind> for &str {
    fn eq(&self, _other: &BitcoinRpcProviderKind) -> bool {
        *self == PROVIDER_KIND
    }
}

impl PartialEq<String> for BitcoinRpcProviderKind {
    fn eq(&self, other: &String) -> bool {
        PROVIDER_KIND == other.as_str()
    }
}

impl PartialEq<BitcoinRpcProviderKind> for String {
    fn eq(&self, _other: &BitcoinRpcProviderKind) -> bool {
        self.as_str() == PROVIDER_KIND
    }
}

impl PartialEq<ProviderKindStr> for BitcoinRpcProviderKind {
    fn eq(&self, other: &ProviderKindStr) -> bool {
        PROVIDER_KIND == other.as_str()
    }
}

/// Error returned when parsing an invalid Bitcoin RPC provider kind string.
#[derive(Debug, thiserror::Error)]
#[error("invalid provider kind: {}, expected: {}", .0, PROVIDER_KIND)]
pub struct BitcoinRpcProviderKindError(String);
