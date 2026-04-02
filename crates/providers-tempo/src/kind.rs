//! Tempo provider kind type and parsing utilities.

use amp_providers_common::kind::ProviderKindStr;

/// The canonical string identifier for Tempo providers.
const PROVIDER_KIND: &str = "tempo";

/// Type-safe representation of the Tempo provider kind.
///
/// This zero-sized type represents the "tempo" provider kind, which interacts
/// with Tempo-compatible JSON-RPC endpoints for blockchain data extraction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(inline))]
#[cfg_attr(
    feature = "schemars",
    schemars(schema_with = "tempo_provider_kind_schema")
)]
pub struct TempoProviderKind;

impl TempoProviderKind {
    /// Returns the canonical string identifier for this provider kind.
    #[inline]
    pub const fn as_str(self) -> &'static str {
        PROVIDER_KIND
    }
}

impl AsRef<str> for TempoProviderKind {
    fn as_ref(&self) -> &str {
        PROVIDER_KIND
    }
}

impl From<TempoProviderKind> for ProviderKindStr {
    fn from(value: TempoProviderKind) -> Self {
        // SAFETY: The constant PROVIDER_KIND is "tempo", which is non-empty
        ProviderKindStr::new_unchecked(value.to_string())
    }
}

#[cfg(feature = "schemars")]
fn tempo_provider_kind_schema(_gen: &mut schemars::SchemaGenerator) -> schemars::Schema {
    let schema_obj = serde_json::json!({
        "const": PROVIDER_KIND
    });
    serde_json::from_value(schema_obj).unwrap()
}

impl std::str::FromStr for TempoProviderKind {
    type Err = TempoProviderKindError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s != PROVIDER_KIND {
            return Err(TempoProviderKindError(s.to_string()));
        }

        Ok(TempoProviderKind)
    }
}

impl std::fmt::Display for TempoProviderKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        PROVIDER_KIND.fmt(f)
    }
}

impl serde::Serialize for TempoProviderKind {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(PROVIDER_KIND)
    }
}

impl<'de> serde::Deserialize<'de> for TempoProviderKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

impl PartialEq<str> for TempoProviderKind {
    fn eq(&self, other: &str) -> bool {
        PROVIDER_KIND == other
    }
}

impl PartialEq<TempoProviderKind> for str {
    fn eq(&self, _other: &TempoProviderKind) -> bool {
        self == PROVIDER_KIND
    }
}

impl PartialEq<&str> for TempoProviderKind {
    fn eq(&self, other: &&str) -> bool {
        PROVIDER_KIND == *other
    }
}

impl PartialEq<TempoProviderKind> for &str {
    fn eq(&self, _other: &TempoProviderKind) -> bool {
        *self == PROVIDER_KIND
    }
}

impl PartialEq<String> for TempoProviderKind {
    fn eq(&self, other: &String) -> bool {
        PROVIDER_KIND == other.as_str()
    }
}

impl PartialEq<TempoProviderKind> for String {
    fn eq(&self, _other: &TempoProviderKind) -> bool {
        self.as_str() == PROVIDER_KIND
    }
}

impl PartialEq<ProviderKindStr> for TempoProviderKind {
    fn eq(&self, other: &ProviderKindStr) -> bool {
        PROVIDER_KIND == other.as_str()
    }
}

/// Error returned when parsing an invalid Tempo RPC provider kind string.
#[derive(Debug, thiserror::Error)]
#[error("invalid provider kind: {}, expected: {}", .0, PROVIDER_KIND)]
pub struct TempoProviderKindError(String);
