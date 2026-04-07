use clap::{Args, Parser, Subcommand};
use datasets_common::{partial_reference::PartialReference, reference::Reference};

use crate::sql;

#[derive(Parser, Debug)]
#[command(name = "ampsync")]
#[command(version)]
#[command(about = "PostgreSQL synchronization tool for Amp datasets")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Synchronize dataset to PostgreSQL
    Sync(SyncConfig),
}

#[derive(Args, Debug, Clone)]
pub struct SyncConfig {
    /// Dataset reference to sync
    ///
    /// Supports flexible formats:
    ///   - Full: namespace/name@revision (e.g., _/eth_rpc@1.0.0)
    ///   - No namespace: name@revision (e.g., eth_rpc@1.0.0, defaults to _ namespace)
    ///   - No revision: namespace/name (e.g., _/eth_rpc, defaults to latest)
    ///   - Minimal: name (e.g., eth_rpc, defaults to _/eth_rpc@latest)
    ///
    /// Revision can be:
    ///   - Semantic version (e.g., 1.0.0)
    ///   - Hash (64-character hex string)
    ///   - 'latest' (resolves to latest version)
    ///   - 'dev' (resolves to development version)
    ///
    /// Can also be set via DATASET environment variable
    #[arg(short = 'd', long, env = "DATASET", required = true)]
    pub dataset: PartialReference,

    /// Tables to synchronize (required, comma-separated)
    ///
    /// Specify which tables from the dataset to sync to PostgreSQL.
    /// Example: --tables logs,blocks,transactions
    ///
    /// Can also be set via TABLES environment variable
    #[arg(
        short = 't',
        long,
        env = "TABLES",
        required = true,
        value_delimiter = ',',
        num_args = 1..
    )]
    pub tables: Vec<String>,

    /// Suffix to append to all destination table names
    ///
    /// When set, all PostgreSQL table names will have this suffix appended.
    /// Example: --table-suffix green
    ///   With TABLES=usdc_transfers → PostgreSQL table: usdc_transfers_green
    ///
    /// Can also be set via TABLE_SUFFIX environment variable
    #[arg(long, env = "TABLE_SUFFIX")]
    pub table_suffix: Option<String>,

    /// Append the dataset revision to destination table names
    ///
    /// When enabled, the sanitized dataset revision is appended to table names.
    /// Example: dataset @1.0.0, TABLES=usdc_transfers → usdc_transfers_v1_0_0
    ///
    /// When combined with --table-suffix, version comes first:
    ///   usdc_transfers_v1_0_0_green
    ///
    /// Can also be set via TABLE_VERSION environment variable
    #[arg(long, env = "TABLE_VERSION", default_value_t = false)]
    pub table_version: bool,

    /// PostgreSQL connection URL (required)
    ///
    /// Format: postgresql://[user]:[password]@[host]:[port]/[database]
    /// Can also be set via DATABASE_URL environment variable
    #[arg(long, env = "DATABASE_URL", required = true)]
    pub database_url: String,

    /// Amp Arrow Flight server address (default: http://localhost:1602)
    ///
    /// Can also be set via AMP_FLIGHT_ADDR environment variable
    #[arg(long, env = "AMP_FLIGHT_ADDR", default_value = "http://localhost:1602")]
    pub amp_flight_addr: String,

    /// Max gRPC decode size in MiB for Arrow Flight responses (default: 32, valid range: 1-512)
    ///
    /// Increase this when syncing high-volume tables that emit larger frames.
    /// Can also be set via AMPSYNC_GRPC_MAX_DECODE_MB environment variable.
    #[arg(
        long,
        env = "AMPSYNC_GRPC_MAX_DECODE_MB",
        default_value_t = 32,
        value_parser = clap::value_parser!(u32).range(1..=512)
    )]
    pub grpc_max_decode_mb: u32,

    /// Maximum database connections (default: 10, valid range: 1-1000)
    ///
    /// Can also be set via MAX_DB_CONNECTIONS environment variable
    #[arg(long, env = "MAX_DB_CONNECTIONS", default_value_t = 10, value_parser = clap::value_parser!(u32).range(1..=1000))]
    pub max_db_connections: u32,

    /// Retention window in blocks for watermark buffer (default: 128, minimum: 64)
    ///
    /// Can also be set via RETENTION_BLOCKS environment variable
    #[arg(long, env = "RETENTION_BLOCKS", default_value_t = 128, value_parser = clap::value_parser!(u64).range(64..))]
    pub retention_blocks: u64,

    /// Authentication token for Arrow Flight
    ///
    /// Bearer token for authenticating requests to the Arrow Flight server (for data streaming).
    ///
    /// The token will be sent as an Authorization header: `Authorization: Bearer <token>`
    ///
    /// Can also be set via AMP_AUTH_TOKEN environment variable
    #[arg(long, env = "AMP_AUTH_TOKEN")]
    pub auth_token: Option<String>,

    /// Health check server port (optional)
    ///
    /// When provided, starts an HTTP server on 0.0.0.0 that exposes a /healthz
    /// endpoint.
    ///
    /// Example: --health-port 8080
    ///
    /// Can also be set via HEALTH_PORT environment variable
    #[arg(long, env = "HEALTH_PORT")]
    pub health_port: Option<u16>,

    /// Strict health check mode (optional)
    ///
    /// When enabled, /healthz returns 503 Service Unavailable if any streaming
    /// task has exhausted its retry attempts and stopped.
    ///
    /// Default behavior (when not specified):
    ///   - Single table: defaults to true (503 if task dies)
    ///   - Multiple tables: defaults to false (200 even if some tasks die)
    ///
    /// Explicit values:
    ///   --strict-health=true   Always return 503 if any task is dead
    ///   --strict-health=false  Always return 200 regardless of task state
    ///
    /// Can also be set via STRICT_HEALTH environment variable
    #[arg(long, env = "STRICT_HEALTH")]
    pub strict_health: Option<bool>,
}

/// Maps a source table name (in the dataset) to a destination table name (in PostgreSQL).
#[derive(Debug, Clone)]
pub struct TableMapping {
    /// Table name in the dataset (used for streaming queries).
    pub source: String,
    /// Table name in PostgreSQL (used for create/insert/delete).
    pub destination: String,
}

/// Errors from building table mappings.
#[derive(Debug, thiserror::Error)]
pub enum TableMappingError {
    #[error("Source table name '{source}' is not a valid identifier")]
    InvalidSourceTable {
        source: String,
        #[source]
        source_err: sql::ValidateIdentifierError,
    },

    #[error(
        "Destination table name '{destination}' (derived from source '{source}') is not a valid identifier"
    )]
    InvalidDestinationTable {
        source: String,
        destination: String,
        #[source]
        source_err: sql::ValidateIdentifierError,
    },

    #[error("Table suffix '{suffix}' is not a valid identifier fragment")]
    InvalidSuffix {
        suffix: String,
        #[source]
        source: sql::ValidateIdentifierError,
    },

    #[error(
        "Duplicate destination table '{destination}' (from source tables '{first}' and '{second}')"
    )]
    DuplicateDestination {
        destination: String,
        first: String,
        second: String,
    },
}

/// Sanitize a dataset revision string for use in a table name.
///
/// Replaces non-alphanumeric characters with underscores and prepends `v`.
/// Example: `1.0.0` → `v1_0_0`, `abc123...` → `vabc123...`
///
/// Note: different revisions may produce the same sanitized output
/// (e.g., `1.0.0` and `1-0-0` both become `v1_0_0`). This is acceptable
/// because a single ampsync instance targets one dataset revision.
fn sanitize_revision(revision: &str) -> String {
    let sanitized: String = revision
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();
    format!("v{}", sanitized)
}

/// Build destination table name from source name, optional version, and optional suffix.
fn build_destination_name(
    source: &str,
    version_segment: Option<&str>,
    suffix: Option<&str>,
) -> String {
    let mut name = source.to_string();
    if let Some(ver) = version_segment {
        name = format!("{}_{}", name, ver);
    }
    if let Some(sfx) = suffix {
        name = format!("{}_{}", name, sfx);
    }
    name
}

/// Build table mappings from config and resolved dataset reference.
///
/// Validates all source and destination table names. When `table_version` is enabled,
/// the sanitized dataset revision is appended. When `table_suffix` is set, it is
/// appended after the version segment.
///
/// # Composition order
///
/// `{source}_v{revision}_{suffix}`
pub fn build_table_mappings(
    tables: &[String],
    dataset: &Reference,
    table_version: bool,
    table_suffix: Option<&str>,
) -> Result<Vec<TableMapping>, TableMappingError> {
    // Normalize empty suffix to None (e.g., TABLE_SUFFIX="" in env)
    let table_suffix = table_suffix.filter(|s| !s.is_empty());

    // Validate suffix as an identifier fragment (must be a valid identifier on its own)
    if let Some(suffix) = table_suffix {
        sql::validate_identifier(suffix).map_err(|err| TableMappingError::InvalidSuffix {
            suffix: suffix.to_string(),
            source: err,
        })?;
    }

    let version_segment = if table_version {
        Some(sanitize_revision(&dataset.revision().to_string()))
    } else {
        None
    };

    let mappings: Vec<TableMapping> = tables
        .iter()
        .map(|source| {
            // Validate source table name
            sql::validate_identifier(source).map_err(|err| {
                TableMappingError::InvalidSourceTable {
                    source: source.clone(),
                    source_err: err,
                }
            })?;

            let destination =
                build_destination_name(source, version_segment.as_deref(), table_suffix);

            // Validate destination table name (it could exceed length limits)
            sql::validate_identifier(&destination).map_err(|err| {
                TableMappingError::InvalidDestinationTable {
                    source: source.clone(),
                    destination: destination.clone(),
                    source_err: err,
                }
            })?;

            Ok(TableMapping {
                source: source.clone(),
                destination,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    // Check for duplicate destination table names
    let mut seen = std::collections::HashMap::with_capacity(mappings.len());
    for mapping in &mappings {
        if let Some(first_source) = seen.insert(&mapping.destination, &mapping.source) {
            return Err(TableMappingError::DuplicateDestination {
                destination: mapping.destination.clone(),
                first: first_source.clone(),
                second: mapping.source.clone(),
            });
        }
    }

    Ok(mappings)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to create a test Reference.
    fn test_reference(revision: &str) -> Reference {
        format!("_/test_dataset@{}", revision).parse().unwrap()
    }

    #[test]
    fn test_build_table_mappings_no_options() {
        let tables = vec!["transfers".to_string(), "blocks".to_string()];
        let dataset = test_reference("1.0.0");
        let mappings = build_table_mappings(&tables, &dataset, false, None).unwrap();

        assert_eq!(mappings.len(), 2);
        assert_eq!(mappings[0].source, "transfers");
        assert_eq!(mappings[0].destination, "transfers");
        assert_eq!(mappings[1].source, "blocks");
        assert_eq!(mappings[1].destination, "blocks");
    }

    #[test]
    fn test_build_table_mappings_with_version() {
        let tables = vec!["transfers".to_string()];
        let dataset = test_reference("1.0.0");
        let mappings = build_table_mappings(&tables, &dataset, true, None).unwrap();

        assert_eq!(mappings[0].source, "transfers");
        assert_eq!(mappings[0].destination, "transfers_v1_0_0");
    }

    #[test]
    fn test_build_table_mappings_with_suffix() {
        let tables = vec!["transfers".to_string()];
        let dataset = test_reference("1.0.0");
        let mappings = build_table_mappings(&tables, &dataset, false, Some("green")).unwrap();

        assert_eq!(mappings[0].source, "transfers");
        assert_eq!(mappings[0].destination, "transfers_green");
    }

    #[test]
    fn test_build_table_mappings_with_version_and_suffix() {
        let tables = vec!["transfers".to_string()];
        let dataset = test_reference("1.0.0");
        let mappings = build_table_mappings(&tables, &dataset, true, Some("green")).unwrap();

        assert_eq!(mappings[0].source, "transfers");
        assert_eq!(mappings[0].destination, "transfers_v1_0_0_green");
    }

    #[test]
    fn test_build_table_mappings_empty_suffix_treated_as_none() {
        let tables = vec!["transfers".to_string()];
        let dataset = test_reference("1.0.0");
        let mappings = build_table_mappings(&tables, &dataset, false, Some("")).unwrap();

        assert_eq!(mappings[0].source, "transfers");
        assert_eq!(mappings[0].destination, "transfers");
    }

    #[test]
    fn test_build_table_mappings_invalid_source() {
        let tables = vec!["bad-table".to_string()];
        let dataset = test_reference("1.0.0");
        let err = build_table_mappings(&tables, &dataset, false, None).unwrap_err();

        assert!(matches!(err, TableMappingError::InvalidSourceTable { .. }));
    }

    #[test]
    fn test_build_table_mappings_invalid_suffix() {
        let tables = vec!["transfers".to_string()];
        let dataset = test_reference("1.0.0");
        let err = build_table_mappings(&tables, &dataset, false, Some("bad-suffix")).unwrap_err();

        assert!(matches!(err, TableMappingError::InvalidSuffix { .. }));
    }

    #[test]
    fn test_build_table_mappings_destination_too_long() {
        // Source is valid but destination with version+suffix exceeds 63 bytes
        let tables = vec!["a".repeat(50)];
        let tables: Vec<String> = tables.into_iter().collect();
        let dataset = test_reference("1.0.0");
        let err =
            build_table_mappings(&tables, &dataset, true, Some("long_suffix_here")).unwrap_err();

        assert!(matches!(
            err,
            TableMappingError::InvalidDestinationTable { .. }
        ));
    }

    #[test]
    fn test_build_table_mappings_duplicate_destination() {
        // Two different source tables that map to the same destination
        let tables = vec!["transfers".to_string(), "transfers".to_string()];
        let dataset = test_reference("1.0.0");
        let err = build_table_mappings(&tables, &dataset, false, None).unwrap_err();

        assert!(matches!(
            err,
            TableMappingError::DuplicateDestination { .. }
        ));
    }

    #[test]
    fn test_sanitize_revision_semver() {
        assert_eq!(sanitize_revision("1.0.0"), "v1_0_0");
        assert_eq!(sanitize_revision("12.3.45"), "v12_3_45");
    }

    #[test]
    fn test_sanitize_revision_hash() {
        let hash = "abc123def456";
        assert_eq!(sanitize_revision(hash), format!("v{}", hash));
    }

    #[test]
    fn test_sanitize_revision_special_chars() {
        assert_eq!(sanitize_revision("1.0.0-beta"), "v1_0_0_beta");
        assert_eq!(sanitize_revision("1.0.0+build"), "v1_0_0_build");
    }

    #[test]
    fn test_build_destination_name_no_options() {
        assert_eq!(build_destination_name("transfers", None, None), "transfers");
    }

    #[test]
    fn test_build_destination_name_version_only() {
        assert_eq!(
            build_destination_name("transfers", Some("v1_0_0"), None),
            "transfers_v1_0_0"
        );
    }

    #[test]
    fn test_build_destination_name_suffix_only() {
        assert_eq!(
            build_destination_name("transfers", None, Some("green")),
            "transfers_green"
        );
    }

    #[test]
    fn test_build_destination_name_both() {
        assert_eq!(
            build_destination_name("transfers", Some("v1_0_0"), Some("green")),
            "transfers_v1_0_0_green"
        );
    }
}
