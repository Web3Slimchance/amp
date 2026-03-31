//! EVM block data is sufficiently complicated that there may be multiple encoding flavors, each with
//! multiple versions. There is no universal encoding, and we're not going to try to enforce one.
//! Each extraction layer can have its own data format. This `firehose` crate defines Firehose
//! dataset kinds and manifest types.

use datasets_common::{hash_reference::HashReference, table_name::TableNameError};
use datasets_raw::dataset::{Dataset as RawDataset, tables_from_manifest};

pub mod tables;

// Re-export dataset kind and manifest types from datasets-raw for backward compatibility
pub use datasets_raw::{
    dataset_kind::{FirehoseDatasetKind, FirehoseDatasetKindError},
    manifest::{FirehoseManifest as Manifest, Table},
};

/// Convert a Firehose manifest into a logical dataset representation.
///
/// Dataset identity (namespace, name, version, hash reference) must be provided externally as they
/// are not part of the manifest.
pub fn dataset(reference: HashReference, manifest: Manifest) -> Result<RawDataset, TableNameError> {
    let network = manifest.network;
    let sorted_by = vec!["block_num".to_string(), "timestamp".to_string()];
    let tables = tables_from_manifest(manifest.tables, sorted_by)?;
    Ok(RawDataset::new(
        reference,
        manifest.kind.into(),
        network.clone(),
        tables,
        Some(manifest.start_block),
        manifest.finalized_blocks_only,
    ))
}
