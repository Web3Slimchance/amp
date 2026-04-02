//! GC job execution.
//!
//! Implements the collection algorithm: stream expired files from the GC manifest,
//! delete their metadata from Postgres, then delete the physical files from object storage.

use std::collections::{BTreeMap, BTreeSet};

use amp_data_store::{DeleteFilesStreamError, physical_table::PhyTableRevisionPath};
use amp_job_core::error::{ErrorDetailsProvider, RetryableErrorExt};
use futures::{StreamExt as _, TryStreamExt as _, stream};
use metadata_db::{files::FileId, gc::GcManifestRow};
use object_store::{Error as ObjectStoreError, path::Path};

use crate::{job_ctx::Context, job_descriptor::JobDescriptor, metrics::GcMetrics};

/// Execute a garbage collection job for a single physical table revision.
///
/// The algorithm:
/// 1. Look up the revision by `location_id` to get its storage path.
/// 2. Stream expired files from the `gc_manifest` table.
/// 3. Delete file metadata rows from Postgres (cascades to `gc_manifest` and `footer_cache`).
/// 4. Delete physical files from object storage.
///
/// Metadata is deleted before physical files. If the process crashes between steps 3 and 4,
/// orphaned files may remain in storage but no dangling metadata references will exist.
#[tracing::instrument(skip_all, err, fields(location_id = %desc.location_id, table_ref))]
pub async fn execute(ctx: Context, desc: JobDescriptor) -> Result<(), Error> {
    let location_id = desc.location_id;

    // Look up the revision to get its storage path
    let revision =
        metadata_db::physical_table_revision::get_by_location_id(&ctx.metadata_db, location_id)
            .await
            .map_err(Error::MetadataDb)?
            .ok_or(Error::LocationNotFound(location_id))?;

    // Record table_ref on the current span now that we have the revision path
    tracing::Span::current().record("table_ref", revision.path.as_str());

    let revision_path: PhyTableRevisionPath = revision.path.into();
    let metrics = ctx.meter.as_ref().map(|m| GcMetrics::new(m, *location_id));

    // Step 1: Stream expired files from the GC manifest
    let found_file_ids_to_paths: BTreeMap<FileId, Path> = ctx
        .data_store
        .get_expired_gc_files(location_id)
        .map_err(Error::FileStream)
        .map(|manifest_row| {
            let GcManifestRow {
                file_id,
                file_path: file_name,
                ..
            } = manifest_row?;

            let path = revision_path.child(file_name.as_str());
            Ok::<_, Error>((file_id, path))
        })
        .try_collect()
        .await?;

    tracing::debug!(
        expired_files = found_file_ids_to_paths.len(),
        "expired files found"
    );

    if found_file_ids_to_paths.is_empty() {
        return Ok(());
    }

    if let Some(m) = &metrics {
        m.record_expired_files_found(found_file_ids_to_paths.len() as u64);
    }

    // Step 2: Delete file metadata from Postgres
    let file_ids_to_delete: Vec<FileId> = found_file_ids_to_paths.keys().copied().collect();
    let paths_to_remove: BTreeSet<Path> =
        metadata_db::files::delete_by_ids(&ctx.metadata_db, &file_ids_to_delete)
            .await
            .map_err(Error::FileMetadataDelete)?
            .into_iter()
            .filter_map(|file_id| found_file_ids_to_paths.get(&file_id).cloned())
            .collect();

    tracing::debug!(
        metadata_entries_deleted = paths_to_remove.len(),
        "metadata entries deleted"
    );

    if let Some(m) = &metrics {
        m.record_metadata_entries_deleted(paths_to_remove.len() as u64);
    }

    // Step 3: Delete physical files from object storage
    let mut delete_stream = ctx
        .data_store
        .delete_files_stream(stream::iter(paths_to_remove).map(Ok).boxed());

    let mut files_deleted: u64 = 0;
    let mut files_not_found: u64 = 0;

    while let Some(result) = delete_stream.next().await {
        match result {
            Ok(path) => {
                tracing::debug!(%path, "deleted expired file");
                files_deleted += 1;
            }
            Err(DeleteFilesStreamError(ObjectStoreError::NotFound { path, .. })) => {
                tracing::debug!(%path, "expired file not found");
                files_not_found += 1;
            }
            Err(DeleteFilesStreamError(err)) => {
                tracing::warn!(
                    error = %err,
                    files_deleted,
                    files_not_found,
                    "collection aborted due to object store error"
                );
                return Err(Error::ObjectStore(err));
            }
        }
    }

    if let Some(m) = &metrics {
        m.record_files_deleted(files_deleted);
        m.record_files_not_found(files_not_found);
    }

    tracing::info!(files_deleted, files_not_found, "collection complete");

    Ok(())
}

/// Errors that can occur during garbage collection job execution.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The target physical table revision does not exist.
    ///
    /// This occurs when the `location_id` in the job descriptor does not match any
    /// revision in the metadata database. The revision may have been deleted between
    /// scheduling and execution.
    ///
    /// This is a fatal (non-retryable) error since the location will not reappear.
    #[error("location not found: {0}")]
    LocationNotFound(metadata_db::physical_table_revision::LocationId),

    /// Failed to query the metadata database for the physical table revision.
    ///
    /// This occurs when looking up the revision by `location_id` at the start of
    /// the GC job. Common causes include database connectivity issues or timeouts.
    #[error("metadata database error")]
    MetadataDb(#[source] metadata_db::Error),

    /// Failed to stream expired files from the GC manifest.
    ///
    /// This occurs during step 1 of the collection algorithm when querying
    /// `gc_manifest` for files whose expiration has passed. Common causes include
    /// database connectivity issues or query timeouts.
    #[error("failed to stream expired files")]
    FileStream(#[source] amp_data_store::StreamExpiredGcFilesError),

    /// Failed to delete file metadata records from Postgres.
    ///
    /// This occurs during step 2 of the collection algorithm when removing
    /// `file_metadata` rows for expired files. Common causes include database
    /// connectivity issues or transaction conflicts.
    #[error("failed to delete file metadata")]
    FileMetadataDelete(#[source] metadata_db::Error),

    /// Failed to delete a physical file from object storage.
    ///
    /// This occurs during step 3 of the collection algorithm when removing
    /// Parquet files from S3/GCS/local storage. Common causes include network
    /// failures, permission errors, or storage service unavailability.
    ///
    /// Note: `NotFound` errors are tolerated (the file is already gone).
    /// Only other object store errors trigger this variant.
    #[error("object store error")]
    ObjectStore(#[source] object_store::Error),
}

impl RetryableErrorExt for Error {
    fn is_retryable(&self) -> bool {
        match self {
            Self::LocationNotFound(_) => false,
            Self::MetadataDb(_) => true,
            Self::FileStream(_) => true,
            Self::FileMetadataDelete(_) => true,
            Self::ObjectStore(_) => true,
        }
    }
}

impl amp_job_core::error::JobErrorExt for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Self::LocationNotFound(_) => "GC_LOCATION_NOT_FOUND",
            Self::MetadataDb(_) => "GC_METADATA_DB",
            Self::FileStream(_) => "GC_FILE_STREAM",
            Self::FileMetadataDelete(_) => "GC_FILE_METADATA_DELETE",
            Self::ObjectStore(_) => "GC_OBJECT_STORE",
        }
    }
}

impl ErrorDetailsProvider for Error {}
