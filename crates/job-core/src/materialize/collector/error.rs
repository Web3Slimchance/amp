use metadata_db::files::FileId;
use object_store::{Error as ObjectStoreError, path::Error as PathError};
use tokio::task::JoinError;

use crate::error::RetryableErrorExt;

pub type CollectionResult<T> = Result<T, CollectorError>;

/// Errors that occur during garbage collection operations
///
/// Garbage collection identifies and removes orphaned or obsolete Parquet files
/// from storage after they've been compacted or are no longer referenced. This
/// error type covers all phases of GC: consistency checks, file deletion, and
/// metadata cleanup.
#[derive(thiserror::Error, Debug)]
pub enum CollectorError {
    /// File not found in object store
    ///
    /// This occurs when attempting to delete a file that doesn't exist in storage.
    /// The file may have already been deleted or the metadata is out of sync.
    ///
    /// Common causes:
    /// - File already deleted by another garbage collection process
    /// - Metadata references non-existent file (orphaned metadata)
    /// - Object store path changed or bucket reconfigured
    /// - Manual file deletion outside of GC process
    ///
    /// This is often not a fatal error - if the file is already gone, the GC
    /// goal is achieved. The metadata should still be cleaned up.
    #[error("file not found: {path}")]
    FileNotFound { path: String },

    /// Object store operation failed
    ///
    /// This occurs when interacting with the underlying object store (S3, GCS,
    /// local filesystem, etc.) fails during file deletion operations.
    ///
    /// Common causes:
    /// - Object store connection lost during deletion
    /// - Network timeouts or connectivity issues
    /// - Insufficient permissions to delete files
    /// - Object store service unavailability
    /// - Rate limiting by object store service
    ///
    /// Object store errors are often transient and retryable. Permanent failures
    /// may indicate configuration or permission issues.
    #[error("object store error")]
    ObjectStore(#[source] ObjectStoreError),

    /// Failed to stream file metadata from database
    ///
    /// This occurs when querying the metadata database for files eligible for
    /// garbage collection fails. The GC process needs to read file metadata to
    /// determine which files can be safely deleted.
    ///
    /// Common causes:
    /// - Database connection lost during query
    /// - Query timeout due to large result set
    /// - Database unavailability
    /// - Insufficient database permissions for SELECT queries
    ///
    /// This prevents garbage collection from proceeding as the list of deletable
    /// files cannot be determined.
    #[error("failed to stream file metadata")]
    FileStream(#[source] amp_data_store::StreamExpiredGcFilesError),

    /// Task join error during garbage collection
    ///
    /// This occurs when a garbage collection task running in a separate thread
    /// or async task panics or is cancelled. GC operations are typically run in
    /// background tasks.
    ///
    /// Common causes:
    /// - Panic in GC code (indicates a bug)
    /// - Task cancelled due to shutdown signal
    /// - Task aborted due to timeout
    /// - Out of memory causing task termination
    ///
    /// This is typically a fatal error indicating either a bug or system resource issue.
    #[error("garbage collection task join error")]
    Join(#[source] JoinError),

    /// Failed to delete file metadata records
    ///
    /// This occurs when removing file metadata from the metadata database after
    /// successfully deleting the files from storage. The files are gone but the
    /// database still has stale metadata.
    ///
    /// Common causes:
    /// - Database connection lost during delete
    /// - Transaction conflicts with concurrent operations
    /// - Insufficient database permissions for DELETE queries
    /// - Database constraint violations preventing deletion
    ///
    /// This leaves orphaned metadata in the database. The metadata should be
    /// cleaned up manually or via retry.
    #[error("failed to delete file metadata for files {file_ids:?}: {err}")]
    FileMetadataDelete {
        #[source]
        err: metadata_db::Error,
        file_ids: Vec<FileId>,
    },

    /// Failed to delete manifest record
    ///
    /// This occurs when removing a file entry from the garbage collection manifest
    /// after the file has been successfully deleted. The GC manifest tracks files
    /// eligible for cleanup.
    ///
    /// Common causes:
    /// - Database connection lost during delete
    /// - Transaction conflicts with concurrent operations
    /// - Record already deleted by another process
    /// - Insufficient database permissions
    ///
    /// This leaves a stale entry in the GC manifest. The entry should be cleaned
    /// up to prevent repeated deletion attempts.
    #[error("failed to delete file {file_id} from gc manifest: {err}")]
    ManifestDelete {
        #[source]
        err: metadata_db::Error,
        file_id: FileId,
    },

    /// Failed to parse URL for file
    ///
    /// This occurs when converting a file path stored in metadata to a URL for
    /// object store operations fails. URLs are used to identify files in the
    /// object store.
    ///
    /// Common causes:
    /// - Corrupted file path in database (invalid URL characters)
    /// - Malformed file:// or s3:// URL in metadata
    /// - Invalid percent-encoding in path
    ///
    /// This indicates corrupted metadata and should be investigated. The file
    /// may be inaccessible for deletion without manual intervention.
    #[error("url parse error for file {file_id}: {err}")]
    Parse {
        file_id: FileId,
        #[source]
        err: url::ParseError,
    },

    /// Invalid object store path
    ///
    /// This occurs when a path cannot be converted to a valid object store path.
    /// Object stores have specific path format requirements.
    ///
    /// Common causes:
    /// - Path contains invalid characters for object store
    /// - Path doesn't match expected format (e.g., S3 bucket/key structure)
    /// - Empty path components
    /// - Path exceeds maximum length limits
    ///
    /// This indicates a mismatch between stored paths and object store requirements.
    #[error("path error")]
    Path(#[source] PathError),

    /// Multiple errors occurred during collection
    ///
    /// This occurs when garbage collection encounters multiple independent failures
    /// across different files or operations. The GC process attempts to continue
    /// despite individual failures, collecting all errors for reporting.
    ///
    /// Common causes:
    /// - Batch deletion with some files failing due to different reasons
    /// - Mixed success/failure when deleting across multiple storage locations
    /// - Partial database connection issues affecting some operations
    ///
    /// Each individual error should be examined to determine if retry is appropriate.
    /// Some files may have been successfully deleted while others failed.
    #[error("multiple errors occurred:\n{}", .errors.iter().enumerate().map(|(i, e)| format!("  {}: {}", i + 1, e)).collect::<Vec<_>>().join("\n"))]
    MultipleErrors { errors: Vec<CollectorError> },
}

impl From<JoinError> for CollectorError {
    fn from(err: JoinError) -> Self {
        Self::Join(err)
    }
}

impl From<ObjectStoreError> for CollectorError {
    fn from(err: ObjectStoreError) -> Self {
        Self::ObjectStore(err)
    }
}

impl RetryableErrorExt for CollectorError {
    fn is_retryable(&self) -> bool {
        match self {
            Self::FileNotFound { .. } => true,
            Self::ObjectStore(_) => true,
            Self::FileStream(_) => true,
            Self::Join(_) => false,
            Self::FileMetadataDelete { .. } => true,
            Self::ManifestDelete { .. } => true,
            Self::Parse { .. } => false,
            Self::Path(_) => false,
            Self::MultipleErrors { errors } => errors.iter().all(|e| e.is_retryable()),
        }
    }
}

impl CollectorError {
    /// Returns a closure that wraps a database error into a [`FileMetadataDelete`](Self::FileMetadataDelete)
    /// error, capturing the affected file IDs.
    pub fn file_metadata_delete<'a>(
        file_ids: impl Iterator<Item = &'a FileId>,
    ) -> impl FnOnce(metadata_db::Error) -> Self {
        move |err| Self::FileMetadataDelete {
            err,
            file_ids: file_ids.cloned().collect(),
        }
    }

    /// Returns a closure that wraps a database error into a [`ManifestDelete`](Self::ManifestDelete)
    /// error for the given file ID.
    pub fn gc_manifest_delete(file_id: FileId) -> impl FnOnce(metadata_db::Error) -> Self {
        move |err| Self::ManifestDelete { err, file_id }
    }

    /// Returns a closure that wraps a URL parse error for the given file ID.
    pub fn parse_error(file_id: FileId) -> impl FnOnce(url::ParseError) -> Self {
        move |err| Self::Parse { file_id, err }
    }
}
