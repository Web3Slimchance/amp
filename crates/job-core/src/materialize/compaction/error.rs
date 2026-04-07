use std::sync::Arc;

use amp_parquet::{retry::RetryableErrorExt as _, writer::ParquetFileWriterCloseError};
use common::{
    parquet::{
        errors::ParquetError, file::properties::WriterProperties as ParquetWriterProperties,
    },
    physical_table::SnapshotError,
    retryable::RetryableErrorExt as _,
};
use datafusion::error::DataFusionError;
use metadata_db::files::FileId;
use tokio::task::JoinError;

use crate::{error::RetryableErrorExt, materialize::writer::WriterProperties};

pub type CompactionResult<T> = Result<T, CompactorError>;

/// Errors that occur during Parquet file compaction operations
///
/// Compaction merges multiple small Parquet files into larger files to improve
/// query performance and reduce storage overhead. This error type covers all
/// phases of compaction: chain building, file reading, writing, and metadata updates.
#[derive(thiserror::Error, Debug)]
pub enum CompactorError {
    /// Failed to build canonical chain for compaction
    ///
    /// This occurs during the chain-building phase when determining which files
    /// should be compacted together. The canonical chain represents the sequence
    /// of Parquet files that will be merged based on block ranges and timestamps.
    ///
    /// Common causes:
    /// - Inconsistent block range metadata in files
    /// - Overlapping block ranges that cannot be merged
    /// - Files missing required metadata for chain construction
    /// - Logic errors in chain building algorithm
    ///
    /// This error prevents compaction from proceeding as there's no valid merge plan.
    #[error("failed to build canonical chain")]
    CanonicalChain(#[source] SnapshotError),

    /// Failed to create compaction writer
    ///
    /// This occurs when initializing a Parquet writer with the given properties
    /// fails. The writer is responsible for encoding and writing the merged data
    /// to the new compacted Parquet file.
    ///
    /// Common causes:
    /// - Invalid Parquet writer properties (compression, page size, etc.)
    /// - Memory allocation failures when creating writer buffers
    /// - Object store connection issues preventing file creation
    /// - Insufficient permissions to write to storage location
    /// - Storage quota exceeded
    ///
    /// The error includes the writer properties that caused the failure for debugging.
    #[error("failed to create writer (writer properties: {opts:?}): {err}")]
    CreateWriter {
        #[source]
        err: ParquetError,
        opts: Box<ParquetWriterProperties>,
    },

    /// Failed to write data to Parquet file
    ///
    /// This occurs during the write phase when encoding and writing data to the
    /// compacted Parquet file fails. This happens after successfully reading source
    /// files but before finalizing the output file.
    ///
    /// Common causes:
    /// - Disk full or storage quota exceeded
    /// - Object store connection lost during write
    /// - Memory allocation failures during encoding
    /// - Data encoding errors (e.g., values exceeding type limits)
    /// - Writer buffer flush failures
    ///
    /// A write failure leaves the compaction incomplete. The partial output file
    /// should be cleaned up, and source files remain unchanged.
    #[error("failed to write to parquet file")]
    FileWrite(#[source] ParquetFileWriterCloseError),

    /// Failed to read/stream data from Parquet files
    ///
    /// This occurs when reading and streaming data from input Parquet files during
    /// compaction. The compactor must read all source files to merge their contents.
    ///
    /// Common causes:
    /// - Corrupted Parquet files (invalid headers, metadata, or data pages)
    /// - Object store connection failures during read
    /// - Files deleted or moved during compaction
    /// - Schema incompatibilities between files
    /// - Decompression errors in compressed Parquet pages
    /// - Memory allocation failures when reading large row groups
    ///
    /// Read failures prevent compaction from completing. Source files should be
    /// investigated for corruption or consistency issues.
    #[error("failed to stream parquet file")]
    FileStream(#[source] DataFusionError),

    /// Task join error during compaction
    ///
    /// This occurs when a compaction task running in a separate thread or async task
    /// panics or is cancelled. Compaction operations are typically run in background
    /// tasks to avoid blocking the main thread.
    ///
    /// Common causes:
    /// - Panic in compaction code (indicates a bug)
    /// - Task cancelled due to shutdown signal
    /// - Task aborted due to timeout
    /// - Out of memory causing task termination
    ///
    /// This is typically a fatal error indicating either a bug or system resource issue.
    #[error("compaction task join error")]
    Join(#[source] JoinError),

    /// Failed to update GC manifest in metadata database
    ///
    /// This occurs when updating the garbage collection manifest with information
    /// about the newly created compacted file. The GC manifest tracks which files
    /// are eligible for cleanup.
    ///
    /// Common causes:
    /// - Database connection lost during update
    /// - Transaction conflicts with concurrent operations
    /// - Database constraint violations
    /// - Insufficient database permissions
    ///
    /// If this fails, the compacted file exists in storage but is not tracked in the
    /// GC manifest. Manual cleanup or retry may be needed.
    #[error("failed to update gc manifest for files {file_ids:?}: {err}")]
    ManifestUpdate {
        #[source]
        err: amp_data_store::ScheduleFilesForGcError,
        file_ids: Arc<[FileId]>,
    },

    /// Failed to delete records from GC manifest
    ///
    /// This occurs when removing old file records from the garbage collection manifest
    /// after successful compaction. Old files should be marked for deletion once the
    /// compacted file is ready.
    ///
    /// Common causes:
    /// - Database connection lost during delete
    /// - Transaction conflicts with concurrent operations
    /// - Records already deleted by another process
    /// - Insufficient database permissions
    ///
    /// If this fails, old file records remain in the GC manifest even though they've
    /// been replaced by the compacted file. This may delay garbage collection.
    #[error("failed to delete from gc manifest for files {file_ids:?}: {err}")]
    ManifestDelete {
        #[source]
        err: metadata_db::Error,
        file_ids: Arc<[FileId]>,
    },

    /// Failed to commit file metadata to database
    ///
    /// This occurs when recording the new compacted file's metadata (path, size,
    /// block ranges, etc.) in the metadata database fails.
    ///
    /// Common causes:
    /// - Database connection lost during commit
    /// - Transaction conflicts with concurrent operations
    /// - Database constraint violations (e.g., duplicate file IDs)
    /// - Insufficient database permissions
    /// - Database out of disk space
    ///
    /// If this fails, the compacted file exists in storage but is not registered
    /// in the metadata database. The file is orphaned and may need manual cleanup.
    #[error("failed to commit file metadata")]
    MetadataCommit(#[source] metadata_db::Error),
}

impl RetryableErrorExt for CompactorError {
    fn is_retryable(&self) -> bool {
        match self {
            Self::CanonicalChain(err) => err.is_retryable(),
            Self::CreateWriter { .. } => true,
            Self::FileWrite(err) => err.is_retryable(),
            Self::FileStream(_) => true,
            Self::Join(_) => false,
            Self::ManifestUpdate { .. } => true,
            Self::ManifestDelete { .. } => true,
            Self::MetadataCommit(_) => true,
        }
    }
}

impl CompactorError {
    /// Wraps a canonical-chain building error.
    pub fn chain_error(err: SnapshotError) -> Self {
        Self::CanonicalChain(err)
    }

    /// Wraps a metadata database commit error.
    pub fn metadata_commit_error(err: metadata_db::Error) -> Self {
        Self::MetadataCommit(err)
    }

    /// Returns a closure that wraps a GC-schedule error into a [`ManifestUpdate`](Self::ManifestUpdate)
    /// error, capturing the affected file IDs.
    pub fn manifest_update_error(
        file_ids: &[FileId],
    ) -> impl FnOnce(amp_data_store::ScheduleFilesForGcError) -> Self {
        move |err| Self::ManifestUpdate {
            err,
            file_ids: Arc::from(file_ids),
        }
    }

    /// Returns a closure that wraps a Parquet error into a [`CreateWriter`](Self::CreateWriter)
    /// error, capturing the writer properties for diagnostics.
    pub fn create_writer_error(opts: &Arc<WriterProperties>) -> impl FnOnce(ParquetError) -> Self {
        move |err| Self::CreateWriter {
            err,
            opts: opts.parquet.clone().into(),
        }
    }
}

impl From<ParquetError> for CompactorError {
    fn from(err: ParquetError) -> Self {
        Self::FileStream(DataFusionError::ParquetError(Box::new(err)))
    }
}

impl From<DataFusionError> for CompactorError {
    fn from(err: DataFusionError) -> Self {
        Self::FileStream(err)
    }
}

impl From<JoinError> for CompactorError {
    fn from(err: JoinError) -> Self {
        Self::Join(err)
    }
}
