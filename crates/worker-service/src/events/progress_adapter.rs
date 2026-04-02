//! Progress reporter adapter for bridging job core progress to event emitter.
//!
//! This module provides a [`WorkerProgressReporter`] that implements the job core crate's
//! [`amp_job_core::materialize::progress::ProgressReporter`] trait and forwards progress updates
//! to the worker's [`EventEmitter`] for Kafka streaming.

use std::{sync::Arc, time::Duration};

use amp_job_core::{
    job_id::JobId,
    materialize::progress::{ProgressUpdate, SyncCompletedInfo, SyncFailedInfo, SyncStartedInfo},
};

use super::EventEmitter;
use crate::kafka::proto;

/// Adapter that bridges job core progress reporting to worker event emission.
///
/// This struct implements [`amp_job_core::materialize::progress::ProgressReporter`] and translates
/// progress updates into proto events that are sent to the configured [`EventEmitter`].
pub struct WorkerProgressReporter {
    job_id: JobId,
    dataset_info: proto::DatasetInfo,
    event_emitter: Arc<dyn EventEmitter>,
}

impl WorkerProgressReporter {
    /// Creates a new progress reporter adapter.
    pub fn new(
        job_id: JobId,
        dataset_info: proto::DatasetInfo,
        event_emitter: Arc<dyn EventEmitter>,
    ) -> Self {
        Self {
            job_id,
            dataset_info,
            event_emitter,
        }
    }
}

impl amp_job_core::materialize::progress::ProgressReporter for WorkerProgressReporter {
    fn report_progress(&self, update: ProgressUpdate) {
        let event = proto::SyncProgress {
            job_id: *self.job_id,
            dataset: Some(self.dataset_info.clone()),
            table_name: update.table_name.to_string(),
            progress: Some(proto::ProgressInfo {
                start_block: update.start_block,
                current_block: update.current_block,
                end_block: update.end_block,
                files_count: update.files_count,
                total_size_bytes: update.total_size_bytes,
            }),
        };

        // Clone the emitter and spawn an async task to emit the event.
        // This is necessary because ProgressReporter::report_progress is synchronous
        // but EventEmitter::emit_sync_progress is asynchronous.
        let emitter = Arc::clone(&self.event_emitter);
        tokio::spawn(async move {
            if tokio::time::timeout(Duration::from_secs(30), emitter.emit_sync_progress(event))
                .await
                .is_err()
            {
                tracing::warn!("emit_sync_progress timeout");
            }
        });
    }

    fn report_sync_started(&self, info: SyncStartedInfo) {
        let event = proto::SyncStarted {
            job_id: *self.job_id,
            dataset: Some(self.dataset_info.clone()),
            table_name: info.table_name.to_string(),
            start_block: info.start_block,
            end_block: info.end_block,
        };

        let emitter = Arc::clone(&self.event_emitter);
        tokio::spawn(async move {
            if tokio::time::timeout(Duration::from_secs(30), emitter.emit_sync_started(event))
                .await
                .is_err()
            {
                tracing::warn!("emit_sync_started timeout");
            }
        });
    }

    fn report_sync_completed(&self, info: SyncCompletedInfo) {
        let event = proto::SyncCompleted {
            job_id: *self.job_id,
            dataset: Some(self.dataset_info.clone()),
            table_name: info.table_name.to_string(),
            final_block: info.final_block,
            duration_millis: info.duration_millis,
        };

        let emitter = Arc::clone(&self.event_emitter);
        tokio::spawn(async move {
            if tokio::time::timeout(Duration::from_secs(30), emitter.emit_sync_completed(event))
                .await
                .is_err()
            {
                tracing::warn!("emit_sync_completed timeout");
            }
        });
    }

    fn report_sync_failed(&self, info: SyncFailedInfo) {
        let event = proto::SyncFailed {
            job_id: *self.job_id,
            dataset: Some(self.dataset_info.clone()),
            table_name: info.table_name.to_string(),
            error_message: info.error_message,
            error_type: info.error_type,
        };

        let emitter = Arc::clone(&self.event_emitter);
        tokio::spawn(async move {
            if tokio::time::timeout(Duration::from_secs(30), emitter.emit_sync_failed(event))
                .await
                .is_err()
            {
                tracing::warn!("emit_sync_failed timeout");
            }
        });
    }
}
