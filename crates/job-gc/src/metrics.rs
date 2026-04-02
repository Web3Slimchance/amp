//! Metrics for GC job execution.

use monitoring::telemetry::metrics::{Counter, KeyValue, Meter};

/// Metrics recorded during garbage collection execution.
pub struct GcMetrics {
    location_id: i64,
    expired_files_found: Counter,
    metadata_entries_deleted: Counter,
    files_deleted: Counter,
    files_not_found: Counter,
}

impl GcMetrics {
    /// Create a new metrics instance for a GC job.
    pub fn new(meter: &Meter, location_id: i64) -> Self {
        Self {
            location_id,
            expired_files_found: Counter::new(
                meter,
                "gc_expired_files_found",
                "Number of expired files found in the GC manifest",
            ),
            metadata_entries_deleted: Counter::new(
                meter,
                "gc_metadata_entries_deleted",
                "Number of file metadata entries deleted from Postgres",
            ),
            files_deleted: Counter::new(
                meter,
                "gc_files_deleted",
                "Number of physical files deleted from object storage",
            ),
            files_not_found: Counter::new(
                meter,
                "gc_files_not_found",
                "Number of expired files already missing from object storage",
            ),
        }
    }

    fn kvs(&self) -> [KeyValue; 1] {
        [KeyValue::new("location_id", self.location_id)]
    }

    pub fn record_expired_files_found(&self, count: u64) {
        self.expired_files_found.inc_by_with_kvs(count, &self.kvs());
    }

    pub fn record_metadata_entries_deleted(&self, count: u64) {
        self.metadata_entries_deleted
            .inc_by_with_kvs(count, &self.kvs());
    }

    pub fn record_files_deleted(&self, count: u64) {
        self.files_deleted.inc_by_with_kvs(count, &self.kvs());
    }

    pub fn record_files_not_found(&self, count: u64) {
        self.files_not_found.inc_by_with_kvs(count, &self.kvs());
    }
}
