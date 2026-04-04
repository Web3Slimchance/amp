use datasets_common::hash_reference::HashReference;
use monitoring::telemetry;
use telemetry::metrics::{Counter, KeyValue, Meter};

/// Metrics for garbage collection operations.
///
/// Created per-job and passed to the collector. Contains only GC counters;
/// compaction metrics live in the sibling
/// [`compaction::metrics`](crate::materialize::compaction::metrics) module.
#[derive(Debug, Clone)]
pub struct CollectorMetrics {
    /// The job ID for this metrics registry.
    pub job_id: i64,
    /// Dataset identity (namespace, name, manifest hash) used as base key-value labels.
    pub dataset_reference: HashReference,

    /// Files successfully deleted by the garbage collector.
    pub files_deleted: Counter,
    /// Files not found during garbage collection.
    pub files_not_found: Counter,
    /// Expired files discovered during garbage collection scans.
    pub expired_files_found: Counter,
    /// Expired metadata entries removed during garbage collection.
    pub expired_entries_deleted: Counter,
    /// Successful garbage collection operations count.
    pub successful_collections: Counter,
    /// Failed garbage collection operations count.
    pub failed_collections: Counter,
}

impl CollectorMetrics {
    /// Creates a new collector metrics registry, initializing all instruments from the given meter.
    pub fn new(meter: &Meter, dataset_reference: HashReference, job_id: i64) -> Self {
        Self {
            job_id,
            dataset_reference,
            files_deleted: Counter::new(
                meter,
                "files_deleted",
                "Counter for files successfully deleted by the garbage collector",
            ),
            files_not_found: Counter::new(
                meter,
                "files_not_found",
                "Counter for files not found during garbage collection",
            ),
            expired_files_found: Counter::new(
                meter,
                "expired_files_found",
                "Counter for expired files discovered during garbage collection scans",
            ),
            expired_entries_deleted: Counter::new(
                meter,
                "expired_entries_deleted",
                "Counter for expired metadata entries removed during garbage collection",
            ),
            successful_collections: Counter::new(
                meter,
                "successful_collections",
                "Counter for successful garbage collection operations",
            ),
            failed_collections: Counter::new(
                meter,
                "failed_collections",
                "Counter for failed garbage collection operations",
            ),
        }
    }

    /// Returns base key-value pairs (dataset, manifest_hash, job_id) attached to every metric.
    pub fn base_kvs(&self) -> Vec<KeyValue> {
        vec![
            KeyValue::new("dataset", self.dataset_reference.as_fqn().to_string()),
            KeyValue::new(
                "manifest_hash",
                self.dataset_reference.hash().as_str().to_string(),
            ),
            KeyValue::new("job_id", self.job_id.to_string()),
        ]
    }

    /// Increments the files-deleted counter by the given amount for the given table.
    pub(crate) fn inc_files_deleted(&self, amount: usize, table: String) {
        let mut kv_pairs = self.base_kvs();
        kv_pairs.extend_from_slice(&[KeyValue::new("table", table)]);
        self.files_deleted.inc_by_with_kvs(amount as u64, &kv_pairs);
    }

    /// Increments the files-not-found counter by the given amount for the given table.
    pub(crate) fn inc_files_not_found(&self, amount: usize, table: String) {
        let mut kv_pairs = self.base_kvs();
        kv_pairs.extend_from_slice(&[KeyValue::new("table", table)]);
        self.files_not_found
            .inc_by_with_kvs(amount as u64, &kv_pairs);
    }

    /// Increments the expired-files-found counter by the given amount for the given table.
    pub(crate) fn inc_expired_files_found(&self, amount: usize, table: String) {
        let mut kv_pairs = self.base_kvs();
        kv_pairs.extend_from_slice(&[KeyValue::new("table", table)]);
        self.expired_files_found
            .inc_by_with_kvs(amount as u64, &kv_pairs);
    }

    /// Increments the expired-entries-deleted counter by the given amount for the given table.
    pub(crate) fn inc_expired_entries_deleted(&self, amount: usize, table: String) {
        let mut kv_pairs = self.base_kvs();
        kv_pairs.extend_from_slice(&[KeyValue::new("table", table)]);
        self.expired_entries_deleted
            .inc_by_with_kvs(amount as u64, &kv_pairs);
    }

    /// Increments the successful garbage collections counter for the given table.
    pub(crate) fn inc_successful_collections(&self, table: String) {
        let mut kv_pairs = self.base_kvs();
        kv_pairs.extend_from_slice(&[KeyValue::new("table", table)]);
        self.successful_collections.inc_with_kvs(&kv_pairs);
    }

    /// Increments the failed garbage collections counter for the given table.
    pub(crate) fn inc_failed_collections(&self, table: String) {
        let mut kv_pairs = self.base_kvs();
        kv_pairs.extend_from_slice(&[KeyValue::new("table", table)]);
        self.failed_collections.inc_with_kvs(&kv_pairs);
    }
}
