use datasets_common::hash_reference::HashReference;
use monitoring::telemetry;
use telemetry::metrics::{Counter, Histogram, KeyValue, Meter};

/// Metrics for compaction operations.
///
/// Created per-job and passed to the compactor. Contains only compaction
/// counters and histograms; garbage-collection metrics live in the sibling
/// [`collector::metrics`](crate::materialize::collector::metrics) module.
#[derive(Debug, Clone)]
pub struct CompactionMetrics {
    /// The job ID for this metrics registry.
    pub job_id: i64,
    /// Dataset identity (namespace, name, manifest hash) used as base key-value labels.
    pub dataset_reference: HashReference,

    /// Number of files read (input) during compaction.
    pub compaction_files_read: Counter,
    /// Total bytes read before compaction.
    pub compaction_bytes_read: Counter,
    /// Total bytes written after compaction.
    pub compaction_bytes_written: Counter,
    /// Duration of compaction operations in milliseconds.
    pub compaction_duration: Histogram<f64>,
    /// Successful compaction operations count.
    pub successful_compactions: Counter,
    /// Failed compaction operations count.
    pub failed_compactions: Counter,
}

impl CompactionMetrics {
    /// Creates a new compaction metrics registry, initializing all instruments from the given meter.
    pub fn new(meter: &Meter, dataset_reference: HashReference, job_id: i64) -> Self {
        Self {
            job_id,
            dataset_reference,
            compaction_files_read: Counter::new(
                meter,
                "compaction_files_read_total",
                "Number of files read (input) during compaction",
            ),
            compaction_bytes_read: Counter::new(
                meter,
                "compaction_bytes_read_total",
                "Total bytes read before compaction",
            ),
            compaction_bytes_written: Counter::new(
                meter,
                "compaction_bytes_written_total",
                "Total bytes written after compaction",
            ),
            compaction_duration: Histogram::new_f64(
                meter,
                "compaction_duration_milliseconds",
                "Duration of compaction operations",
                "milliseconds",
            ),
            successful_compactions: Counter::new(
                meter,
                "successful_compactions",
                "Counter for successful compaction operations",
            ),
            failed_compactions: Counter::new(
                meter,
                "failed_compactions",
                "Counter for failed compaction operations",
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

    /// Records a compaction operation's I/O and duration.
    ///
    /// Compaction ratio can be calculated in Prometheus as:
    /// `compaction_bytes_written_total / compaction_bytes_read_total`
    pub(crate) fn record_compaction(
        &self,
        table: String,
        location_id: i64,
        input_file_count: u64,
        input_bytes: u64,
        output_bytes: u64,
        duration_millis: f64,
    ) {
        let mut kv_pairs = self.base_kvs();
        kv_pairs.extend_from_slice(&[
            KeyValue::new("table", table),
            KeyValue::new("location_id", location_id),
        ]);

        self.compaction_files_read
            .inc_by_with_kvs(input_file_count, &kv_pairs);
        self.compaction_bytes_read
            .inc_by_with_kvs(input_bytes, &kv_pairs);
        self.compaction_bytes_written
            .inc_by_with_kvs(output_bytes, &kv_pairs);
        self.compaction_duration
            .record_with_kvs(duration_millis, &kv_pairs);
    }

    /// Increments the successful compactions counter for the given table.
    pub(crate) fn inc_successful_compactions(&self, table: String) {
        let mut kv_pairs = self.base_kvs();
        kv_pairs.extend_from_slice(&[KeyValue::new("table", table)]);
        self.successful_compactions.inc_with_kvs(&kv_pairs);
    }

    /// Increments the failed compactions counter for the given table.
    pub(crate) fn inc_failed_compactions(&self, table: String) {
        let mut kv_pairs = self.base_kvs();
        kv_pairs.extend_from_slice(&[KeyValue::new("table", table)]);
        self.failed_compactions.inc_with_kvs(&kv_pairs);
    }
}
