use datasets_common::hash_reference::HashReference;
use monitoring::telemetry;
use telemetry::metrics::{Counter, Gauge, Histogram, KeyValue, Meter};

/// Metrics registry for the raw materialize job.
///
/// Tracks ingestion, write, freshness, and dump metrics per dataset/job.
#[derive(Debug, Clone)]
pub struct MetricsRegistry {
    /// The job ID associated with this metrics registry.
    pub job_id: i64,
    /// Dataset identity (namespace, name, manifest hash) used as base key-value labels.
    pub dataset_reference: HashReference,
    /// Total rows ingested across all tables in this dataset.
    pub rows_ingested: Counter,
    /// Total Parquet files written across all tables.
    pub files_written: Counter,
    /// Total uncompressed bytes written across all tables.
    pub bytes_written: Counter,
    /// Total write calls made to dataset writers.
    pub write_calls: Counter,
    /// Latest block number successfully dumped for a dataset/table.
    pub latest_block: Gauge<u64>,
    /// Unix timestamp (seconds) of the end block in the latest segment written.
    pub latest_segment_timestamp: Gauge<u64>,
    /// Delay in seconds between segment end block timestamp and write time.
    pub latest_segment_delay: Gauge<u64>,
    /// Duration of dump operations from start to completion in milliseconds.
    pub dump_duration: Histogram<f64>,
    /// Count of dump errors encountered during materialization.
    pub dump_errors: Counter,
}

impl MetricsRegistry {
    /// Creates a new metrics registry, initializing all instruments from the given meter.
    pub fn new(meter: &Meter, dataset_reference: HashReference, job_id: i64) -> Self {
        Self {
            job_id,
            dataset_reference,
            rows_ingested: Counter::new(
                meter,
                "rows_ingested_total",
                "Total number of rows ingested into datasets",
            ),
            files_written: Counter::new(
                meter,
                "files_written_total",
                "Total number of files written for datasets",
            ),
            bytes_written: Counter::new(
                meter,
                "bytes_written_total",
                "Total bytes written for datasets (uncompressed)",
            ),
            write_calls: Counter::new(
                meter,
                "write_calls_total",
                "Total number of write calls made to dataset writers",
            ),
            latest_block: Gauge::new_u64(
                meter,
                "latest_block_number",
                "Latest block number successfully dumped for a dataset/table",
                "block_number",
            ),
            latest_segment_timestamp: Gauge::new_u64(
                meter,
                "latest_segment_timestamp",
                "Unix timestamp (seconds) of the end block in the latest segment written",
                "seconds",
            ),
            latest_segment_delay: Gauge::new_u64(
                meter,
                "latest_segment_delay_seconds",
                "Delay in seconds between segment end block timestamp and write time",
                "seconds",
            ),
            dump_duration: Histogram::new_f64(
                meter,
                "dump_duration_milliseconds",
                "Time from dump job start to completion",
                "milliseconds",
            ),
            dump_errors: Counter::new(
                meter,
                "dump_errors_total",
                "Total number of dump errors encountered",
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

    /// Increments the rows-ingested counter for the given table and location.
    pub fn record_ingestion_rows(&self, rows: u64, table: String, location_id: i64) {
        let mut kv_pairs = self.base_kvs();
        kv_pairs.extend_from_slice(&[
            KeyValue::new("table", table),
            KeyValue::new("location_id", location_id),
        ]);
        self.rows_ingested.inc_by_with_kvs(rows, &kv_pairs);
    }

    /// Records bytes written and increments the write-calls counter for the given table and location.
    pub fn record_write_call(&self, bytes: u64, table: String, location_id: i64) {
        let mut kv_pairs = self.base_kvs();
        kv_pairs.extend_from_slice(&[
            KeyValue::new("table", table),
            KeyValue::new("location_id", location_id),
        ]);
        self.bytes_written.inc_by_with_kvs(bytes, &kv_pairs);
        self.write_calls.inc_with_kvs(&kv_pairs);
    }

    /// Increments the files-written counter for the given table and location.
    pub fn record_file_written(&self, table: String, location_id: i64) {
        let mut kv_pairs = self.base_kvs();
        kv_pairs.extend_from_slice(&[
            KeyValue::new("table", table),
            KeyValue::new("location_id", location_id),
        ]);
        self.files_written.inc_with_kvs(&kv_pairs);
    }

    /// Records table freshness by setting the latest segment timestamp and computing the delay
    /// between that timestamp and the current wall-clock time.
    pub fn record_table_freshness(
        &self,
        table: String,
        location_id: i64,
        latest_segment_timestamp: u64,
    ) {
        let mut kv_pairs = self.base_kvs();
        kv_pairs.extend_from_slice(&[
            KeyValue::new("table", table),
            KeyValue::new("location_id", location_id),
        ]);

        self.latest_segment_timestamp
            .record_with_kvs(latest_segment_timestamp, &kv_pairs);

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time before UNIX epoch")
            .as_secs();
        let delay = now.saturating_sub(latest_segment_timestamp);
        self.latest_segment_delay.record_with_kvs(delay, &kv_pairs);
    }

    /// Updates the latest block number gauge for the given table and location.
    pub fn set_latest_block(&self, block_number: u64, table: String, location_id: i64) {
        let mut kv_pairs = self.base_kvs();
        kv_pairs.extend_from_slice(&[
            KeyValue::new("table", table),
            KeyValue::new("location_id", location_id),
        ]);
        self.latest_block.record_with_kvs(block_number, &kv_pairs);
    }

    /// Records the duration of a completed dump operation in milliseconds.
    pub fn record_dump_duration(&self, duration_millis: f64, table: String) {
        let mut kv_pairs = self.base_kvs();
        kv_pairs.extend_from_slice(&[KeyValue::new("table", table)]);
        self.dump_duration
            .record_with_kvs(duration_millis, &kv_pairs);
    }

    /// Increments the dump-errors counter for the given table.
    pub fn record_dump_error(&self, table: String) {
        let mut kv_pairs = self.base_kvs();
        kv_pairs.extend_from_slice(&[KeyValue::new("table", table)]);
        self.dump_errors.inc_with_kvs(&kv_pairs);
    }
}
