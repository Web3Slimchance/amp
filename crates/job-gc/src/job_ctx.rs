//! Context for GC job execution.

use amp_data_store::DataStore;
use monitoring::telemetry::metrics::Meter;

/// Dependencies required to execute a GC job.
#[derive(Debug, Clone)]
pub struct Context {
    /// Connection to object storage (S3/GCS/local FS).
    pub data_store: DataStore,
    /// OpenTelemetry meter for recording GC metrics.
    pub meter: Option<Meter>,
}
