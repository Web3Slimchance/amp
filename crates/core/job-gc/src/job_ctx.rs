//! Context for GC job execution.

use amp_data_store::DataStore;
use metadata_db::MetadataDb;
use monitoring::telemetry::metrics::Meter;

/// Dependencies required to execute a GC job.
pub struct Context {
    /// Connection to the metadata database (Postgres).
    pub metadata_db: MetadataDb,
    /// Connection to object storage (S3/GCS/local FS).
    pub data_store: DataStore,
    /// OpenTelemetry meter for recording GC metrics.
    pub meter: Option<Meter>,
}
