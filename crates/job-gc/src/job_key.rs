//! Idempotency key computation for garbage collection jobs.
//!
//! Computes idempotency keys by combining the GC job kind discriminator
//! with a location ID, producing a deterministic hash that prevents duplicate
//! job scheduling for the same physical table revision.

use amp_job_core::job_key::{self, JobKey};
use metadata_db::physical_table_revision::LocationId;

use crate::job_kind::JOB_KIND;

/// Compute an idempotency key for a GC job.
///
/// The key is derived by hashing `{job_kind}:{location_id}`,
/// producing a deterministic 64-character hex string.
pub fn idempotency_key(location_id: LocationId) -> JobKey {
    job_key::hash(format!("{JOB_KIND}:{location_id}"))
}
