//! Idempotency key computation for raw dataset materialization jobs
//!
//! This module computes idempotency keys by combining the raw job kind discriminator
//! with a dataset reference, producing a deterministic hash that prevents duplicate
//! job scheduling for the same dataset version.

use amp_job_core::job_key::{self, JobKey};
use datasets_common::hash_reference::HashReference;

use crate::job_kind::JOB_KIND;

/// Compute an idempotency key for a raw materialization job.
///
/// The key is derived by hashing `{job_kind}:{namespace}/{name}@{manifest_hash}`,
/// producing a deterministic 64-character hex string.
pub fn idempotency_key(reference: &HashReference) -> JobKey {
    job_key::hash(format!("{JOB_KIND}:{reference}"))
}
