//! # Job Core
//!
//! Shared job-related types used by both controller and worker services.
//! This crate provides the common vocabulary types for job management
//! without pulling in worker-specific dependencies.

pub mod job_id;
pub mod job_key;
pub mod retry_strategy;
pub mod status;
