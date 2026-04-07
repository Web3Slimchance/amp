//! # Job Core
//!
//! Shared job-related types used by both controller and worker services.
//! This crate provides the common vocabulary types for job management
//! without pulling in worker-specific dependencies.

pub mod ctx;
pub mod error;
pub mod events;
pub mod job_id;
pub mod job_key;
#[cfg(feature = "events-kafka")]
pub mod kafka;
pub mod materialize;
pub mod proto;
pub mod retry_strategy;
pub mod status;
pub mod trigger;
