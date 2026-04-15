//! Amp Controller Service
//!
//! The controller service provides the admin API for managing Amp operations.

pub mod build_info;
pub mod ctx;
pub mod handlers;
mod router;
mod scheduler;
pub mod service;
mod trigger;

#[cfg(feature = "utoipa")]
pub use router::generate_openapi_spec;
pub(crate) use router::router;
