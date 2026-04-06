//! Job event streaming module.
//!
//! This module provides infrastructure for emitting job lifecycle events
//! (sync.started, sync.progress, sync.completed, sync.failed) to external systems.

mod emitter;
#[cfg(feature = "events-kafka")]
mod kafka;
#[cfg(feature = "events-kafka")]
mod progress_adapter;

pub use self::emitter::{EventEmitter, NoOpEmitter};
#[cfg(feature = "events-kafka")]
pub use self::{kafka::KafkaEventEmitter, progress_adapter::JobProgressReporter};
