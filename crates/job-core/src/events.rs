//! Job event streaming module.
//!
//! This module provides infrastructure for emitting job lifecycle events
//! (sync.started, sync.progress, sync.completed, sync.failed) to external systems.

mod emitter;
#[cfg(feature = "events-kafka")]
mod kafka;
mod progress_adapter;

#[cfg(feature = "events-kafka")]
pub use self::kafka::KafkaEventEmitter;
pub use self::{
    emitter::{EventEmitter, NoOpEmitter},
    progress_adapter::JobProgressReporter,
};
