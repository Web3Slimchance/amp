//! Kafka client for job event streaming.
//!
//! This module provides a Kafka producer for emitting job lifecycle events
//! (sync.started, sync.progress, sync.completed, sync.failed) to a Kafka topic.
//!
//! Events are encoded using Protocol Buffers for compact, schema-enforced messages.
//!
//! # Example
//!
//! ```ignore
//! use amp_config::KafkaEventsConfig;
//! use amp_job_core::kafka::KafkaProducer;
//!
//! let config = KafkaEventsConfig {
//!     brokers: vec!["localhost:9092".to_string()],
//!     topic: "amp.worker.events".to_string(),
//!     partitions: 16,
//! };
//!
//! let producer = KafkaProducer::new(&config).await?;
//!
//! // Send an event with partition key and protobuf payload
//! producer.send("ethereum/mainnet/abc123/blocks", &encoded_event).await?;
//! ```

mod client;

pub use client::{Error, KafkaConfig, KafkaProducer};
