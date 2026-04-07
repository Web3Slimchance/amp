//! Job error reporting: retryability classification and structured error details.
//!
//! This module consolidates two concerns:
//!
//! - **Retryability** ([`RetryableErrorExt`], [`JobErrorExt`]): classifying errors as transient
//!   (retryable) or permanent (fatal), with machine-readable error codes.
//! - **Structured details** ([`ErrorDetailsProvider`], [`ErrorDetailPayload`]): attaching
//!   key-value context to errors so failure information is persisted as structured JSONB.

/// Type-erased job error for heterogeneous job dispatch.
///
/// All concrete job error types implement [`Error`], so they can be boxed into
/// this type at the dispatch boundary without a wrapper enum.
pub type JobError = Box<dyn Error>;

/// Object-safe supertrait combining all job error capabilities.
///
/// Automatically implemented for any type satisfying the bounds, enabling
/// `Box<dyn JobError>` as an erased error type for heterogeneous job dispatch.
///
/// `Box<dyn JobError>` itself implements [`std::error::Error`], [`RetryableErrorExt`],
/// [`JobErrorExt`], and [`ErrorDetailsProvider`] by delegating through the vtable,
/// so it can be used seamlessly wherever those traits are expected.
pub trait Error: JobErrorExt + ErrorDetailsProvider + Send {}

impl<T: JobErrorExt + ErrorDetailsProvider + Send> Error for T {}

impl std::error::Error for Box<dyn Error> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        (**self).source()
    }
}

impl RetryableErrorExt for Box<dyn Error> {
    fn is_retryable(&self) -> bool {
        (**self).is_retryable()
    }
}

impl JobErrorExt for Box<dyn Error> {
    fn error_code(&self) -> &'static str {
        (**self).error_code()
    }
}

impl ErrorDetailsProvider for Box<dyn Error> {
    fn error_details(&self) -> serde_json::Map<String, serde_json::Value> {
        (**self).error_details()
    }

    fn detail_source(&self) -> Option<&dyn ErrorDetailsProvider> {
        (**self).detail_source()
    }
}

/// Extension trait for classifying errors as retryable or fatal.
///
/// The primary method is [`is_retryable`](RetryableErrorExt::is_retryable), which returns `true`
/// when the error is transient and the operation may succeed on retry. The default for
/// [`is_fatal`](RetryableErrorExt::is_fatal) is the logical inverse, so implementors only need to
/// define `is_retryable`.
///
/// **Fail-safe convention:** new or forgotten variants default to *non-retryable* (fatal).
/// This prevents infinite retry loops when a new error variant is added but `is_retryable`
/// is not updated to cover it.
pub trait RetryableErrorExt: std::error::Error {
    /// Returns `true` if the error is transient and the operation may succeed on retry.
    fn is_retryable(&self) -> bool;

    /// Returns `true` if the error is permanent and the operation should not be retried.
    fn is_fatal(&self) -> bool {
        !self.is_retryable()
    }
}

/// Extension of [`RetryableErrorExt`] that associates a machine-readable error code with each
/// error variant.
///
/// Error codes use `SCREAMING_SNAKE_CASE` and should uniquely identify the error variant within
/// the crate so that, when shared by a user or searched in the codebase, the exact error source
/// can be located.
pub trait JobErrorExt: RetryableErrorExt {
    /// A machine-readable error code identifying this specific error variant.
    fn error_code(&self) -> &'static str;
}

/// Structured error detail persisted as JSONB on job failure.
///
/// Serialized via `serde` and converted to [`metadata_db::job_events::EventDetail`]
/// at the service boundary.
///
/// The type parameter `T` allows job implementations to attach domain-specific
/// context (e.g. dataset reference, manifest hash). Defaults to `()` when no
/// extra context is needed.
#[derive(Debug, serde::Serialize)]
pub struct ErrorDetailPayload<T: serde::Serialize + serde::de::DeserializeOwned = ()> {
    /// Error code identifying the specific error variant
    /// (e.g. `"GET_DATASET"`, `"PARTITION_TASK"`).
    pub error_code: String,
    /// Human-readable error description from `Display` impl.
    pub error_message: String,
    /// Optional structured context accompanying the error.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_context: Option<ErrorContext<T>>,
}

/// Contextual information accompanying an error detail.
///
/// Contains the error's causal chain, retry metadata, and an extensible
/// `extra` field for job-specific data flattened into the JSON output.
#[derive(Debug, serde::Serialize)]
pub struct ErrorContext<T: serde::Serialize + serde::de::DeserializeOwned = ()> {
    /// Causal error chain from `std::error::Error::source()` traversal,
    /// ordered from immediate cause to root cause.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub stack_trace: Vec<String>,
    /// Retry attempt index at the time of failure.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attempt_index: Option<i32>,
    /// Job-specific extra context, flattened into the surrounding JSON object.
    ///
    /// Defaults to `()` (serializes to no fields). Job implementations supply
    /// a concrete type with `#[serde(skip_serializing_if)]` on its fields.
    #[serde(flatten)]
    pub extra: T,
}

/// Trait for errors that contribute structured details to error payloads.
///
/// Each error layer contributes only its own context via [`error_details`](Self::error_details).
/// The [`detail_source`](Self::detail_source) method chains to inner errors so a collector can
/// walk the typed chain without downcasting.
pub trait ErrorDetailsProvider {
    /// Key-value pairs this error layer contributes.
    ///
    /// Only this layer's own context -- never aggregate from inner errors.
    fn error_details(&self) -> serde_json::Map<String, serde_json::Value> {
        serde_json::Map::new()
    }

    /// Next error in the chain that also provides details.
    fn detail_source(&self) -> Option<&dyn ErrorDetailsProvider> {
        None
    }
}

/// Walk the [`ErrorDetailsProvider`] chain and merge all detail maps.
///
/// First occurrence wins -- the detail closest to the failure site takes priority
/// when the same key appears at multiple levels.
pub fn collect_error_details(
    provider: &dyn ErrorDetailsProvider,
) -> serde_json::Map<String, serde_json::Value> {
    let mut details = serde_json::Map::new();
    let mut current: Option<&dyn ErrorDetailsProvider> = Some(provider);
    while let Some(p) = current {
        for (k, v) in p.error_details() {
            details.entry(k).or_insert(v);
        }
        current = p.detail_source();
    }
    details
}
