//! Typed retry strategy configuration
//!
//! Deserialized from the `retry_strategy` field inside the job descriptor JSONB.
//! The metadata-db layer stores this as opaque JSON; this module provides the
//! typed interpretation used by the scheduler.

use std::{fmt, time::Duration};

use serde::de;

/// Retry strategy for a job, deserialized from the descriptor's `retry_strategy` field.
///
/// Each variant carries only the parameters relevant to that strategy.
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "strategy", rename_all = "snake_case")]
pub enum RetryStrategy {
    /// Never retry — transition to FATAL on first failure.
    None,

    /// Retry up to `max_attempts` times, then mark FATAL.
    Bounded {
        /// Maximum number of retry attempts (required, >= 1).
        max_attempts: MaxAttempts,
        /// Backoff configuration. Defaults to exponential (base=1s, mult=2.0).
        #[serde(default)]
        backoff: Backoff,
    },

    /// Retry indefinitely until the job is explicitly stopped.
    UnlessStopped {
        /// Backoff configuration. Defaults to exponential (base=1s, mult=2.0).
        #[serde(default)]
        backoff: Backoff,
    },
}

impl RetryStrategy {
    /// Parse a `RetryStrategy` from a job descriptor JSON string.
    ///
    /// Extracts the `retry_strategy` field from the descriptor object and
    /// deserializes it. Returns `None` if the field is missing or malformed.
    pub fn from_descriptor(descriptor_json: &str) -> Option<Self> {
        #[derive(serde::Deserialize)]
        struct Extract {
            retry_strategy: Option<RetryStrategy>,
        }
        serde_json::from_str::<Extract>(descriptor_json)
            .ok()?
            .retry_strategy
    }

    /// Returns `true` if retries are exhausted for the given attempt index (0-based).
    pub fn is_exhausted(&self, attempt_index: u32) -> bool {
        match self {
            RetryStrategy::None => true,
            RetryStrategy::Bounded { max_attempts, .. } => attempt_index >= max_attempts.get(),
            RetryStrategy::UnlessStopped { .. } => false,
        }
    }

    /// Returns `true` if the backoff variant is `ExponentialWithJitter`.
    pub fn needs_jitter(&self) -> bool {
        matches!(
            self,
            RetryStrategy::Bounded {
                backoff: Backoff::ExponentialWithJitter { .. },
                ..
            } | RetryStrategy::UnlessStopped {
                backoff: Backoff::ExponentialWithJitter { .. }
            }
        )
    }

    /// Compute the backoff delay for a given attempt index (0-based).
    /// Does NOT include jitter — caller applies jitter for `ExponentialWithJitter`.
    pub fn compute_delay(&self, attempt_index: u32) -> Duration {
        match self {
            RetryStrategy::None => Duration::ZERO,
            RetryStrategy::Bounded { backoff, .. } | RetryStrategy::UnlessStopped { backoff } => {
                backoff.compute_delay(attempt_index)
            }
        }
    }
}

impl Default for RetryStrategy {
    /// Matches current behavior: unlimited retries with exponential backoff.
    fn default() -> Self {
        Self::UnlessStopped {
            backoff: Backoff::default(),
        }
    }
}

impl fmt::Display for RetryStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RetryStrategy::None => write!(f, "none"),
            RetryStrategy::Bounded { max_attempts, .. } => {
                write!(f, "bounded(max_attempts={})", max_attempts.get())
            }
            RetryStrategy::UnlessStopped { .. } => write!(f, "unless_stopped"),
        }
    }
}

/// Backoff algorithm with variant-specific parameters.
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Backoff {
    /// Fixed delay: `delay = base_delay_secs` for every attempt.
    Fixed {
        #[serde(default = "BaseDelaySecs::default")]
        base_delay_secs: BaseDelaySecs,
    },

    /// Exponential: `delay = base_delay_secs * multiplier ^ attempt_index`.
    Exponential {
        #[serde(flatten)]
        params: ExponentialParams,
    },

    /// Exponential base delay with bounded jitter applied by the caller.
    ExponentialWithJitter {
        #[serde(flatten)]
        params: ExponentialParams,
    },
}

impl Backoff {
    /// Compute the backoff delay for a given attempt index (0-based).
    /// Does NOT include jitter — caller applies jitter for `ExponentialWithJitter`.
    fn compute_delay(&self, attempt_index: u32) -> Duration {
        match self {
            Backoff::Fixed { base_delay_secs } => Duration::from_secs(base_delay_secs.get()),
            Backoff::Exponential { params } | Backoff::ExponentialWithJitter { params } => {
                params.compute_delay(attempt_index)
            }
        }
    }
}

impl Default for Backoff {
    fn default() -> Self {
        Self::Exponential {
            params: ExponentialParams::default(),
        }
    }
}

/// Shared parameters for exponential backoff variants.
///
/// Cross-field invariant: when `max_delay_secs` is present it must be
/// `>= base_delay_secs`. This is enforced by the custom `Deserialize` impl
/// and the `new()` constructor.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize)]
pub struct ExponentialParams {
    base_delay_secs: BaseDelaySecs,
    multiplier: Multiplier,
    /// Optional cap on computed delay (seconds).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_delay_secs: Option<u64>,
}

/// Errors when constructing [`ExponentialParams`] with invalid values
#[derive(Debug, thiserror::Error)]
pub enum ExponentialParamsError {
    /// The `base_delay_secs` value is invalid
    #[error("invalid base delay")]
    BaseDelay(#[source] BaseDelaySecsError),

    /// The `multiplier` value is invalid
    #[error("invalid multiplier")]
    Multiplier(#[source] MultiplierError),

    /// The `max_delay_secs` cap is less than `base_delay_secs`
    ///
    /// This makes the cap unreachable from the first attempt.
    #[error("max_delay_secs ({max}) must be >= base_delay_secs ({base})")]
    MaxDelayBelowBase {
        /// The provided max delay cap
        max: u64,
        /// The provided base delay
        base: u64,
    },
}

impl ExponentialParams {
    /// Create a new `ExponentialParams` with the given values.
    ///
    /// Returns an error if any field is invalid or `max_delay_secs < base_delay_secs`.
    pub fn new(
        base_delay_secs: u64,
        multiplier: f64,
        max_delay_secs: Option<u64>,
    ) -> Result<Self, ExponentialParamsError> {
        let base =
            BaseDelaySecs::new(base_delay_secs).map_err(ExponentialParamsError::BaseDelay)?;
        let mult = Multiplier::new(multiplier).map_err(ExponentialParamsError::Multiplier)?;

        if let Some(max) = max_delay_secs
            && max < base_delay_secs
        {
            return Err(ExponentialParamsError::MaxDelayBelowBase {
                max,
                base: base_delay_secs,
            });
        }

        Ok(Self {
            base_delay_secs: base,
            multiplier: mult,
            max_delay_secs,
        })
    }

    /// Returns the base delay in seconds.
    pub fn base_delay_secs(&self) -> u64 {
        self.base_delay_secs.get()
    }

    /// Returns the multiplier.
    pub fn multiplier(&self) -> f64 {
        self.multiplier.get()
    }

    /// Returns the optional maximum delay cap in seconds.
    pub fn max_delay_secs(&self) -> Option<u64> {
        self.max_delay_secs
    }

    fn compute_delay(&self, attempt_index: u32) -> Duration {
        let base = self.base_delay_secs.get() as f64;
        let cap = self.max_delay_secs.map(|c| c as f64).unwrap_or(f64::MAX);
        // Clamp exponent to avoid f64 overflow → infinity → Duration panic.
        let exp = attempt_index.min(63) as i32;
        let raw = base * self.multiplier.get().powi(exp);
        Duration::from_secs_f64(raw.clamp(0.0, cap))
    }
}

impl Eq for ExponentialParams {}

/// Helper for deserializing `ExponentialParams` with cross-field validation.
impl<'de> de::Deserialize<'de> for ExponentialParams {
    fn deserialize<D: de::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        /// Raw helper that mirrors the wire format without validation.
        #[derive(serde::Deserialize)]
        struct Raw {
            #[serde(default = "BaseDelaySecs::default")]
            base_delay_secs: BaseDelaySecs,
            #[serde(default = "Multiplier::default")]
            multiplier: Multiplier,
            #[serde(default)]
            max_delay_secs: Option<u64>,
        }

        let raw = Raw::deserialize(deserializer)?;

        if let Some(max) = raw.max_delay_secs
            && max < raw.base_delay_secs.get()
        {
            return Err(de::Error::custom(
                "max_delay_secs must be >= base_delay_secs",
            ));
        }

        Ok(Self {
            base_delay_secs: raw.base_delay_secs,
            multiplier: raw.multiplier,
            max_delay_secs: raw.max_delay_secs,
        })
    }
}

/// Maximum retry attempts for a bounded strategy. Must be at least 1.
///
/// Validates on construction and deserialization.
#[derive(Debug, Clone, Copy, Eq, PartialEq, serde::Serialize)]
#[serde(transparent)]
pub struct MaxAttempts(u32);

/// The provided `max_attempts` value is zero
///
/// A bounded retry strategy requires at least one attempt to be meaningful.
#[derive(Debug, thiserror::Error)]
#[error("max_attempts must be at least 1, got {value}")]
pub struct MaxAttemptsError {
    value: u32,
}

impl MaxAttempts {
    /// Create a new `MaxAttempts`, returning an error if the value is zero.
    pub fn new(value: u32) -> Result<Self, MaxAttemptsError> {
        if value == 0 {
            return Err(MaxAttemptsError { value });
        }
        Ok(Self(value))
    }

    /// Returns the inner value.
    pub fn get(self) -> u32 {
        self.0
    }
}

impl<'de> de::Deserialize<'de> for MaxAttempts {
    fn deserialize<D: de::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = u32::deserialize(deserializer)?;
        Self::new(value).map_err(de::Error::custom)
    }
}

/// Base delay in seconds. Must be at least 1.
///
/// Validates on construction and deserialization — invalid values are rejected
/// at parse time rather than via a post-hoc `validate()` call.
#[derive(Debug, Clone, Copy, Eq, PartialEq, serde::Serialize)]
#[serde(transparent)]
pub struct BaseDelaySecs(u64);

/// The provided `base_delay_secs` value is zero
///
/// A delay of zero seconds is meaningless for backoff configuration.
#[derive(Debug, thiserror::Error)]
#[error("base_delay_secs must be at least 1, got {value}")]
pub struct BaseDelaySecsError {
    value: u64,
}

impl BaseDelaySecs {
    /// Create a new `BaseDelaySecs`, returning an error if the value is zero.
    pub fn new(value: u64) -> Result<Self, BaseDelaySecsError> {
        if value == 0 {
            return Err(BaseDelaySecsError { value });
        }
        Ok(Self(value))
    }

    /// Returns the inner value.
    pub fn get(self) -> u64 {
        self.0
    }
}

impl Default for BaseDelaySecs {
    fn default() -> Self {
        Self(1)
    }
}

impl<'de> de::Deserialize<'de> for BaseDelaySecs {
    fn deserialize<D: de::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = u64::deserialize(deserializer)?;
        Self::new(value).map_err(de::Error::custom)
    }
}

/// Exponential backoff multiplier. Must be finite and positive.
///
/// Validates on construction and deserialization.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize)]
#[serde(transparent)]
pub struct Multiplier(f64);

/// The provided `multiplier` value is not a finite positive number
///
/// This occurs when the multiplier is NaN, infinite, zero, or negative.
#[derive(Debug, thiserror::Error)]
#[error("multiplier must be a finite positive number, got {value}")]
pub struct MultiplierError {
    value: f64,
}

impl Multiplier {
    /// Create a new `Multiplier`, returning an error if the value is not finite
    /// and positive.
    pub fn new(value: f64) -> Result<Self, MultiplierError> {
        if !value.is_finite() || value <= 0.0 {
            return Err(MultiplierError { value });
        }
        Ok(Self(value))
    }

    /// Returns the inner value.
    pub fn get(self) -> f64 {
        self.0
    }
}

impl Default for Multiplier {
    fn default() -> Self {
        Self(2.0)
    }
}

impl Eq for Multiplier {}

impl<'de> de::Deserialize<'de> for Multiplier {
    fn deserialize<D: de::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = f64::deserialize(deserializer)?;
        Self::new(value).map_err(de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_delay_with_default_strategy_returns_exponential_backoff() {
        //* Given
        let strategy = RetryStrategy::default();

        //* When
        let delay = strategy.compute_delay(0);

        //* Then
        assert_eq!(
            delay,
            Duration::from_secs(1),
            "attempt 0 should use base delay"
        );
    }

    #[test]
    fn compute_delay_with_default_strategy_doubles_each_attempt() {
        //* Given
        let strategy = RetryStrategy::default();

        //* When
        let delays: Vec<_> = (0..=3).map(|i| strategy.compute_delay(i)).collect();

        //* Then
        assert_eq!(
            delays,
            vec![
                Duration::from_secs(1),
                Duration::from_secs(2),
                Duration::from_secs(4),
                Duration::from_secs(8),
            ],
            "delays should follow 2^n exponential backoff"
        );
    }

    #[test]
    fn compute_delay_with_fixed_backoff_returns_constant_delay() {
        //* Given
        let strategy = RetryStrategy::UnlessStopped {
            backoff: Backoff::Fixed {
                base_delay_secs: BaseDelaySecs::new(5).unwrap(),
            },
        };

        //* When
        let delay_first = strategy.compute_delay(0);
        let delay_late = strategy.compute_delay(99);

        //* Then
        assert_eq!(
            delay_first,
            Duration::from_secs(5),
            "first attempt should use base delay"
        );
        assert_eq!(
            delay_late,
            Duration::from_secs(5),
            "late attempt should still use base delay"
        );
    }

    #[test]
    fn compute_delay_with_max_delay_cap_clamps_to_cap() {
        //* Given
        let strategy = RetryStrategy::UnlessStopped {
            backoff: Backoff::Exponential {
                params: ExponentialParams::new(1, 2.0, Some(60)).unwrap(),
            },
        };

        //* When
        let delay = strategy.compute_delay(10);

        //* Then
        assert_eq!(
            delay,
            Duration::from_secs(60),
            "delay should be capped at max_delay_secs"
        );
    }

    #[test]
    fn compute_delay_with_large_attempt_index_does_not_panic() {
        //* Given
        let strategy = RetryStrategy::UnlessStopped {
            backoff: Backoff::Exponential {
                params: ExponentialParams::new(1, 2.0, Some(3600)).unwrap(),
            },
        };

        //* When
        let delay = strategy.compute_delay(10_000);

        //* Then
        assert_eq!(
            delay,
            Duration::from_secs(3600),
            "should clamp to cap without panicking"
        );
    }

    #[test]
    fn compute_delay_with_none_strategy_returns_zero() {
        //* When
        let delay = RetryStrategy::None.compute_delay(0);

        //* Then
        assert_eq!(
            delay,
            Duration::ZERO,
            "none strategy should return zero delay"
        );
    }

    #[test]
    fn is_exhausted_with_none_strategy_returns_true() {
        //* Given
        let strategy = RetryStrategy::None;

        //* When
        let exhausted = strategy.is_exhausted(0);

        //* Then
        assert!(exhausted, "none strategy should be exhausted immediately");
    }

    #[test]
    fn is_exhausted_with_bounded_strategy_respects_max_attempts() {
        //* Given
        let strategy = RetryStrategy::Bounded {
            max_attempts: MaxAttempts::new(3).unwrap(),
            backoff: Backoff::default(),
        };

        //* When
        let before_limit = strategy.is_exhausted(2);
        let at_limit = strategy.is_exhausted(3);

        //* Then
        assert!(
            !before_limit,
            "should not be exhausted before reaching max_attempts"
        );
        assert!(at_limit, "should be exhausted at max_attempts");
    }

    #[test]
    fn is_exhausted_with_unless_stopped_returns_false() {
        //* When
        let exhausted = RetryStrategy::default().is_exhausted(1000);

        //* Then
        assert!(!exhausted, "unless_stopped should never be exhausted");
    }

    #[test]
    fn from_descriptor_with_valid_json_returns_strategy() {
        //* Given
        let descriptor = r#"{"retry_strategy":{"strategy":"bounded","max_attempts":5,"backoff":{"kind":"fixed","base_delay_secs":10}}}"#;

        //* When
        let strategy = RetryStrategy::from_descriptor(descriptor);

        //* Then
        assert_eq!(
            strategy,
            Some(RetryStrategy::Bounded {
                max_attempts: MaxAttempts::new(5).unwrap(),
                backoff: Backoff::Fixed {
                    base_delay_secs: BaseDelaySecs::new(10).unwrap(),
                },
            }),
            "should parse retry_strategy from descriptor JSON"
        );
    }

    #[test]
    fn from_descriptor_without_retry_strategy_field_returns_none() {
        //* Given
        let descriptor = r#"{"test":"job"}"#;

        //* When
        let strategy = RetryStrategy::from_descriptor(descriptor);

        //* Then
        assert_eq!(
            strategy, None,
            "should return None when retry_strategy field is absent"
        );
    }

    #[test]
    fn from_descriptor_with_invalid_json_returns_none() {
        //* When
        let strategy = RetryStrategy::from_descriptor("not json");

        //* Then
        assert_eq!(strategy, None, "should return None for malformed JSON");
    }

    #[test]
    fn needs_jitter_with_exponential_with_jitter_returns_true() {
        //* Given
        let strategy = RetryStrategy::Bounded {
            max_attempts: MaxAttempts::new(3).unwrap(),
            backoff: Backoff::ExponentialWithJitter {
                params: ExponentialParams::default(),
            },
        };

        //* Then
        assert!(
            strategy.needs_jitter(),
            "ExponentialWithJitter should need jitter"
        );
    }

    #[test]
    fn needs_jitter_with_exponential_returns_false() {
        //* Given
        let strategy = RetryStrategy::Bounded {
            max_attempts: MaxAttempts::new(3).unwrap(),
            backoff: Backoff::Exponential {
                params: ExponentialParams::default(),
            },
        };

        //* Then
        assert!(
            !strategy.needs_jitter(),
            "plain Exponential should not need jitter"
        );
    }

    #[test]
    fn needs_jitter_with_fixed_returns_false() {
        //* Given
        let strategy = RetryStrategy::UnlessStopped {
            backoff: Backoff::Fixed {
                base_delay_secs: BaseDelaySecs::new(5).unwrap(),
            },
        };

        //* Then
        assert!(
            !strategy.needs_jitter(),
            "Fixed backoff should not need jitter"
        );
    }

    // --- Deserialization rejection tests (replaces validate() tests) ---

    #[test]
    fn deserialize_rejects_zero_max_attempts() {
        //* Given
        let json = r#"{"strategy":"bounded","max_attempts":0}"#;

        //* When
        let result = serde_json::from_str::<RetryStrategy>(json);

        //* Then
        let err = result.expect_err("zero max_attempts should be rejected at parse time");
        assert!(
            err.to_string().contains("max_attempts must be at least 1"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn deserialize_rejects_nan_multiplier() {
        //* Given — NaN is not valid JSON, so we test via the constructor
        let result = Multiplier::new(f64::NAN);

        //* Then
        assert!(
            result.is_err(),
            "NaN multiplier should be rejected at construction"
        );
    }

    #[test]
    fn deserialize_rejects_negative_multiplier() {
        //* Given
        let json = r#"{"strategy":"bounded","max_attempts":3,"backoff":{"kind":"exponential","multiplier":-1.0}}"#;

        //* When
        let result = serde_json::from_str::<RetryStrategy>(json);

        //* Then
        let err = result.expect_err("negative multiplier should be rejected at parse time");
        assert!(
            err.to_string()
                .contains("multiplier must be a finite positive number"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn deserialize_rejects_zero_base_delay() {
        //* Given
        let json =
            r#"{"strategy":"unless_stopped","backoff":{"kind":"fixed","base_delay_secs":0}}"#;

        //* When
        let result = serde_json::from_str::<RetryStrategy>(json);

        //* Then
        let err = result.expect_err("zero base delay should be rejected at parse time");
        assert!(
            err.to_string()
                .contains("base_delay_secs must be at least 1"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn deserialize_rejects_max_delay_less_than_base_delay() {
        //* Given
        let json = r#"{"strategy":"bounded","max_attempts":3,"backoff":{"kind":"exponential","base_delay_secs":60,"max_delay_secs":10}}"#;

        //* When
        let result = serde_json::from_str::<RetryStrategy>(json);

        //* Then
        let err = result.expect_err("max_delay < base_delay should be rejected at parse time");
        assert!(
            err.to_string()
                .contains("max_delay_secs must be >= base_delay_secs"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn deserialize_accepts_valid_bounded_strategy() {
        //* Given
        let json = r#"{"strategy":"bounded","max_attempts":5,"backoff":{"kind":"exponential","base_delay_secs":1,"multiplier":2.0,"max_delay_secs":60}}"#;

        //* When
        let result = serde_json::from_str::<RetryStrategy>(json);

        //* Then
        assert!(result.is_ok(), "valid strategy should parse successfully");
    }

    #[test]
    fn deserialize_accepts_none_strategy() {
        //* Given
        let json = r#"{"strategy":"none"}"#;

        //* When
        let result = serde_json::from_str::<RetryStrategy>(json);

        //* Then
        assert!(result.is_ok(), "none strategy should parse successfully");
    }

    #[test]
    fn retry_strategy_serde_round_trip_preserves_values() {
        //* Given
        let strategy = RetryStrategy::Bounded {
            max_attempts: MaxAttempts::new(5).unwrap(),
            backoff: Backoff::ExponentialWithJitter {
                params: ExponentialParams::new(2, 3.0, Some(120)).unwrap(),
            },
        };

        //* When
        let json = serde_json::to_string(&strategy).expect("serialization should succeed");
        let deserialized: RetryStrategy =
            serde_json::from_str(&json).expect("deserialization should succeed");

        //* Then
        assert_eq!(
            strategy, deserialized,
            "round-trip should preserve all values"
        );
    }
}
