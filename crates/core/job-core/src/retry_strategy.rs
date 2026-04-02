//! Typed retry strategy configuration
//!
//! Deserialized from the `retry_strategy` field inside the job descriptor JSONB.
//! The metadata-db layer stores this as opaque JSON; this module provides the
//! typed interpretation used by the scheduler.

use std::time::Duration;

/// Retry strategy for a job, deserialized from the descriptor's `retry_strategy` field.
///
/// Each variant carries only the parameters relevant to that strategy.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "strategy", rename_all = "snake_case")]
pub enum RetryStrategy {
    /// Never retry — transition to FATAL on first failure.
    None,

    /// Retry up to `max_attempts` times, then mark FATAL.
    Bounded {
        /// Maximum number of retry attempts (required).
        max_attempts: u32,
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

/// Backoff algorithm with variant-specific parameters.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Backoff {
    /// Fixed delay: `delay = base_delay_secs` for every attempt.
    Fixed {
        #[serde(default = "default_base_delay_secs")]
        base_delay_secs: u64,
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

/// Shared parameters for exponential backoff variants.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ExponentialParams {
    #[serde(default = "default_base_delay_secs")]
    pub base_delay_secs: u64,
    #[serde(default = "default_multiplier")]
    pub multiplier: f64,
    /// Optional cap on computed delay (seconds).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_delay_secs: Option<u64>,
}

fn default_base_delay_secs() -> u64 {
    1
}

fn default_multiplier() -> f64 {
    2.0
}

impl ExponentialParams {
    fn compute_delay(&self, attempt_index: u32) -> Duration {
        let base = self.base_delay_secs as f64;
        let cap = self.max_delay_secs.map(|c| c as f64).unwrap_or(f64::MAX);
        // Clamp exponent to avoid f64 overflow → infinity → Duration panic.
        let exp = attempt_index.min(63) as i32;
        let raw = base * self.multiplier.powi(exp);
        Duration::from_secs_f64(raw.clamp(0.0, cap))
    }
}

impl Default for ExponentialParams {
    fn default() -> Self {
        Self {
            base_delay_secs: default_base_delay_secs(),
            multiplier: default_multiplier(),
            max_delay_secs: None,
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

impl Default for RetryStrategy {
    /// Matches current behavior: unlimited retries with exponential backoff.
    fn default() -> Self {
        Self::UnlessStopped {
            backoff: Backoff::default(),
        }
    }
}

impl Backoff {
    /// Compute the backoff delay for a given attempt index (0-based).
    /// Does NOT include jitter — caller applies jitter for `ExponentialWithJitter`.
    fn compute_delay(&self, attempt_index: u32) -> Duration {
        match self {
            Backoff::Fixed { base_delay_secs } => Duration::from_secs(*base_delay_secs),
            Backoff::Exponential { params } | Backoff::ExponentialWithJitter { params } => {
                params.compute_delay(attempt_index)
            }
        }
    }
}

impl RetryStrategy {
    /// Parse a `RetryStrategy` from a job descriptor JSON string.
    ///
    /// Extracts the `retry_strategy` field from the descriptor object and
    /// deserializes it. Returns `None` if the field is missing or malformed.
    pub fn from_descriptor(descriptor_json: &str) -> Option<Self> {
        let value: serde_json::Value = serde_json::from_str(descriptor_json).ok()?;
        let retry_obj = value.get("retry_strategy")?;
        serde_json::from_value(retry_obj.clone()).ok()
    }

    /// Returns `true` if retries are exhausted for the given attempt index (0-based).
    pub fn is_exhausted(&self, attempt_index: u32) -> bool {
        match self {
            RetryStrategy::None => true,
            RetryStrategy::Bounded { max_attempts, .. } => attempt_index >= *max_attempts,
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
            backoff: Backoff::Fixed { base_delay_secs: 5 },
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
                params: ExponentialParams {
                    max_delay_secs: Some(60),
                    ..ExponentialParams::default()
                },
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
                params: ExponentialParams {
                    max_delay_secs: Some(3600),
                    ..ExponentialParams::default()
                },
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
            max_attempts: 3,
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
                max_attempts: 5,
                backoff: Backoff::Fixed {
                    base_delay_secs: 10
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
            max_attempts: 3,
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
            max_attempts: 3,
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
            backoff: Backoff::Fixed { base_delay_secs: 5 },
        };

        //* Then
        assert!(
            !strategy.needs_jitter(),
            "Fixed backoff should not need jitter"
        );
    }

    #[test]
    fn retry_strategy_serde_round_trip_preserves_values() {
        //* Given
        let strategy = RetryStrategy::Bounded {
            max_attempts: 5,
            backoff: Backoff::ExponentialWithJitter {
                params: ExponentialParams {
                    base_delay_secs: 2,
                    multiplier: 3.0,
                    max_delay_secs: Some(120),
                },
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
