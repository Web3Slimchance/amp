//! Shared retry strategy CLI arguments for ampctl commands.
//!
//! Provides a reusable [`RetryStrategyArgs`] struct that can be flattened into
//! any command requiring retry strategy configuration.

use amp_job_core::retry_strategy::{Backoff, ExponentialParams, MaxAttempts, RetryStrategy};

/// CLI arguments for configuring a job's retry strategy.
///
/// Flatten this into any command's `Args` struct to add retry strategy flags.
/// Call [`RetryStrategyArgs::resolve`] to validate and convert to a
/// [`RetryStrategy`].
#[derive(Debug, clap::Args)]
pub struct RetryStrategyArgs {
    /// Retry strategy for the job
    ///
    /// Controls how failed jobs are retried:
    /// - "none": Never retry — fail immediately on first error
    /// - "bounded": Retry up to --retry-max-attempts times, then fail
    /// - "unless_stopped": Retry indefinitely until the job is explicitly stopped
    ///
    /// If not specified, the server's default strategy is used.
    #[arg(long)]
    pub retry_strategy: Option<StrategyKind>,

    /// Maximum number of retry attempts (required for "bounded" strategy)
    ///
    /// Must be at least 1. Only valid when --retry-strategy=bounded.
    #[arg(long)]
    pub retry_max_attempts: Option<u32>,

    /// Backoff algorithm for delays between retries
    ///
    /// Controls how delay grows between retry attempts:
    /// - "fixed": Constant delay of --retry-base-delay-secs every attempt
    /// - "exponential": base_delay * multiplier^attempt (default)
    /// - "exponential_with_jitter": Same as exponential with random jitter
    ///
    /// Defaults to "exponential" when a retry strategy with backoff is used.
    #[arg(long)]
    pub retry_backoff: Option<BackoffKind>,

    /// Base delay in seconds between retries (>= 1, default: 1)
    ///
    /// For fixed backoff, this is the constant delay.
    /// For exponential backoff, this is the initial delay before multiplier scaling.
    #[arg(long)]
    pub retry_base_delay_secs: Option<u64>,

    /// Backoff multiplier for exponential strategies (default: 2.0)
    ///
    /// Must be a positive finite number. The delay for attempt N is:
    /// base_delay_secs * multiplier^N
    ///
    /// Only applicable to exponential and exponential-with-jitter backoff.
    #[arg(long)]
    pub retry_multiplier: Option<f64>,

    /// Maximum delay cap in seconds for exponential backoff
    ///
    /// Prevents delays from growing unboundedly. Must be >= --retry-base-delay-secs.
    #[arg(long)]
    pub retry_max_delay_secs: Option<u64>,
}

/// Parsed retry strategy kind from CLI input.
#[derive(Debug, Clone, Copy)]
pub enum StrategyKind {
    /// Never retry.
    None,
    /// Retry up to a bounded number of times.
    Bounded,
    /// Retry indefinitely.
    UnlessStopped,
}

impl std::str::FromStr for StrategyKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "none" => Ok(Self::None),
            "bounded" => Ok(Self::Bounded),
            "unless_stopped" => Ok(Self::UnlessStopped),
            _ => Err(format!(
                "invalid retry strategy '{s}': expected one of: none, bounded, unless_stopped"
            )),
        }
    }
}

/// Parsed backoff kind from CLI input.
#[derive(Debug, Clone, Copy)]
pub enum BackoffKind {
    /// Fixed delay between retries.
    Fixed,
    /// Exponential backoff.
    Exponential,
    /// Exponential backoff with jitter.
    ExponentialWithJitter,
}

impl std::str::FromStr for BackoffKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "fixed" => Ok(Self::Fixed),
            "exponential" => Ok(Self::Exponential),
            "exponential_with_jitter" => Ok(Self::ExponentialWithJitter),
            _ => Err(format!(
                "invalid backoff kind '{s}': expected one of: fixed, exponential, exponential_with_jitter"
            )),
        }
    }
}

/// Errors from resolving retry strategy CLI arguments.
#[derive(Debug, thiserror::Error)]
pub enum ResolveError {
    /// Required `--retry-max-attempts` flag was not provided
    ///
    /// This occurs when `--retry-strategy=bounded` is specified without
    /// the required `--retry-max-attempts` flag.
    #[error("--retry-max-attempts is required when --retry-strategy=bounded")]
    MaxAttemptsRequired,

    /// `--retry-max-attempts` was provided for a non-bounded strategy
    ///
    /// This occurs when `--retry-max-attempts` is specified alongside
    /// `--retry-strategy=none` or `--retry-strategy=unless-stopped`, which
    /// do not use a max attempts limit.
    #[error("--retry-max-attempts is only valid when --retry-strategy=bounded")]
    MaxAttemptsNotApplicable,

    /// Backoff flags were provided without a backoff-capable strategy
    ///
    /// This occurs when backoff-related flags are specified without
    /// `--retry-strategy` or with `--retry-strategy=none`, which does not
    /// support backoff configuration.
    #[error(
        "backoff flags (--retry-backoff, --retry-base-delay-secs, --retry-multiplier, --retry-max-delay-secs) are only valid when --retry-strategy is bounded or unless_stopped"
    )]
    BackoffNotApplicable,

    /// The `--retry-max-attempts` value failed validation
    ///
    /// This occurs when the provided max attempts value is zero.
    #[error("invalid --retry-max-attempts")]
    InvalidMaxAttempts(#[source] amp_job_core::retry_strategy::MaxAttemptsError),

    /// Backoff parameter validation failed
    ///
    /// This occurs when backoff parameters are individually invalid (e.g.,
    /// zero base delay) or violate cross-field constraints (e.g.,
    /// `--retry-max-delay-secs` less than `--retry-base-delay-secs`).
    #[error("invalid backoff parameters")]
    InvalidExponentialParams(#[source] amp_job_core::retry_strategy::ExponentialParamsError),
}

impl RetryStrategyArgs {
    /// Validate and convert CLI arguments into an optional [`RetryStrategy`].
    ///
    /// Returns `Ok(None)` when `--retry-strategy` is not provided.
    pub fn resolve(&self) -> Result<Option<RetryStrategy>, ResolveError> {
        let Some(kind) = self.retry_strategy else {
            // No strategy specified — check no stray backoff flags were passed.
            if self.has_any_backoff_flag() || self.retry_max_attempts.is_some() {
                // If someone passed backoff flags without a strategy,
                // Return error
                return Err(ResolveError::BackoffNotApplicable);
            }
            return Ok(None);
        };

        match kind {
            StrategyKind::None => {
                if self.retry_max_attempts.is_some() {
                    return Err(ResolveError::MaxAttemptsNotApplicable);
                }
                if self.has_any_backoff_flag() {
                    return Err(ResolveError::BackoffNotApplicable);
                }
                Ok(Some(RetryStrategy::None))
            }
            StrategyKind::Bounded => {
                let Some(raw_attempts) = self.retry_max_attempts else {
                    return Err(ResolveError::MaxAttemptsRequired);
                };
                let max_attempts =
                    MaxAttempts::new(raw_attempts).map_err(ResolveError::InvalidMaxAttempts)?;
                let backoff = self.resolve_backoff()?;
                Ok(Some(RetryStrategy::Bounded {
                    max_attempts,
                    backoff,
                }))
            }
            StrategyKind::UnlessStopped => {
                if self.retry_max_attempts.is_some() {
                    return Err(ResolveError::MaxAttemptsNotApplicable);
                }
                let backoff = self.resolve_backoff()?;
                Ok(Some(RetryStrategy::UnlessStopped { backoff }))
            }
        }
    }

    /// Build a [`Backoff`] from the backoff-related flags, using defaults where
    /// flags are omitted.
    fn resolve_backoff(&self) -> Result<Backoff, ResolveError> {
        let backoff_kind = self.retry_backoff.unwrap_or(BackoffKind::Exponential);
        let base = self.retry_base_delay_secs.unwrap_or(1);
        let multiplier = self.retry_multiplier.unwrap_or(2.0);
        let max_delay = self.retry_max_delay_secs;

        match backoff_kind {
            BackoffKind::Fixed => {
                let base_delay_secs = amp_job_core::retry_strategy::BaseDelaySecs::new(base)
                    .map_err(|err| {
                        ResolveError::InvalidExponentialParams(
                            amp_job_core::retry_strategy::ExponentialParamsError::BaseDelay(err),
                        )
                    })?;
                Ok(Backoff::Fixed { base_delay_secs })
            }
            BackoffKind::Exponential => {
                let params = ExponentialParams::new(base, multiplier, max_delay)
                    .map_err(ResolveError::InvalidExponentialParams)?;
                Ok(Backoff::Exponential { params })
            }
            BackoffKind::ExponentialWithJitter => {
                let params = ExponentialParams::new(base, multiplier, max_delay)
                    .map_err(ResolveError::InvalidExponentialParams)?;
                Ok(Backoff::ExponentialWithJitter { params })
            }
        }
    }

    /// Returns `true` if any backoff-related flag was explicitly provided.
    fn has_any_backoff_flag(&self) -> bool {
        self.retry_backoff.is_some()
            || self.retry_base_delay_secs.is_some()
            || self.retry_multiplier.is_some()
            || self.retry_max_delay_secs.is_some()
    }
}

#[cfg(test)]
mod tests {
    use amp_job_core::retry_strategy::{Backoff, RetryStrategy};

    use super::*;

    /// Helper to build [`RetryStrategyArgs`] with all fields defaulted to `None`.
    fn args() -> RetryStrategyArgs {
        RetryStrategyArgs {
            retry_strategy: None,
            retry_max_attempts: None,
            retry_backoff: None,
            retry_base_delay_secs: None,
            retry_multiplier: None,
            retry_max_delay_secs: None,
        }
    }

    #[test]
    fn resolve_with_no_flags_returns_none() {
        //* When
        let result = args().resolve();

        //* Then
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn resolve_with_strategy_none_returns_none_variant() {
        //* Given
        let mut a = args();
        a.retry_strategy = Some(StrategyKind::None);

        //* When
        let result = a.resolve();

        //* Then
        assert_eq!(result.unwrap(), Some(RetryStrategy::None));
    }

    #[test]
    fn resolve_with_bounded_and_max_attempts_returns_bounded() {
        //* Given
        let mut a = args();
        a.retry_strategy = Some(StrategyKind::Bounded);
        a.retry_max_attempts = Some(3);

        //* When
        let result = a.resolve();

        //* Then
        let strategy = result.unwrap().expect("should return Some");
        assert!(
            matches!(strategy, RetryStrategy::Bounded { .. }),
            "expected Bounded variant"
        );
    }

    #[test]
    fn resolve_with_bounded_uses_default_exponential_backoff() {
        //* Given
        let mut a = args();
        a.retry_strategy = Some(StrategyKind::Bounded);
        a.retry_max_attempts = Some(5);

        //* When
        let result = a.resolve();

        //* Then
        let strategy = result.unwrap().expect("should return Some");
        match strategy {
            RetryStrategy::Bounded { backoff, .. } => {
                assert!(
                    matches!(backoff, Backoff::Exponential { .. }),
                    "expected default Exponential backoff"
                );
            }
            _ => panic!("expected Bounded variant"),
        }
    }

    #[test]
    fn resolve_with_bounded_without_max_attempts_fails() {
        //* Given
        let mut a = args();
        a.retry_strategy = Some(StrategyKind::Bounded);

        //* When
        let result = a.resolve();

        //* Then
        assert!(
            matches!(result, Err(ResolveError::MaxAttemptsRequired)),
            "expected MaxAttemptsRequired error"
        );
    }

    #[test]
    fn resolve_with_max_attempts_on_none_strategy_fails() {
        //* Given
        let mut a = args();
        a.retry_strategy = Some(StrategyKind::None);
        a.retry_max_attempts = Some(3);

        //* When
        let result = a.resolve();

        //* Then
        assert!(
            matches!(result, Err(ResolveError::MaxAttemptsNotApplicable)),
            "expected MaxAttemptsNotApplicable error"
        );
    }

    #[test]
    fn resolve_with_max_attempts_on_unless_stopped_fails() {
        //* Given
        let mut a = args();
        a.retry_strategy = Some(StrategyKind::UnlessStopped);
        a.retry_max_attempts = Some(3);

        //* When
        let result = a.resolve();

        //* Then
        assert!(
            matches!(result, Err(ResolveError::MaxAttemptsNotApplicable)),
            "expected MaxAttemptsNotApplicable error"
        );
    }

    #[test]
    fn resolve_with_backoff_flags_on_none_strategy_fails() {
        //* Given
        let mut a = args();
        a.retry_strategy = Some(StrategyKind::None);
        a.retry_backoff = Some(BackoffKind::Fixed);

        //* When
        let result = a.resolve();

        //* Then
        assert!(
            matches!(result, Err(ResolveError::BackoffNotApplicable)),
            "expected BackoffNotApplicable error"
        );
    }

    #[test]
    fn resolve_with_backoff_flags_without_strategy_fails() {
        //* Given
        let mut a = args();
        a.retry_base_delay_secs = Some(5);

        //* When
        let result = a.resolve();

        //* Then
        assert!(
            matches!(result, Err(ResolveError::BackoffNotApplicable)),
            "expected BackoffNotApplicable error"
        );
    }

    #[test]
    fn resolve_with_unless_stopped_and_fixed_backoff_returns_correct_variant() {
        //* Given
        let mut a = args();
        a.retry_strategy = Some(StrategyKind::UnlessStopped);
        a.retry_backoff = Some(BackoffKind::Fixed);
        a.retry_base_delay_secs = Some(10);

        //* When
        let result = a.resolve();

        //* Then
        let strategy = result.unwrap().expect("should return Some");
        match strategy {
            RetryStrategy::UnlessStopped {
                backoff: Backoff::Fixed { base_delay_secs },
            } => {
                assert_eq!(base_delay_secs.get(), 10);
            }
            _ => panic!("expected UnlessStopped with Fixed backoff"),
        }
    }

    #[test]
    fn resolve_with_max_delay_below_base_delay_fails() {
        //* Given
        let mut a = args();
        a.retry_strategy = Some(StrategyKind::Bounded);
        a.retry_max_attempts = Some(3);
        a.retry_base_delay_secs = Some(60);
        a.retry_max_delay_secs = Some(10);

        //* When
        let result = a.resolve();

        //* Then
        assert!(
            matches!(result, Err(ResolveError::InvalidExponentialParams(_))),
            "expected InvalidExponentialParams error"
        );
    }

    #[test]
    fn resolve_with_zero_max_attempts_fails() {
        //* Given
        let mut a = args();
        a.retry_strategy = Some(StrategyKind::Bounded);
        a.retry_max_attempts = Some(0);

        //* When
        let result = a.resolve();

        //* Then
        assert!(
            matches!(result, Err(ResolveError::InvalidMaxAttempts(_))),
            "expected InvalidMaxAttempts error"
        );
    }

    #[test]
    fn strategy_kind_from_str_with_valid_inputs_succeeds() {
        //* Then
        assert!(matches!(
            "none".parse::<StrategyKind>(),
            Ok(StrategyKind::None)
        ));
        assert!(matches!(
            "bounded".parse::<StrategyKind>(),
            Ok(StrategyKind::Bounded)
        ));
        assert!(matches!(
            "unless_stopped".parse::<StrategyKind>(),
            Ok(StrategyKind::UnlessStopped)
        ));
    }

    #[test]
    fn strategy_kind_from_str_with_invalid_input_fails() {
        //* When
        let result = "invalid".parse::<StrategyKind>();

        //* Then
        assert!(result.is_err());
    }

    #[test]
    fn backoff_kind_from_str_with_valid_inputs_succeeds() {
        //* Then
        assert!(matches!(
            "fixed".parse::<BackoffKind>(),
            Ok(BackoffKind::Fixed)
        ));
        assert!(matches!(
            "exponential".parse::<BackoffKind>(),
            Ok(BackoffKind::Exponential)
        ));
        assert!(matches!(
            "exponential_with_jitter".parse::<BackoffKind>(),
            Ok(BackoffKind::ExponentialWithJitter)
        ));
    }

    #[test]
    fn backoff_kind_from_str_with_invalid_input_fails() {
        //* When
        let result = "invalid".parse::<BackoffKind>();

        //* Then
        assert!(result.is_err());
    }
}
