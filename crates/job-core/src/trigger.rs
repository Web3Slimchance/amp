//! Typed trigger configuration
//!
//! Deserialized from the `trigger` field inside the job descriptor JSONB.
//! The metadata-db layer stores this as opaque JSON; this module provides the
//! typed interpretation used by the scheduler to decide *when* a job should be
//! scheduled.

use std::fmt;

use chrono::{DateTime, FixedOffset, Utc};
use serde::de;

/// Trigger configuration for a job, deserialized from the descriptor's `trigger` field.
///
/// Determines when the scheduler should create a `SCHEDULED` event for a job.
/// Each variant carries only the parameters relevant to that trigger type.
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Trigger {
    /// Schedule immediately once after creation (or after being enabled).
    /// No periodic re-scheduling after success.
    OneShot,

    /// Fixed-cadence scheduling: next fire time is `created_at + n * every_secs`.
    ///
    /// If a previous run is still `RUNNING` when the next tick fires, the tick
    /// is skipped (single-flight).
    Interval {
        /// Duration between triggers in seconds. Must be >= 1.
        every_secs: IntervalSecs,
    },

    /// Cron-based scheduling on cron boundaries.
    ///
    /// By default, only the next upcoming fire time is scheduled (no catch-up).
    Cron {
        /// Crontab expression (5-field standard format).
        schedule: CronSchedule,
        /// UTC offset (e.g., "+00:00", "+05:30"). Defaults to "+00:00".
        #[serde(default)]
        time_zone: UtcOffset,
    },
}

impl Trigger {
    /// Parse a `Trigger` from a job descriptor JSON string.
    ///
    /// Extracts the `trigger` field from the descriptor object and
    /// deserializes it. Returns `None` if the field is missing or malformed.
    pub fn from_descriptor(descriptor_json: &str) -> Option<Self> {
        #[derive(serde::Deserialize)]
        struct Extract {
            trigger: Option<Trigger>,
        }
        serde_json::from_str::<Extract>(descriptor_json)
            .ok()?
            .trigger
    }

    /// Returns `true` for periodic triggers (`Interval` and `Cron`).
    pub fn is_periodic(&self) -> bool {
        matches!(self, Trigger::Interval { .. } | Trigger::Cron { .. })
    }

    /// Compute the next fire time after `now` for this trigger.
    ///
    /// - `OneShot`: returns `None` (no periodic scheduling).
    /// - `Interval`: computes the smallest `created_at + n * every_secs > now`.
    /// - `Cron`: computes the next cron fire time after `now`.
    ///
    /// Does NOT include jitter -- the caller adds jitter separately.
    pub fn next_fire_time(
        &self,
        now: DateTime<Utc>,
        created_at: DateTime<Utc>,
    ) -> Option<DateTime<Utc>> {
        match self {
            Trigger::OneShot => None,
            Trigger::Interval { every_secs } => {
                let anchor = created_at;
                let interval_secs = every_secs.get();
                let elapsed = now - anchor;
                if elapsed.num_seconds() < 0 {
                    // now is before anchor -- next fire is the anchor itself
                    return Some(anchor);
                }
                // Find smallest n such that anchor + n * interval > now.
                // elapsed.num_seconds() and interval_secs are both positive and
                // fit in i64 (IntervalSecs caps at i64::MAX as u64).
                let elapsed_secs = elapsed.num_seconds();
                let n = elapsed_secs.checked_div(interval_secs)?.checked_add(1)?;
                let offset_secs = interval_secs.checked_mul(n)?;
                Some(anchor + chrono::Duration::seconds(offset_secs))
            }
            Trigger::Cron {
                schedule,
                time_zone,
            } => {
                let offset = time_zone.as_fixed_offset();
                let now_in_tz = now.with_timezone(&offset);
                let sched = schedule.as_cron_schedule();
                let next = sched.after(&now_in_tz).next()?;
                Some(next.with_timezone(&Utc))
            }
        }
    }
}

impl Default for Trigger {
    /// Matches current behavior: one-shot scheduling.
    fn default() -> Self {
        Self::OneShot
    }
}

impl fmt::Display for Trigger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Trigger::OneShot => write!(f, "one_shot"),
            Trigger::Interval { every_secs, .. } => {
                write!(f, "interval(every_secs={})", every_secs.get())
            }
            Trigger::Cron { schedule, .. } => write!(f, "cron({})", schedule.as_str()),
        }
    }
}

impl<'de> de::Deserialize<'de> for Trigger {
    fn deserialize<D: de::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        /// Mirror of `Trigger` used only for deserialization so we can
        /// leverage the derived serde tag dispatch.
        #[derive(serde::Deserialize)]
        #[serde(tag = "type", rename_all = "snake_case")]
        enum TriggerRaw {
            OneShot,
            Interval {
                every_secs: IntervalSecs,
            },
            Cron {
                schedule: CronSchedule,
                #[serde(default)]
                time_zone: UtcOffset,
            },
        }

        let raw = TriggerRaw::deserialize(deserializer)?;
        let trigger = match raw {
            TriggerRaw::OneShot => Trigger::OneShot,
            TriggerRaw::Interval { every_secs } => Trigger::Interval { every_secs },
            TriggerRaw::Cron {
                schedule,
                time_zone,
            } => Trigger::Cron {
                schedule,
                time_zone,
            },
        };
        Ok(trigger)
    }
}

/// Interval duration in seconds. Must be at least 1.
///
/// Validates on construction and deserialization -- invalid values are rejected
/// at parse time rather than via a post-hoc `validate()` call.
#[derive(Debug, Clone, Copy, Eq, PartialEq, serde::Serialize)]
#[serde(transparent)]
pub struct IntervalSecs(i64);

/// The provided `every_secs` value is invalid.
///
/// Returned when attempting to construct an `IntervalSecs` with a value
/// that is zero or negative. An interval of zero seconds would cause a
/// tight scheduling loop.
#[derive(Debug, thiserror::Error)]
#[error("every_secs must be at least 1, got {value}")]
pub struct IntervalSecsError {
    value: i64,
}

impl IntervalSecs {
    /// Create a new `IntervalSecs`, returning an error if the value is less than 1.
    pub fn new(value: i64) -> Result<Self, IntervalSecsError> {
        if value < 1 {
            return Err(IntervalSecsError { value });
        }
        Ok(Self(value))
    }

    /// Returns the inner value.
    pub fn get(self) -> i64 {
        self.0
    }
}

impl<'de> de::Deserialize<'de> for IntervalSecs {
    fn deserialize<D: de::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = i64::deserialize(deserializer)?;
        Self::new(value).map_err(de::Error::custom)
    }
}

/// A validated cron schedule expression.
///
/// Validates on construction that the expression is a parseable cron string
/// (5-field standard, 6-field with seconds, or 7-field with year).
/// Once constructed, the schedule is guaranteed to be valid.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CronSchedule(String);

/// The provided cron expression is invalid.
///
/// Returned when attempting to construct a `CronSchedule` with an expression
/// that cannot be parsed as a valid cron schedule.
#[derive(Debug, thiserror::Error)]
#[error(
    "invalid cron schedule: '{expression}'. Expected 5-field cron expression (e.g., '0 */6 * * *')"
)]
pub struct CronScheduleError {
    expression: String,
}

impl CronSchedule {
    /// Create a new `CronSchedule`, returning an error if the expression is invalid.
    pub fn new(expression: String) -> Result<Self, CronScheduleError> {
        if parse_cron_schedule(&expression).is_none() {
            return Err(CronScheduleError { expression });
        }
        Ok(Self(expression))
    }

    /// Returns the inner string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns the parsed `cron::Schedule`.
    ///
    /// This re-parses the stored expression. The parse is guaranteed to
    /// succeed because the expression was validated on construction.
    fn as_cron_schedule(&self) -> cron::Schedule {
        // SAFETY: expression was validated in `new` / deserialization
        parse_cron_schedule(&self.0).expect("CronSchedule invariant: expression is valid")
    }
}

impl serde::Serialize for CronSchedule {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl<'de> de::Deserialize<'de> for CronSchedule {
    fn deserialize<D: de::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let expression = String::deserialize(deserializer)?;
        Self::new(expression).map_err(de::Error::custom)
    }
}

/// A validated UTC offset string in "+HH:MM" or "-HH:MM" format.
///
/// Validates on construction that the string is a valid UTC offset.
/// Once constructed, the offset is guaranteed to be valid.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct UtcOffset(String);

/// The provided UTC offset string is invalid.
///
/// Returned when attempting to construct a `UtcOffset` with a string
/// that does not match the expected "+HH:MM" or "-HH:MM" format.
#[derive(Debug, thiserror::Error)]
#[error(
    "invalid time_zone: '{value}'. Expected UTC offset format '+HH:MM' or '-HH:MM' (e.g., '+00:00', '+05:30')"
)]
pub struct UtcOffsetError {
    value: String,
}

impl UtcOffset {
    /// Create a new `UtcOffset`, returning an error if the format is invalid.
    pub fn new(value: String) -> Result<Self, UtcOffsetError> {
        if parse_utc_offset_str(&value).is_none() {
            return Err(UtcOffsetError { value });
        }
        Ok(Self(value))
    }

    /// Returns the inner string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns the parsed `FixedOffset`.
    ///
    /// This re-parses the stored string. The parse is guaranteed to
    /// succeed because the value was validated on construction.
    fn as_fixed_offset(&self) -> FixedOffset {
        // SAFETY: value was validated in `new` / deserialization
        parse_utc_offset_str(&self.0).expect("UtcOffset invariant: value is valid")
    }
}

impl Default for UtcOffset {
    fn default() -> Self {
        // SAFETY: "+00:00" is a valid UTC offset
        Self("+00:00".to_owned())
    }
}

impl serde::Serialize for UtcOffset {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl<'de> de::Deserialize<'de> for UtcOffset {
    fn deserialize<D: de::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(de::Error::custom)
    }
}

/// Convert a 5-field cron expression to the 6-field format expected by the `cron` crate.
///
/// The `cron` crate requires a seconds field as the first field. Standard 5-field
/// cron expressions (minute, hour, day-of-month, month, day-of-week) are converted
/// by prepending `0` (fire at second 0).
///
/// Returns `None` if the expression is invalid.
fn parse_cron_schedule(expr: &str) -> Option<cron::Schedule> {
    let field_count = expr.split_whitespace().count();
    let cron_expr = match field_count {
        5 => format!("0 {expr}"), // Prepend seconds=0
        6 | 7 => expr.to_owned(), // Already has seconds (and optional year)
        _ => return None,
    };
    cron_expr.parse().ok()
}

/// Parse a UTC offset string like "+05:30" or "-03:00" into a `FixedOffset`.
///
/// Returns `None` for invalid formats.
fn parse_utc_offset_str(s: &str) -> Option<FixedOffset> {
    // Expected format: "+HH:MM" or "-HH:MM"
    if s.len() != 6 {
        return None;
    }
    let sign = match s.as_bytes().first()? {
        b'+' => 1,
        b'-' => -1,
        _ => return None,
    };
    let hours: i32 = s[1..3].parse().ok()?;
    if s.as_bytes()[3] != b':' {
        return None;
    }
    let minutes: i32 = s[4..6].parse().ok()?;
    if hours > 23 || minutes > 59 {
        return None;
    }
    let total_secs = sign * (hours * 3600 + minutes * 60);
    FixedOffset::east_opt(total_secs)
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Default and Display ---

    #[test]
    fn default_with_no_arguments_returns_one_shot() {
        //* When
        let trigger = Trigger::default();

        //* Then
        assert_eq!(trigger, Trigger::OneShot);
    }

    #[test]
    fn fmt_display_with_one_shot_returns_one_shot_string() {
        //* When
        let result = Trigger::OneShot.to_string();

        //* Then
        assert_eq!(result, "one_shot");
    }

    #[test]
    fn fmt_display_with_interval_returns_interval_string() {
        //* Given
        let trigger = Trigger::Interval {
            every_secs: IntervalSecs::new(60).expect("valid interval"),
        };

        //* When
        let result = trigger.to_string();

        //* Then
        assert_eq!(result, "interval(every_secs=60)");
    }

    #[test]
    fn fmt_display_with_cron_returns_cron_string() {
        //* Given
        let trigger = Trigger::Cron {
            schedule: CronSchedule::new("0 * * * *".to_owned()).expect("valid cron schedule"),
            time_zone: UtcOffset::default(),
        };

        //* When
        let result = trigger.to_string();

        //* Then
        assert_eq!(result, "cron(0 * * * *)");
    }

    // --- is_periodic ---

    #[test]
    fn is_periodic_with_one_shot_returns_false() {
        //* When
        let result = Trigger::OneShot.is_periodic();

        //* Then
        assert!(!result);
    }

    #[test]
    fn is_periodic_with_interval_returns_true() {
        //* Given
        let trigger = Trigger::Interval {
            every_secs: IntervalSecs::new(30).expect("valid interval"),
        };

        //* When
        let result = trigger.is_periodic();

        //* Then
        assert!(result);
    }

    #[test]
    fn is_periodic_with_cron_returns_true() {
        //* Given
        let trigger = Trigger::Cron {
            schedule: CronSchedule::new("*/5 * * * *".to_owned()).expect("valid cron schedule"),
            time_zone: UtcOffset::default(),
        };

        //* When
        let result = trigger.is_periodic();

        //* Then
        assert!(result);
    }

    // --- from_descriptor ---

    #[test]
    fn from_descriptor_with_valid_one_shot_returns_trigger() {
        //* Given
        let descriptor = r#"{"trigger":{"type":"one_shot"}}"#;

        //* When
        let trigger = Trigger::from_descriptor(descriptor);

        //* Then
        assert_eq!(trigger, Some(Trigger::OneShot));
    }

    #[test]
    fn from_descriptor_with_valid_interval_returns_trigger() {
        //* Given
        let descriptor = r#"{"trigger":{"type":"interval","every_secs":300}}"#;

        //* When
        let trigger = Trigger::from_descriptor(descriptor);

        //* Then
        assert_eq!(
            trigger,
            Some(Trigger::Interval {
                every_secs: IntervalSecs::new(300).expect("valid interval"),
            })
        );
    }

    #[test]
    fn from_descriptor_with_valid_cron_returns_trigger() {
        //* Given
        let descriptor =
            r#"{"trigger":{"type":"cron","schedule":"0 * * * *","time_zone":"+05:30"}}"#;

        //* When
        let trigger = Trigger::from_descriptor(descriptor);

        //* Then
        assert_eq!(
            trigger,
            Some(Trigger::Cron {
                schedule: CronSchedule::new("0 * * * *".to_owned()).expect("valid cron schedule"),
                time_zone: UtcOffset::new("+05:30".to_owned()).expect("valid utc offset"),
            })
        );
    }

    #[test]
    fn from_descriptor_without_trigger_field_returns_none() {
        //* Given
        let descriptor = r#"{"kind":"materialize-raw"}"#;

        //* When
        let trigger = Trigger::from_descriptor(descriptor);

        //* Then
        assert_eq!(trigger, None);
    }

    #[test]
    fn from_descriptor_with_invalid_json_returns_none() {
        //* When
        let trigger = Trigger::from_descriptor("not json");

        //* Then
        assert_eq!(trigger, None);
    }

    // --- Serde round-trip ---

    #[test]
    fn deserialize_with_one_shot_round_trips() {
        //* Given
        let trigger = Trigger::OneShot;

        //* When
        let json = serde_json::to_string(&trigger).expect("serialization should succeed");
        let deserialized: Trigger =
            serde_json::from_str(&json).expect("deserialization should succeed");

        //* Then
        assert_eq!(trigger, deserialized);
    }

    #[test]
    fn deserialize_with_cron_round_trips() {
        //* Given
        let trigger = Trigger::Cron {
            schedule: CronSchedule::new("0 0 * * *".to_owned()).expect("valid cron schedule"),
            time_zone: UtcOffset::new("+05:30".to_owned()).expect("valid utc offset"),
        };

        //* When
        let json = serde_json::to_string(&trigger).expect("serialization should succeed");
        let deserialized: Trigger =
            serde_json::from_str(&json).expect("deserialization should succeed");

        //* Then
        assert_eq!(trigger, deserialized);
    }

    // --- Deserialization rejection tests ---

    #[test]
    fn deserialize_with_zero_interval_secs_fails() {
        //* Given
        let json = r#"{"type":"interval","every_secs":0}"#;

        //* When
        let result = serde_json::from_str::<Trigger>(json);

        //* Then
        let err = result.expect_err("zero every_secs should be rejected");
        assert!(
            err.to_string().contains("every_secs must be at least 1"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn deserialize_with_invalid_cron_schedule_fails() {
        //* Given
        let json = r#"{"type":"cron","schedule":"not a cron"}"#;

        //* When
        let result = serde_json::from_str::<Trigger>(json);

        //* Then
        let err = result.expect_err("invalid cron schedule should be rejected");
        assert!(
            err.to_string().contains("invalid cron schedule"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn deserialize_with_invalid_time_zone_fails() {
        //* Given
        let json = r#"{"type":"cron","schedule":"0 * * * *","time_zone":"America/New_York"}"#;

        //* When
        let result = serde_json::from_str::<Trigger>(json);

        //* Then
        let err = result.expect_err("IANA timezone should be rejected");
        assert!(
            err.to_string().contains("invalid time_zone"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn deserialize_with_valid_interval_succeeds() {
        //* Given
        let json = r#"{"type":"interval","every_secs":60}"#;

        //* When
        let result = serde_json::from_str::<Trigger>(json);

        //* Then
        assert!(result.is_ok(), "valid interval should parse successfully");
    }

    #[test]
    fn deserialize_with_interval_only_required_fields_succeeds() {
        //* Given
        let json = r#"{"type":"interval","every_secs":120}"#;

        //* When
        let result = serde_json::from_str::<Trigger>(json);

        //* Then
        let trigger = result.expect("interval with only every_secs should parse");
        assert_eq!(
            trigger,
            Trigger::Interval {
                every_secs: IntervalSecs::new(120).expect("valid interval"),
            }
        );
    }

    #[test]
    fn deserialize_with_cron_without_time_zone_defaults_to_utc() {
        //* Given
        let json = r#"{"type":"cron","schedule":"0 * * * *"}"#;

        //* When
        let result = serde_json::from_str::<Trigger>(json);

        //* Then
        let trigger = result.expect("cron without time_zone should parse");
        assert_eq!(
            trigger,
            Trigger::Cron {
                schedule: CronSchedule::new("0 * * * *".to_owned()).expect("valid cron schedule"),
                time_zone: UtcOffset::default(),
            }
        );
    }

    #[test]
    fn deserialize_with_cron_valid_utc_offset_succeeds() {
        //* Given
        let json = r#"{"type":"cron","schedule":"0 * * * *","time_zone":"+05:30"}"#;

        //* When
        let result = serde_json::from_str::<Trigger>(json);

        //* Then
        assert!(result.is_ok(), "valid UTC offset should parse successfully");
    }

    // --- IntervalSecs ---

    #[test]
    fn new_with_zero_value_returns_error() {
        //* When
        let result = IntervalSecs::new(0);

        //* Then
        assert!(result.is_err(), "zero should be rejected");
    }

    #[test]
    fn new_with_one_returns_interval_secs() {
        //* When
        let result = IntervalSecs::new(1);

        //* Then
        let secs = result.expect("should accept 1");
        assert_eq!(secs.get(), 1);
    }

    #[test]
    fn new_with_large_value_returns_interval_secs() {
        //* When
        let result = IntervalSecs::new(86400);

        //* Then
        let secs = result.expect("should accept 86400");
        assert_eq!(secs.get(), 86400);
    }

    // --- CronSchedule ---

    #[test]
    fn new_with_valid_five_field_cron_returns_schedule() {
        //* When
        let result = CronSchedule::new("0 * * * *".to_owned());

        //* Then
        let schedule = result.expect("valid 5-field cron should parse");
        assert_eq!(schedule.as_str(), "0 * * * *");
    }

    #[test]
    fn new_with_invalid_cron_returns_error() {
        //* When
        let result = CronSchedule::new("not a cron".to_owned());

        //* Then
        assert!(result.is_err(), "invalid cron should be rejected");
    }

    // --- UtcOffset ---

    #[test]
    fn new_with_valid_positive_offset_returns_utc_offset() {
        //* When
        let result = UtcOffset::new("+05:30".to_owned());

        //* Then
        let offset = result.expect("valid positive offset should parse");
        assert_eq!(
            offset.as_fixed_offset().local_minus_utc(),
            5 * 3600 + 30 * 60
        );
    }

    #[test]
    fn new_with_valid_negative_offset_returns_utc_offset() {
        //* When
        let result = UtcOffset::new("-03:00".to_owned());

        //* Then
        let offset = result.expect("valid negative offset should parse");
        assert_eq!(offset.as_fixed_offset().local_minus_utc(), -(3 * 3600));
    }

    #[test]
    fn new_with_utc_offset_returns_zero() {
        //* When
        let result = UtcOffset::new("+00:00".to_owned());

        //* Then
        let offset = result.expect("UTC offset should parse");
        assert_eq!(offset.as_fixed_offset().local_minus_utc(), 0);
    }

    #[test]
    fn new_with_iana_timezone_returns_error() {
        //* When
        let result = UtcOffset::new("America/New_York".to_owned());

        //* Then
        assert!(result.is_err(), "IANA timezone should be rejected");
    }

    #[test]
    fn new_with_invalid_format_returns_error() {
        //* When
        let utc = UtcOffset::new("UTC".to_owned());
        let no_colon = UtcOffset::new("0530".to_owned());
        let short = UtcOffset::new("+5:30".to_owned());

        //* Then
        assert!(utc.is_err(), "'UTC' should be rejected");
        assert!(no_colon.is_err(), "'0530' should be rejected");
        assert!(short.is_err(), "'+5:30' should be rejected");
    }

    // --- next_fire_time ---

    #[test]
    fn next_fire_time_with_one_shot_returns_none() {
        //* Given
        let trigger = Trigger::OneShot;
        let now = Utc::now();

        //* When
        let result = trigger.next_fire_time(now, now);

        //* Then
        assert_eq!(result, None);
    }

    #[test]
    fn next_fire_time_with_interval_returns_next_tick_after_now() {
        //* Given
        let anchor: DateTime<Utc> = "2026-01-01T00:00:00Z".parse().expect("valid datetime");
        let now: DateTime<Utc> = "2026-01-01T00:02:30Z".parse().expect("valid datetime");
        let trigger = Trigger::Interval {
            every_secs: IntervalSecs::new(60).expect("valid interval"),
        };

        //* When
        let next = trigger.next_fire_time(now, anchor);

        //* Then
        // anchor + 3*60 = 2026-01-01T00:03:00Z (smallest n such that anchor + n*60 > now)
        let expected: DateTime<Utc> = "2026-01-01T00:03:00Z".parse().expect("valid datetime");
        assert_eq!(next, Some(expected));
    }

    #[test]
    fn next_fire_time_with_interval_uses_created_at_as_anchor() {
        //* Given
        let created: DateTime<Utc> = "2026-01-01T00:00:00Z".parse().expect("valid datetime");
        let now: DateTime<Utc> = "2026-01-01T00:01:30Z".parse().expect("valid datetime");
        let trigger = Trigger::Interval {
            every_secs: IntervalSecs::new(60).expect("valid interval"),
        };

        //* When
        let next = trigger.next_fire_time(now, created);

        //* Then
        let expected: DateTime<Utc> = "2026-01-01T00:02:00Z".parse().expect("valid datetime");
        assert_eq!(next, Some(expected));
    }

    #[test]
    fn next_fire_time_with_cron_returns_next_cron_tick() {
        //* Given
        let now: DateTime<Utc> = "2026-01-01T00:00:30Z".parse().expect("valid datetime");
        let trigger = Trigger::Cron {
            schedule: CronSchedule::new("* * * * *".to_owned()).expect("valid cron schedule"),
            time_zone: UtcOffset::default(),
        };

        //* When
        let next = trigger.next_fire_time(now, now);

        //* Then
        let expected: DateTime<Utc> = "2026-01-01T00:01:00Z".parse().expect("valid datetime");
        assert_eq!(next, Some(expected));
    }
}
