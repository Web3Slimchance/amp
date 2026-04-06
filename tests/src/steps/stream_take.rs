//! Test step for taking data from registered streams.
use anyhow::Result;
use regex::Regex;

use crate::testlib::fixtures::FlightClient;

/// Test step that takes a specified number of rows from a registered stream.
///
/// This step retrieves data from a previously registered stream and validates
/// the results against expected outcomes.
#[derive(Debug, serde::Deserialize)]
pub struct Step {
    /// The name of this test step.
    pub name: String,
    /// The name of the stream to take data from.
    pub stream: String,
    /// The number of rows to take from the stream.
    pub take: usize,
    /// The expected results for validation.
    #[serde(flatten)]
    pub results: SqlTestResult,
}

impl Step {
    /// Takes data from the stream and validates the results.
    ///
    /// Retrieves the specified number of rows from the named stream using
    /// the Flight client, then validates the actual results against expected results.
    pub async fn run(&self, client: &mut FlightClient) -> Result<()> {
        tracing::debug!("Taking {} rows from stream '{}'", self.take, self.stream);

        let actual_result = client.take_from_stream(&self.stream, self.take).await;

        self.results.assert_eq(actual_result)
    }
}

/// Test result validation for SQL operations.
///
/// Represents either expected success with specific results or expected
/// failure with a specific error message substring.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(untagged)]
pub enum SqlTestResult {
    /// Expected successful result with JSON-encoded data.
    Success {
        results: String,
        #[serde(rename = "recordBatchCount", default)]
        record_batch_count: Option<usize>,
    },
    /// Expected successful result where each field value is a regex pattern.
    /// String values in the expected JSON are treated as regex patterns that
    /// must match the actual string representation of the corresponding field.
    SuccessPattern {
        #[serde(rename = "resultsPattern")]
        results_pattern: String,
    },
    /// Expected failure with a specific error message substring.
    Failure { failure: String },
}

impl SqlTestResult {
    /// Validates actual results against expected results.
    ///
    /// For success cases, compares JSON-serialized results for equality and optionally
    /// validates the number of record batches returned.
    /// For failure cases, checks that the error message contains the expected substring.
    pub(crate) fn assert_eq(
        &self,
        actual_result: Result<(serde_json::Value, usize)>,
    ) -> Result<()> {
        match self {
            SqlTestResult::Success {
                results: expected_json_str,
                record_batch_count,
            } => {
                let expected: serde_json::Value = serde_json::from_str(expected_json_str)?;
                let (actual, actual_batch_count) = actual_result?;

                pretty_assertions::assert_str_eq!(
                    actual.to_string(),
                    expected.to_string(),
                    "Test returned unexpected results",
                );

                if let Some(expected_batch_count) = record_batch_count {
                    assert_eq!(
                        actual_batch_count, *expected_batch_count,
                        "Expected {} record batch(es), got {}",
                        expected_batch_count, actual_batch_count
                    );
                }
            }

            SqlTestResult::SuccessPattern { results_pattern } => {
                let expected: serde_json::Value = serde_json::from_str(results_pattern)?;
                let (actual, _) = actual_result?;
                assert_json_matches_pattern(&actual, &expected);
            }

            SqlTestResult::Failure { failure } => {
                let expected_substring = failure.trim();
                let actual_error = actual_result.expect_err("expected failure, got success");

                if !actual_error.to_string().contains(expected_substring) {
                    panic!(
                        "Expected substring: \"{}\"\nActual error: \"{}\"",
                        expected_substring, actual_error
                    );
                }
            }
        }
        Ok(())
    }
}

/// Recursively asserts that `actual` matches `pattern`, where string values in
/// `pattern` are treated as regex patterns. Non-string values must match exactly.
fn assert_json_matches_pattern(actual: &serde_json::Value, pattern: &serde_json::Value) {
    match (actual, pattern) {
        (serde_json::Value::Array(actual_arr), serde_json::Value::Array(pattern_arr)) => {
            assert_eq!(
                actual_arr.len(),
                pattern_arr.len(),
                "Array length mismatch: actual {} vs pattern {}",
                actual_arr.len(),
                pattern_arr.len()
            );
            for (i, (a, p)) in actual_arr.iter().zip(pattern_arr.iter()).enumerate() {
                assert_json_matches_pattern_ctx(a, p, &format!("[{i}]"));
            }
        }
        _ => assert_json_matches_pattern_ctx(actual, pattern, ""),
    }
}

fn assert_json_matches_pattern_ctx(
    actual: &serde_json::Value,
    pattern: &serde_json::Value,
    ctx: &str,
) {
    use serde_json::Value;
    match (actual, pattern) {
        (Value::Object(actual_map), Value::Object(pattern_map)) => {
            let actual_keys: std::collections::BTreeSet<_> = actual_map.keys().collect();
            let pattern_keys: std::collections::BTreeSet<_> = pattern_map.keys().collect();
            assert_eq!(actual_keys, pattern_keys, "Key mismatch at {ctx}");
            for (key, pattern_val) in pattern_map {
                assert_json_matches_pattern_ctx(
                    &actual_map[key],
                    pattern_val,
                    &format!("{ctx}.{key}"),
                );
            }
        }
        (Value::Array(actual_arr), Value::Array(pattern_arr)) => {
            assert_eq!(
                actual_arr.len(),
                pattern_arr.len(),
                "Array length mismatch at {ctx}"
            );
            for (i, (a, p)) in actual_arr.iter().zip(pattern_arr.iter()).enumerate() {
                assert_json_matches_pattern_ctx(a, p, &format!("{ctx}[{i}]"));
            }
        }
        (actual_val, Value::String(pattern_str)) => {
            let actual_str = match actual_val {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            let re = Regex::new(pattern_str)
                .unwrap_or_else(|e| panic!("Invalid regex at {ctx}: {pattern_str:?}: {e}"));
            assert!(
                re.is_match(&actual_str),
                "Regex mismatch at {ctx}: pattern {pattern_str:?} did not match {actual_str:?}",
            );
        }
        (actual_val, pattern_val) => {
            assert_eq!(actual_val, pattern_val, "Value mismatch at {ctx}");
        }
    }
}
