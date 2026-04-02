//! SHA-256 hashing for job idempotency keys.
//!
//! Provides the [`JobKey`] newtype and [`hash`] function used to compute
//! deterministic idempotency keys for worker jobs.

use metadata_db::jobs::IdempotencyKey;
use sha2::{Digest as _, Sha256};

/// Computes a SHA-256 hash of the provided data and returns it as a [`JobKey`].
pub fn hash(data: impl AsRef<[u8]>) -> JobKey {
    let result = Sha256::digest(data.as_ref());
    let bytes: [u8; 32] = result.into();
    JobKey(hex::encode(bytes))
}

/// A SHA-256 hex-encoded job key.
///
/// `JobKey` wraps a 64-character hex string produced by [`hash`]. It exists
/// primarily to provide a type-safe conversion into [`IdempotencyKey`] that
/// centralizes the `from_owned_unchecked` call, so worker crates never need
/// to call unchecked constructors directly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobKey(String);

impl JobKey {
    /// Returns a reference to the inner hex string.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the `JobKey` and returns the inner `String`.
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl std::fmt::Display for JobKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if f.alternate() {
            f.write_str(&self.0[..7])
        } else {
            f.write_str(&self.0)
        }
    }
}

impl From<JobKey> for IdempotencyKey<'static> {
    fn from(key: JobKey) -> Self {
        // SAFETY: The inner string is a 64-char hex string produced by SHA-256,
        // which satisfies IdempotencyKey's non-empty invariant.
        IdempotencyKey::from_owned_unchecked(key.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_with_known_input_produces_expected_output() {
        //* Given
        let data = b"hello world";
        let expected = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9";

        //* When
        let result = hash(data);

        //* Then
        assert_eq!(result.as_str(), expected);
    }

    #[test]
    fn job_key_converts_into_idempotency_key() {
        //* Given
        let key = hash("test-job:ref");

        //* When
        let idempotency_key: IdempotencyKey<'static> = key.into();

        //* Then
        assert!(!idempotency_key.as_str().is_empty());
    }
}
