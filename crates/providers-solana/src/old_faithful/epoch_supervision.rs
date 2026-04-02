//! Debug-only guard that detects overlapping epoch streams.
//!
//! When two streams attempt to process the same epoch concurrently, the
//! [`Guard`] will panic, surfacing the bug early in development rather than
//! producing silently corrupted data.

use std::{collections::HashSet, sync::Mutex};

/// Guard that tracks in-progress epochs to detect overlapping Solana streams in debug builds.
///
/// # Panics
///
/// Panics if an attempt is made to [create](Guard::new) a guard for an epoch that is already
/// in progress, or if a guard is dropped for an epoch that is not currently in progress.
pub(crate) struct Guard<'a> {
    epoch: solana_clock::Epoch,
    in_progress_epochs: &'a Mutex<HashSet<solana_clock::Epoch>>,
}

impl<'a> Guard<'a> {
    pub(crate) fn new(
        in_progress_epochs: &'a Mutex<HashSet<solana_clock::Epoch>>,
        epoch: solana_clock::Epoch,
    ) -> Self {
        let mut epochs_in_progress = in_progress_epochs.lock().unwrap();
        let is_new = epochs_in_progress.insert(epoch);
        assert!(
            is_new,
            "epoch {epoch} already in progress, overlapping Solana streams are not allowed"
        );
        Self {
            epoch,
            in_progress_epochs,
        }
    }
}

impl<'a> Drop for Guard<'a> {
    fn drop(&mut self) {
        let mut epochs_in_progress = self.in_progress_epochs.lock().unwrap();
        let removed = epochs_in_progress.remove(&self.epoch);
        assert!(
            removed,
            "epoch {} was not in progress during drop, this should never happen",
            self.epoch
        );
    }
}
