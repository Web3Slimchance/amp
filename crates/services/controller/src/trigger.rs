//! In-memory trigger timer system for periodic job scheduling.
//!
//! Maintains a map of active periodic job timers. Each timer fires at the job's
//! trigger cadence and sends a message to the trigger task, which reschedules the job.
//!
//! The timer map is derived state — fully reconstructable from the event log at any
//! time. A crash and restart rebuilds it from scratch via [`recover_from_db`].

use std::collections::HashMap;

use amp_job_core::{job_id::JobId, status::JobStatus, trigger::Trigger};
use chrono::{DateTime, Utc};
use metadata_db::MetadataDb;
use monitoring::logging;
use tokio::sync::mpsc;

use crate::scheduler::reschedule_and_notify;

/// Identity of a periodic trigger: the job, its trigger config, and the anchor time.
#[derive(Debug)]
pub struct PeriodicJobTrigger {
    pub job_id: JobId,
    pub trigger: Trigger,
    pub created_at: DateTime<Utc>,
}

/// A command sent to the trigger task when a timer fires or a lifecycle event occurs.
#[derive(Debug)]
pub enum TriggerCommand {
    /// A timer has fired for a periodic job — evaluate and reschedule it.
    Fire(PeriodicJobTrigger),
    /// Register a new periodic timer (e.g., after a job reaches a terminal state).
    Register(PeriodicJobTrigger),
}

/// Entry in the in-memory timer map.
struct TimerEntry {
    /// Handle to the spawned timer task so we can abort it on removal.
    abort_handle: tokio::task::AbortHandle,
}

/// Recover periodic trigger timers from the database at boot.
///
/// Queries all terminal jobs (COMPLETED, ERROR, FATAL), parses their trigger config,
/// and registers a timer for each periodic job. This is the only DB query for triggers —
/// after boot, all scheduling is driven by in-memory timers.
pub async fn recover_from_db(
    metadata_db: &MetadataDb,
    tx: &mpsc::Sender<TriggerCommand>,
) -> Result<(), metadata_db::Error> {
    let terminal_jobs = metadata_db::jobs::get_terminal_with_descriptors(metadata_db).await?;

    let mut count = 0u32;
    for job_with_desc in terminal_jobs {
        let Some(descriptor) = job_with_desc.descriptor else {
            continue;
        };

        let job_id: JobId = job_with_desc.job.id.into();
        let trigger = Trigger::from_descriptor(descriptor.as_str()).unwrap_or_default();
        if !trigger.is_periodic() {
            continue;
        }
        if tx
            .send(TriggerCommand::Register(PeriodicJobTrigger {
                job_id,
                trigger,
                created_at: job_with_desc.job.created_at,
            }))
            .await
            .is_err()
        {
            tracing::error!(
                jobs_recovered = count,
                "trigger timer channel closed during boot recovery"
            );
            return Ok(());
        }
        count += 1;
    }

    if count > 0 {
        tracing::info!(count, "recovered periodic trigger timers from database");
    }

    Ok(())
}

/// Run the trigger evaluation task.
///
/// Listens for [`TriggerCommand`]s and manages the in-memory timer map.
/// When a timer fires, it reschedules the job via the scheduler.
///
/// This task runs for the lifetime of the controller service.
pub async fn run(
    metadata_db: &MetadataDb,
    tx: mpsc::Sender<TriggerCommand>,
    mut rx: mpsc::Receiver<TriggerCommand>,
) {
    let mut timers: HashMap<JobId, TimerEntry> = HashMap::new();

    while let Some(cmd) = rx.recv().await {
        match cmd {
            TriggerCommand::Fire(periodic) => {
                handle_fire(metadata_db, &tx, &mut timers, &periodic).await;
            }
            TriggerCommand::Register(periodic) => {
                register_timer(&mut timers, &tx, periodic);
            }
        }
    }
}

/// Register (or replace) a timer for a periodic job.
fn register_timer(
    timers: &mut HashMap<JobId, TimerEntry>,
    tx: &mpsc::Sender<TriggerCommand>,
    periodic: PeriodicJobTrigger,
) {
    let job_id = periodic.job_id;

    if let Some(old) = timers.remove(&job_id) {
        old.abort_handle.abort();
    }

    let now = Utc::now();
    let Some(next_fire) = periodic.trigger.next_fire_time(now, periodic.created_at) else {
        return;
    };

    let delay = (next_fire - now)
        .to_std()
        .unwrap_or(std::time::Duration::ZERO);

    let tx = tx.clone();

    let handle = tokio::spawn(async move {
        tokio::time::sleep(delay).await;
        let _ = tx.send(TriggerCommand::Fire(periodic)).await;
    });

    timers.insert(
        job_id,
        TimerEntry {
            abort_handle: handle.abort_handle(),
        },
    );

    tracing::debug!(
        job_id = %job_id,
        delay_secs = delay.as_secs(),
        "registered trigger timer"
    );
}

/// Handle a timer fire: reschedule the job and register the next timer.
async fn handle_fire(
    metadata_db: &MetadataDb,
    tx: &mpsc::Sender<TriggerCommand>,
    timers: &mut HashMap<JobId, TimerEntry>,
    periodic: &PeriodicJobTrigger,
) {
    let job_id = periodic.job_id;
    let trigger = &periodic.trigger;
    let created_at = periodic.created_at;

    timers.remove(&job_id);

    let job = match metadata_db::jobs::get_by_id(metadata_db, &job_id).await {
        Ok(Some(job)) => job,
        Ok(None) => {
            tracing::warn!(job_id = %job_id, "triggered job not found, dropping timer");
            return;
        }
        Err(err) => {
            tracing::error!(
                job_id = %job_id,
                error = %err,
                error_source = logging::error_source(&err),
                "failed to get job for trigger fire"
            );
            return;
        }
    };

    let status: JobStatus = job.status.into();

    // Only fire for terminal states
    match status {
        JobStatus::Completed | JobStatus::Fatal => {}
        JobStatus::Error => {
            // Mark ERROR → FATAL first (abandon current retry cycle)
            if let Err(err) =
                metadata_db::job_status::mark_fatal_from_error(metadata_db, job_id, None).await
            {
                tracing::error!(
                    job_id = %job_id,
                    error = %err,
                    error_source = logging::error_source(&err),
                    "failed to mark ERROR job as FATAL before trigger reschedule"
                );
                return;
            }
            tracing::info!(
                job_id = %job_id,
                trigger = %trigger,
                "marked ERROR job as FATAL, trigger will reschedule fresh"
            );
        }
        // Job is still active — skip this tick (single-flight) but re-register
        // the next timer so the cadence continues.
        JobStatus::Scheduled | JobStatus::Running => {
            tracing::warn!(
                job_id = %job_id,
                status = %status,
                "trigger fired but job is still active, skipping (single-flight)"
            );
            register_timer(
                timers,
                tx,
                PeriodicJobTrigger {
                    job_id,
                    trigger: trigger.clone(),
                    created_at,
                },
            );
            return;
        }
        // Job is stopped or being stopped — trigger should not continue.
        JobStatus::Stopped
        | JobStatus::StopRequested
        | JobStatus::Stopping
        | JobStatus::Unknown => {
            tracing::debug!(
                job_id = %job_id,
                status = %status,
                "trigger fired but job is stopped, dropping timer"
            );
            return;
        }
    }

    // Load descriptor from latest SCHEDULED event
    let descriptor =
        match metadata_db::job_events::get_latest_descriptor(metadata_db, &job_id).await {
            Ok(Some(desc)) => desc,
            Ok(None) => {
                tracing::error!(job_id = %job_id, "no descriptor found for triggered job");
                return;
            }
            Err(err) => {
                tracing::error!(
                    job_id = %job_id,
                    error = %err,
                    error_source = logging::error_source(&err),
                    "failed to get descriptor for triggered job"
                );
                return;
            }
        };

    let retry_index =
        match metadata_db::job_events::get_attempt_count_since_last_completed(metadata_db, &job_id)
            .await
        {
            Ok(count) => count,
            Err(err) => {
                tracing::error!(
                    job_id = %job_id,
                    error = %err,
                    error_source = logging::error_source(&err),
                    "failed to get attempt count for triggered job"
                );
                return;
            }
        };

    let result = reschedule_and_notify(
        metadata_db,
        job.id,
        job.node_id.clone(),
        descriptor,
        retry_index,
    )
    .await;

    match &result {
        Ok(()) => {
            tracing::info!(
                job_id = %job_id,
                trigger = %trigger,
                "periodic job rescheduled by trigger"
            );
            // Immediately register the next timer using the original anchor.
            // This keeps the cadence wall-clock-aligned regardless of how long
            // the job takes to run.
            register_timer(
                timers,
                tx,
                PeriodicJobTrigger {
                    job_id,
                    trigger: trigger.clone(),
                    created_at,
                },
            );
        }
        Err(err) => {
            tracing::error!(
                job_id = %job_id,
                error = %err,
                error_source = logging::error_source(&err),
                "failed to reschedule triggered job"
            );
        }
    }
}
