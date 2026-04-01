//! Job creation handler

use amp_job_core::job_id::JobId;
use axum::{Json, extract::State, http::StatusCode};
use metadata_db::physical_table_revision::LocationId;
use monitoring::logging;

use crate::{
    ctx::Ctx,
    error::{ErrorResponse, IntoErrorResponse},
    scheduler,
};

/// Handler for the `POST /jobs` endpoint
///
/// Schedules a new job for execution by an available worker.
/// Currently supports GC jobs only.
///
/// ## Request Body
/// - `kind`: The job type (currently only `"gc"`)
/// - Additional fields depend on the job kind
///
/// ### GC job
/// ```json
/// { "kind": "gc", "location_id": 42 }
/// ```
///
/// ## Response
/// - **202 Accepted**: Job scheduled successfully
/// - **400 Bad Request**: Invalid request body or unsupported job kind
/// - **409 Conflict**: An active job with the same idempotency key already exists
/// - **500 Internal Server Error**: Scheduler error
#[tracing::instrument(skip_all, err)]
pub async fn handler(
    State(ctx): State<Ctx>,
    json: Result<Json<CreateJobRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<(StatusCode, Json<CreateJobResponse>), ErrorResponse> {
    let Json(req) = json.map_err(|err| {
        tracing::debug!(error = %err, "invalid request body");
        Error::InvalidBody(err)
    })?;

    let (idempotency_key, descriptor) = match req {
        CreateJobRequest::Gc { location_id } => {
            let location_id = LocationId::try_from(location_id).map_err(|_| {
                tracing::debug!(location_id, "invalid location ID");
                Error::InvalidLocationId(location_id)
            })?;

            let key = amp_job_gc::job_key::idempotency_key(location_id);
            let desc = scheduler::JobDescriptor::from(amp_job_gc::job_descriptor::JobDescriptor {
                location_id,
            });
            (key, desc)
        }
    };

    let job_id = ctx
        .scheduler
        .schedule_job(idempotency_key.into(), descriptor, None)
        .await
        .map_err(|err| {
            tracing::error!(
                error = %err,
                error_source = logging::error_source(&err),
                "failed to schedule job"
            );
            Error::Scheduler(err)
        })?;

    tracing::info!(%job_id, "job scheduled");
    Ok((StatusCode::ACCEPTED, Json(CreateJobResponse { job_id })))
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid request body
    #[error("invalid request body: {0}")]
    InvalidBody(#[source] axum::extract::rejection::JsonRejection),

    /// Invalid location ID
    #[error("invalid location ID: {0}")]
    InvalidLocationId(i64),

    /// Scheduler error
    #[error("failed to schedule job")]
    Scheduler(#[source] scheduler::ScheduleJobError),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidBody(_) => "INVALID_BODY",
            Error::InvalidLocationId(_) => "INVALID_LOCATION_ID",
            Error::Scheduler(err) => match err {
                scheduler::ScheduleJobError::NoWorkersAvailable => "NO_WORKERS_AVAILABLE",
                scheduler::ScheduleJobError::ActiveJobConflict { .. } => "ACTIVE_JOB_CONFLICT",
                _ => "SCHEDULER_ERROR",
            },
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidBody(_) => StatusCode::BAD_REQUEST,
            Error::InvalidLocationId(_) => StatusCode::BAD_REQUEST,
            Error::Scheduler(err) => match err {
                scheduler::ScheduleJobError::NoWorkersAvailable => StatusCode::BAD_REQUEST,
                scheduler::ScheduleJobError::ActiveJobConflict { .. } => StatusCode::CONFLICT,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
        }
    }
}

/// Request body for creating a job.
///
/// Dispatches on the `kind` field to determine the job type.
#[derive(Debug, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum CreateJobRequest {
    /// Schedule a garbage collection job for a physical table revision.
    Gc {
        /// The location ID of the physical table revision to garbage collect.
        location_id: i64,
    },
}

/// Response body for a created job.
#[derive(Debug, serde::Serialize)]
pub struct CreateJobResponse {
    /// The ID of the scheduled job.
    pub job_id: JobId,
}
