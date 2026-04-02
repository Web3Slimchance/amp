//! Job creation handler

use amp_job_core::job_id::JobId;
use amp_job_gc::job_descriptor::JobDescriptor as GcJobDescriptor;
use amp_job_materialize_datasets_derived::job_descriptor::JobDescriptor as MaterializeDerivedJobDescriptor;
use amp_job_materialize_datasets_raw::job_descriptor::JobDescriptor as MaterializeRawJobDescriptor;
use axum::{
    Json,
    extract::{State, rejection::JsonRejection},
    http::StatusCode,
};
use datasets_common::hash_reference::HashReference;
use monitoring::logging;

use crate::{
    ctx::Ctx,
    error::{ErrorResponse, IntoErrorResponse},
    scheduler,
};

/// Handler for the `POST /jobs` endpoint
///
/// Schedules a new job for execution by an available worker. The job type is
/// determined by the `kind` field in the request body (`gc`, `materialize-raw`,
/// or `materialize-derived`). Additional fields depend on the job kind and are
/// defined by the corresponding job descriptor type.
///
/// ## Response
/// - **202 Accepted**: Job scheduled successfully
/// - **400 Bad Request**: Invalid request body or unsupported job kind
/// - **409 Conflict**: An active job with the same idempotency key already exists
/// - **500 Internal Server Error**: Scheduler error
///
/// ## Error Codes
/// - `INVALID_BODY`: The request body is not valid JSON or has missing/invalid fields
/// - `NO_WORKERS_AVAILABLE`: No active workers to handle the job
/// - `ACTIVE_JOB_CONFLICT`: An active job with the same idempotency key already exists
/// - `SCHEDULER_ERROR`: Internal scheduler failure
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        post,
        path = "/jobs",
        tag = "jobs",
        operation_id = "jobs_create",
        request_body = CreateJobRequest,
        responses(
            (status = 202, description = "Job scheduled successfully", body = CreateJobResponse),
            (status = 400, description = "Bad request (invalid body or no workers)", body = ErrorResponse),
            (status = 409, description = "Active job conflict", body = ErrorResponse),
            (status = 500, description = "Internal server error", body = ErrorResponse)
        )
    )
)]
pub async fn handler(
    State(ctx): State<Ctx>,
    json: Result<Json<CreateJobRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<CreateJobResponse>), ErrorResponse> {
    let Json(req) = json.map_err(|err| {
        tracing::debug!(error = %err, "invalid request body");
        Error::InvalidBody(err)
    })?;

    // Build the idempotency key and job descriptor based on the job kind
    let (idempotency_key, descriptor, worker_id) = match req {
        CreateJobRequest::Gc {
            descriptor,
            worker_id,
        } => {
            let key = amp_job_gc::job_key::idempotency_key(descriptor.location_id);
            let desc = scheduler::JobDescriptor::from(descriptor);
            (key, desc, worker_id)
        }
        CreateJobRequest::MaterializeRaw {
            descriptor,
            worker_id,
        } => {
            let reference = HashReference::new(
                descriptor.dataset_namespace.clone(),
                descriptor.dataset_name.clone(),
                descriptor.manifest_hash.clone(),
            );
            let key = amp_job_materialize_datasets_raw::job_key::idempotency_key(&reference);
            let desc = scheduler::JobDescriptor::from(descriptor);
            (key, desc, worker_id)
        }
        CreateJobRequest::MaterializeDerived {
            descriptor,
            worker_id,
        } => {
            let reference = HashReference::new(
                descriptor.dataset_namespace.clone(),
                descriptor.dataset_name.clone(),
                descriptor.manifest_hash.clone(),
            );
            let key = amp_job_materialize_datasets_derived::job_key::idempotency_key(&reference);
            let desc = scheduler::JobDescriptor::from(descriptor);
            (key, desc, worker_id)
        }
    };

    // Schedule the job and return the assigned job ID
    let job_id = ctx
        .scheduler
        .schedule_job(idempotency_key.into(), descriptor, worker_id)
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

/// Request body for creating a job.
///
/// Dispatches on the `kind` field to determine the job type.
/// Fields for each variant are defined by the corresponding job descriptor type.
#[derive(Debug, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub enum CreateJobRequest {
    /// Schedule a garbage collection job for a physical table revision.
    Gc {
        /// The job descriptor fields (flattened from the GC job descriptor).
        #[serde(flatten)]
        #[cfg_attr(feature = "utoipa", schema(value_type = Object))]
        descriptor: GcJobDescriptor,
        /// Optional worker selector (exact ID or glob pattern).
        #[serde(default)]
        #[cfg_attr(feature = "utoipa", schema(value_type = Option<String>))]
        worker_id: Option<scheduler::NodeSelector>,
    },
    /// Schedule a raw dataset materialization job.
    MaterializeRaw {
        /// The job descriptor fields (flattened from the raw materialization descriptor).
        #[serde(flatten)]
        #[cfg_attr(feature = "utoipa", schema(value_type = Object))]
        descriptor: MaterializeRawJobDescriptor,
        /// Optional worker selector (exact ID or glob pattern).
        #[serde(default)]
        #[cfg_attr(feature = "utoipa", schema(value_type = Option<String>))]
        worker_id: Option<scheduler::NodeSelector>,
    },
    /// Schedule a derived dataset materialization job.
    MaterializeDerived {
        /// The job descriptor fields (flattened from the derived materialization descriptor).
        #[serde(flatten)]
        #[cfg_attr(feature = "utoipa", schema(value_type = Object))]
        descriptor: MaterializeDerivedJobDescriptor,
        /// Optional worker selector (exact ID or glob pattern).
        #[serde(default)]
        #[cfg_attr(feature = "utoipa", schema(value_type = Option<String>))]
        worker_id: Option<scheduler::NodeSelector>,
    },
}

/// Response body for a created job.
#[derive(Debug, serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct CreateJobResponse {
    /// The ID of the scheduled job.
    #[cfg_attr(feature = "utoipa", schema(value_type = i64))]
    pub job_id: JobId,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid request body
    #[error("invalid request body: {0}")]
    InvalidBody(#[source] JsonRejection),

    /// Scheduler error
    #[error("failed to schedule job")]
    Scheduler(#[source] scheduler::ScheduleJobError),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidBody(_) => "INVALID_BODY",
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
            Error::Scheduler(err) => match err {
                scheduler::ScheduleJobError::NoWorkersAvailable => StatusCode::BAD_REQUEST,
                scheduler::ScheduleJobError::ActiveJobConflict { .. } => StatusCode::CONFLICT,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
        }
    }
}
