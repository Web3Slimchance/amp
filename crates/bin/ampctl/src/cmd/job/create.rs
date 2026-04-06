//! Job creation command.
//!
//! Creates a new job via the admin API's `POST /jobs` endpoint.

use amp_client_admin::jobs::{CreateError, CreateJobRequest};
use monitoring::logging;

use crate::args::GlobalArgs;

/// Create a new job via the admin API.
#[tracing::instrument(skip_all, fields(admin_url = %global.admin_url))]
pub async fn run(Args { global, kind }: Args) -> Result<(), Error> {
    let client = global.build_client().map_err(Error::ClientBuildError)?;

    let request = match kind {
        JobKind::Gc(gc_args) => CreateJobRequest::Gc {
            location_id: gc_args.location_id,
            // TODO: Accept retry_strategy from args
            retry_strategy: None,
        },
    };

    tracing::debug!("Creating job via admin API");

    let job_id = client.jobs().create(&request).await.map_err(|err| {
        tracing::error!(error = %err, error_source = logging::error_source(&err), "Failed to create job");
        Error::CreateError(err)
    })?;

    let result = CreateResult { job_id, request };
    global.print(&result).map_err(Error::JsonSerialization)?;

    Ok(())
}

/// Errors for job creation operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to build client
    #[error("failed to build admin API client")]
    ClientBuildError(#[source] crate::args::BuildClientError),

    /// Error creating job via admin API
    #[error("failed to create job")]
    CreateError(#[source] CreateError),

    /// Failed to serialize result to JSON
    #[error("failed to serialize result to JSON")]
    JsonSerialization(#[source] serde_json::Error),
}

/// Command-line arguments for the `job create` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    pub global: GlobalArgs,

    /// The job kind to create
    #[command(subcommand)]
    pub kind: JobKind,
}

/// Supported job kinds for creation.
#[derive(Debug, clap::Subcommand)]
pub enum JobKind {
    /// Schedule a garbage collection job for a physical table revision
    Gc(GcArgs),
}

/// Arguments for creating a GC job.
#[derive(Debug, clap::Args)]
pub struct GcArgs {
    /// The location ID of the physical table revision to garbage collect
    pub location_id: i64,
}

/// Result of a job creation operation.
#[derive(serde::Serialize)]
struct CreateResult {
    job_id: amp_job_core::job_id::JobId,
    #[serde(flatten)]
    request: CreateJobRequest,
}

impl std::fmt::Display for CreateResult {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        writeln!(
            f,
            "{} Job {} created",
            console::style("✓").green().bold(),
            self.job_id,
        )
    }
}
