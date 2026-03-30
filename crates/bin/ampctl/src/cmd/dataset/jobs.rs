//! Dataset jobs listing command.
//!
//! Retrieves and displays all jobs for a specific dataset revision through the admin API by:
//! 1. Creating a client for the admin API
//! 2. Using the client's dataset list_jobs method
//! 3. Displaying the jobs list
//!
//! # Dataset Reference Format
//!
//! `namespace/name@version` (e.g., `graph/eth_mainnet@1.0.0`)
//!
//! # Configuration
//!
//! - Admin URL: `--admin-url` flag or `AMP_ADMIN_URL` env var (default: `http://localhost:1610`)
//! - Logging: `AMP_LOG` env var (`error`, `warn`, `info`, `debug`, `trace`)

use amp_client_admin as client;
use datasets_common::reference::Reference;
use monitoring::logging;

use crate::args::GlobalArgs;

/// Command-line arguments for the `dataset jobs` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    pub global: GlobalArgs,

    /// The dataset reference (format: namespace/name@revision)
    pub reference: Reference,
}

/// List all jobs for a dataset revision by retrieving them from the admin API.
///
/// Retrieves job information and displays it based on the output format.
///
/// # Errors
///
/// Returns [`Error`] for API errors (400/404/500) or network failures.
#[tracing::instrument(skip_all, fields(admin_url = %global.admin_url, reference = %reference))]
pub async fn run(Args { global, reference }: Args) -> Result<(), Error> {
    tracing::debug!("Retrieving jobs for dataset revision from admin API");

    let jobs = get_jobs(&global, &reference).await?;
    let result = JobsResult { reference, jobs };
    global.print(&result).map_err(Error::JsonFormattingError)?;

    Ok(())
}

/// Retrieve all jobs for a dataset revision from the admin API.
///
/// Creates a client and uses the dataset list_jobs method.
#[tracing::instrument(skip_all)]
async fn get_jobs(
    global: &GlobalArgs,
    reference: &Reference,
) -> Result<Vec<client::jobs::JobInfo>, Error> {
    let client = global.build_client().map_err(Error::ClientBuildError)?;

    let jobs = client.datasets().list_jobs(reference).await.map_err(|err| {
        tracing::error!(error = %err, error_source = logging::error_source(&err), "Failed to list jobs");
        Error::ClientError(err)
    })?;

    Ok(jobs)
}

/// Result wrapper for jobs list output.
#[derive(serde::Serialize)]
struct JobsResult {
    #[serde(skip)]
    reference: Reference,
    jobs: Vec<client::jobs::JobInfo>,
}

impl std::fmt::Display for JobsResult {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        writeln!(f, "Jobs for dataset: {}", self.reference)?;

        if self.jobs.is_empty() {
            writeln!(f, "\nNo jobs found")?;
        } else {
            writeln!(f, "\n{} job(s):", self.jobs.len())?;
            for job in &self.jobs {
                writeln!(f)?;
                writeln!(f, "  ID:         {}", job.id)?;
                writeln!(f, "  Status:     {}", job.status)?;
                writeln!(f, "  Node:       {}", job.node_id)?;
                writeln!(f, "  Created:    {}", job.created_at)?;
                writeln!(f, "  Updated:    {}", job.updated_at)?;
            }
        }
        Ok(())
    }
}

/// Errors for dataset jobs listing operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to build client
    #[error("failed to build admin API client")]
    ClientBuildError(#[source] crate::args::BuildClientError),

    /// Client error from the API
    #[error("client error")]
    ClientError(#[source] client::datasets::ListJobsError),

    /// Failed to format JSON for display
    #[error("failed to format jobs JSON")]
    JsonFormattingError(#[source] serde_json::Error),
}
