//! Service context
use std::sync::Arc;

use amp_controller_admin_jobs::scheduler::Scheduler;
use amp_data_store::DataStore;
use amp_datasets_registry::DatasetsRegistry;
use amp_providers_registry::ProvidersRegistry;
use amp_worker_core::node_id::NodeId;
use common::{datasets_cache::DatasetsCache, udfs::eth_call::EthCallUdfsCache};
use metadata_db::MetadataDb;

use crate::build_info::BuildInfo;

/// The Admin API context
#[derive(Clone)]
pub struct Ctx {
    pub metadata_db: MetadataDb,
    /// Datasets registry for manifest and version tag operations.
    pub datasets_registry: DatasetsRegistry,
    /// Providers registry for provider configuration operations.
    pub providers_registry: ProvidersRegistry,
    /// Datasets cache for loading datasets.
    pub datasets_cache: DatasetsCache,
    /// EthCall UDFs cache for eth_call UDF creation.
    pub ethcall_udfs_cache: EthCallUdfsCache,
    /// Job scheduler for triggering and reconciling dataset sync jobs.
    pub scheduler: Arc<dyn Scheduler>,
    /// Object store for output data (used by dataset restore handler)
    pub data_store: DataStore,
    /// Build information (version, git SHA, etc.)
    pub build_info: BuildInfo,
}

/// Bridges the admin-api's [`Scheduler`] trait to the
/// [`WorkerService`](amp_controller_admin_system::workers::worker_service::WorkerService)
/// trait expected by `amp-controller-admin-system`.
pub(crate) struct WorkerServiceImpl(pub(crate) Arc<dyn Scheduler>);

#[async_trait::async_trait]
impl amp_controller_admin_system::workers::worker_service::WorkerService for WorkerServiceImpl {
    async fn list_workers(
        &self,
    ) -> Result<
        Vec<metadata_db::workers::Worker>,
        amp_controller_admin_system::workers::worker_service::WorkerServiceError,
    > {
        self.0.list_workers().await.map_err(|err| {
            amp_controller_admin_system::workers::worker_service::WorkerServiceError(err.0)
        })
    }

    async fn get_worker_by_id(
        &self,
        id: &NodeId,
    ) -> Result<
        Option<metadata_db::workers::Worker>,
        amp_controller_admin_system::workers::worker_service::WorkerServiceError,
    > {
        self.0.get_worker_by_id(id).await.map_err(|err| {
            amp_controller_admin_system::workers::worker_service::WorkerServiceError(err.0)
        })
    }
}

/// Bridges the admin-api's [`Scheduler`] trait to the
/// [`RevisionGuard`](amp_controller_admin_tables::revisions::revision_guard::RevisionGuard)
/// trait expected by `amp-controller-admin-tables`.
///
/// Converts `metadata_db::jobs::JobId` to the worker-core job ID, delegates to
/// [`SchedulerJobs::get_job`](amp_controller_admin_jobs::scheduler::SchedulerJobs::get_job), and maps the
/// result to the bool contract: `true` = active writer (blocks the operation),
/// `false` = terminal or not found (safe to proceed).
pub(crate) struct RevisionGuardImpl(pub(crate) Arc<dyn Scheduler>);

#[async_trait::async_trait]
impl amp_controller_admin_tables::revisions::revision_guard::RevisionGuard for RevisionGuardImpl {
    async fn check_writer_job(
        &self,
        job_id: metadata_db::jobs::JobId,
    ) -> Result<bool, amp_controller_admin_tables::revisions::revision_guard::RevisionGuardError>
    {
        let worker_job_id: amp_worker_core::jobs::job_id::JobId = job_id.into();
        let job = self.0.get_job(worker_job_id).await.map_err(|err| {
            amp_controller_admin_tables::revisions::revision_guard::RevisionGuardError(err.0)
        })?;

        Ok(matches!(job, Some(job) if !job.status.is_terminal()))
    }
}
