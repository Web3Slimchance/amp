//! Service context

use std::sync::Arc;

use amp_data_store::DataStore;
use amp_datasets_registry::DatasetsRegistry;
use common::datasets_cache::DatasetsCache;

use crate::scheduler::Scheduler;

/// The controller-admin-jobs context
#[derive(Clone)]
pub struct Ctx {
    /// Job scheduler for triggering and managing dataset sync jobs.
    pub scheduler: Arc<dyn Scheduler>,
    /// Datasets registry for resolving dataset revisions.
    pub datasets_registry: DatasetsRegistry,
    /// Object store for output data (used by job progress handler)
    pub data_store: DataStore,
    /// Datasets cache for loading datasets.
    pub datasets_cache: DatasetsCache,
}
