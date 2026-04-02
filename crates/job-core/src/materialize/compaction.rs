use std::sync::Arc;

use amp_data_store::DataStore;
use common::physical_table::PhysicalTable;
use metadata_db::MetadataDb;
use tokio::task::JoinHandle;

use self::{collector::AmpCollectorInnerTask, compactor::TaskResult};
use crate::materialize::{WriterProperties, metrics::MetricsRegistry};

pub mod algorithm;
pub mod collector;
pub mod compactor;
pub mod config;
pub mod error;
pub mod plan;

pub struct AmpCompactorTask {
    inner: JoinHandle<TaskResult<AmpCollectorInnerTask>>,
}

impl AmpCompactorTask {
    fn new(inner: JoinHandle<TaskResult<AmpCollectorInnerTask>>) -> Self {
        Self { inner }
    }

    pub fn start(
        metadata_db: MetadataDb,
        store: DataStore,
        props: Arc<WriterProperties>,
        table: Arc<PhysicalTable>,
        metrics: Option<Arc<MetricsRegistry>>,
    ) -> Self {
        let inner = AmpCollectorInnerTask::start(metadata_db, store, props, table, metrics);
        Self::new(inner)
    }

    pub fn is_finished(&self) -> bool {
        self.inner.is_finished()
    }

    pub async fn join_current_then_spawn_new(&mut self) -> TaskResult<()> {
        let handle = &mut self.inner;

        let inner = handle.await??;

        self.inner = tokio::spawn(inner.try_run());

        Ok(())
    }
}
