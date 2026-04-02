/// The logical descriptor of a job, as stored in the `descriptor` column of the `jobs`
/// metadata DB table.
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum JobDescriptor {
    MaterializeRaw(amp_job_materialize_datasets_raw::job_descriptor::JobDescriptor),
    MaterializeDerived(amp_job_materialize_datasets_derived::job_descriptor::JobDescriptor),
    Gc(amp_job_gc::job_descriptor::JobDescriptor),
}
