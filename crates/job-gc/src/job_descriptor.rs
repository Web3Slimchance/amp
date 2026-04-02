use metadata_db::physical_table_revision::LocationId;

use crate::job_kind::GcJobKind;

/// Job descriptor for garbage collection.
///
/// Contains the fields needed to execute a GC job for a single physical table revision.
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct JobDescriptor {
    /// The physical table revision to collect garbage for.
    pub location_id: LocationId,
}

impl From<JobDescriptor> for metadata_db::job_events::EventDetailOwned {
    fn from(desc: JobDescriptor) -> Self {
        #[derive(serde::Serialize)]
        struct Tagged<'a> {
            kind: GcJobKind,
            #[serde(flatten)]
            inner: &'a JobDescriptor,
        }

        // SAFETY: `to_raw_value` only fails on non-string map keys which cannot occur
        // with a flat struct containing only a primitive integer field.
        let raw = serde_json::value::to_raw_value(&Tagged {
            kind: GcJobKind,
            inner: &desc,
        })
        .expect("JobDescriptor serialization is infallible");

        metadata_db::job_events::EventDetail::from_owned_unchecked(raw)
    }
}

impl TryFrom<&metadata_db::job_events::EventDetail<'_>> for JobDescriptor {
    type Error = InvalidJobDescriptorError;

    fn try_from(raw: &metadata_db::job_events::EventDetail<'_>) -> Result<Self, Self::Error> {
        #[derive(serde::Deserialize)]
        struct TaggedOwned {
            #[allow(dead_code)]
            kind: GcJobKind,
            #[serde(flatten)]
            inner: JobDescriptor,
        }

        let tagged: TaggedOwned =
            serde_json::from_str(raw.as_str()).map_err(InvalidJobDescriptorError)?;
        Ok(tagged.inner)
    }
}

/// Error returned when an [`EventDetail`] cannot be converted into a [`JobDescriptor`].
#[derive(Debug, thiserror::Error)]
#[error("invalid job descriptor")]
pub struct InvalidJobDescriptorError(#[source] pub serde_json::Error);
