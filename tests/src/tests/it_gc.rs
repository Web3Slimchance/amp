//! Integration tests for GC job execution.
//!
//! These tests verify the full collection algorithm: expired file streaming,
//! metadata deletion, and physical file deletion.

use std::time::Duration;

use amp_data_store::DataStore;
use amp_job_core::job_id::JobId;
use amp_job_gc::{job_ctx::Context, job_descriptor::JobDescriptor};
use futures::TryStreamExt;
use metadata_db::{
    datasets::{DatasetName, DatasetNamespace},
    files::{self, FileName},
    gc, physical_table,
    physical_table_revision::{self, LocationId, TablePath},
};
use tempfile::TempDir;
use url::Url;

use crate::testlib::fixtures::MetadataDb as MetadataDbFixture;

// --- GC execution tests ---

#[tokio::test]
async fn gc_deletes_expired_files() {
    //* Given
    let ctx = GcTestCtx::setup("gc_deletes_expired").await;
    let job_id = JobId::try_from(1_i64).expect("job id should be valid");

    let revision_path = "test_ns/test_dataset/aaaaaaaaaa/test_table";
    let location_id = ctx.register_revision(revision_path).await;

    // Create physical files on disk
    let revision_dir = ctx.test_dir.path().join(revision_path);
    std::fs::create_dir_all(&revision_dir).expect("failed to create revision dir");
    std::fs::write(revision_dir.join("file_a.parquet"), b"test data a")
        .expect("failed to write file_a");
    std::fs::write(revision_dir.join("file_b.parquet"), b"test data b")
        .expect("failed to write file_b");

    // Register file metadata in Postgres
    let base_url = Url::parse(&format!(
        "file://{}/{}/",
        ctx.test_dir.path().display(),
        revision_path,
    ))
    .expect("failed to parse URL");

    ctx.register_file(
        location_id,
        "file_a.parquet",
        &base_url.join("file_a.parquet").unwrap(),
    )
    .await;
    ctx.register_file(
        location_id,
        "file_b.parquet",
        &base_url.join("file_b.parquet").unwrap(),
    )
    .await;

    let file_ids = ctx.file_ids(location_id).await;
    assert_eq!(file_ids.len(), 2, "should have 2 registered files");

    // Schedule files for GC with 1s duration (minimum allowed by DB constraint)
    gc::upsert(&ctx.conn, location_id, &file_ids, Duration::from_secs(1))
        .await
        .expect("failed to upsert GC manifest");

    // Wait for the files to expire
    tokio::time::sleep(Duration::from_secs(2)).await;

    //* When
    amp_job_gc::job_impl::execute(ctx.gc_context(), JobDescriptor { location_id }, job_id)
        .await
        .expect("GC execution failed");

    //* Then
    let remaining_files = ctx.file_ids(location_id).await;
    assert!(
        remaining_files.is_empty(),
        "all file metadata should be deleted"
    );

    let expired: Vec<gc::GcManifestRow> = gc::stream_expired(&ctx.conn, location_id)
        .try_collect()
        .await
        .expect("failed to stream expired");
    assert!(expired.is_empty(), "GC manifest should be empty");

    assert!(
        !revision_dir.join("file_a.parquet").exists(),
        "file_a.parquet should be deleted from disk"
    );
    assert!(
        !revision_dir.join("file_b.parquet").exists(),
        "file_b.parquet should be deleted from disk"
    );
}

#[tokio::test]
async fn gc_with_no_expired_files_is_a_noop() {
    //* Given
    let ctx = GcTestCtx::setup("gc_noop").await;
    let job_id = JobId::try_from(1_i64).expect("job id should be valid");

    let revision_path = "test_ns/test_dataset/aaaaaaaaaa/test_table";
    let location_id = ctx.register_revision(revision_path).await;

    //* When
    amp_job_gc::job_impl::execute(ctx.gc_context(), JobDescriptor { location_id }, job_id)
        .await
        .expect("GC execution should succeed with no expired files");

    //* Then — no errors, no panics
}

#[tokio::test]
async fn gc_with_nonexistent_location_returns_error() {
    //* Given
    let ctx = GcTestCtx::setup("gc_nonexistent").await;
    let job_id = JobId::try_from(1_i64).expect("job id should be valid");

    let desc = JobDescriptor {
        location_id: LocationId::try_from(999999i64).unwrap(),
    };

    //* When
    let result = amp_job_gc::job_impl::execute(ctx.gc_context(), desc, job_id).await;

    //* Then
    assert!(result.is_err(), "should fail with LocationNotFound");
    assert!(
        matches!(
            result.unwrap_err(),
            amp_job_gc::job_impl::Error::LocationNotFound(_)
        ),
        "error should be LocationNotFound"
    );
}

// --- Test helpers ---

/// Test context for GC tests.
///
/// Wraps the shared test fixtures (metadata DB, test directory) and provides
/// convenience helpers for setting up test data.
struct GcTestCtx {
    conn: metadata_db::MetadataDb,
    test_dir: TempDir,
    _metadata_db: MetadataDbFixture,
}

impl GcTestCtx {
    /// Create a new GC test context with an isolated Postgres instance and temp directory.
    async fn setup(test_name: &str) -> Self {
        monitoring::logging::init();

        let metadata_db = MetadataDbFixture::new().await;
        let conn = metadata_db.conn_pool().clone();

        // Register a worker (required for foreign key constraints on job tables)
        let worker_id = metadata_db::workers::WorkerNodeId::from_ref_unchecked("gc-test-worker");
        let raw: Box<serde_json::value::RawValue> =
            serde_json::from_str("{}").expect("empty JSON should be valid");
        let worker_info = metadata_db::workers::WorkerInfo::from_owned_unchecked(raw);
        metadata_db::workers::register(&conn, worker_id, worker_info)
            .await
            .expect("failed to register worker");

        let test_dir = tempfile::Builder::new()
            .prefix(&format!("gc_test__{test_name}__"))
            .tempdir()
            .expect("failed to create test dir");

        Self {
            conn,
            test_dir,
            _metadata_db: metadata_db,
        }
    }

    /// Create a DataStore backed by the test directory's filesystem.
    fn data_store(&self) -> DataStore {
        let data_url = amp_object_store::url::ObjectStoreUrl::new(format!(
            "file://{}/",
            self.test_dir.path().display()
        ))
        .expect("failed to create object store URL");

        DataStore::new(self.conn.clone(), data_url, 0).expect("failed to create data store")
    }

    /// Register a physical table and revision, returning its location ID.
    async fn register_revision(&self, path: &str) -> LocationId {
        let namespace = DatasetNamespace::from_ref_unchecked("test_ns");
        let name = DatasetName::from_ref_unchecked("test_dataset");
        let hash = metadata_db::manifests::ManifestHash::from_ref_unchecked(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        );
        let table_name = physical_table::TableName::from_ref_unchecked("test_table");

        physical_table::register(&self.conn, &namespace, &name, &hash, &table_name)
            .await
            .expect("failed to register physical table");

        let metadata_json = serde_json::json!({
            "dataset_namespace": "test_ns",
            "dataset_name": "test_dataset",
            "manifest_hash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "table_name": "test_table",
        });
        let raw = serde_json::value::to_raw_value(&metadata_json)
            .expect("test metadata should serialize");
        let metadata = physical_table_revision::RevisionMetadata::from_owned_unchecked(raw);
        let table_path = TablePath::from_ref_unchecked(path);

        physical_table_revision::register(&self.conn, table_path, metadata)
            .await
            .expect("failed to register revision")
    }

    /// Register a file in the metadata database for a given location.
    async fn register_file(&self, location_id: LocationId, file_name: &str, url: &Url) {
        files::register(
            &self.conn,
            location_id,
            url,
            FileName::from_ref_unchecked(file_name),
            100,
            None,
            None,
            serde_json::json!({}),
            &vec![0u8; 10],
        )
        .await
        .expect("failed to register file");
    }

    /// Get all file IDs for a given location.
    async fn file_ids(&self, location_id: LocationId) -> Vec<files::FileId> {
        files::stream_by_location_id_with_details(&self.conn, location_id)
            .map_ok(|f| f.id)
            .try_collect()
            .await
            .expect("failed to stream files")
    }

    /// Build a GC execution context.
    fn gc_context(&self) -> Context {
        Context {
            data_store: self.data_store(),
            meter: None,
        }
    }
}
