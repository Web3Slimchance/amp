//! Amp Admin API router and OpenAPI specification

use std::sync::Arc;

use axum::Router;

use crate::ctx::{Ctx, RevisionGuardImpl, WorkerServiceImpl};

/// Create the admin API router with all routes registered
///
/// Returns a router configured with all admin API endpoints.
pub(crate) fn router(ctx: Ctx) -> Router<()> {
    let tables_ctx = amp_controller_admin_tables::ctx::Ctx {
        metadata_db: ctx.metadata_db.clone(),
        datasets_registry: ctx.datasets_registry.clone(),
        datasets_cache: ctx.datasets_cache.clone(),
        revision_guard: Arc::new(RevisionGuardImpl(ctx.scheduler.clone())),
        data_store: ctx.data_store.clone(),
    };
    let system_ctx = amp_controller_admin_system::ctx::Ctx {
        worker_service: Arc::new(WorkerServiceImpl(ctx.scheduler.clone())),
    };
    let providers_ctx = amp_controller_admin_providers::ctx::Ctx {
        providers_registry: ctx.providers_registry.clone(),
    };
    let datasets_ctx = amp_controller_admin_datasets::ctx::Ctx {
        metadata_db: ctx.metadata_db.clone(),
        datasets_registry: ctx.datasets_registry.clone(),
        datasets_cache: ctx.datasets_cache.clone(),
        ethcall_udfs_cache: ctx.ethcall_udfs_cache.clone(),
        data_store: ctx.data_store.clone(),
    };
    let jobs_ctx = amp_controller_admin_jobs::ctx::Ctx {
        scheduler: ctx.scheduler.clone(),
        datasets_registry: ctx.datasets_registry.clone(),
        data_store: ctx.data_store.clone(),
        datasets_cache: ctx.datasets_cache.clone(),
    };

    Router::new()
        .with_state(ctx)
        .merge(amp_controller_admin_jobs::router().with_state(jobs_ctx))
        .merge(amp_controller_admin_datasets::router().with_state(datasets_ctx))
        .merge(amp_controller_admin_system::router().with_state(system_ctx))
        .merge(amp_controller_admin_providers::router().with_state(providers_ctx))
        .merge(amp_controller_admin_tables::router().with_state(tables_ctx))
}

#[cfg(feature = "utoipa")]
#[derive(utoipa::OpenApi)]
#[openapi(
    info(
        title = "Amp Admin API",
        version = "1.0.0",
        description = include_str!("../SPEC_DESCRIPTION.md")
    ),
    paths(
        // Dataset endpoints
        amp_controller_admin_datasets::datasets::handlers::list_all::handler,
        amp_controller_admin_datasets::datasets::handlers::list_versions::handler,
        amp_controller_admin_jobs::datasets::handlers::list_jobs::handler,
        amp_controller_admin_datasets::datasets::handlers::get::handler,
        amp_controller_admin_datasets::datasets::handlers::get_manifest::handler,
        amp_controller_admin_datasets::datasets::handlers::register::handler,
        amp_controller_admin_jobs::datasets::handlers::deploy::handler,
        amp_controller_admin_tables::datasets::handlers::restore::handler,
        amp_controller_admin_tables::datasets::handlers::restore_table::handler,
        amp_controller_admin_datasets::datasets::handlers::delete::handler,
        amp_controller_admin_datasets::datasets::handlers::delete_version::handler,
        // Manifest endpoints
        amp_controller_admin_datasets::manifests::handlers::list_all::handler,
        amp_controller_admin_datasets::manifests::handlers::register::handler,
        amp_controller_admin_datasets::manifests::handlers::get_by_id::handler,
        amp_controller_admin_datasets::manifests::handlers::delete_by_id::handler,
        amp_controller_admin_datasets::manifests::handlers::list_datasets::handler,
        amp_controller_admin_datasets::manifests::handlers::prune::handler,
        // Job endpoints
        amp_controller_admin_jobs::jobs::handlers::create::handler,
        amp_controller_admin_jobs::jobs::handlers::get_all::handler,
        amp_controller_admin_jobs::jobs::handlers::get_by_id::handler,
        amp_controller_admin_jobs::jobs::handlers::stop::handler,
        amp_controller_admin_jobs::jobs::handlers::progress::handler,
        amp_controller_admin_jobs::jobs::handlers::events::handler,
        amp_controller_admin_jobs::jobs::handlers::event_by_id::handler,
        amp_controller_admin_jobs::jobs::handlers::delete::handler,
        amp_controller_admin_jobs::jobs::handlers::delete_by_id::handler,
        // Provider endpoints
        amp_controller_admin_providers::providers::handlers::get_all::handler,
        amp_controller_admin_providers::providers::handlers::get_by_id::handler,
        amp_controller_admin_providers::providers::handlers::create::handler,
        amp_controller_admin_providers::providers::handlers::delete_by_id::handler,
        // Files endpoints
        amp_controller_admin_tables::files::handlers::get_by_id::handler,
        // Schema endpoints
        amp_controller_admin_datasets::schema::handler,
        // Revision endpoints
        amp_controller_admin_tables::revisions::handlers::list::handler,
        amp_controller_admin_tables::revisions::handlers::restore::handler,
        amp_controller_admin_tables::revisions::handlers::activate::handler,
        amp_controller_admin_tables::revisions::handlers::deactivate::handler,
        amp_controller_admin_tables::revisions::handlers::get_by_id::handler,
        amp_controller_admin_tables::revisions::handlers::create::handler,
        amp_controller_admin_tables::revisions::handlers::delete::handler,
        amp_controller_admin_tables::revisions::handlers::truncate::handler,
        amp_controller_admin_tables::revisions::handlers::prune::handler,
        // Worker endpoints
        amp_controller_admin_system::workers::handlers::get_all::handler,
        amp_controller_admin_system::workers::handlers::get_by_id::handler,
    ),
    components(schemas(
        // Common schemas
        crate::handlers::error::ErrorResponse,
        // Manifest schemas
        amp_controller_admin_datasets::manifests::handlers::list_all::ManifestsResponse,
        amp_controller_admin_datasets::manifests::handlers::list_all::ManifestInfo,
        amp_controller_admin_datasets::manifests::handlers::register::RegisterManifestResponse,
        amp_controller_admin_datasets::manifests::handlers::list_datasets::ManifestDatasetsResponse,
        amp_controller_admin_datasets::manifests::handlers::list_datasets::Dataset,
        amp_controller_admin_datasets::manifests::handlers::prune::PruneResponse,
        // Dataset schemas
        amp_controller_admin_datasets::datasets::handlers::get::DatasetInfo,
        amp_controller_admin_datasets::datasets::handlers::list_all::DatasetsResponse,
        amp_controller_admin_datasets::datasets::handlers::list_all::DatasetSummary,
        amp_controller_admin_datasets::datasets::handlers::list_versions::VersionsResponse,
        amp_controller_admin_datasets::datasets::handlers::list_versions::VersionInfo,
        amp_controller_admin_datasets::datasets::handlers::register::RegisterRequest,
        amp_controller_admin_datasets::datasets::handlers::register::RegisterResponse,
        amp_controller_admin_jobs::datasets::handlers::deploy::DeployRequest,
        amp_controller_admin_jobs::datasets::handlers::deploy::DeployResponse,
        amp_controller_admin_tables::datasets::handlers::restore::RestoreResponse,
        amp_controller_admin_tables::datasets::handlers::restore::RestoredTableInfo,
        amp_controller_admin_tables::datasets::handlers::restore_table::RestoreTablePayload,
        // Job schemas
        amp_controller_admin_jobs::jobs::handlers::create::CreateJobRequest,
        amp_controller_admin_jobs::jobs::handlers::create::CreateJobResponse,
        amp_controller_admin_jobs::jobs::handlers::progress::JobProgressResponse,
        amp_controller_admin_jobs::jobs::handlers::progress::TableProgress,
        amp_controller_admin_jobs::jobs::handlers::events::JobEventsResponse,
        amp_controller_admin_jobs::jobs::handlers::events::JobEventInfo,
        amp_controller_admin_jobs::jobs::handlers::event_by_id::JobEventDetailResponse,
        amp_controller_admin_jobs::jobs::handlers::job_info::JobInfo,
        amp_controller_admin_jobs::jobs::handlers::get_all::JobsResponse,
        amp_controller_admin_jobs::jobs::handlers::delete::JobStatusFilter,
        // Provider schemas
        amp_controller_admin_providers::providers::handlers::provider_info::ProviderInfo,
        amp_controller_admin_providers::providers::handlers::get_all::ProvidersResponse,
        // File schemas
        amp_controller_admin_tables::files::handlers::get_by_id::FileInfo,
        // Schema schemas
        amp_controller_admin_datasets::schema::SchemaRequest,
        amp_controller_admin_datasets::schema::SchemaResponse,
        // Revision schemas
        amp_controller_admin_tables::revisions::handlers::restore::RestoreResponse,
        amp_controller_admin_tables::revisions::handlers::activate::ActivationPayload,
        amp_controller_admin_tables::revisions::handlers::deactivate::DeactivationPayload,
        amp_controller_admin_tables::revisions::handlers::get_by_id::RevisionInfo,
        amp_controller_admin_tables::revisions::handlers::get_by_id::RevisionMetadataInfo,
        amp_controller_admin_tables::revisions::handlers::create::CreatePayload,
        amp_controller_admin_tables::revisions::handlers::create::CreateRevisionResponse,
        amp_controller_admin_tables::revisions::handlers::truncate::TruncateResponse,
        amp_controller_admin_tables::revisions::handlers::prune::PruneResponse,
        // Worker schemas
        amp_controller_admin_system::workers::handlers::get_all::WorkerInfo,
        amp_controller_admin_system::workers::handlers::get_all::WorkersResponse,
        amp_controller_admin_system::workers::handlers::get_by_id::WorkerDetailResponse,
        amp_controller_admin_system::workers::handlers::get_by_id::WorkerMetadata,
    )),
    tags(
        (name = "datasets", description = "Dataset management endpoints"),
        (name = "jobs", description = "Job management endpoints"),
        (name = "manifests", description = "Manifest management endpoints"),
        (name = "providers", description = "Provider management endpoints"),
        (name = "files", description = "File access endpoints"),
        (name = "schema", description = "Schema generation endpoints"),
        (name = "revisions", description = "Revision management endpoints"),
        (name = "workers", description = "Worker management endpoints"),
    )
)]
struct ApiDoc;

#[cfg(feature = "utoipa")]
pub fn generate_openapi_spec() -> utoipa::openapi::OpenApi {
    <ApiDoc as utoipa::OpenApi>::openapi()
}
