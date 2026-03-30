use axum::{
    Router,
    routing::{get, post, put},
};

pub mod ctx;
pub mod datasets;
pub mod error;
pub mod jobs;
pub mod scheduler;

use self::{ctx::Ctx, datasets::handlers as dataset_handlers, jobs::handlers as job_handlers};

pub fn router() -> Router<Ctx> {
    Router::new()
        .route(
            "/datasets/{namespace}/{name}/versions/{revision}/deploy",
            post(dataset_handlers::deploy::handler),
        )
        .route(
            "/datasets/{namespace}/{name}/versions/{revision}/jobs",
            get(dataset_handlers::list_jobs::handler),
        )
        .route(
            "/jobs",
            get(job_handlers::get_all::handler)
                .post(job_handlers::create::handler)
                .delete(job_handlers::delete::handler),
        )
        .route(
            "/jobs/{id}",
            get(job_handlers::get_by_id::handler).delete(job_handlers::delete_by_id::handler),
        )
        .route("/jobs/{id}/stop", put(job_handlers::stop::handler))
        .route("/jobs/{id}/progress", get(job_handlers::progress::handler))
        .route("/jobs/{id}/events", get(job_handlers::events::handler))
        .route(
            "/jobs/{id}/events/{event_id}",
            get(job_handlers::event_by_id::handler),
        )
}
