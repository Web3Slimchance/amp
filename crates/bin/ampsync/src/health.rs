//! Health check HTTP server

use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use axum::{Router, http::StatusCode, routing::get};
use tokio::net::TcpListener;

/// Shared health state for tracking task liveness.
///
/// When `strict_health` is enabled, the health endpoint returns 503 if
/// any task has exhausted its retry attempts.
#[derive(Clone)]
pub struct HealthState {
    /// Whether strict health checking is enabled
    strict: bool,
    /// Whether all tasks are healthy (true = healthy, false = at least one dead)
    tasks_healthy: Arc<AtomicBool>,
}

impl HealthState {
    /// Create a new health state.
    ///
    /// # Arguments
    /// * `strict` - Whether to return 503 when tasks are unhealthy
    pub fn new(strict: bool) -> Self {
        Self {
            strict,
            tasks_healthy: Arc::new(AtomicBool::new(true)),
        }
    }

    /// Mark tasks as unhealthy (called when a task exhausts retries).
    pub fn mark_unhealthy(&self) {
        self.tasks_healthy.store(false, Ordering::Relaxed);
    }

    /// Check if health endpoint should return OK.
    // Always healthy when not in strict mode.
    fn is_healthy(&self) -> bool {
        if self.strict {
            self.tasks_healthy.load(Ordering::Relaxed)
        } else {
            true
        }
    }
}

/// Start the health check HTTP server.
pub async fn serve(
    addr: SocketAddr,
    health_state: HealthState,
) -> Result<
    (
        SocketAddr,
        impl std::future::Future<Output = Result<(), std::io::Error>>,
    ),
    std::io::Error,
> {
    let listener = TcpListener::bind(addr).await?;
    let bound_addr = listener.local_addr()?;

    let app = Router::new().route(
        "/healthz",
        get(move || {
            let state = health_state.clone();
            async move {
                if state.is_healthy() {
                    StatusCode::OK
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                }
            }
        }),
    );

    let fut = async move { axum::serve(listener, app).await };

    Ok((bound_addr, fut))
}
