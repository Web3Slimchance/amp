//! Context extraction for job execution.
//!
//! The [`FromCtx`] trait allows job-specific contexts to declare how they are
//! extracted from a larger, shared context (e.g. a worker-level context that
//! holds all dependencies). This is analogous to
//! [`axum::extract::FromRef`](https://docs.rs/axum/latest/axum/extract/trait.FromRef.html),
//! adapted for the job execution model.

/// Extract a value of type `Self` from a shared context of type `Ctx`.
///
/// This trait enables job-specific contexts to be constructed from a larger
/// shared context without the shared context needing to know about the
/// specific job types.
///
/// # Extracting shared dependencies
///
/// The primary use case is extracting job-specific contexts from a
/// worker-level context that holds all shared dependencies:
///
/// ```ignore
/// fn run_job<C>(worker_ctx: &C)
/// where
///     GcCtx: FromCtx<C>,
/// {
///     let gc_ctx = GcCtx::from_ctx(worker_ctx);
///     // ...
/// }
/// ```
///
/// # Identity implementation
///
/// Every type trivially implements `FromCtx` for itself via a blanket impl,
/// returning a clone. This means existing code that passes the context
/// directly continues to work.
pub trait FromCtx<Ctx> {
    /// Create a value of this type from the given context.
    fn from_ctx(ctx: &Ctx) -> Self;
}

/// Every type can be extracted from itself by cloning.
impl<T: Clone> FromCtx<T> for T {
    fn from_ctx(ctx: &T) -> Self {
        ctx.clone()
    }
}
