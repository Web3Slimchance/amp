//! Axum-style job router for dispatching job descriptors to handlers.
//!
//! The router follows a builder pattern inspired by
//! [`axum::Router`](https://docs.rs/axum/latest/axum/struct.Router.html):
//!
//! 1. **Build** the router by registering handlers with [`JobRouter::route`].
//! 2. **Bind state** with [`JobRouter::with_state`] to produce a [`JobDispatcher`].
//! 3. **Dispatch** incoming job descriptors via [`JobDispatcher::dispatch`].
//!
//! ```ignore
//! let dispatcher = JobRouter::new()
//!     .route("gc", gc::execute)
//!     .route("materialize-raw", materialize_raw::execute)
//!     .with_state(worker_ctx);
//!
//! let fut = dispatcher.dispatch(descriptor_json, job_id);
//! ```

use std::{collections::BTreeMap, pin::Pin};

use amp_job_core::{
    ctx::FromCtx,
    error::{Error, JobError},
    job_id::JobId,
};
use serde::de::DeserializeOwned;

/// A pinned, boxed, `Send` future, the standard return type for job functions.
type JobFuture = Pin<Box<dyn Future<Output = Result<(), JobError>> + Send + 'static>>;

/// Type-erased job handler.
///
/// Each registered job function is wrapped in a [`TypedHandler`] that
/// implements this trait, erasing the concrete types behind dynamic dispatch.
trait Handler<Ctx>: Send + Sync {
    /// Deserialize the descriptor from raw JSON and produce a type-erased job future.
    fn dispatch(&self, ctx: &Ctx, raw: &str, job_id: JobId)
    -> Result<JobFuture, serde_json::Error>;
}

/// Concrete handler wrapping an async job function.
///
/// The function is called on each dispatch, producing a future that is
/// type-erased behind [`JobFuture`].
struct TypedHandler<F, C, D, E>(F, std::marker::PhantomData<fn(C, D) -> E>);

impl<Ctx, F, C, D, E, Fut> Handler<Ctx> for TypedHandler<F, C, D, E>
where
    Ctx: Send + Sync + 'static,
    C: FromCtx<Ctx> + Clone + Send + 'static,
    D: DeserializeOwned + Send + 'static,
    E: Error + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    F: Fn(C, D, JobId) -> Fut + Send + Sync,
{
    fn dispatch(
        &self,
        ctx: &Ctx,
        raw: &str,
        job_id: JobId,
    ) -> Result<JobFuture, serde_json::Error> {
        let desc: D = serde_json::from_str(raw)?;
        let job_ctx = C::from_ctx(ctx);
        let fut = self.0(job_ctx, desc, job_id);

        Ok(Box::pin(async move {
            fut.await.map_err(|err| Box::new(err) as JobError)
        }))
    }
}

/// Stateless job router builder.
///
/// Register job functions with [`route`](Self::route), then call
/// [`with_state`](Self::with_state) to bind the shared context and produce a
/// [`JobDispatcher`] ready for use.
///
/// The `Ctx` type parameter represents the shared context that job-specific
/// contexts are extracted from via [`FromCtx`].
pub struct JobRouter<Ctx> {
    handlers: BTreeMap<String, Box<dyn Handler<Ctx>>>,
}

impl<Ctx: Send + Sync + 'static> JobRouter<Ctx> {
    /// Create an empty router.
    #[must_use]
    pub fn new() -> Self {
        Self {
            handlers: BTreeMap::new(),
        }
    }

    /// Register a job function for the given `kind` tag value.
    ///
    /// The `kind` must match the value of the `"kind"` field in the JSON
    /// descriptor (the serde internally-tagged enum discriminant).
    ///
    /// The function `f` is called on each dispatch with the extracted context,
    /// deserialized descriptor, and job ID.
    ///
    /// # Panics
    ///
    /// Panics if a handler is already registered for the given `kind`.
    #[must_use]
    pub fn route<C, D, E, Fut, F>(mut self, kind: &str, f: F) -> Self
    where
        C: FromCtx<Ctx> + Clone + Send + 'static,
        D: DeserializeOwned + Send + 'static,
        E: Error + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        F: Fn(C, D, JobId) -> Fut + Send + Sync + 'static,
    {
        let prev = self.handlers.insert(
            kind.to_owned(),
            Box::new(TypedHandler(f, std::marker::PhantomData)),
        );
        assert!(
            prev.is_none(),
            "duplicate job route: a handler for kind \"{kind}\" is already registered"
        );
        self
    }

    /// Bind the shared context, producing a [`JobDispatcher`] ready to dispatch jobs.
    #[must_use]
    pub fn with_state(self, ctx: Ctx) -> JobDispatcher<Ctx> {
        JobDispatcher {
            ctx,
            handlers: self.handlers,
        }
    }
}

impl<Ctx: Send + Sync + 'static> Default for JobRouter<Ctx> {
    fn default() -> Self {
        Self::new()
    }
}

/// Finalized job dispatcher holding shared state.
///
/// Produced by [`JobRouter::with_state`]. Extracts the `"kind"` tag from
/// incoming JSON descriptors, looks up the registered handler, and delegates
/// deserialization and execution.
pub struct JobDispatcher<Ctx> {
    ctx: Ctx,
    handlers: BTreeMap<String, Box<dyn Handler<Ctx>>>,
}

impl<Ctx: Send + Sync + 'static> JobDispatcher<Ctx> {
    /// Dispatch a raw JSON job descriptor to the registered handler.
    pub fn dispatch(
        &self,
        descriptor_json: &str,
        job_id: JobId,
    ) -> Result<JobFuture, DispatchError> {
        let tag: KindTag =
            serde_json::from_str(descriptor_json).map_err(DispatchError::DeserializeKindTag)?;
        let handler = self
            .handlers
            .get(tag.kind)
            .ok_or_else(|| DispatchError::UnknownKind(tag.kind.to_owned()))?;
        handler
            .dispatch(&self.ctx, descriptor_json, job_id)
            .map_err(DispatchError::DeserializeDescriptor)
    }
}

/// Errors that can occur when dispatching a job descriptor to a handler.
#[derive(Debug, thiserror::Error)]
pub enum DispatchError {
    /// Failed to extract the job kind tag from the descriptor JSON.
    ///
    /// The incoming descriptor could not be parsed far enough to read the
    /// `"kind"` discriminant field. This typically means the payload is not
    /// valid JSON or is missing the required `"kind"` field.
    #[error("failed to deserialize job kind tag from descriptor")]
    DeserializeKindTag(#[source] serde_json::Error),

    /// No handler is registered for the given job kind.
    ///
    /// The `"kind"` field was successfully extracted but no route was
    /// registered for that value via [`JobRouter::route`].
    #[error("no handler registered for job kind '{0}'")]
    UnknownKind(String),

    /// Failed to deserialize the full job descriptor for the matched handler.
    ///
    /// The kind tag was valid and a handler was found, but the handler could
    /// not deserialize the complete descriptor into its expected type.
    #[error("failed to deserialize job descriptor")]
    DeserializeDescriptor(#[source] serde_json::Error),
}

/// Lightweight helper for extracting only the `kind` discriminant from a JSON
/// descriptor without parsing the full payload.
#[derive(serde::Deserialize)]
struct KindTag<'a> {
    kind: &'a str,
}
