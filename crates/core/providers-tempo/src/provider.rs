use std::{
    future::Future,
    num::NonZeroU32,
    path::Path,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use alloy::{
    providers::{
        ProviderBuilder as AlloyProviderBuilder, RootProvider as AlloyRootProvider, WsConnect,
    },
    rpc::client::ClientBuilder,
    transports::{Authorization as TransportAuthorization, TransportError, http::reqwest::Client},
};
use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};
use headers::{Authorization, HeaderMap, HeaderMapExt, HeaderName, HeaderValue};
use tempo_alloy::TempoNetwork;
use tower::{Layer, Service};
use url::Url;

use crate::config::{AuthHeaderName, AuthToken};

/// Type alias for a Tempo RPC provider using the Tempo network type.
pub type TempoRpcAlloyProvider = AlloyRootProvider<TempoNetwork>;

/// Authentication configuration for Tempo RPC providers.
pub enum Auth {
    /// Standard `Authorization: Bearer <token>` header.
    Bearer(AuthToken),
    /// Custom header: `<name>: <value>`.
    CustomHeader {
        name: AuthHeaderName,
        value: AuthToken,
    },
}

/// Create an HTTP/HTTPS Tempo RPC provider.
///
/// # Errors
///
/// Returns an error if the HTTP client cannot be built (e.g., TLS backend
/// initialization failure).
pub fn new_http(
    url: Url,
    auth: Option<Auth>,
    rate_limit: Option<NonZeroU32>,
    timeout: Duration,
) -> Result<TempoRpcAlloyProvider, HttpBuildError> {
    let mut http_client_builder = Client::builder();
    http_client_builder = if let Some(auth) = auth {
        let mut headers = HeaderMap::new();
        match auth {
            // SAFETY: AuthToken and AuthHeaderName are validated at deserialization
            // time via HeaderValue::try_from and HeaderName::try_from checks.
            Auth::Bearer(token) => {
                headers.typed_insert(
                    Authorization::bearer(&token.into_inner())
                        .expect("validated at config parse time"),
                );
            }
            Auth::CustomHeader { name, value: token } => {
                let name = HeaderName::try_from(name.into_inner())
                    .expect("validated at config parse time");
                let mut value = HeaderValue::try_from(token.into_inner())
                    .expect("validated at config parse time");
                value.set_sensitive(true);
                headers.insert(name, value);
            }
        }
        http_client_builder.default_headers(headers)
    } else {
        http_client_builder
    };
    let http_client = http_client_builder
        .timeout(timeout)
        .build()
        .map_err(HttpBuildError)?;

    let client_builder = ClientBuilder::default();
    let client = if let Some(rl) = rate_limit {
        client_builder
            .layer(RateLimitLayer::new(rl))
            .http_with_client(http_client, url)
    } else {
        client_builder.http_with_client(http_client, url)
    };

    Ok(AlloyProviderBuilder::new()
        .disable_recommended_fillers()
        .network::<TempoNetwork>()
        .connect_client(client))
}

/// Error building the HTTP client for a Tempo RPC provider.
///
/// Occurs when the reqwest HTTP client cannot be constructed, typically due to
/// TLS backend initialization failure.
#[derive(Debug, thiserror::Error)]
#[error("failed to build HTTP client")]
pub struct HttpBuildError(#[source] alloy::transports::http::reqwest::Error);

/// Create an IPC Tempo RPC provider.
pub async fn new_ipc<P: AsRef<Path>>(
    path: P,
    rate_limit: Option<NonZeroU32>,
) -> Result<TempoRpcAlloyProvider, TransportError> {
    let client_builder = ClientBuilder::default();
    let client = if let Some(rl) = rate_limit {
        client_builder
            .layer(RateLimitLayer::new(rl))
            .ipc(path.as_ref().to_path_buf().into())
            .await?
    } else {
        client_builder
            .ipc(path.as_ref().to_path_buf().into())
            .await?
    };

    Ok(AlloyProviderBuilder::new()
        .disable_recommended_fillers()
        .network::<TempoNetwork>()
        .connect_client(client))
}

/// Create a WebSocket Tempo RPC provider.
pub async fn new_ws(
    url: Url,
    auth: Option<Auth>,
    rate_limit: Option<NonZeroU32>,
) -> Result<TempoRpcAlloyProvider, TransportError> {
    let mut ws_connect = WsConnect::new(url);
    ws_connect = if let Some(a) = auth {
        let token = match a {
            Auth::Bearer(token) => token,
            Auth::CustomHeader { value: token, .. } => {
                tracing::warn!(
                    "custom auth header unsupported for WebSocket, fell back to Authorization header"
                );
                token
            }
        };
        ws_connect.with_auth(TransportAuthorization::raw(token.into_inner()))
    } else {
        ws_connect
    };

    let client_builder = ClientBuilder::default();
    let client = if let Some(rl) = rate_limit {
        client_builder
            .layer(RateLimitLayer::new(rl))
            .ws(ws_connect)
            .await?
    } else {
        client_builder.ws(ws_connect).await?
    };

    Ok(AlloyProviderBuilder::new()
        .disable_recommended_fillers()
        .network::<TempoNetwork>()
        .connect_client(client))
}

struct RateLimitLayer {
    limiter: Arc<DefaultDirectRateLimiter>,
}

impl RateLimitLayer {
    fn new(rate_limit: NonZeroU32) -> Self {
        let quota = Quota::per_minute(rate_limit);
        let limiter = Arc::new(RateLimiter::direct(quota));
        RateLimitLayer { limiter }
    }
}

impl<S> Layer<S> for RateLimitLayer {
    type Service = RateLimitService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RateLimitService {
            inner,
            limiter: Arc::clone(&self.limiter),
        }
    }
}

#[derive(Clone)]
struct RateLimitService<S> {
    inner: S,
    limiter: Arc<DefaultDirectRateLimiter>,
}

impl<S, Request> Service<Request> for RateLimitService<S>
where
    S: Service<Request> + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let inner_fut = self.inner.call(req);
        let limiter = Arc::clone(&self.limiter);

        Box::pin(async move {
            limiter.until_ready().await;
            inner_fut.await
        })
    }
}
