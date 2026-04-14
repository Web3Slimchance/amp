use std::time::Duration;

use alloy::{
    network::AnyNetwork,
    providers::{Provider as _, ProviderBuilder},
    rpc::client::ClientBuilder,
};
use datasets_common::block_num::BlockNum;
use futures::StreamExt as _;
use tokio::sync::watch;
use tracing::Instrument as _;
use url::Url;

use crate::provider::{Auth, ws_connect_with_auth};

const DEFAULT_MIN_BACKOFF: Duration = Duration::from_secs(2);
const DEFAULT_MAX_BACKOFF: Duration = Duration::from_secs(200);

/// Backoff configuration for reconnection attempts.
#[derive(Debug, Clone)]
pub struct BackoffConfig {
    pub min: Duration,
    pub max: Duration,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            min: DEFAULT_MIN_BACKOFF,
            max: DEFAULT_MAX_BACKOFF,
        }
    }
}

/// Best-effort new-head notifier backed by a WebSocket `eth_subscribe("newHeads")` subscription.
///
/// Spawns a background task that maintains a WebSocket subscription and sends the latest
/// block number whenever a new block header arrives. If the WebSocket connection fails,
/// the task reconnects with exponential backoff. The consumer should race this against a
/// poll-interval timer so that polling remains the reliability backstop.
pub struct NewHeadNotifier {
    rx: watch::Receiver<BlockNum>,
}

impl NewHeadNotifier {
    /// Spawn a background task that subscribes to new heads and notifies via channel.
    pub fn spawn(url: Url, auth: Option<Auth>, backoff: BackoffConfig) -> Self {
        // Initial value 0 is never seen by the consumer — `changed()` only resolves
        // after the sender writes a new value.
        let (tx, rx) = watch::channel(0);

        tokio::spawn(subscription_loop(url, auth, backoff, tx).in_current_span());

        Self { rx }
    }

    /// Wait until a new head arrives or the background task has terminated.
    pub async fn notified(&mut self) -> Notification {
        match self.rx.changed().await {
            Ok(()) => Notification::NewHead(*self.rx.borrow_and_update()),
            Err(_) => Notification::Stopped,
        }
    }
}

/// Result of waiting for a new-head notification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Notification {
    /// A new block header was received from the WebSocket subscription.
    NewHead(BlockNum),
    /// The background subscription task has stopped (e.g. channel closed).
    /// The consumer should fall back to polling only.
    Stopped,
}

/// Long-running subscription loop with automatic reconnection and exponential backoff.
async fn subscription_loop(
    url: Url,
    auth: Option<Auth>,
    backoff_config: BackoffConfig,
    tx: watch::Sender<BlockNum>,
) {
    let mut backoff = backoff_config.min;

    loop {
        if tx.is_closed() {
            tracing::debug!("new-head notifier consumer dropped, shutting down");
            return;
        }

        match connect_and_subscribe(&url, auth.as_ref(), &tx).await {
            Ok(()) => {
                // Stream ended cleanly (server closed connection). Reset backoff.
                tracing::debug!("new-head subscription stream ended, reconnecting");
                backoff = backoff_config.min;
            }
            Err(e) => {
                tracing::debug!(
                    error = %e,
                    backoff_secs = backoff.as_secs(),
                    "new-head subscription failed, reconnecting after backoff",
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(backoff_config.max);
            }
        }
    }
}

/// Connect, subscribe, and forward notifications until the stream ends.
async fn connect_and_subscribe(
    url: &Url,
    auth: Option<&Auth>,
    tx: &watch::Sender<BlockNum>,
) -> Result<(), SubscriptionError> {
    let ws_connect = ws_connect_with_auth(url, auth);

    let client = ClientBuilder::default()
        .pubsub(ws_connect)
        .await
        .map_err(SubscriptionError::Connect)?;

    let provider = ProviderBuilder::new()
        .disable_recommended_fillers()
        .network::<AnyNetwork>()
        .connect_client(client);

    let sub = provider
        .subscribe_blocks()
        .await
        .map_err(SubscriptionError::Subscribe)?;

    tracing::debug!("subscribed to new heads via WebSocket");

    let mut stream = sub.into_stream();
    loop {
        let header = tokio::select! {
            maybe_header = stream.next() => match maybe_header {
                Some(header) => header,
                None => return Ok(()),
            },
            // Receiver dropped — shut down immediately without waiting for the next header.
            () = tx.closed() => return Ok(()),
        };
        let block_num = header.inner.number;
        // watch::send always succeeds unless receiver is dropped.
        if tx.send(block_num).is_err() {
            return Ok(());
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum SubscriptionError {
    #[error("WebSocket connection failed")]
    Connect(#[source] alloy::transports::TransportError),
    #[error("eth_subscribe failed")]
    Subscribe(#[source] alloy::transports::RpcError<alloy::transports::TransportErrorKind>),
}
