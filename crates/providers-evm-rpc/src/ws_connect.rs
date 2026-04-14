//! WebSocket connection with custom header support.
//!
//! Alloy's `WsConnect` only supports setting the `Authorization` header on the WebSocket
//! handshake. This module provides a `PubSubConnect` implementation that supports arbitrary
//! custom headers (e.g. `drpc-key: <TOKEN>`).

use std::time::Duration;

use alloy::{
    pubsub::{ConnectionHandle, ConnectionInterface, PubSubConnect},
    rpc::json_rpc::PubSubItem,
    transports::{TransportErrorKind, TransportResult},
};
use futures::{SinkExt as _, StreamExt as _};
use tokio::time::sleep;
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest};

const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);

/// A WebSocket connector that supports custom HTTP headers on the handshake.
#[derive(Clone, Debug)]
pub struct WsConnectWithHeaders {
    url: String,
    headers: Vec<(String, String)>,
}

impl WsConnectWithHeaders {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            headers: Vec::new(),
        }
    }

    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }
}

impl PubSubConnect for WsConnectWithHeaders {
    fn is_local(&self) -> bool {
        alloy::transports::utils::guess_local_url(&self.url)
    }

    async fn connect(&self) -> TransportResult<ConnectionHandle> {
        let mut request = self
            .url
            .as_str()
            .into_client_request()
            .map_err(TransportErrorKind::custom)?;

        for (name, value) in &self.headers {
            let header_name =
                http::HeaderName::try_from(name.as_str()).map_err(TransportErrorKind::custom)?;
            let mut header_value =
                http::HeaderValue::try_from(value.as_str()).map_err(TransportErrorKind::custom)?;
            header_value.set_sensitive(true);
            request.headers_mut().insert(header_name, header_value);
        }

        let (socket, _) = tokio_tungstenite::connect_async(request)
            .await
            .map_err(TransportErrorKind::custom)?;

        let (handle, interface) = ConnectionHandle::new();

        tokio::spawn(ws_backend_loop(socket, interface));

        // Don't let alloy's PubSub layer retry — we handle reconnection ourselves
        // in the subscription_loop.
        Ok(handle.with_max_retries(0))
    }
}

/// Backend loop that bridges a tungstenite WebSocket with alloy's PubSub interface.
///
/// Adapted from alloy-transport-ws's `WsBackend::spawn`.
async fn ws_backend_loop(
    socket: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    mut interface: ConnectionInterface,
) {
    let (mut sink, mut stream) = socket.split();
    let mut errored = false;
    let mut expecting_pong = false;
    let keepalive = sleep(KEEPALIVE_INTERVAL);
    tokio::pin!(keepalive);

    loop {
        tokio::select! {
            biased;
            // Forward outgoing messages from the alloy frontend to the WS server.
            inst = interface.recv_from_frontend() => {
                match inst {
                    Some(msg) => {
                        keepalive.set(sleep(KEEPALIVE_INTERVAL));
                        if let Err(err) = sink.send(Message::Text(msg.get().to_owned().into())).await {
                            tracing::error!(%err, "WS send error");
                            errored = true;
                            break;
                        }
                    }
                    None => break,
                }
            }
            // Keepalive ping.
            _ = &mut keepalive => {
                if expecting_pong {
                    tracing::error!("WS server missed a pong");
                    errored = true;
                    break;
                }
                keepalive.set(sleep(KEEPALIVE_INTERVAL));
                if let Err(err) = sink.send(Message::Ping(Default::default())).await {
                    tracing::error!(%err, "WS ping error");
                    errored = true;
                    break;
                }
                expecting_pong = true;
            }
            // Incoming messages from the WS server.
            resp = stream.next() => {
                match resp {
                    Some(Ok(Message::Text(text))) => {
                        match serde_json::from_str::<PubSubItem>(&text) {
                            Ok(item) => {
                                if interface.send_to_frontend(item).is_err() {
                                    errored = true;
                                    break;
                                }
                            }
                            Err(err) => {
                                tracing::error!(%err, "WS deserialize error");
                                errored = true;
                                break;
                            }
                        }
                    }
                    Some(Ok(Message::Pong(_))) => {
                        expecting_pong = false;
                    }
                    Some(Ok(Message::Ping(_))) => {}
                    Some(Ok(Message::Close(frame))) => {
                        tracing::error!(?frame, "WS server closed connection");
                        errored = true;
                        break;
                    }
                    Some(Ok(Message::Binary(_) | Message::Frame(_))) => {
                        tracing::error!("unexpected WS message type");
                        errored = true;
                        break;
                    }
                    Some(Err(err)) => {
                        tracing::error!(%err, "WS connection error");
                        errored = true;
                        break;
                    }
                    None => {
                        tracing::error!("WS server has gone away");
                        errored = true;
                        break;
                    }
                }
            }
        }
    }

    if errored {
        interface.close_with_error();
    }
}
