//! Metrics instrumentation for Old Faithful CAR file streaming.
//!
//! Provides [`MonitoredAsyncRead`], an [`AsyncRead`](tokio::io::AsyncRead)
//! wrapper that records byte throughput, download duration, and error counts
//! for each epoch's CAR file read.

use std::{pin::Pin, sync::Arc};

use amp_providers_common::{network_id::NetworkId, provider_name::ProviderName};

/// Context for OF1 streaming that can be passed to functions that need to report metrics.
#[derive(Debug, Clone)]
pub struct Context {
    pub provider: ProviderName,
    pub network: NetworkId,
    pub registry: Arc<crate::metrics::MetricsRegistry>,
}

pub(crate) struct MonitoredAsyncRead<R> {
    inner: R,
    monitor: Option<AsyncReadMonitor>,
}

impl<R> MonitoredAsyncRead<R> {
    pub(crate) fn new(inner: R, epoch: solana_clock::Epoch, metrics: Option<Context>) -> Self {
        let monitor = metrics.map(|metrics| AsyncReadMonitor {
            epoch,
            bytes_read_chunk: 0,
            started_at: std::time::Instant::now(),
            provider: metrics.provider,
            network: metrics.network,
            registry: metrics.registry,
        });
        Self { inner, monitor }
    }
}

impl<R: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for MonitoredAsyncRead<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.get_mut();
        let before = buf.filled().len();
        let result = Pin::new(&mut this.inner).poll_read(cx, buf);

        if let Some(m) = this.monitor.as_mut() {
            match &result {
                std::task::Poll::Ready(Ok(())) => {
                    let bytes_read = (buf.filled().len() - before) as u64;
                    if bytes_read > 0 {
                        m.record_bytes_read(bytes_read);
                    } else {
                        m.record_car_processed();
                    }
                }
                std::task::Poll::Ready(Err(_)) => {
                    m.record_car_download_error();
                }
                std::task::Poll::Pending => {}
            }
        }

        result
    }
}

struct AsyncReadMonitor {
    epoch: solana_clock::Epoch,
    bytes_read_chunk: u64,
    started_at: std::time::Instant,
    provider: ProviderName,
    network: NetworkId,
    registry: Arc<crate::metrics::MetricsRegistry>,
}

impl AsyncReadMonitor {
    const BYTES_READ_RECORD_THRESHOLD: u64 = 10 * 1024 * 1024; // 10 MiB

    /// Record the number of bytes read and report to metrics if the reporting threshold is reached.
    fn record_bytes_read(&mut self, n: u64) {
        self.bytes_read_chunk += n;

        if self.bytes_read_chunk >= Self::BYTES_READ_RECORD_THRESHOLD {
            self.registry.record_of1_car_download_bytes(
                self.bytes_read_chunk,
                self.epoch,
                &self.provider,
                &self.network,
            );
            self.bytes_read_chunk = 0;
        }
    }

    /// Record any remaining bytes read that didn't reach the reporting threshold.
    fn flush_bytes_read(&mut self) {
        if self.bytes_read_chunk > 0 {
            self.registry.record_of1_car_download_bytes(
                self.bytes_read_chunk,
                self.epoch,
                &self.provider,
                &self.network,
            );
            self.bytes_read_chunk = 0;
        }
    }

    fn record_car_processed(&mut self) {
        let elapsed = self.started_at.elapsed().as_secs_f64();
        self.registry
            .record_of1_car_processed(elapsed, self.epoch, &self.provider, &self.network);
        self.flush_bytes_read();
    }

    fn record_car_download_error(&mut self) {
        self.registry
            .record_of1_car_download_error(self.epoch, &self.provider, &self.network);
    }
}

impl Drop for AsyncReadMonitor {
    fn drop(&mut self) {
        self.flush_bytes_read();
    }
}
