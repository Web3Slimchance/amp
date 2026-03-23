use amp_providers_common::network_id::NetworkId;
use monitoring::telemetry;

/// Metrics registry for Tempo RPC provider operations.
///
/// Tracks request counts, durations, batch sizes, and error rates for Tempo
/// RPC calls, broken down by provider name, network, and method.
#[derive(Debug, Clone)]
pub struct MetricsRegistry {
    /// Total number of Tempo RPC requests made.
    pub rpc_requests: telemetry::metrics::Counter,
    /// Duration of individual Tempo RPC requests in milliseconds.
    pub rpc_request_duration: telemetry::metrics::Histogram<f64>,
    /// Total number of Tempo RPC errors encountered.
    pub rpc_errors: telemetry::metrics::Counter,
    /// Number of requests per RPC batch.
    pub rpc_batch_size: telemetry::metrics::Histogram<u64>,
}

impl MetricsRegistry {
    /// Create a new metrics registry from an OpenTelemetry meter.
    pub fn new(meter: &telemetry::metrics::Meter) -> Self {
        Self {
            rpc_requests: telemetry::metrics::Counter::new(
                meter,
                "tempo_rpc_requests_total",
                "Total number of Tempo RPC requests",
            ),
            rpc_request_duration: telemetry::metrics::Histogram::new_f64(
                meter,
                "tempo_rpc_request_duration",
                "Duration of Tempo RPC requests",
                "milliseconds",
            ),
            rpc_errors: telemetry::metrics::Counter::new(
                meter,
                "tempo_rpc_errors_total",
                "Total number of Tempo RPC errors",
            ),
            rpc_batch_size: telemetry::metrics::Histogram::new_u64(
                meter,
                "tempo_rpc_batch_size_requests",
                "Number of requests per RPC batch",
                "requests",
            ),
        }
    }

    pub(crate) fn record_single_request(
        &self,
        duration_millis: f64,
        provider: &str,
        network: &NetworkId,
        method: &str,
    ) {
        let kv_pairs = [
            telemetry::metrics::KeyValue::new("provider", provider.to_string()),
            telemetry::metrics::KeyValue::new("network", network.to_string()),
            telemetry::metrics::KeyValue::new("method", method.to_string()),
        ];
        self.rpc_requests.inc_with_kvs(&kv_pairs);
        self.rpc_request_duration
            .record_with_kvs(duration_millis, &kv_pairs);
    }

    pub(crate) fn record_batch_request(
        &self,
        duration_millis: f64,
        batch_size: u64,
        provider: &str,
        network: &NetworkId,
    ) {
        let kv_pairs = [
            telemetry::metrics::KeyValue::new("provider", provider.to_string()),
            telemetry::metrics::KeyValue::new("network", network.to_string()),
        ];
        self.rpc_requests.inc_with_kvs(&kv_pairs);
        self.rpc_request_duration
            .record_with_kvs(duration_millis, &kv_pairs);
        self.rpc_batch_size.record_with_kvs(batch_size, &kv_pairs);
    }

    pub(crate) fn record_error(&self, provider: &str, network: &NetworkId) {
        let kv_pairs = [
            telemetry::metrics::KeyValue::new("provider", provider.to_string()),
            telemetry::metrics::KeyValue::new("network", network.to_string()),
        ];
        self.rpc_errors.inc_with_kvs(&kv_pairs);
    }
}
