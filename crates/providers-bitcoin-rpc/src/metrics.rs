use monitoring::telemetry;

/// Metrics registry for Bitcoin RPC observability.
#[derive(Debug, Clone)]
pub struct MetricsRegistry {
    /// Total number of Bitcoin RPC requests made.
    pub rpc_requests: telemetry::metrics::Counter,
    /// Duration of Bitcoin RPC requests.
    pub rpc_request_duration: telemetry::metrics::Histogram<u64>,
    /// Total number of Bitcoin RPC errors encountered.
    pub rpc_errors: telemetry::metrics::Counter,
}

impl MetricsRegistry {
    pub fn new(meter: &telemetry::metrics::Meter) -> Self {
        Self {
            rpc_requests: telemetry::metrics::Counter::new(
                meter,
                "bitcoin_rpc_requests_total",
                "Total number of Bitcoin RPC requests",
            ),
            rpc_request_duration: telemetry::metrics::Histogram::new_u64(
                meter,
                "bitcoin_rpc_request_duration_milliseconds",
                "Duration of Bitcoin RPC requests",
                "milliseconds",
            ),
            rpc_errors: telemetry::metrics::Counter::new(
                meter,
                "bitcoin_rpc_errors_total",
                "Total number of Bitcoin RPC errors",
            ),
        }
    }

    pub(crate) fn record_rpc_request(
        &self,
        duration_millis: u64,
        provider: &str,
        network: &str,
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

    pub(crate) fn record_rpc_error(&self, provider: &str, network: &str) {
        let kv_pairs = [
            telemetry::metrics::KeyValue::new("provider", provider.to_string()),
            telemetry::metrics::KeyValue::new("network", network.to_string()),
        ];
        self.rpc_errors.inc_with_kvs(&kv_pairs);
    }
}
