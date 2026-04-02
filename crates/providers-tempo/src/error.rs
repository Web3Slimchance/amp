use alloy::primitives::ruint::FromUintError;
use datasets_raw::rows::TableRowError;
use tokio::sync::AcquireError;

/// Errors that occur during batched RPC request execution.
///
/// Returned when a batch of JSON-RPC calls fails either due to a transport error
/// or because the rate-limiting semaphore was closed.
#[derive(Debug, thiserror::Error)]
pub enum BatchingError {
    /// The RPC batch request failed.
    ///
    /// Occurs when the underlying HTTP/WS/IPC transport returns an error for a
    /// batched JSON-RPC request.
    #[error("RPC batch request failed: {0}")]
    Request(#[source] BatchRequestError),

    /// Failed to acquire a permit from the rate limiter.
    ///
    /// Occurs when the concurrency-limiting semaphore has been closed, typically
    /// during shutdown.
    #[error("rate limiter semaphore closed: {0}")]
    RateLimitAcquire(#[source] AcquireError),
}

/// Error wrapper for RPC transport failures.
///
/// Wraps an [`alloy::transports::TransportError`] that occurred while sending a
/// batch request or awaiting individual responses within a batch.
#[derive(Debug, thiserror::Error)]
#[error("RPC client error")]
pub struct BatchRequestError(#[source] pub alloy::transports::TransportError);

/// Errors that occur when converting RPC responses to table rows.
///
/// Returned by the row-conversion pipeline when block, transaction, or receipt
/// data from the RPC cannot be mapped to the dataset schema.
#[derive(Debug, thiserror::Error)]
pub enum RpcToRowsError {
    /// Transaction and receipt counts don't match for a block.
    ///
    /// Occurs when the number of transactions in a block does not equal the
    /// number of receipts returned by the RPC, indicating an inconsistent
    /// response.
    #[error(
        "mismatched tx and receipt count for block {block_num}: {tx_count} txs, {receipt_count} receipts"
    )]
    TxReceiptCountMismatch {
        block_num: u64,
        tx_count: usize,
        receipt_count: usize,
    },

    /// Transaction and receipt hashes don't match.
    ///
    /// Occurs when a transaction hash does not match its corresponding receipt
    /// hash, indicating the receipts are out of order or belong to a different
    /// block.
    #[error(
        "mismatched tx and receipt hash for block {block_num}: tx {tx_hash}, receipt {receipt_hash}"
    )]
    TxReceiptHashMismatch {
        block_num: u64,
        tx_hash: String,
        receipt_hash: String,
    },

    /// Failed to convert RPC data to row format.
    ///
    /// Occurs when an individual field in a transaction, block header, or log
    /// cannot be converted to the expected dataset column type.
    #[error("row conversion failed")]
    ToRow(#[source] ToRowError),

    /// Failed to build the final table rows.
    ///
    /// Occurs when the row builder cannot produce a valid Arrow record batch
    /// from the converted row data.
    #[error("table build failed")]
    TableRow(#[source] TableRowError),
}

/// Errors during individual field conversion to row format.
///
/// Returned when a single field from an RPC response cannot be mapped to the
/// corresponding dataset column.
#[derive(Debug, thiserror::Error)]
pub enum ToRowError {
    /// A required field is missing from the RPC response.
    ///
    /// Occurs when a field that is mandatory in the dataset schema (e.g.,
    /// `block_hash`, `transaction_index`) is `None` in the RPC response.
    #[error("missing field: {0}")]
    Missing(&'static str),

    /// A numeric field overflowed during type conversion.
    ///
    /// Occurs when an RPC numeric value (e.g., `U256`, `u64`) does not fit
    /// into the target column type (e.g., `i128`, `u32`).
    #[error("overflow in field {0}: {1}")]
    Overflow(&'static str, #[source] OverflowSource),
}

/// Source of numeric overflow errors during field conversion.
///
/// Distinguishes between standard integer conversion failures and big integer
/// (U256) conversion failures for better diagnostics.
#[derive(Debug, thiserror::Error)]
pub enum OverflowSource {
    /// Overflow from standard integer type conversion.
    ///
    /// Occurs when a `TryFrom` conversion between standard integer types fails
    /// (e.g., `u64` to `u32`).
    #[error("{0}")]
    Int(#[source] std::num::TryFromIntError),

    /// Overflow from big integer (U256) conversion.
    ///
    /// Occurs when a `U256` value does not fit into the target `i128` column
    /// type.
    #[error("{0}")]
    BigInt(#[source] FromUintError<i128>),
}

/// Errors that occur when fetching block receipts during unbatched block streaming.
///
/// When streaming blocks with per-block receipt fetching, the Tempo RPC client
/// makes a separate request for each block's receipts. These errors cover
/// transport failures and missing receipt data.
#[derive(Debug, thiserror::Error)]
pub enum FetchReceiptsError {
    /// The RPC call to fetch receipts failed.
    ///
    /// This occurs when the transport layer encounters an error while fetching
    /// receipts for a specific block. Common causes include network timeouts,
    /// connection drops, or RPC node errors.
    #[error("error fetching receipts for block {block_num}")]
    Rpc {
        block_num: u64,
        #[source]
        err: ClientError,
    },

    /// No receipts were returned for a block.
    ///
    /// The RPC returned `null` for a block that was expected to have receipts.
    /// This may indicate an incomplete RPC response or a node that does not
    /// support the `eth_getBlockReceipts` method.
    #[error("no receipts returned for block {block_num}")]
    Empty { block_num: u64 },
}

/// Error connecting to a Tempo RPC provider.
///
/// Returned when the RPC transport (HTTP, WebSocket, or IPC) cannot be
/// established during client initialization.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// Transport-level error (WebSocket/IPC connection failure).
    ///
    /// Occurs when the underlying transport cannot be established.
    #[error("provider error: {0}")]
    Transport(#[source] alloy::transports::TransportError),

    /// HTTP client build failure.
    ///
    /// Occurs when the reqwest HTTP client cannot be constructed (e.g., TLS
    /// backend initialization failure).
    #[error("HTTP client build error: {0}")]
    HttpBuild(#[from] crate::provider::HttpBuildError),
}
