use datasets_raw::rows::TableRowError;

/// Errors during Bitcoin RPC request execution.
#[derive(thiserror::Error, Debug)]
pub enum RpcError {
    /// HTTP transport error.
    #[error("HTTP request failed")]
    Http(#[source] reqwest::Error),

    /// RPC returned an error response.
    #[error("RPC error {code}: {message}")]
    Rpc { code: i64, message: String },

    /// Failed to parse an expected hex string.
    #[error("hex decode failed for field '{field}': {source}")]
    HexDecode {
        field: &'static str,
        #[source]
        source: hex::FromHexError,
    },

    /// A required field was missing.
    #[error("missing required field: {0}")]
    MissingField(&'static str),
}

/// Errors that occur when converting RPC responses to table rows.
#[derive(Debug, thiserror::Error)]
pub enum RpcToRowsError {
    /// RPC call failed.
    #[error("RPC call failed for block {block_num}")]
    Rpc {
        block_num: u64,
        #[source]
        source: RpcError,
    },

    /// Failed to convert RPC data to row format.
    #[error("row conversion failed")]
    Convert(#[source] ConvertError),

    /// Failed to build the final table rows.
    #[error("table build failed")]
    TableRow(#[source] TableRowError),
}

/// Errors during individual field conversion.
#[derive(thiserror::Error, Debug)]
pub enum ConvertError {
    /// A required field was missing from the RPC response.
    #[error("missing field: {0}")]
    Missing(&'static str),

    /// Numeric overflow during type conversion.
    #[error("overflow in field '{0}'")]
    Overflow(&'static str, #[source] std::num::TryFromIntError),

    /// Decoded bytes have wrong length for a hash field.
    #[error("invalid hash length for field '{field}': expected 32 bytes, got {actual}")]
    InvalidHashLength { field: &'static str, actual: usize },

    /// Invalid hex string.
    #[error("hex decode failed for field '{field}': {source}")]
    HexDecode {
        field: &'static str,
        #[source]
        source: hex::FromHexError,
    },

    /// Invalid bits value.
    #[error("invalid bits field '{0}'")]
    InvalidBits(String),

    /// BTC value string could not be parsed.
    #[error("invalid BTC value '{0}'")]
    InvalidBtcValue(String),

    /// Integer satoshi overflow.
    #[error("satoshi overflow for value '{0}'")]
    SatoshiOverflow(String),
}

/// Error connecting to a Bitcoin RPC provider.
#[derive(thiserror::Error, Debug)]
pub enum ClientError {
    /// Failed to build the HTTP client.
    #[error("failed to build HTTP client")]
    HttpBuild(#[source] reqwest::Error),
}
