//! An example used to debug failed deserialization of bincode serialized transaction metadata
//! read from CAR files.
//!
//! The example contains a minimal custom deserialization setup that mirrors how the stored
//! transaction metadata types in [solana_storage_proto] are deserialized, with the addition
//! of logging the name of the field that caused the deserialization failure.
//!
//! The intended workflow for debugging a deserialization failure is:
//!   - a deserialization failure occurs when reading transaction metadata from a CAR file
//!     during a sync.
//!   - the bincode data that caused the failure is logged (this is already in place).
//!   - the logged data is copied into the main function of this example, allowing the failure
//!     to be reproduced until the root cause is identified and fixed.
//!
//! NOTE: This example mirrors the following types:
//!  - [`solana_storage_proto::StoredTransactionStatusMeta`]
//!  - [`solana_storage_proto::legacy_v2::StoredTransactionStatusMeta`]
//!  - [`solana_storage_proto::legacy_v1::StoredTransactionStatusMeta`]
//!
//!  If there are any changes to the structure of these types (e.g. fields added/removed/renamed),
//!  the corresponding struct and custom deserialization implementation in this example should be
//!  updated to match the new structure.

use std::fmt;

use serde::{
    Deserialize, Deserializer,
    de::{self, SeqAccess, Visitor},
};

#[allow(dead_code)]
struct StoredTransactionStatusMeta {
    status: Result<(), solana_transaction_error::TransactionError>,
    fee: u64,
    pre_balances: Vec<u64>,
    post_balances: Vec<u64>,
    // Fields below were added in newer versions of Solana; older data may not have them or their
    // fields may be truncated.
    inner_instructions: Option<Vec<solana_transaction_status_client_types::InnerInstructions>>,
    log_messages: Option<Vec<String>>,
    pre_token_balances: Option<Vec<solana_storage_proto::StoredTransactionTokenBalance>>,
    post_token_balances: Option<Vec<solana_storage_proto::StoredTransactionTokenBalance>>,
    rewards: Option<Vec<solana_storage_proto::StoredExtendedReward>>,
    // Fields below are not present in legacy transaction meta.
    return_data: Option<solana_transaction_context::TransactionReturnData>,
    compute_units_consumed: Option<u64>,
    cost_units: Option<u64>,
}

#[allow(dead_code)]
struct LegacyV2StoredTransactionStatusMeta {
    status: Result<(), solana_transaction_error::TransactionError>,
    fee: u64,
    pre_balances: Vec<u64>,
    post_balances: Vec<u64>,
    // Fields below were added in newer versions of Solana; older data may not have them or their
    // fields may be truncated.
    inner_instructions: Option<Vec<solana_storage_proto::legacy_v2::StoredInnerInstructions>>,
    log_messages: Option<Vec<String>>,
    pre_token_balances: Option<Vec<solana_storage_proto::legacy_v2::StoredTransactionTokenBalance>>,
    post_token_balances:
        Option<Vec<solana_storage_proto::legacy_v2::StoredTransactionTokenBalance>>,
    rewards: Option<Vec<solana_storage_proto::legacy_v2::StoredExtendedReward>>,
    return_data: Option<solana_transaction_context::TransactionReturnData>,
}

#[allow(dead_code)]
struct LegacyV1StoredTransactionStatusMeta {
    status: Result<(), solana_transaction_error::TransactionError>,
    fee: u64,
    pre_balances: Vec<u64>,
    post_balances: Vec<u64>,
    // Fields below were added in newer versions of Solana; older data may not have them or their
    // fields may be truncated.
    inner_instructions: Option<Vec<solana_storage_proto::legacy_v1::StoredInnerInstructions>>,
    log_messages: Option<Vec<String>>,
    pre_token_balances: Option<Vec<solana_storage_proto::legacy_v1::StoredTransactionTokenBalance>>,
    post_token_balances:
        Option<Vec<solana_storage_proto::legacy_v1::StoredTransactionTokenBalance>>,
    rewards: Option<Vec<solana_storage_proto::legacy_v1::StoredExtendedReward>>,
}

fn main() {
    /// An example of bincode serialized `StoredTransactionStatusMeta` with only the required 
    /// fields (i.e. all versioned fields are missing).
    /// 
    /// When debugging a deserialization failure, create a new byte slice with the bytes that
    /// caused the failure, then run this example.
    #[rustfmt::skip]
    const TXN_STATUS_META_EXAMPLE: &[u8] = &[
        // status: Ok(())
        0, 0, 0, 0, 
        // fee: 1
        1, 0, 0, 0, 0, 0, 0, 0, 
        // pre_balances: [2, 1]
        2, 0, 0, 0, 0, 0, 0, 0, // length = 2
        2, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 
        // post_balances: [1, 1]
        2, 0, 0, 0, 0, 0, 0, 0, // length = 2
        1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 
    ];

    tracing_subscriber::fmt::fmt()
        .with_env_filter(env_filter_or_info())
        .init();

    if let Ok(meta) = solana_storage_proto::StoredTransactionStatusMetaVersioned::from_bincode(
        TXN_STATUS_META_EXAMPLE,
    ) {
        let version = match meta {
            solana_storage_proto::StoredTransactionStatusMetaVersioned::Current(_) => "current",
            solana_storage_proto::StoredTransactionStatusMetaVersioned::LegacyV2(_) => "legacy v2",
            solana_storage_proto::StoredTransactionStatusMetaVersioned::LegacyV1(_) => "legacy v1",
        };
        tracing::info!(
            "successfully deserialized transaction meta with standard deserialization (version: {version})"
        );

        return;
    }
    match bincode::deserialize::<StoredTransactionStatusMeta>(TXN_STATUS_META_EXAMPLE) {
        Ok(_) => {
            tracing::info!("successfully deserialized current transaction meta");
        }
        Err(err) => {
            match bincode::deserialize::<LegacyV2StoredTransactionStatusMeta>(
                TXN_STATUS_META_EXAMPLE,
            ) {
                Ok(_) => {
                    tracing::info!("successfully deserialized legacy v2 transaction meta");
                }
                Err(err_v2) => {
                    match bincode::deserialize::<LegacyV1StoredTransactionStatusMeta>(
                        TXN_STATUS_META_EXAMPLE,
                    ) {
                        Ok(_) => {
                            tracing::info!("successfully deserialized legacy v1 transaction meta");
                        }
                        Err(err_v1) => {
                            let err = solana_storage_proto::BincodeDecodeAllError {
                                current_err: err,
                                legacy_v2_err: err_v2,
                                legacy_v1_err: err_v1,
                            };
                            tracing::error!(
                                error = ?err,
                                error_source = monitoring::logging::error_source(&err),
                                "failed to deserialize transaction meta with any known version"
                            );
                        }
                    }
                }
            }
        }
    }
}

/// Custom deserialization for [`StoredTransactionStatusMeta`].
const _: () = {
    const FIELDS: &[&str] = &[
        "status",
        "fee",
        "pre_balances",
        "post_balances",
        "inner_instructions",
        "log_messages",
        "pre_token_balances",
        "post_token_balances",
        "rewards",
        "return_data",
        "compute_units_consumed",
        "cost_units",
    ];

    impl<'de> Deserialize<'de> for StoredTransactionStatusMeta {
        fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            deserializer.deserialize_struct(
                "StoredTransactionStatusMeta",
                FIELDS,
                StoredTransactionStatusMetaVisitor,
            )
        }
    }

    struct StoredTransactionStatusMetaVisitor;

    impl<'de> Visitor<'de> for StoredTransactionStatusMetaVisitor {
        type Value = StoredTransactionStatusMeta;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("struct StoredTransactionStatusMeta")
        }

        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let _span = tracing::info_span!("deserializing tx meta").entered();

            // Required fields — present in all versions of this struct.
            let status = seq
                .next_element()?
                .ok_or_else(|| de::Error::invalid_length(0, &self))?;
            let fee = seq
                .next_element()?
                .ok_or_else(|| de::Error::invalid_length(1, &self))?;
            let pre_balances = seq
                .next_element()?
                .ok_or_else(|| de::Error::invalid_length(2, &self))?;
            let post_balances = seq
                .next_element()?
                .ok_or_else(|| de::Error::invalid_length(3, &self))?;

            // Versioned fields — older data may end before these; fall back to Default.
            let inner_instructions = next_or_default(&mut seq, "inner_instructions")?;
            let log_messages = next_or_default(&mut seq, "log_messages")?;
            let pre_token_balances = next_or_default(&mut seq, "pre_token_balances")?;
            let post_token_balances = next_or_default(&mut seq, "post_token_balances")?;
            let rewards = next_or_default(&mut seq, "rewards")?;
            let return_data = next_or_default(&mut seq, "return_data")?;
            let compute_units_consumed = next_or_default(&mut seq, "compute_units_consumed")?;
            let cost_units = next_or_default(&mut seq, "cost_units")?;

            Ok(StoredTransactionStatusMeta {
                status,
                fee,
                pre_balances,
                post_balances,
                inner_instructions,
                log_messages,
                pre_token_balances,
                post_token_balances,
                rewards,
                return_data,
                compute_units_consumed,
                cost_units,
            })
        }
    }
};

/// Custom deserialization for [`LegacyV2StoredTransactionStatusMeta`].
const _: () = {
    const FIELDS: &[&str] = &[
        "status",
        "fee",
        "pre_balances",
        "post_balances",
        "inner_instructions",
        "log_messages",
        "pre_token_balances",
        "post_token_balances",
        "rewards",
        "return_data",
    ];

    impl<'de> Deserialize<'de> for LegacyV2StoredTransactionStatusMeta {
        fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            deserializer.deserialize_struct(
                "LegacyStoredTransactionStatusMeta",
                FIELDS,
                LegacyV2StoredTransactionStatusMetaVisitor,
            )
        }
    }

    struct LegacyV2StoredTransactionStatusMetaVisitor;

    impl<'de> Visitor<'de> for LegacyV2StoredTransactionStatusMetaVisitor {
        type Value = LegacyV2StoredTransactionStatusMeta;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("struct LegacyStoredTransactionStatusMeta")
        }

        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let _span = tracing::info_span!("deserializing legacy v2 tx meta").entered();

            // Required fields — present in all versions of this struct.
            let status = seq
                .next_element()?
                .ok_or_else(|| de::Error::invalid_length(0, &self))?;
            let fee = seq
                .next_element()?
                .ok_or_else(|| de::Error::invalid_length(1, &self))?;
            let pre_balances = seq
                .next_element()?
                .ok_or_else(|| de::Error::invalid_length(2, &self))?;
            let post_balances = seq
                .next_element()?
                .ok_or_else(|| de::Error::invalid_length(3, &self))?;

            // Versioned fields — older data may end before these; fall back to Default.
            let inner_instructions = next_or_default(&mut seq, "inner_instructions")?;
            let log_messages = next_or_default(&mut seq, "log_messages")?;
            let pre_token_balances = next_or_default(&mut seq, "pre_token_balances")?;
            let post_token_balances = next_or_default(&mut seq, "post_token_balances")?;
            let rewards = next_or_default(&mut seq, "rewards")?;
            let return_data = next_or_default(&mut seq, "return_data")?;

            Ok(LegacyV2StoredTransactionStatusMeta {
                status,
                fee,
                pre_balances,
                post_balances,
                inner_instructions,
                log_messages,
                pre_token_balances,
                post_token_balances,
                rewards,
                return_data,
            })
        }
    }
};

/// Custom deserialization for [`LegacyV1StoredTransactionStatusMeta`].
const _: () = {
    const FIELDS: &[&str] = &[
        "status",
        "fee",
        "pre_balances",
        "post_balances",
        "inner_instructions",
        "log_messages",
        "pre_token_balances",
        "post_token_balances",
        "rewards",
    ];

    impl<'de> Deserialize<'de> for LegacyV1StoredTransactionStatusMeta {
        fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            deserializer.deserialize_struct(
                "LegacyStoredTransactionStatusMeta",
                FIELDS,
                LegacyStoredTransactionStatusMetaVisitor,
            )
        }
    }

    struct LegacyStoredTransactionStatusMetaVisitor;

    impl<'de> Visitor<'de> for LegacyStoredTransactionStatusMetaVisitor {
        type Value = LegacyV1StoredTransactionStatusMeta;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("struct LegacyStoredTransactionStatusMeta")
        }

        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let _span = tracing::info_span!("deserializing legacy tx meta").entered();

            // Required fields — present in all versions of this struct.
            let status = seq
                .next_element()?
                .ok_or_else(|| de::Error::invalid_length(0, &self))?;
            let fee = seq
                .next_element()?
                .ok_or_else(|| de::Error::invalid_length(1, &self))?;
            let pre_balances = seq
                .next_element()?
                .ok_or_else(|| de::Error::invalid_length(2, &self))?;
            let post_balances = seq
                .next_element()?
                .ok_or_else(|| de::Error::invalid_length(3, &self))?;

            // Versioned fields — older data may end before these; fall back to Default.
            let inner_instructions = next_or_default(&mut seq, "inner_instructions")?;
            let log_messages = next_or_default(&mut seq, "log_messages")?;
            let pre_token_balances = next_or_default(&mut seq, "pre_token_balances")?;
            let post_token_balances = next_or_default(&mut seq, "post_token_balances")?;
            let rewards = next_or_default(&mut seq, "rewards")?;

            Ok(LegacyV1StoredTransactionStatusMeta {
                status,
                fee,
                pre_balances,
                post_balances,
                inner_instructions,
                log_messages,
                pre_token_balances,
                post_token_balances,
                rewards,
            })
        }
    }
};

/// Read the next element, returning `T::default()` if bincode signals EOF.
fn next_or_default<'de, T, A>(seq: &mut A, field_name: &'static str) -> Result<T, A::Error>
where
    T: Deserialize<'de> + Default,
    A: SeqAccess<'de>,
{
    /// We need a wrapper type so we can intercept the EOF error inside `T::deserialize`
    /// (where bincode raises it) rather than letting it bubble up through `SeqAccess`.
    struct Wrap<T>(T);

    impl<'de, T: Deserialize<'de> + Default> Deserialize<'de> for Wrap<T> {
        fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            default_on_eof(d).map(Wrap)
        }
    }

    Ok(seq
        .next_element::<Wrap<T>>()
        .inspect_err(|e| {
            tracing::error!("error deserializing field '{field_name}': {e}");
        })?
        .map(|w| w.0)
        .unwrap_or_default())
}

/// Deserialize `T`, returning `T::default()` if bincode hits an unexpected EOF.
/// This makes structs forward-compatible: fields appended to the end of a struct
/// can be read from old data that predates those fields.
fn default_on_eof<'de, T, D>(d: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    fn is_eof_error(e: String) -> bool {
        e == "io error: unexpected end of file" || e == "io error: failed to fill whole buffer"
    }

    match T::deserialize(d) {
        Err(e) if is_eof_error(e.to_string()) => Ok(T::default()),
        other => other,
    }
}

fn env_filter_or_info() -> tracing_subscriber::EnvFilter {
    tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
}
