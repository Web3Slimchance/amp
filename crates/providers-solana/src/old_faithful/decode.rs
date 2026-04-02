//! Decoding of CAR file nodes into [`DecodedSlot`] values.
//!
//! CAR files store block data as a DAG of nodes (blocks, entries, transactions,
//! rewards). This module reassembles those nodes, decompresses ZSTD-compressed
//! fields, and decodes protobuf or bincode payloads into the types used by the
//! rest of the pipeline.

use yellowstone_faithful_car_parser as car_parser;

use crate::error::OldFaithfulStreamError;

const TX_STATUS_META_FIELD: &str = "transaction status meta";
const BLOCK_REWARDS_FIELD: &str = "block rewards";

pub type DecodedTransactionStatusMeta = DecodedField<
    solana_storage_proto::confirmed_block::TransactionStatusMeta,
    solana_storage_proto::StoredTransactionStatusMetaVersioned,
>;

pub type DecodedBlockRewards = DecodedField<
    solana_storage_proto::confirmed_block::Rewards,
    solana_storage_proto::StoredExtendedRewards,
>;

pub enum DecodedField<P, B> {
    Proto(P),
    Bincode(B),
}

#[derive(Default)]
pub struct DecodedSlot {
    pub slot: solana_clock::Slot,
    pub parent_slot: solana_clock::Slot,
    pub blockhash: [u8; 32],
    pub prev_blockhash: [u8; 32],
    pub block_height: Option<u64>,
    pub blocktime: i64,
    pub transactions: Vec<solana_sdk::transaction::VersionedTransaction>,
    pub transaction_metas: Vec<Option<DecodedTransactionStatusMeta>>,
    pub block_rewards: Option<DecodedBlockRewards>,
}

impl DecodedSlot {
    /// Create a dummy `DecodedSlot` with the given slot number and default values for all
    /// other fields. This can be used for testing or as a placeholder when only the slot
    /// number is relevant.
    ///
    /// NOTE: The reason this is marked as `pub` is because it is used in integration tests
    /// in the `tests` crate.
    #[doc(hidden)]
    pub fn dummy(slot: solana_clock::Slot) -> Self {
        Self {
            slot,
            parent_slot: slot.saturating_sub(1),
            ..Default::default()
        }
    }
}

/// Read an entire block worth of nodes from the given node reader and decode them into
/// a [DecodedSlot].
///
/// Inspired by the Old Faithful CAR parser example:
/// <https://github.com/lamports-dev/yellowstone-faithful-car-parser/blob/master/src/bin/counter.rs>
pub(crate) async fn read_and_decode_slot<R: tokio::io::AsyncRead + Unpin>(
    node_reader: &mut car_parser::node::NodeReader<R>,
    prev_blockhash: [u8; 32],
) -> Result<Option<DecodedSlot>, OldFaithfulStreamError> {
    // Once we reach `Node::Block`, the node map will contain all of the nodes needed to reassemble
    // that block.
    let nodes = car_parser::node::Nodes::read_until_block(node_reader)
        .await
        .map_err(OldFaithfulStreamError::NodeParse)?;

    let block = match nodes.nodes.last() {
        // Expected block node.
        Some((_, car_parser::node::Node::Block(block))) => block,
        // Reached end of CAR file.
        None | Some((_, car_parser::node::Node::Epoch(_))) => return Ok(None),
        Some((cid, node)) => {
            return Err(OldFaithfulStreamError::UnexpectedNode {
                kind: node.kind(),
                cid: (*cid).into(),
            });
        }
    };

    let mut transactions = Vec::new();
    let mut transaction_metas = Vec::new();

    for entry_cid in &block.entries {
        let Some(car_parser::node::Node::Entry(entry)) = nodes.nodes.get(entry_cid) else {
            return Err(OldFaithfulStreamError::MissingNode {
                expected: "entry",
                cid: entry_cid.to_string(),
            });
        };
        for tx_cid in &entry.transactions {
            let Some(car_parser::node::Node::Transaction(tx)) = nodes.nodes.get(tx_cid) else {
                return Err(OldFaithfulStreamError::MissingNode {
                    expected: "transaction",
                    cid: tx_cid.to_string(),
                });
            };

            nodes
                .reassemble_dataframes(&tx.data)
                .map_err(OldFaithfulStreamError::DataframeReassembly)
                .and_then(|tx_df| {
                    bincode::deserialize(&tx_df).map_err(OldFaithfulStreamError::Bincode)
                })
                .map(|tx| {
                    transactions.push(tx);
                })?;
            nodes
                .reassemble_dataframes(&tx.metadata)
                .map_err(OldFaithfulStreamError::DataframeReassembly)
                .and_then(|meta_df| {
                    if meta_df.is_empty() {
                        Ok(None)
                    } else {
                        zstd_decompress(TX_STATUS_META_FIELD, &meta_df)
                            .and_then(|meta| decode_tx_status_meta(block.slot, &meta))
                            .map(Some)
                    }
                })
                .map(|meta| {
                    transaction_metas.push(meta);
                })?;
        }
    }

    let block_rewards = nodes
        .nodes
        .get(&block.rewards)
        .map(|rewards| {
            let car_parser::node::Node::Rewards(rewards) = rewards else {
                return Err(OldFaithfulStreamError::UnexpectedNode {
                    kind: rewards.kind(),
                    cid: block.rewards.to_string(),
                });
            };
            if rewards.slot != block.slot {
                return Err(OldFaithfulStreamError::RewardSlotMismatch {
                    expected: block.slot,
                    found: rewards.slot,
                });
            }

            nodes
                .reassemble_dataframes(&rewards.data)
                .map_err(OldFaithfulStreamError::DataframeReassembly)
                .and_then(|block_rewards_df| {
                    zstd_decompress(BLOCK_REWARDS_FIELD, &block_rewards_df)
                })
                .and_then(|rewards_df| {
                    decode_proto_or_bincode(block.slot, BLOCK_REWARDS_FIELD, rewards_df.as_slice())
                })
        })
        .transpose()?;

    let blockhash = {
        // Hash of the last entry has the same value as that block's `blockhash` in
        // CAR files.
        let last_entry_cid = block.entries.last().expect("at least one entry");
        let last_entry_node = nodes.nodes.get(last_entry_cid);
        let Some(car_parser::node::Node::Entry(last_entry)) = last_entry_node else {
            return Err(OldFaithfulStreamError::MissingNode {
                expected: "entry",
                cid: last_entry_cid.to_string(),
            });
        };
        last_entry.hash.clone().try_into().map_err(|vec: Vec<u8>| {
            OldFaithfulStreamError::TryIntoArray {
                expected_len: 32,
                actual_len: vec.len(),
            }
        })?
    };

    let blocktime =
        block
            .meta
            .blocktime
            .try_into()
            .map_err(|_| OldFaithfulStreamError::BlocktimeOverflow {
                slot: block.slot,
                blocktime: block.meta.blocktime,
            })?;

    let block = DecodedSlot {
        slot: block.slot,
        parent_slot: block.meta.parent_slot,
        blockhash,
        prev_blockhash,
        block_height: block.meta.block_height,
        blocktime,
        transactions,
        transaction_metas,
        block_rewards,
    };

    Ok(Some(block))
}

/// Attempt to decode a field read from a CAR file as either protobuf or bincode encoded.
/// Fail if both decoding attempts fail. All fields that need to be decoded this way are
/// ZSTD compressed, so the input data to this function is expected to already be
/// decompressed.
///
/// For some epochs transaction metadata / block rewards are stored as protobuf encoded,
/// while for others they are stored as bincode encoded. This function handles both cases.
fn decode_proto_or_bincode<P, B>(
    slot: solana_clock::Slot,
    field_name: &'static str,
    decompressed_input: &[u8],
) -> Result<DecodedField<P, B>, OldFaithfulStreamError>
where
    P: prost::Message + Default,
    B: serde::de::DeserializeOwned,
{
    match prost::Message::decode(decompressed_input).map(DecodedField::Proto) {
        Ok(data_proto) => Ok(data_proto),
        Err(prost_err) => {
            match bincode::deserialize(decompressed_input).map(DecodedField::Bincode) {
                Ok(data_bincode) => Ok(data_bincode),
                Err(bincode_err) => {
                    let err = OldFaithfulStreamError::DecodeField {
                        slot,
                        field_name,
                        prost_err: prost_err.to_string(),
                        bincode_err: bincode_err.to_string(),
                    };
                    Err(err)
                }
            }
        }
    }
}

/// Decode transaction metadata that may be encoded in either protobuf or bincode format,
/// depending on the epoch. Bincode deserialization handles multiple legacy formats internally
/// via [`solana_storage_proto::StoredTransactionStatusMetaVersioned`].
///
/// Transaction metadata passed in should already be ZSTD decompressed.
fn decode_tx_status_meta(
    slot: solana_clock::Slot,
    decompressed_tx_meta: &[u8],
) -> Result<DecodedTransactionStatusMeta, OldFaithfulStreamError> {
    // Try protobuf first.
    match prost::Message::decode(decompressed_tx_meta) {
        Ok(proto_meta) => Ok(DecodedField::Proto(proto_meta)),
        Err(prost_err) => {
            // Try all bincode versions (current, legacy v2, legacy v1).
            match solana_storage_proto::StoredTransactionStatusMetaVersioned::from_bincode(
                decompressed_tx_meta,
            ) {
                Ok(meta) => Ok(DecodedField::Bincode(meta)),
                Err(bincode_err) => {
                    let err = OldFaithfulStreamError::DecodeField {
                        slot,
                        field_name: TX_STATUS_META_FIELD,
                        prost_err: prost_err.to_string(),
                        bincode_err: bincode_err.to_string(),
                    };
                    // Logging the full decompressed transaction metadata can be helpful for
                    // debugging decoding issues, even though it can be large and clutter the
                    // logs.
                    tracing::error!(
                        data = ?decompressed_tx_meta,
                        error = ?err,
                        error_source = monitoring::logging::error_source(&err),
                        "failed to decode transaction status meta"
                    );

                    Err(err)
                }
            }
        }
    }
}

fn zstd_decompress(
    field_name: &'static str,
    input: &[u8],
) -> Result<Vec<u8>, OldFaithfulStreamError> {
    zstd::decode_all(input).map_err(|err| OldFaithfulStreamError::Zstd {
        field_name,
        error: err.to_string(),
    })
}
