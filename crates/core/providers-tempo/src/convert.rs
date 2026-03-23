//! Pure transformation functions for converting Tempo RPC responses to dataset rows.

use std::time::Duration;

use alloy::{
    consensus::{BlockHeader as _, Transaction as _},
    eips::Typed2718,
    hex::ToHexExt,
    network::{BlockResponse, Network, ReceiptResponse as _, TransactionResponse as _},
    rpc::types::Log as RpcLog,
};
use amp_providers_common::network_id::NetworkId;
use datasets_common::{block_range::BlockRange, network_id::NetworkId as DatasetNetworkId};
use datasets_raw::{
    Timestamp,
    evm::{
        EvmCurrency,
        tables::logs::{Log, LogRowsBuilder},
    },
    rows::Rows,
    tempo::tables::{
        blocks::{Block, BlockRowsBuilder},
        transactions::{
            AAAuthorizationRow, Call, FeePayerSignature, KeyAuthorizationRow, KeyType,
            SignatureType as DsSignatureType, TempoSignatureRow, TokenLimitRow, Transaction,
            TransactionRowsBuilder,
        },
    },
};
use tempo_alloy::{
    TempoNetwork,
    primitives::{
        TempoTxEnvelope,
        transaction::{PrimitiveSignature, SignatureType, TempoSignature},
    },
    rpc::TempoTransactionReceipt,
};

use crate::error::{OverflowSource, RpcToRowsError, ToRowError};

pub(crate) type TempoRpcBlock = <TempoNetwork as Network>::BlockResponse;
pub(crate) type TempoRpcTransaction = <TempoNetwork as Network>::TransactionResponse;

pub(crate) fn rpc_to_rows(
    block: TempoRpcBlock,
    receipts: Vec<TempoTransactionReceipt>,
    network: &NetworkId,
) -> Result<Rows, RpcToRowsError> {
    if block.transactions.len() != receipts.len() {
        return Err(RpcToRowsError::TxReceiptCountMismatch {
            block_num: block.number(),
            tx_count: block.transactions.len(),
            receipt_count: receipts.len(),
        });
    }
    let tx_receipt_pairs = block.transactions.clone().into_transactions().zip(receipts);

    let header = rpc_header_to_row(block.header()).map_err(RpcToRowsError::ToRow)?;
    let mut logs = Vec::new();
    let mut transactions = Vec::new();

    for (idx, (tx, mut receipt)) in tx_receipt_pairs.enumerate() {
        if tx.tx_hash() != receipt.transaction_hash() {
            return Err(RpcToRowsError::TxReceiptHashMismatch {
                block_num: header.block_num,
                tx_hash: tx.tx_hash().encode_hex(),
                receipt_hash: receipt.transaction_hash().encode_hex(),
            });
        }
        // Move the logs out of the nested structure.
        let receipt_logs = std::mem::take(&mut receipt.inner.inner.receipt.logs);
        for log in receipt_logs {
            logs.push(rpc_log_to_row(log, header.timestamp).map_err(RpcToRowsError::ToRow)?);
        }
        transactions.push(
            rpc_transaction_to_row(&header, tx, receipt, idx).map_err(RpcToRowsError::ToRow)?,
        );
    }

    let dataset_network_id = DatasetNetworkId::new_unchecked(network.to_string());

    let block_range = BlockRange {
        numbers: header.block_num..=header.block_num,
        network: dataset_network_id,
        hash: header.hash.into(),
        prev_hash: header.parent_hash.into(),
        timestamp: Some(header.timestamp.0.as_secs()),
    };

    let header_row = {
        let mut builder = BlockRowsBuilder::with_capacity_for(&header);
        builder.append(&header);
        builder
            .build(block_range.clone())
            .map_err(RpcToRowsError::TableRow)?
    };

    let logs_row = {
        let total_data_size = logs.iter().map(|log| log.data.len()).sum();
        let mut builder = LogRowsBuilder::with_capacity(logs.len(), total_data_size);
        for log in &logs {
            builder.append(log);
        }
        builder
            .build(block_range.clone())
            .map_err(RpcToRowsError::TableRow)?
    };

    let transactions_row = {
        let total_input_size: usize = transactions
            .iter()
            .map(|t| t.input.as_ref().map_or(0, |i| i.len()))
            .sum();
        let mut builder =
            TransactionRowsBuilder::with_capacity(transactions.len(), total_input_size);
        for tx in &transactions {
            builder.append(tx);
        }
        builder
            .build(block_range.clone())
            .map_err(RpcToRowsError::TableRow)?
    };

    Ok(Rows::new(vec![header_row, logs_row, transactions_row]))
}

fn rpc_header_to_row(header: &tempo_alloy::rpc::TempoHeaderResponse) -> Result<Block, ToRowError> {
    use alloy::network::primitives::HeaderResponse as _;

    Ok(Block {
        block_num: header.number(),
        timestamp: Timestamp(Duration::from_secs(header.timestamp())),
        hash: header.hash().into(),
        parent_hash: header.parent_hash().into(),
        ommers_hash: header.ommers_hash().into(),
        miner: header.beneficiary().into(),
        state_root: header.state_root().into(),
        transactions_root: header.transactions_root().into(),
        receipt_root: header.receipts_root().into(),
        logs_bloom: <[u8; 256]>::from(header.logs_bloom()).into(),
        difficulty: EvmCurrency::try_from(header.difficulty())
            .map_err(|err| ToRowError::Overflow("difficulty", OverflowSource::BigInt(err)))?,
        total_difficulty: header
            .total_difficulty
            .map(EvmCurrency::try_from)
            .transpose()
            .map_err(|err| ToRowError::Overflow("total_difficulty", OverflowSource::BigInt(err)))?,
        gas_limit: header.gas_limit(),
        gas_used: header.gas_used(),
        extra_data: header.extra_data().to_vec(),
        mix_hash: *header.mix_hash().unwrap_or_default(),
        nonce: u64::from(header.nonce().unwrap_or_default()),
        base_fee_per_gas: header.base_fee_per_gas().map(EvmCurrency::from),
        withdrawals_root: header.withdrawals_root().map(Into::into),
        blob_gas_used: header.blob_gas_used(),
        excess_blob_gas: header.excess_blob_gas(),
        parent_beacon_root: header.parent_beacon_block_root().map(Into::into),
        requests_hash: header.requests_hash().map(Into::into),
        // Tempo-specific Fields
        general_gas_limit: header.general_gas_limit,
        shared_gas_limit: header.shared_gas_limit,
        timestamp_millis_part: header.timestamp_millis_part,
    })
}

fn rpc_log_to_row(log: RpcLog, timestamp: Timestamp) -> Result<Log, ToRowError> {
    Ok(Log {
        block_hash: log
            .block_hash
            .ok_or(ToRowError::Missing("block_hash"))?
            .into(),
        block_num: log
            .block_number
            .ok_or(ToRowError::Missing("block_number"))?,
        timestamp,
        tx_index: u32::try_from(
            log.transaction_index
                .ok_or(ToRowError::Missing("transaction_index"))?,
        )
        .map_err(|err| ToRowError::Overflow("transaction_index", OverflowSource::Int(err)))?,
        tx_hash: log
            .transaction_hash
            .ok_or(ToRowError::Missing("transaction_hash"))?
            .into(),
        log_index: u32::try_from(log.log_index.ok_or(ToRowError::Missing("log_index"))?)
            .map_err(|err| ToRowError::Overflow("log_index", OverflowSource::Int(err)))?,
        address: log.address().into(),
        topic0: log.topics().first().cloned().map(Into::into),
        topic1: log.topics().get(1).cloned().map(Into::into),
        topic2: log.topics().get(2).cloned().map(Into::into),
        topic3: log.topics().get(3).cloned().map(Into::into),
        data: log.data().data.to_vec(),
    })
}

fn rpc_transaction_to_row(
    block: &Block,
    tx: TempoRpcTransaction,
    receipt: TempoTransactionReceipt,
    tx_index: usize,
) -> Result<Transaction, ToRowError> {
    let envelope: &TempoTxEnvelope = tx.inner.inner();

    // For standard EVM txs: r/s/v_parity are Some, signature is None.
    // For Tempo AA txs: r/s/v_parity are None, signature is Some (multi-type).
    let (r, s, v_parity, tx_signature, chain_id) = match envelope {
        TempoTxEnvelope::Legacy(signed) => {
            let sig = signed.signature();
            (
                Some(sig.r().to_be_bytes::<32>()),
                Some(sig.s().to_be_bytes::<32>()),
                Some(sig.v()),
                None,
                signed.tx().chain_id(),
            )
        }
        TempoTxEnvelope::Eip2930(signed) => {
            let sig = signed.signature();
            (
                Some(sig.r().to_be_bytes::<32>()),
                Some(sig.s().to_be_bytes::<32>()),
                Some(sig.v()),
                None,
                signed.tx().chain_id(),
            )
        }
        TempoTxEnvelope::Eip1559(signed) => {
            let sig = signed.signature();
            (
                Some(sig.r().to_be_bytes::<32>()),
                Some(sig.s().to_be_bytes::<32>()),
                Some(sig.v()),
                None,
                signed.tx().chain_id(),
            )
        }
        TempoTxEnvelope::Eip7702(signed) => {
            let sig = signed.signature();
            (
                Some(sig.r().to_be_bytes::<32>()),
                Some(sig.s().to_be_bytes::<32>()),
                Some(sig.v()),
                None,
                signed.tx().chain_id(),
            )
        }
        TempoTxEnvelope::AA(aa_signed) => (
            None,
            None,
            None,
            Some(tempo_sig_to_row(
                aa_signed.signature(),
                &aa_signed.signature_hash(),
            )),
            Some(aa_signed.tx().chain_id),
        ),
    };

    let tx_type = tx.ty();
    let is_tempo_aa = matches!(envelope, TempoTxEnvelope::AA(_));

    let (
        fee_token,
        nonce_key,
        calls,
        fee_payer_signature,
        key_authorization,
        aa_authorization_list,
        valid_before,
        valid_after,
    ) = if let TempoTxEnvelope::AA(aa_signed) = envelope {
        let tt = aa_signed.tx();
        (
            tt.fee_token.map(|a| a.0.0),
            Some(tt.nonce_key.to_be_bytes::<32>()),
            Some(
                tt.calls
                    .iter()
                    .map(|c| Call {
                        to: c.to.to().map(|a| a.0.0),
                        value: c.value.to_string(),
                        input: c.input.to_vec(),
                    })
                    .collect::<Vec<_>>(),
            ),
            tt.fee_payer_signature.map(|sig| FeePayerSignature {
                r: sig.r().to_be_bytes::<32>(),
                s: sig.s().to_be_bytes::<32>(),
                y_parity: sig.v(),
            }),
            tt.key_authorization.as_ref().map(|ka| KeyAuthorizationRow {
                chain_id: ka.authorization.chain_id,
                key_type: match ka.authorization.key_type {
                    SignatureType::Secp256k1 => KeyType::Secp256k1,
                    SignatureType::P256 => KeyType::P256,
                    SignatureType::WebAuthn => KeyType::WebAuthn,
                },
                key_id: ka.authorization.key_id.0.0,
                expiry: ka.authorization.expiry,
                limits: ka.authorization.limits.as_ref().map(|limits| {
                    limits
                        .iter()
                        .map(|l| TokenLimitRow {
                            token: l.token.0.0,
                            limit: l.limit.to_string(),
                        })
                        .collect()
                }),
                signature: primitive_sig_to_row(&ka.signature),
            }),
            if tt.tempo_authorization_list.is_empty() {
                Some(vec![])
            } else {
                Some(
                    tt.tempo_authorization_list
                        .iter()
                        .map(|auth| {
                            let inner = auth.inner();
                            AAAuthorizationRow {
                                chain_id: inner.chain_id.to::<u64>(),
                                address: inner.address.0.0,
                                nonce: inner.nonce,
                                signature: tempo_sig_to_row(
                                    auth.signature(),
                                    &auth.signature_hash(),
                                ),
                            }
                        })
                        .collect(),
                )
            },
            tt.valid_before,
            tt.valid_after,
        )
    } else {
        (None, None, None, None, None, None, None, None)
    };

    Ok(Transaction {
        block_hash: block.hash,
        block_num: block.block_num,
        timestamp: block.timestamp,
        tx_index: u32::try_from(tx_index)
            .map_err(|err| ToRowError::Overflow("tx_index", OverflowSource::Int(err)))?,
        tx_hash: tx.tx_hash().0,
        to: if is_tempo_aa {
            None
        } else {
            tx.to().map(|addr| addr.0.0)
        },
        from: tx.as_recovered().signer().into(),
        nonce: tx.nonce(),
        chain_id,
        gas_limit: tx.gas_limit(),
        gas_used: receipt.gas_used(),
        receipt_cumulative_gas_used: receipt.cumulative_gas_used(),
        r#type: tx_type.into(),
        max_fee_per_gas: i128::try_from(envelope.max_fee_per_gas())
            .map_err(|err| ToRowError::Overflow("max_fee_per_gas", OverflowSource::Int(err)))?,
        max_priority_fee_per_gas: tx
            .max_priority_fee_per_gas()
            .map(i128::try_from)
            .transpose()
            .map_err(|err| {
                ToRowError::Overflow("max_priority_fee_per_gas", OverflowSource::Int(err))
            })?,
        gas_price: alloy::network::TransactionResponse::gas_price(&tx)
            .map(i128::try_from)
            .transpose()
            .map_err(|err| ToRowError::Overflow("gas_price", OverflowSource::Int(err)))?,
        status: receipt.status(),
        state_root: receipt.state_root().map(|root| root.0),
        value: if is_tempo_aa {
            None
        } else {
            Some(tx.value().to_string())
        },
        input: if is_tempo_aa {
            None
        } else {
            Some(tx.input().to_vec())
        },
        r,
        s,
        v_parity,
        signature: tx_signature,
        // Tempo-specific
        fee_token,
        nonce_key,
        calls,
        fee_payer_signature,
        key_authorization,
        aa_authorization_list,
        valid_before,
        valid_after,
        access_list: match envelope {
            TempoTxEnvelope::Legacy(_) => None,
            TempoTxEnvelope::Eip2930(signed) => Some(convert_access_list(&signed.tx().access_list)),
            TempoTxEnvelope::Eip1559(signed) => Some(convert_access_list(&signed.tx().access_list)),
            TempoTxEnvelope::Eip7702(signed) => Some(convert_access_list(&signed.tx().access_list)),
            TempoTxEnvelope::AA(aa_signed) => {
                Some(convert_access_list(&aa_signed.tx().access_list))
            }
        },
    })
}

fn convert_access_list(
    access_list: &alloy::eips::eip2930::AccessList,
) -> Vec<([u8; 20], Vec<[u8; 32]>)> {
    access_list
        .0
        .iter()
        .map(|item| {
            let address: [u8; 20] = item.address.0.0;
            let storage_keys: Vec<[u8; 32]> = item.storage_keys.iter().map(|key| key.0).collect();
            (address, storage_keys)
        })
        .collect()
}

/// Convert a `PrimitiveSignature` to a `TempoSignatureRow` with full type-specific fields.
fn primitive_sig_to_row(sig: &PrimitiveSignature) -> TempoSignatureRow {
    match sig {
        PrimitiveSignature::Secp256k1(s) => TempoSignatureRow {
            r#type: DsSignatureType::Secp256k1,
            r: s.r().to_be_bytes::<32>(),
            s: s.s().to_be_bytes::<32>(),
            y_parity: Some(s.v()),
            ..Default::default()
        },
        PrimitiveSignature::P256(p) => TempoSignatureRow {
            r#type: DsSignatureType::P256,
            r: p.r.0,
            s: p.s.0,
            pub_key_x: Some(p.pub_key_x.0),
            pub_key_y: Some(p.pub_key_y.0),
            pre_hash: Some(p.pre_hash),
            ..Default::default()
        },
        PrimitiveSignature::WebAuthn(w) => TempoSignatureRow {
            r#type: DsSignatureType::WebAuthn,
            r: w.r.0,
            s: w.s.0,
            pub_key_x: Some(w.pub_key_x.0),
            pub_key_y: Some(w.pub_key_y.0),
            webauthn_data: Some(w.webauthn_data.to_vec()),
            ..Default::default()
        },
    }
}

/// Convert a `TempoSignature` (Primitive or Keychain) to a `TempoSignatureRow`.
///
/// `sig_hash` is needed to recover the `key_id` for Keychain signatures with secp256k1 inner keys.
/// For P256/WebAuthn, `key_id` is derived from the public key without needing the hash.
fn tempo_sig_to_row(sig: &TempoSignature, sig_hash: &alloy::primitives::B256) -> TempoSignatureRow {
    match sig {
        TempoSignature::Primitive(p) => primitive_sig_to_row(p),
        TempoSignature::Keychain(k) => {
            let mut row = primitive_sig_to_row(&k.signature);
            // Derive key_id: for P256/WebAuthn from pub keys, for secp256k1 via sig recovery.
            row.key_id = match &k.signature {
                PrimitiveSignature::P256(p) => Some(
                    tempo_alloy::primitives::transaction::derive_p256_address(
                        &p.pub_key_x,
                        &p.pub_key_y,
                    )
                    .0
                    .0,
                ),
                PrimitiveSignature::WebAuthn(w) => Some(
                    tempo_alloy::primitives::transaction::derive_p256_address(
                        &w.pub_key_x,
                        &w.pub_key_y,
                    )
                    .0
                    .0,
                ),
                PrimitiveSignature::Secp256k1(_) => k.key_id(sig_hash).ok().map(|a| a.0.0),
            };
            row
        }
    }
}
