//! Bitcoin RPC block-streaming client.

use std::{
    num::{NonZeroU32, NonZeroU64},
    sync::Arc,
    time::Duration,
};

use amp_providers_common::{
    network_id::NetworkId as ProviderNetworkId, provider_name::ProviderName,
};
use datasets_common::{
    block_num::BlockNum, block_range::BlockRange, network_id::NetworkId as DatasetNetworkId,
};
use datasets_raw::{
    Timestamp,
    client::{BlockStreamError, BlockStreamResultExt, BlockStreamer, LatestBlockError},
    rows::Rows,
};
use futures::Stream;

use crate::{
    error::{ConvertError, RpcToRowsError},
    rpc::{BitcoinRpcClient, RpcBlock, RpcInput, RpcOutput},
    tables::{
        blocks::{Block, BlockRowsBuilder},
        inputs::{Input, InputRowsBuilder},
        outputs::{Output, OutputRowsBuilder},
        transactions::{Transaction, TransactionRowsBuilder},
    },
};

/// Bitcoin RPC block-streaming client.
///
/// Connects to a Bitcoin Core JSON-RPC endpoint and streams blocks with their
/// transactions, inputs, and outputs as dataset rows.
#[derive(Clone)]
pub struct Client {
    rpc: BitcoinRpcClient,
    network: DatasetNetworkId,
    provider_name: ProviderName,
    limiter: Arc<tokio::sync::Semaphore>,
    request_limit: usize,
    confirmations: u32,
}

impl Client {
    /// Create a new Bitcoin RPC client.
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        url: url::Url,
        network: ProviderNetworkId,
        provider_name: ProviderName,
        auth: Option<(String, String)>,
        rate_limit: Option<NonZeroU32>,
        concurrent_request_limit: Option<u16>,
        confirmations: u32,
        timeout: Duration,
        meter: Option<&monitoring::telemetry::metrics::Meter>,
    ) -> Result<Self, crate::error::ClientError> {
        let rpc = BitcoinRpcClient::new(
            url,
            auth,
            rate_limit,
            timeout,
            provider_name.to_string(),
            network.to_string(),
            meter,
        )
        .map_err(crate::error::ClientError::HttpBuild)?;
        let request_limit = concurrent_request_limit.unwrap_or(8).max(1) as usize;
        let limiter = Arc::new(tokio::sync::Semaphore::new(request_limit));
        let dataset_network = DatasetNetworkId::new_unchecked(network.to_string());
        Ok(Self {
            rpc,
            network: dataset_network,
            provider_name,
            limiter,
            request_limit,
            confirmations,
        })
    }
}

impl BlockStreamer for Client {
    async fn block_stream(
        self,
        start: BlockNum,
        end: BlockNum,
    ) -> impl Stream<Item = Result<Rows, BlockStreamError>> + Send {
        use futures::StreamExt as _;

        let request_limit = self.request_limit;
        let rpc = self.rpc;
        let network = self.network;
        let limiter = self.limiter;

        futures::stream::iter(start..=end)
            .map(move |block_num| {
                let rpc = rpc.clone();
                let limiter = limiter.clone();
                let network = network.clone();
                async move {
                    let Ok(_permit) = limiter.acquire().await else {
                        return Err("concurrency limiter closed").recoverable();
                    };
                    let hash = match rpc.get_block_hash(block_num).await {
                        Ok(h) => h,
                        Err(e) => {
                            return Err(RpcToRowsError::Rpc {
                                block_num,
                                source: e,
                            })
                            .recoverable();
                        }
                    };
                    let block = match rpc.get_block(&hash).await {
                        Ok(b) => b,
                        Err(e) => {
                            return Err(RpcToRowsError::Rpc {
                                block_num,
                                source: e,
                            })
                            .recoverable();
                        }
                    };
                    rpc_to_rows(block, &network).recoverable()
                }
            })
            .buffered(request_limit)
    }

    #[tracing::instrument(skip(self), err)]
    async fn latest_block(
        &mut self,
        finalized: bool,
    ) -> Result<Option<BlockNum>, LatestBlockError> {
        let Ok(_permit) = self.limiter.acquire().await else {
            return Err("concurrency limiter closed".into());
        };
        let count = self.rpc.get_block_count().await?;
        if finalized {
            Ok(Some(count.saturating_sub(self.confirmations as u64)))
        } else {
            Ok(Some(count))
        }
    }

    fn bucket_size(&self) -> Option<NonZeroU64> {
        None
    }

    fn provider_name(&self) -> &str {
        &self.provider_name
    }
}

/// Convert an RPC block to dataset rows.
pub fn rpc_to_rows(block: RpcBlock, network: &DatasetNetworkId) -> Result<Rows, RpcToRowsError> {
    let block_num = block.height;
    let ts = secs_to_timestamp(block.time as u64);
    let hash = decode_hash(&block.hash, "hash").map_err(RpcToRowsError::Convert)?;
    let parent_hash = block
        .previousblockhash
        .as_deref()
        .map(|s| decode_hash(s, "previousblockhash"))
        .transpose()
        .map_err(RpcToRowsError::Convert)?;
    let merkle_root =
        decode_hash(&block.merkleroot, "merkleroot").map_err(RpcToRowsError::Convert)?;
    let bits = decode_bits(&block.bits).map_err(RpcToRowsError::Convert)?;
    let median_time = secs_to_timestamp(block.mediantime as u64);

    let block_row = Block {
        block_num,
        timestamp: ts,
        hash,
        parent_hash,
        merkle_root,
        nonce: block.nonce,
        bits,
        difficulty: block.difficulty,
        version: block.version,
        size: block.size,
        stripped_size: block.strippedsize,
        weight: block.weight,
        tx_count: block.n_tx,
        median_time,
    };

    let tx_count = block.tx.len();

    let mut blocks_builder = BlockRowsBuilder::with_capacity_for(&block_row);
    blocks_builder.append(&block_row);

    let mut txs_builder = TransactionRowsBuilder::with_capacity(tx_count);
    let mut inputs_builder =
        InputRowsBuilder::with_capacity(block.tx.iter().map(|t| t.vin.len()).sum::<usize>().max(1));
    let mut outputs_builder = OutputRowsBuilder::with_capacity(
        block.tx.iter().map(|t| t.vout.len()).sum::<usize>().max(1),
    );

    for (tx_index, tx) in block.tx.iter().enumerate() {
        let tx_index = tx_index as u32;
        let is_coinbase = tx.vin.first().is_some_and(|i| i.coinbase.is_some());
        let txid = decode_hash(&tx.txid, "txid").map_err(RpcToRowsError::Convert)?;
        let tx_hash = decode_hash(&tx.hash, "hash").map_err(RpcToRowsError::Convert)?;

        let tx_row = Transaction {
            block_num,
            timestamp: ts,
            block_hash: hash,
            tx_index,
            txid,
            hash: tx_hash,
            version: tx.version,
            size: tx.size,
            vsize: tx.vsize,
            weight: tx.weight,
            locktime: tx.locktime,
            is_coinbase,
        };
        txs_builder.append(&tx_row);

        // Inputs
        for (input_index, vin) in tx.vin.iter().enumerate() {
            let input_row =
                convert_input(block_num, ts, hash, tx_index, txid, input_index as u32, vin)
                    .map_err(RpcToRowsError::Convert)?;
            inputs_builder.append(&input_row);
        }

        // Outputs
        for vout in tx.vout.iter() {
            let output_row = convert_output(block_num, ts, hash, tx_index, txid, vout)
                .map_err(RpcToRowsError::Convert)?;
            outputs_builder.append(&output_row);
        }
    }

    let prev_hash_bytes = parent_hash.unwrap_or([0u8; 32]);
    let range = BlockRange {
        numbers: block_num..=block_num,
        network: network.clone(),
        hash: hash.into(),
        prev_hash: prev_hash_bytes.into(),
        timestamp: Some(block.time as u64),
    };

    let blocks_rows = blocks_builder
        .build(range.clone())
        .map_err(RpcToRowsError::TableRow)?;
    let txs_rows = txs_builder
        .build(range.clone())
        .map_err(RpcToRowsError::TableRow)?;
    let inputs_rows = inputs_builder
        .build(range.clone())
        .map_err(RpcToRowsError::TableRow)?;
    let outputs_rows = outputs_builder
        .build(range)
        .map_err(RpcToRowsError::TableRow)?;

    Ok(Rows::new(vec![
        blocks_rows,
        txs_rows,
        inputs_rows,
        outputs_rows,
    ]))
}

fn convert_input(
    block_num: u64,
    timestamp: Timestamp,
    block_hash: [u8; 32],
    tx_index: u32,
    txid: [u8; 32],
    input_index: u32,
    vin: &RpcInput,
) -> Result<Input, ConvertError> {
    let spent_txid = vin
        .txid
        .as_deref()
        .map(|s| decode_hash(s, "vin.txid"))
        .transpose()?;

    let coinbase = vin
        .coinbase
        .as_deref()
        .map(|s| {
            hex::decode(s).map_err(|e| ConvertError::HexDecode {
                field: "coinbase",
                source: e,
            })
        })
        .transpose()?;

    let script_sig = vin
        .script_sig
        .as_ref()
        .map(|s| {
            hex::decode(&s.hex).map_err(|e| ConvertError::HexDecode {
                field: "scriptSig.hex",
                source: e,
            })
        })
        .transpose()?
        .unwrap_or_default();

    let witness = vin
        .txinwitness
        .as_ref()
        .map(|items| {
            items
                .iter()
                .map(|s| {
                    hex::decode(s).map_err(|e| ConvertError::HexDecode {
                        field: "txinwitness",
                        source: e,
                    })
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?;

    Ok(Input {
        block_num,
        timestamp,
        block_hash,
        tx_index,
        txid,
        input_index,
        spent_txid,
        spent_vout: vin.vout,
        coinbase,
        script_sig,
        sequence: vin.sequence,
        witness,
    })
}

fn convert_output(
    block_num: u64,
    timestamp: Timestamp,
    block_hash: [u8; 32],
    tx_index: u32,
    txid: [u8; 32],
    vout: &RpcOutput,
) -> Result<Output, ConvertError> {
    let script_pubkey =
        hex::decode(&vout.script_pubkey.hex).map_err(|e| ConvertError::HexDecode {
            field: "scriptPubKey.hex",
            source: e,
        })?;
    let value_sat = btc_to_sat(&vout.value)?;

    Ok(Output {
        block_num,
        timestamp,
        block_hash,
        tx_index,
        txid,
        output_index: vout.n,
        value_sat,
        script_pubkey,
        script_pubkey_type: vout.script_pubkey.type_.clone(),
        script_pubkey_address: vout.script_pubkey.address.clone(),
    })
}

/// Decode a hex-encoded hash string into a fixed 32-byte array.
fn decode_hash(hex_str: &str, field: &'static str) -> Result<[u8; 32], ConvertError> {
    let bytes = hex::decode(hex_str).map_err(|e| ConvertError::HexDecode { field, source: e })?;
    let actual = bytes.len();
    bytes
        .try_into()
        .map_err(|_| ConvertError::InvalidHashLength { field, actual })
}

/// Decode Bitcoin's compact `bits` field (hex string) to a u32.
fn decode_bits(bits_str: &str) -> Result<u32, ConvertError> {
    u32::from_str_radix(bits_str, 16).map_err(|_| ConvertError::InvalidBits(bits_str.to_string()))
}

/// Convert a BTC value string to integer satoshis using pure integer arithmetic.
fn btc_to_sat(btc: &str) -> Result<i64, ConvertError> {
    let (int_str, frac_str) = match btc.split_once('.') {
        Some((i, f)) => (i, f),
        None => (btc, ""),
    };

    let int_sats = int_str
        .parse::<i64>()
        .map_err(|_| ConvertError::InvalidBtcValue(btc.to_string()))?
        .checked_mul(100_000_000)
        .ok_or_else(|| ConvertError::SatoshiOverflow(btc.to_string()))?;

    // Truncate to 8 decimal places then right-pad with zeros to exactly 8 digits.
    let frac_8 = format!("{:0<8}", &frac_str[..frac_str.len().min(8)]);
    let frac_sats = frac_8
        .parse::<i64>()
        .map_err(|_| ConvertError::InvalidBtcValue(btc.to_string()))?;

    int_sats
        .checked_add(frac_sats)
        .ok_or_else(|| ConvertError::SatoshiOverflow(btc.to_string()))
}

/// Convert Unix seconds to a `Timestamp` (nanosecond duration).
fn secs_to_timestamp(secs: u64) -> Timestamp {
    Timestamp(Duration::from_secs(secs))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::{RpcBlock, RpcInput, RpcOutput, RpcScriptPubKey, RpcTransaction};

    fn minimal_block() -> RpcBlock {
        RpcBlock {
            hash: "0000000000000000000000000000000000000000000000000000000000000001".to_string(),
            height: 1,
            version: 1,
            merkleroot: "0000000000000000000000000000000000000000000000000000000000000002"
                .to_string(),
            time: 1_000_000,
            mediantime: 999_999,
            nonce: 42,
            bits: "1d00ffff".to_string(),
            difficulty: 1.0,
            n_tx: 1,
            previousblockhash: Some(
                "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
            ),
            strippedsize: 250,
            size: 250,
            weight: 1000,
            tx: vec![RpcTransaction {
                txid: "0000000000000000000000000000000000000000000000000000000000000010"
                    .to_string(),
                hash: "0000000000000000000000000000000000000000000000000000000000000010"
                    .to_string(),
                version: 1,
                size: 100,
                vsize: 100,
                weight: 400,
                locktime: 0,
                vin: vec![RpcInput {
                    coinbase: Some("04ffff001d0104".to_string()),
                    txid: None,
                    vout: None,
                    script_sig: None,
                    txinwitness: None,
                    sequence: 0xffff_ffff,
                }],
                vout: vec![RpcOutput {
                    value: "50.0".to_string(),
                    n: 0,
                    script_pubkey: RpcScriptPubKey {
                        hex: "76a914000000000000000000000000000000000000000088ac".to_string(),
                        type_: "pubkeyhash".to_string(),
                        address: Some("1BitcoinAddress000000000000000".to_string()),
                    },
                }],
            }],
        }
    }

    #[test]
    fn rpc_to_rows_minimal_block() {
        let network: DatasetNetworkId = "bitcoin-mainnet".parse().expect("valid network id");
        let block = minimal_block();
        let rows = rpc_to_rows(block, &network).expect("conversion succeeds");

        // We should get 4 TableRows: blocks, transactions, inputs, outputs
        let row_vec: Vec<_> = rows.into_iter().collect();
        assert_eq!(row_vec.len(), 4);

        let blocks = &row_vec[0];
        let txs = &row_vec[1];
        let inputs = &row_vec[2];
        let outputs = &row_vec[3];

        assert_eq!(blocks.rows.num_rows(), 1);
        assert_eq!(txs.rows.num_rows(), 1);
        assert_eq!(inputs.rows.num_rows(), 1);
        assert_eq!(outputs.rows.num_rows(), 1);
    }

    #[test]
    fn decode_bits_valid() {
        assert_eq!(
            decode_bits("1d00ffff").expect("valid hex bits string"),
            0x1d00ffff
        );
    }

    #[test]
    fn btc_to_sat_conversion() {
        assert_eq!(
            btc_to_sat("50.0").expect("50 BTC fits in i64"),
            5_000_000_000
        );
        assert_eq!(btc_to_sat("0.00000001").expect("1 satoshi fits in i64"), 1);
        assert_eq!(btc_to_sat("0.0000001").expect("10 satoshi fits in i64"), 10);
    }

    #[test]
    fn decode_hash_valid() {
        let hash = decode_hash(
            "0000000000000000000000000000000000000000000000000000000000000001",
            "test",
        )
        .expect("valid 32-byte hash hex");
        assert_eq!(hash[31], 1);
    }
}
