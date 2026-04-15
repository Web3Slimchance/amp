use std::sync::{Arc, LazyLock};

use datasets_common::{
    block_num::RESERVED_BLOCK_NUM_COLUMN_NAME, block_range::BlockRange, network_id::NetworkId,
    watermark_columns::RESERVED_TS_COLUMN_NAME,
};
use datasets_raw::{
    Timestamp,
    arrow::{
        ArrayRef, BinaryBuilder, DataType, Field, FixedSizeBinaryBuilder, ListBuilder, Schema,
        SchemaRef, TimestampArrayBuilder, UInt32Builder, UInt64Builder,
    },
    dataset::Table,
    rows::{TableRowError, TableRows},
    timestamp_type,
};

static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| Arc::new(schema()));

pub const TABLE_NAME: &str = "inputs";

pub fn table(network: NetworkId) -> Table {
    let name = TABLE_NAME.parse().expect("table name is valid");
    Table::new(
        name,
        SCHEMA.clone(),
        network,
        vec!["block_num".to_string(), "timestamp".to_string()],
    )
}

/// Prefer using the pre-computed SCHEMA
fn schema() -> Schema {
    Schema::new(vec![
        Field::new(RESERVED_BLOCK_NUM_COLUMN_NAME, DataType::UInt64, false),
        Field::new(RESERVED_TS_COLUMN_NAME, timestamp_type(), false),
        Field::new("block_num", DataType::UInt64, false),
        Field::new("timestamp", timestamp_type(), false),
        Field::new("block_hash", DataType::FixedSizeBinary(32), false),
        Field::new("tx_index", DataType::UInt32, false),
        Field::new("txid", DataType::FixedSizeBinary(32), false),
        Field::new("input_index", DataType::UInt32, false),
        Field::new("spent_txid", DataType::FixedSizeBinary(32), true),
        Field::new("spent_vout", DataType::UInt32, true),
        Field::new("coinbase", DataType::Binary, true),
        Field::new("script_sig", DataType::Binary, false),
        Field::new("sequence", DataType::UInt32, false),
        Field::new(
            "witness",
            DataType::List(Arc::new(Field::new("element", DataType::Binary, false))),
            true,
        ),
    ])
}

/// A transaction input (`vin` entry) extracted from a Bitcoin transaction.
#[derive(Debug, Default)]
pub struct Input {
    /// Block height containing the parent transaction.
    pub block_num: u64,
    /// Unix timestamp of the containing block, in nanoseconds.
    pub timestamp: Timestamp,
    /// Hash of the containing block (32 bytes).
    pub block_hash: [u8; 32],
    /// Zero-based position of the parent transaction within the block.
    pub tx_index: u32,
    /// ID of the transaction that contains this input (32 bytes).
    pub txid: [u8; 32],
    /// Zero-based position of this input within its transaction.
    pub input_index: u32,
    /// ID of the transaction whose output is being spent; `None` for coinbase inputs.
    pub spent_txid: Option<[u8; 32]>,
    /// Output index within `spent_txid` that is being spent; `None` for coinbase inputs.
    pub spent_vout: Option<u32>,
    /// Raw coinbase script bytes; present only for the coinbase input of each block.
    pub coinbase: Option<Vec<u8>>,
    /// Unlocking script (scriptSig) as raw bytes; empty for native SegWit inputs.
    pub script_sig: Vec<u8>,
    /// Sequence number, used for opt-in RBF and relative timelocks.
    pub sequence: u32,
    /// Segregated witness stack items (one `Vec<u8>` per stack element); `None` if no witness.
    pub witness: Option<Vec<Vec<u8>>>,
}

pub struct InputRowsBuilder {
    special_block_num: UInt64Builder,
    special_ts: TimestampArrayBuilder,
    block_num: UInt64Builder,
    timestamp: TimestampArrayBuilder,
    block_hash: FixedSizeBinaryBuilder,
    tx_index: UInt32Builder,
    txid: FixedSizeBinaryBuilder,
    input_index: UInt32Builder,
    spent_txid: FixedSizeBinaryBuilder,
    spent_vout: UInt32Builder,
    coinbase: BinaryBuilder,
    script_sig: BinaryBuilder,
    sequence: UInt32Builder,
    witness: ListBuilder<BinaryBuilder>,
}

impl InputRowsBuilder {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            special_block_num: UInt64Builder::with_capacity(capacity),
            special_ts: TimestampArrayBuilder::with_capacity(capacity),
            block_num: UInt64Builder::with_capacity(capacity),
            timestamp: TimestampArrayBuilder::with_capacity(capacity),
            block_hash: FixedSizeBinaryBuilder::with_capacity(capacity, 32),
            tx_index: UInt32Builder::with_capacity(capacity),
            txid: FixedSizeBinaryBuilder::with_capacity(capacity, 32),
            input_index: UInt32Builder::with_capacity(capacity),
            spent_txid: FixedSizeBinaryBuilder::with_capacity(capacity, 32),
            spent_vout: UInt32Builder::with_capacity(capacity),
            coinbase: BinaryBuilder::with_capacity(capacity, 0),
            script_sig: BinaryBuilder::with_capacity(capacity, 0),
            sequence: UInt32Builder::with_capacity(capacity),
            witness: ListBuilder::new(BinaryBuilder::with_capacity(0, 0)).with_field(Field::new(
                "element",
                DataType::Binary,
                false,
            )),
        }
    }

    pub fn append(&mut self, row: &Input) {
        self.special_block_num.append_value(row.block_num);
        self.special_ts.append_value(row.timestamp);
        self.block_num.append_value(row.block_num);
        self.timestamp.append_value(row.timestamp);
        // SAFETY: all hash fields are exactly 32 bytes
        self.block_hash.append_value(row.block_hash).unwrap();
        self.tx_index.append_value(row.tx_index);
        self.txid.append_value(row.txid).unwrap();
        self.input_index.append_value(row.input_index);
        match row.spent_txid {
            Some(h) => self.spent_txid.append_value(h).unwrap(),
            None => self.spent_txid.append_null(),
        }
        match row.spent_vout {
            Some(v) => self.spent_vout.append_value(v),
            None => self.spent_vout.append_null(),
        }
        match &row.coinbase {
            Some(cb) => self.coinbase.append_value(cb),
            None => self.coinbase.append_null(),
        }
        self.script_sig.append_value(&row.script_sig);
        self.sequence.append_value(row.sequence);
        match &row.witness {
            Some(items) => {
                for item in items {
                    self.witness.values().append_value(item);
                }
                self.witness.append(true);
            }
            None => self.witness.append(false),
        }
    }

    pub fn build(self, range: BlockRange) -> Result<TableRows, TableRowError> {
        let Self {
            mut special_block_num,
            mut special_ts,
            mut block_num,
            mut timestamp,
            mut block_hash,
            mut tx_index,
            mut txid,
            mut input_index,
            mut spent_txid,
            mut spent_vout,
            mut coinbase,
            mut script_sig,
            mut sequence,
            mut witness,
        } = self;

        let columns: Vec<ArrayRef> = vec![
            Arc::new(special_block_num.finish()),
            Arc::new(special_ts.finish()),
            Arc::new(block_num.finish()),
            Arc::new(timestamp.finish()),
            Arc::new(block_hash.finish()),
            Arc::new(tx_index.finish()),
            Arc::new(txid.finish()),
            Arc::new(input_index.finish()),
            Arc::new(spent_txid.finish()),
            Arc::new(spent_vout.finish()),
            Arc::new(coinbase.finish()),
            Arc::new(script_sig.finish()),
            Arc::new(sequence.finish()),
            Arc::new(witness.finish()),
        ];

        TableRows::new(table(range.network.clone()), range, columns)
    }
}

#[cfg(test)]
mod tests {
    use datasets_common::block_range::BlockRange;

    use super::{Input, InputRowsBuilder};

    #[test]
    fn default_to_arrow() {
        let input = Input::default();
        let rows = {
            let mut builder = InputRowsBuilder::with_capacity(1);
            builder.append(&input);
            builder
                .build(BlockRange {
                    numbers: input.block_num..=input.block_num,
                    network: "bitcoin-mainnet".parse().expect("valid network id"),
                    hash: input.block_hash.into(),
                    prev_hash: Default::default(),
                    timestamp: None,
                })
                .expect("valid input row build")
        };
        assert_eq!(rows.rows.num_columns(), 14);
        assert_eq!(rows.rows.num_rows(), 1);
    }
}
