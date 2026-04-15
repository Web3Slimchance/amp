use std::sync::{Arc, LazyLock};

use datasets_common::{
    block_num::RESERVED_BLOCK_NUM_COLUMN_NAME, block_range::BlockRange, network_id::NetworkId,
    watermark_columns::RESERVED_TS_COLUMN_NAME,
};
use datasets_raw::{
    Timestamp,
    arrow::{
        ArrayRef, BinaryBuilder, DataType, Field, FixedSizeBinaryBuilder, Int64Builder, Schema,
        SchemaRef, StringBuilder, TimestampArrayBuilder, UInt32Builder, UInt64Builder,
    },
    dataset::Table,
    rows::{TableRowError, TableRows},
    timestamp_type,
};

static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| Arc::new(schema()));

pub const TABLE_NAME: &str = "outputs";

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
        Field::new("output_index", DataType::UInt32, false),
        Field::new("value_sat", DataType::Int64, false),
        Field::new("script_pubkey", DataType::Binary, false),
        Field::new("script_pubkey_type", DataType::Utf8, false),
        Field::new("script_pubkey_address", DataType::Utf8, true),
    ])
}

/// A transaction output (`vout` entry) extracted from a Bitcoin transaction.
#[derive(Debug, Default)]
pub struct Output {
    /// Block height containing the parent transaction.
    pub block_num: u64,
    /// Unix timestamp of the containing block, in nanoseconds.
    pub timestamp: Timestamp,
    /// Hash of the containing block (32 bytes).
    pub block_hash: [u8; 32],
    /// Zero-based position of the parent transaction within the block.
    pub tx_index: u32,
    /// ID of the transaction that contains this output (32 bytes).
    pub txid: [u8; 32],
    /// Zero-based index of this output within its transaction (`vout` index).
    pub output_index: u32,
    /// Output value in satoshis (1 BTC = 100,000,000 satoshis).
    pub value_sat: i64,
    /// Locking script (scriptPubKey) as raw bytes.
    pub script_pubkey: Vec<u8>,
    /// Script type as reported by Bitcoin Core (e.g. `"pubkeyhash"`, `"witness_v0_keyhash"`,
    /// `"nulldata"`).
    pub script_pubkey_type: String,
    /// Destination address; `None` for unspendable outputs such as `OP_RETURN`.
    pub script_pubkey_address: Option<String>,
}

pub struct OutputRowsBuilder {
    special_block_num: UInt64Builder,
    special_ts: TimestampArrayBuilder,
    block_num: UInt64Builder,
    timestamp: TimestampArrayBuilder,
    block_hash: FixedSizeBinaryBuilder,
    tx_index: UInt32Builder,
    txid: FixedSizeBinaryBuilder,
    output_index: UInt32Builder,
    value_sat: Int64Builder,
    script_pubkey: BinaryBuilder,
    script_pubkey_type: StringBuilder,
    script_pubkey_address: StringBuilder,
}

impl OutputRowsBuilder {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            special_block_num: UInt64Builder::with_capacity(capacity),
            special_ts: TimestampArrayBuilder::with_capacity(capacity),
            block_num: UInt64Builder::with_capacity(capacity),
            timestamp: TimestampArrayBuilder::with_capacity(capacity),
            block_hash: FixedSizeBinaryBuilder::with_capacity(capacity, 32),
            tx_index: UInt32Builder::with_capacity(capacity),
            txid: FixedSizeBinaryBuilder::with_capacity(capacity, 32),
            output_index: UInt32Builder::with_capacity(capacity),
            value_sat: Int64Builder::with_capacity(capacity),
            script_pubkey: BinaryBuilder::with_capacity(capacity, 0),
            script_pubkey_type: StringBuilder::new(),
            script_pubkey_address: StringBuilder::new(),
        }
    }

    pub fn append(&mut self, row: &Output) {
        self.special_block_num.append_value(row.block_num);
        self.special_ts.append_value(row.timestamp);
        self.block_num.append_value(row.block_num);
        self.timestamp.append_value(row.timestamp);
        // Unwrap: size is fixed at 32 bytes
        self.block_hash.append_value(row.block_hash).unwrap();
        self.tx_index.append_value(row.tx_index);
        self.txid.append_value(row.txid).unwrap();
        self.output_index.append_value(row.output_index);
        self.value_sat.append_value(row.value_sat);
        self.script_pubkey.append_value(&row.script_pubkey);
        self.script_pubkey_type
            .append_value(&row.script_pubkey_type);
        match &row.script_pubkey_address {
            Some(addr) => self.script_pubkey_address.append_value(addr),
            None => self.script_pubkey_address.append_null(),
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
            mut output_index,
            mut value_sat,
            mut script_pubkey,
            mut script_pubkey_type,
            mut script_pubkey_address,
        } = self;

        let columns: Vec<ArrayRef> = vec![
            Arc::new(special_block_num.finish()),
            Arc::new(special_ts.finish()),
            Arc::new(block_num.finish()),
            Arc::new(timestamp.finish()),
            Arc::new(block_hash.finish()),
            Arc::new(tx_index.finish()),
            Arc::new(txid.finish()),
            Arc::new(output_index.finish()),
            Arc::new(value_sat.finish()),
            Arc::new(script_pubkey.finish()),
            Arc::new(script_pubkey_type.finish()),
            Arc::new(script_pubkey_address.finish()),
        ];

        TableRows::new(table(range.network.clone()), range, columns)
    }
}

#[cfg(test)]
mod tests {
    use datasets_common::block_range::BlockRange;

    use super::{Output, OutputRowsBuilder};

    #[test]
    fn default_to_arrow() {
        let output = Output::default();
        let rows = {
            let mut builder = OutputRowsBuilder::with_capacity(1);
            builder.append(&output);
            builder
                .build(BlockRange {
                    numbers: output.block_num..=output.block_num,
                    network: "bitcoin-mainnet".parse().expect("valid network id"),
                    hash: output.block_hash.into(),
                    prev_hash: Default::default(),
                    timestamp: None,
                })
                .expect("valid output row build")
        };
        assert_eq!(rows.rows.num_columns(), 12);
        assert_eq!(rows.rows.num_rows(), 1);
    }
}
