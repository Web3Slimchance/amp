use std::sync::{Arc, LazyLock};

use datasets_common::{
    block_num::RESERVED_BLOCK_NUM_COLUMN_NAME, block_range::BlockRange, network_id::NetworkId,
    watermark_columns::RESERVED_TS_COLUMN_NAME,
};
use datasets_raw::{
    Timestamp,
    arrow::{
        ArrayRef, BooleanBuilder, DataType, Field, FixedSizeBinaryBuilder, Int32Builder, Schema,
        SchemaRef, TimestampArrayBuilder, UInt32Builder, UInt64Builder,
    },
    dataset::Table,
    rows::{TableRowError, TableRows},
    timestamp_type,
};

static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| Arc::new(schema()));

pub const TABLE_NAME: &str = "transactions";

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
        Field::new("hash", DataType::FixedSizeBinary(32), false),
        Field::new("version", DataType::Int32, false),
        Field::new("size", DataType::UInt32, false),
        Field::new("vsize", DataType::UInt32, false),
        Field::new("weight", DataType::UInt32, false),
        Field::new("locktime", DataType::UInt32, false),
        Field::new("is_coinbase", DataType::Boolean, false),
    ])
}

/// A Bitcoin transaction extracted from a block.
#[derive(Debug, Default)]
pub struct Transaction {
    /// Block height containing this transaction.
    pub block_num: u64,
    /// Unix timestamp of the containing block, in nanoseconds.
    pub timestamp: Timestamp,
    /// Hash of the containing block (32 bytes).
    pub block_hash: [u8; 32],
    /// Zero-based position of this transaction within the block.
    pub tx_index: u32,
    /// Transaction ID: SHA-256d of the serialised transaction excluding witness data (32 bytes).
    pub txid: [u8; 32],
    /// Transaction hash: SHA-256d of the full serialised transaction including witness data (32
    /// bytes). Equals `txid` for pre-SegWit transactions.
    pub hash: [u8; 32],
    /// Transaction version, used to signal feature support.
    pub version: i32,
    /// Total serialised transaction size in bytes, including witness data.
    pub size: u32,
    /// Virtual size in vbytes used for fee calculation (`ceil(weight / 4)`).
    pub vsize: u32,
    /// Transaction weight in weight units (`non_witness_size * 3 + total_size`).
    pub weight: u32,
    /// Earliest block height or Unix timestamp at which this transaction may be included.
    pub locktime: u32,
    /// `true` if this is the coinbase transaction (first in block, creates new coins).
    pub is_coinbase: bool,
}

pub struct TransactionRowsBuilder {
    special_block_num: UInt64Builder,
    special_ts: TimestampArrayBuilder,
    block_num: UInt64Builder,
    timestamp: TimestampArrayBuilder,
    block_hash: FixedSizeBinaryBuilder,
    tx_index: UInt32Builder,
    txid: FixedSizeBinaryBuilder,
    hash: FixedSizeBinaryBuilder,
    version: Int32Builder,
    size: UInt32Builder,
    vsize: UInt32Builder,
    weight: UInt32Builder,
    locktime: UInt32Builder,
    is_coinbase: BooleanBuilder,
}

impl TransactionRowsBuilder {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            special_block_num: UInt64Builder::with_capacity(capacity),
            special_ts: TimestampArrayBuilder::with_capacity(capacity),
            block_num: UInt64Builder::with_capacity(capacity),
            timestamp: TimestampArrayBuilder::with_capacity(capacity),
            block_hash: FixedSizeBinaryBuilder::with_capacity(capacity, 32),
            tx_index: UInt32Builder::with_capacity(capacity),
            txid: FixedSizeBinaryBuilder::with_capacity(capacity, 32),
            hash: FixedSizeBinaryBuilder::with_capacity(capacity, 32),
            version: Int32Builder::with_capacity(capacity),
            size: UInt32Builder::with_capacity(capacity),
            vsize: UInt32Builder::with_capacity(capacity),
            weight: UInt32Builder::with_capacity(capacity),
            locktime: UInt32Builder::with_capacity(capacity),
            is_coinbase: BooleanBuilder::with_capacity(capacity),
        }
    }

    pub fn append(&mut self, row: &Transaction) {
        self.special_block_num.append_value(row.block_num);
        self.special_ts.append_value(row.timestamp);
        self.block_num.append_value(row.block_num);
        self.timestamp.append_value(row.timestamp);
        // Unwrap: size is fixed at 32 bytes
        self.block_hash.append_value(row.block_hash).unwrap();
        self.tx_index.append_value(row.tx_index);
        self.txid.append_value(row.txid).unwrap();
        self.hash.append_value(row.hash).unwrap();
        self.version.append_value(row.version);
        self.size.append_value(row.size);
        self.vsize.append_value(row.vsize);
        self.weight.append_value(row.weight);
        self.locktime.append_value(row.locktime);
        self.is_coinbase.append_value(row.is_coinbase);
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
            mut hash,
            mut version,
            mut size,
            mut vsize,
            mut weight,
            mut locktime,
            mut is_coinbase,
        } = self;

        let columns: Vec<ArrayRef> = vec![
            Arc::new(special_block_num.finish()),
            Arc::new(special_ts.finish()),
            Arc::new(block_num.finish()),
            Arc::new(timestamp.finish()),
            Arc::new(block_hash.finish()),
            Arc::new(tx_index.finish()),
            Arc::new(txid.finish()),
            Arc::new(hash.finish()),
            Arc::new(version.finish()),
            Arc::new(size.finish()),
            Arc::new(vsize.finish()),
            Arc::new(weight.finish()),
            Arc::new(locktime.finish()),
            Arc::new(is_coinbase.finish()),
        ];

        TableRows::new(table(range.network.clone()), range, columns)
    }
}

#[cfg(test)]
mod tests {
    use datasets_common::block_range::BlockRange;

    use super::{Transaction, TransactionRowsBuilder};

    #[test]
    fn default_to_arrow() {
        let tx = Transaction::default();
        let rows = {
            let mut builder = TransactionRowsBuilder::with_capacity(1);
            builder.append(&tx);
            builder
                .build(BlockRange {
                    numbers: tx.block_num..=tx.block_num,
                    network: "bitcoin-mainnet".parse().expect("valid network id"),
                    hash: tx.block_hash.into(),
                    prev_hash: Default::default(),
                    timestamp: None,
                })
                .expect("valid transaction row build")
        };
        assert_eq!(rows.rows.num_columns(), 14);
        assert_eq!(rows.rows.num_rows(), 1);
    }
}
