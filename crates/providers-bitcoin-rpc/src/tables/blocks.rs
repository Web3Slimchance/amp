use std::sync::{Arc, LazyLock};

use datasets_common::{
    block_num::RESERVED_BLOCK_NUM_COLUMN_NAME, block_range::BlockRange, network_id::NetworkId,
    watermark_columns::RESERVED_TS_COLUMN_NAME,
};
use datasets_raw::{
    Timestamp,
    arrow::{
        ArrayRef, DataType, Field, FixedSizeBinaryBuilder, Float64Builder, Int32Builder, Schema,
        SchemaRef, TimestampArrayBuilder, UInt32Builder, UInt64Builder,
    },
    dataset::Table,
    rows::{TableRowError, TableRows},
    timestamp_type,
};

static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| Arc::new(schema()));

pub const TABLE_NAME: &str = "blocks";

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
        Field::new("hash", DataType::FixedSizeBinary(32), false),
        Field::new("parent_hash", DataType::FixedSizeBinary(32), true),
        Field::new("merkle_root", DataType::FixedSizeBinary(32), false),
        Field::new("nonce", DataType::UInt32, false),
        Field::new("bits", DataType::UInt32, false),
        Field::new("difficulty", DataType::Float64, false),
        Field::new("version", DataType::Int32, false),
        Field::new("size", DataType::UInt32, false),
        Field::new("stripped_size", DataType::UInt32, false),
        Field::new("weight", DataType::UInt32, false),
        Field::new("tx_count", DataType::UInt32, false),
        Field::new("median_time", timestamp_type(), false),
    ])
}

/// A single Bitcoin block header row extracted via the Bitcoin Core RPC.
#[derive(Debug, Default)]
pub struct Block {
    /// Block height (distance from genesis block).
    pub block_num: u64,
    /// Unix timestamp of when the block was mined, in nanoseconds.
    pub timestamp: Timestamp,
    /// SHA-256d block hash (32 bytes, decoded from hex).
    pub hash: [u8; 32],
    /// Hash of the preceding block. `None` only for the genesis block.
    pub parent_hash: Option<[u8; 32]>,
    /// Merkle root of all transactions in the block (32 bytes).
    pub merkle_root: [u8; 32],
    /// Proof-of-work nonce chosen by the miner.
    pub nonce: u32,
    /// Compact difficulty target (`nBits`), decoded from its hex representation.
    pub bits: u32,
    /// Mining difficulty as a floating-point multiple of the minimum difficulty.
    pub difficulty: f64,
    /// Block version signalling softfork readiness.
    pub version: i32,
    /// Total serialised block size in bytes (including witness data).
    pub size: u32,
    /// Block size in bytes with witness data stripped (base size).
    pub stripped_size: u32,
    /// Block weight in weight units (`base_size * 3 + total_size`).
    pub weight: u32,
    /// Number of transactions included in the block.
    pub tx_count: u32,
    /// Median-time-past: median Unix timestamp of the preceding 11 blocks, in nanoseconds.
    pub median_time: Timestamp,
}

pub struct BlockRowsBuilder {
    special_block_num: UInt64Builder,
    special_ts: TimestampArrayBuilder,
    block_num: UInt64Builder,
    timestamp: TimestampArrayBuilder,
    hash: FixedSizeBinaryBuilder,
    parent_hash: FixedSizeBinaryBuilder,
    merkle_root: FixedSizeBinaryBuilder,
    nonce: UInt32Builder,
    bits: UInt32Builder,
    difficulty: Float64Builder,
    version: Int32Builder,
    size: UInt32Builder,
    stripped_size: UInt32Builder,
    weight: UInt32Builder,
    tx_count: UInt32Builder,
    median_time: TimestampArrayBuilder,
}

impl BlockRowsBuilder {
    pub fn with_capacity_for(_block: &Block) -> Self {
        Self {
            special_block_num: UInt64Builder::with_capacity(1),
            special_ts: TimestampArrayBuilder::with_capacity(1),
            block_num: UInt64Builder::with_capacity(1),
            timestamp: TimestampArrayBuilder::with_capacity(1),
            hash: FixedSizeBinaryBuilder::with_capacity(1, 32),
            parent_hash: FixedSizeBinaryBuilder::with_capacity(1, 32),
            merkle_root: FixedSizeBinaryBuilder::with_capacity(1, 32),
            nonce: UInt32Builder::with_capacity(1),
            bits: UInt32Builder::with_capacity(1),
            difficulty: Float64Builder::with_capacity(1),
            version: Int32Builder::with_capacity(1),
            size: UInt32Builder::with_capacity(1),
            stripped_size: UInt32Builder::with_capacity(1),
            weight: UInt32Builder::with_capacity(1),
            tx_count: UInt32Builder::with_capacity(1),
            median_time: TimestampArrayBuilder::with_capacity(1),
        }
    }

    pub fn append(&mut self, row: &Block) {
        self.special_block_num.append_value(row.block_num);
        self.special_ts.append_value(row.timestamp);
        self.block_num.append_value(row.block_num);
        self.timestamp.append_value(row.timestamp);
        // Unwrap: size is fixed at 32 bytes
        self.hash.append_value(row.hash).unwrap();
        match row.parent_hash {
            Some(h) => self.parent_hash.append_value(h).unwrap(),
            None => self.parent_hash.append_null(),
        }
        self.merkle_root.append_value(row.merkle_root).unwrap();
        self.nonce.append_value(row.nonce);
        self.bits.append_value(row.bits);
        self.difficulty.append_value(row.difficulty);
        self.version.append_value(row.version);
        self.size.append_value(row.size);
        self.stripped_size.append_value(row.stripped_size);
        self.weight.append_value(row.weight);
        self.tx_count.append_value(row.tx_count);
        self.median_time.append_value(row.median_time);
    }

    pub fn build(self, range: BlockRange) -> Result<TableRows, TableRowError> {
        let Self {
            mut special_block_num,
            mut special_ts,
            mut block_num,
            mut timestamp,
            mut hash,
            mut parent_hash,
            mut merkle_root,
            mut nonce,
            mut bits,
            mut difficulty,
            mut version,
            mut size,
            mut stripped_size,
            mut weight,
            mut tx_count,
            mut median_time,
        } = self;

        let columns: Vec<ArrayRef> = vec![
            Arc::new(special_block_num.finish()),
            Arc::new(special_ts.finish()),
            Arc::new(block_num.finish()),
            Arc::new(timestamp.finish()),
            Arc::new(hash.finish()),
            Arc::new(parent_hash.finish()),
            Arc::new(merkle_root.finish()),
            Arc::new(nonce.finish()),
            Arc::new(bits.finish()),
            Arc::new(difficulty.finish()),
            Arc::new(version.finish()),
            Arc::new(size.finish()),
            Arc::new(stripped_size.finish()),
            Arc::new(weight.finish()),
            Arc::new(tx_count.finish()),
            Arc::new(median_time.finish()),
        ];

        TableRows::new(table(range.network.clone()), range, columns)
    }
}

#[cfg(test)]
mod tests {
    use datasets_common::block_range::BlockRange;

    use super::{Block, BlockRowsBuilder};

    #[test]
    fn default_to_arrow() {
        let block = Block::default();
        let rows = {
            let mut builder = BlockRowsBuilder::with_capacity_for(&block);
            builder.append(&block);
            builder
                .build(BlockRange {
                    numbers: block.block_num..=block.block_num,
                    network: "bitcoin-mainnet".parse().expect("valid network id"),
                    hash: block.hash.into(),
                    prev_hash: Default::default(),
                    timestamp: None,
                })
                .expect("valid block row build")
        };
        assert_eq!(rows.rows.num_columns(), 16);
        assert_eq!(rows.rows.num_rows(), 1);
    }
}
