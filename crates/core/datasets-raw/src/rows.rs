//! Raw table row types for dataset extraction.

use arrow::{
    array::{ArrayRef, AsArray as _, RecordBatch},
    datatypes::{DataType, Schema, TimeUnit, UInt64Type},
};
use datasets_common::{
    block_num::{BlockNum, RESERVED_BLOCK_NUM_COLUMN_NAME},
    block_range::BlockRange,
    dataset::Table as _,
    watermark_columns::RESERVED_TS_COLUMN_NAME,
};

use crate::dataset::Table;

pub struct Rows(Vec<TableRows>);

impl Rows {
    pub fn new(rows: Vec<TableRows>) -> Self {
        assert!(!rows.is_empty());
        assert!(rows.iter().skip(1).all(|r| r.range == rows[0].range));
        Self(rows)
    }

    pub fn block_num(&self) -> BlockNum {
        self.0[0].block_num()
    }
}

impl IntoIterator for Rows {
    type Item = TableRows;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a Rows {
    type Item = &'a TableRows;
    type IntoIter = std::slice::Iter<'a, TableRows>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

/// A record batch associated with a single block of chain data, for populating raw datasets.
pub struct TableRows {
    pub table: Table,
    pub rows: RecordBatch,
    pub range: BlockRange,
}

impl TableRows {
    pub fn new(
        table: Table,
        range: BlockRange,
        columns: Vec<ArrayRef>,
    ) -> Result<Self, TableRowError> {
        let table_name = table.name().to_string();
        let schema = table.schema().clone();
        let rows = RecordBatch::try_new(schema, columns)?;
        Self::check_invariants(&range, &rows).map_err(|source| TableRowError::Invariants {
            table: table_name,
            source,
        })?;
        Ok(Self { table, rows, range })
    }

    pub fn block_num(&self) -> BlockNum {
        self.range.start()
    }

    /// Projects the record batch to the provided schema when it is a superset.
    pub fn project(&mut self, schema: &Schema) -> Result<(), TableRowError> {
        let rows_schema = self.rows.schema();

        let schemas_match = rows_schema.fields().len() == schema.fields().len()
            && rows_schema
                .fields()
                .iter()
                .zip(schema.fields())
                .all(|(left, right)| {
                    left.name() == right.name()
                        && data_types_compatible(left.data_type(), right.data_type())
                });

        if schemas_match {
            return Ok(());
        }

        let mut projection = Vec::with_capacity(schema.fields().len());

        for target_field in schema.fields() {
            let Some(index) = rows_schema
                .fields()
                .iter()
                .position(|f| f.name() == target_field.name())
            else {
                return Err(TableRowError::SchemaProjection {
                    expected: schema.fields().iter().map(|f| f.name().clone()).collect(),
                    actual: rows_schema
                        .fields()
                        .iter()
                        .map(|f| f.name().clone())
                        .collect(),
                });
            };

            let candidate = rows_schema.field(index);
            if !data_types_compatible(candidate.data_type(), target_field.data_type()) {
                return Err(TableRowError::SchemaTypeMismatch {
                    field: target_field.name().clone(),
                    expected_type: target_field.data_type().clone(),
                    actual_type: candidate.data_type().clone(),
                });
            }

            projection.push(index);
        }

        self.rows = self.rows.project(&projection)?;

        Ok(())
    }

    fn check_invariants(
        range: &BlockRange,
        rows: &RecordBatch,
    ) -> Result<(), CheckInvariantsError> {
        if range.start() != range.end() {
            return Err(CheckInvariantsError::InvalidBlockRange);
        }
        if rows.num_rows() == 0 {
            return Ok(());
        }

        let block_nums = rows
            .column_by_name(RESERVED_BLOCK_NUM_COLUMN_NAME)
            .ok_or(CheckInvariantsError::MissingBlockNumColumn)?;
        let block_nums = block_nums
            .as_primitive_opt::<UInt64Type>()
            .ok_or(CheckInvariantsError::InvalidBlockNumColumnType)?;

        // Unwrap: `rows` is not empty.
        let start = arrow::compute::kernels::aggregate::min(block_nums).unwrap();
        let end = arrow::compute::kernels::aggregate::max(block_nums).unwrap();
        if start != range.start() {
            return Err(CheckInvariantsError::UnexpectedBlockNum(start));
        };
        if end != range.start() {
            return Err(CheckInvariantsError::UnexpectedBlockNum(end));
        };

        // Validate _ts column has the correct type, if present.
        if let Some(ts_col) = rows.column_by_name(RESERVED_TS_COLUMN_NAME)
            && !matches!(
                ts_col.data_type(),
                DataType::Timestamp(TimeUnit::Nanosecond, Some(tz)) if tz.as_ref() == "+00:00"
            )
        {
            return Err(CheckInvariantsError::InvalidTsColumnType);
        }

        Ok(())
    }
}

/// Errors that occur when validating table row invariants.
///
/// These errors represent violations of structural requirements for raw dataset tables,
/// such as block range consistency and required column presence/types.
#[derive(Debug, thiserror::Error)]
pub enum CheckInvariantsError {
    /// Block range does not contain exactly one block number
    ///
    /// This occurs when the block range start and end differ, violating the requirement
    /// that TableRows must represent data from a single block.
    ///
    /// Raw dataset tables are organized by individual blocks, and each TableRows instance
    /// must contain data from exactly one block number.
    #[error("block range must contain a single block number")]
    InvalidBlockRange,

    /// Required `_block_num` column is missing from the record batch
    ///
    /// This occurs when the Arrow RecordBatch does not contain the special `_block_num`
    /// column that tracks which block each row belongs to.
    ///
    /// All raw dataset tables require the `_block_num` column for block-level partitioning
    /// and filtering operations.
    #[error("missing _block_num column")]
    MissingBlockNumColumn,

    /// The `_block_num` column has incorrect data type
    ///
    /// This occurs when the `_block_num` column exists but is not of type UInt64.
    ///
    /// The `_block_num` column must be UInt64 to properly represent blockchain block numbers.
    #[error("_block_num column is not uint64")]
    InvalidBlockNumColumnType,

    /// The `_ts` column has incorrect data type
    #[error("_ts column is not Timestamp(Nanosecond, UTC)")]
    InvalidTsColumnType,

    /// Row contains block number that doesn't match the expected range
    ///
    /// This occurs when one or more rows have a `_block_num` value that differs from
    /// the block number specified in the range. All rows must have the same block number
    /// matching the range's single block.
    ///
    /// This check validates data consistency between the block range metadata and the
    /// actual block numbers in the row data.
    #[error("contains unexpected block_num: {0}")]
    UnexpectedBlockNum(BlockNum),
}

/// Errors that occur when creating TableRows instances.
///
/// This error type is used by `TableRows::new()` and represents failures during
/// record batch construction and validation.
#[derive(Debug, thiserror::Error)]
pub enum TableRowError {
    /// Failed to construct Arrow RecordBatch from columns
    ///
    /// This occurs when Arrow cannot create a valid RecordBatch from the provided
    /// columns and schema. The underlying Arrow error provides specific details.
    ///
    /// Common causes:
    /// - Column count doesn't match schema field count
    /// - Column types don't match schema types
    /// - Column lengths are inconsistent
    /// - Invalid array data or corrupted memory buffers
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),

    /// Record batch schema cannot be projected onto target schema due to missing fields
    #[error(
        "cannot project table rows onto target schema; expected fields {expected:?}, actual {actual:?}"
    )]
    SchemaProjection {
        expected: Vec<String>,
        actual: Vec<String>,
    },

    /// A field exists in both schemas but has incompatible data types
    #[error(
        "cannot project table rows onto target schema; field {field:?} has type {actual_type} but expected {expected_type}"
    )]
    SchemaTypeMismatch {
        field: String,
        expected_type: DataType,
        actual_type: DataType,
    },

    /// Table rows violate structural invariants
    ///
    /// This occurs when the constructed RecordBatch violates requirements for raw
    /// dataset tables, such as missing required columns, incorrect block numbers,
    /// or invalid block ranges.
    ///
    /// The source error provides specific details about which invariant was violated.
    #[error("malformed table {table}: {source}")]
    Invariants {
        table: String,
        #[source]
        source: CheckInvariantsError,
    },
}

/// Compares two Arrow data types for structural compatibility, ignoring the inner
/// field name of List/LargeList types. This allows schemas using different
/// conventional names (e.g., "item" vs "element") for list elements to be treated
/// as compatible.
///
/// TODO: Consider replacing with `DFSchema::datatype_is_logically_equal` when
/// upgrading to DataFusion v53+. Note that method has broader semantics (e.g.,
/// treats Dictionary<K,V> as equal to V) so evaluate whether that's acceptable.
fn data_types_compatible(a: &DataType, b: &DataType) -> bool {
    match (a, b) {
        (DataType::List(a_field), DataType::List(b_field))
        | (DataType::LargeList(a_field), DataType::LargeList(b_field)) => {
            a_field.is_nullable() == b_field.is_nullable()
                && data_types_compatible(a_field.data_type(), b_field.data_type())
        }
        (DataType::Struct(a_fields), DataType::Struct(b_fields)) => {
            a_fields.len() == b_fields.len()
                && a_fields.iter().zip(b_fields.iter()).all(|(af, bf)| {
                    af.name() == bf.name()
                        && af.is_nullable() == bf.is_nullable()
                        && data_types_compatible(af.data_type(), bf.data_type())
                })
        }
        _ => a == b,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{ArrayRef, Int64Array, ListArray, RecordBatch, UInt64Array},
        buffer::OffsetBuffer,
        datatypes::{DataType, Field, Schema},
    };
    use datasets_common::{
        block_num::RESERVED_BLOCK_NUM_COLUMN_NAME, block_range::BlockRange,
        watermark_columns::RESERVED_TS_COLUMN_NAME,
    };

    use super::TableRows;
    use crate::{TimestampArrayType, dataset::Table, timestamp_type};

    #[test]
    fn project_tolerates_list_inner_field_name() {
        let block_num: u64 = 1;
        let network: datasets_common::network_id::NetworkId =
            "test".parse().expect("valid network id");

        // Schema with list inner field named "item"
        let item_schema = Arc::new(Schema::new(vec![
            Field::new(RESERVED_BLOCK_NUM_COLUMN_NAME, DataType::UInt64, false),
            Field::new(RESERVED_TS_COLUMN_NAME, timestamp_type(), false),
            Field::new(
                "values",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, false))),
                false,
            ),
        ]));

        // Target schema with list inner field named "element"
        let element_schema = Schema::new(vec![
            Field::new(RESERVED_BLOCK_NUM_COLUMN_NAME, DataType::UInt64, false),
            Field::new(RESERVED_TS_COLUMN_NAME, timestamp_type(), false),
            Field::new(
                "values",
                DataType::List(Arc::new(Field::new("element", DataType::Int64, false))),
                false,
            ),
        ]);

        let block_nums: ArrayRef = Arc::new(UInt64Array::from(vec![block_num]));
        let timestamps: ArrayRef = Arc::new(
            TimestampArrayType::from(vec![Some(1_000_000_000)]).with_data_type(timestamp_type()),
        );
        let list_values = Int64Array::from(vec![42]);
        let list_col: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Int64, false)),
            OffsetBuffer::from_lengths([1]),
            Arc::new(list_values),
            None,
        ));

        let rows =
            RecordBatch::try_new(item_schema.clone(), vec![block_nums, timestamps, list_col])
                .expect("valid record batch");

        let table = Table::new(
            "test_table".parse().expect("valid table name"),
            item_schema,
            network.clone(),
            vec![],
        );

        let range = BlockRange {
            numbers: block_num..=block_num,
            network,
            hash: Default::default(),
            prev_hash: Default::default(),
            timestamp: None,
        };

        let mut table_rows = TableRows { table, rows, range };

        table_rows
            .project(&element_schema)
            .expect("projection should succeed with different list inner field names");
    }
}
