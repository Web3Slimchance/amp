//! Vectorized EVM event log decoding.
//!
//! Decodes EVM event topics and body data directly from Arrow buffers,
//! bypassing alloy's per-row `DynSolType`/`DynSolValue` machinery.
//! For fixed-size ABI types, this operates on contiguous byte buffers
//! without per-row dynamic dispatch or intermediate allocations.

use std::sync::Arc;

use alloy::{
    dyn_abi::{DynSolType, DynSolValue},
    primitives::{I256, U256},
};
use datafusion::error::DataFusionError;
use tracing::trace;

use super::evm_common::{
    DEC_128_MAX_BINARY_PREC, DEC_256_MAX_BINARY_PREC, DEC128_PREC, DEC256_PREC, Event,
    append_null_value_to_builder, append_sol_value_to_builder, sol_to_arrow_type,
};
use crate::{
    Bytes32ArrayType,
    arrow::{
        array::{
            Array, BinaryArray, BooleanArray, BooleanBufferBuilder, Decimal128Array,
            Decimal256Array, FixedSizeBinaryArray, Int8Array, Int16Array, Int32Array, Int64Array,
            StringBuilder, StructArray, StructBuilder, UInt8Array, UInt16Array, UInt32Array,
            UInt64Array,
        },
        buffer::{Buffer, NullBuffer, ScalarBuffer},
        datatypes::{Fields, i256 as ArrowI256},
    },
};

/// Decode EVM event log columns into a StructArray using vectorized operations.
///
/// Topics are decoded in bulk from contiguous FixedSizeBinary(32) buffers.
/// Body fields use bulk extraction for fixed-layout events, falling back
/// to row-by-row alloy decode for events with dynamic types.
pub fn decode_columnar(
    event: &Event,
    topic1: &Bytes32ArrayType,
    topic2: &Bytes32ArrayType,
    topic3: &Bytes32ArrayType,
    data: &BinaryArray,
) -> Result<Arc<dyn Array>, DataFusionError> {
    let mut columns: Vec<Arc<dyn Array>> = Vec::new();
    let topic_cols = [topic1, topic2, topic3];

    // Decode each indexed topic as a full column
    for (i, topic_type) in event.topic_types().iter().enumerate() {
        columns.push(decode_topic_vectorized(topic_cols[i], topic_type)?);
    }

    // Decode body fields
    let body_types = event.body_types();
    if !body_types.is_empty() {
        if is_fixed_layout(body_types) {
            let body_columns = decode_body_fixed_layout(data, body_types)?;
            columns.extend(body_columns);
        } else {
            let body_columns =
                decode_body_row_by_row(data, body_types, event.body_tuple(), event.event_name())?;
            columns.extend(body_columns);
        }
    }

    // Assemble StructArray from independent column arrays
    let fields = event.fields()?;
    Ok(Arc::new(StructArray::try_new(fields, columns, None)?))
}

/// Check if a Solidity type occupies a fixed 32-byte slot in ABI encoding.
fn is_abi_fixed_type(ty: &DynSolType) -> bool {
    matches!(
        ty,
        DynSolType::Bool
            | DynSolType::Int(_)
            | DynSolType::Uint(_)
            | DynSolType::FixedBytes(_)
            | DynSolType::Address
    )
}

/// Check if all body types have fixed ABI layout (each field = one 32-byte slot).
fn is_fixed_layout(body_types: &[DynSolType]) -> bool {
    body_types.iter().all(is_abi_fixed_type)
}

// ============================================================
// Topic decoding — operates on contiguous FixedSizeBinary(32)
// ============================================================

/// Decode a single topic column into the appropriate Arrow array type.
fn decode_topic_vectorized(
    topic_col: &Bytes32ArrayType,
    topic_type: &DynSolType,
) -> Result<Arc<dyn Array>, DataFusionError> {
    let num_rows = topic_col.len();
    if num_rows == 0 {
        return empty_array_for_topic(topic_type);
    }

    // Account for array offset into the underlying buffer
    let base = topic_col.offset() * 32;
    let src = &topic_col.value_data()[base..base + num_rows * 32];
    let nulls = topic_col.nulls().cloned();

    match topic_type {
        DynSolType::Address => decode_address_from_slots(src, num_rows, nulls),
        DynSolType::Bool => decode_bool_from_slots(src, num_rows, nulls),
        DynSolType::Uint(bits) => decode_uint_from_slots(src, num_rows, *bits, nulls),
        DynSolType::Int(bits) => decode_int_from_slots(src, num_rows, *bits, nulls),
        DynSolType::FixedBytes(n) => {
            let n = *n;
            let mut dst = Vec::with_capacity(num_rows * n);
            for i in 0..num_rows {
                dst.extend_from_slice(&src[i * 32..i * 32 + n]);
            }
            Ok(Arc::new(FixedSizeBinaryArray::new(
                n as i32,
                Buffer::from(dst),
                nulls,
            )))
        }
        // Reference types (string, bytes, arrays, tuples) stored as keccak256 hash.
        // Return the input column as-is (FixedSizeBinary(32)).
        _ => Ok(Arc::new(topic_col.clone())),
    }
}

fn empty_array_for_topic(topic_type: &DynSolType) -> Result<Arc<dyn Array>, DataFusionError> {
    use datafusion::arrow::array::new_empty_array;

    use super::evm_common::sol_to_arrow_type_for_indexed;
    let dt = sol_to_arrow_type_for_indexed(topic_type)?;
    Ok(new_empty_array(&dt))
}

/// Extract bytes [12..32] from each 32-byte slot → FixedSizeBinary(20)
fn decode_address_from_slots(
    src: &[u8],
    num_rows: usize,
    nulls: Option<NullBuffer>,
) -> Result<Arc<dyn Array>, DataFusionError> {
    let mut dst = Vec::with_capacity(num_rows * 20);
    for i in 0..num_rows {
        dst.extend_from_slice(&src[i * 32 + 12..i * 32 + 32]);
    }
    Ok(Arc::new(FixedSizeBinaryArray::new(
        20,
        Buffer::from(dst),
        nulls,
    )))
}

/// Read byte 31 from each 32-byte slot → BooleanArray
fn decode_bool_from_slots(
    src: &[u8],
    num_rows: usize,
    nulls: Option<NullBuffer>,
) -> Result<Arc<dyn Array>, DataFusionError> {
    let mut bits = BooleanBufferBuilder::new(num_rows);
    for i in 0..num_rows {
        bits.append(src[i * 32 + 31] != 0);
    }
    Ok(Arc::new(BooleanArray::new(bits.finish(), nulls)))
}

/// Decode unsigned integer from 32-byte big-endian slots.
fn decode_uint_from_slots(
    src: &[u8],
    num_rows: usize,
    bits: usize,
    nulls: Option<NullBuffer>,
) -> Result<Arc<dyn Array>, DataFusionError> {
    match bits {
        8 => {
            let values: Vec<u8> = (0..num_rows).map(|i| src[i * 32 + 31]).collect();
            Ok(Arc::new(UInt8Array::new(ScalarBuffer::from(values), nulls)))
        }
        16 => {
            let values: Vec<u16> = (0..num_rows)
                .map(|i| u16::from_be_bytes(src[i * 32 + 30..i * 32 + 32].try_into().unwrap()))
                .collect();
            Ok(Arc::new(UInt16Array::new(
                ScalarBuffer::from(values),
                nulls,
            )))
        }
        n if n <= 32 => {
            let values: Vec<u32> = (0..num_rows)
                .map(|i| u32::from_be_bytes(src[i * 32 + 28..i * 32 + 32].try_into().unwrap()))
                .collect();
            Ok(Arc::new(UInt32Array::new(
                ScalarBuffer::from(values),
                nulls,
            )))
        }
        n if n <= 64 => {
            let values: Vec<u64> = (0..num_rows)
                .map(|i| u64::from_be_bytes(src[i * 32 + 24..i * 32 + 32].try_into().unwrap()))
                .collect();
            Ok(Arc::new(UInt64Array::new(
                ScalarBuffer::from(values),
                nulls,
            )))
        }
        n if n <= DEC_128_MAX_BINARY_PREC => {
            // Big-endian u128 in bytes [16..32], stored as Decimal128
            let values: Vec<i128> = (0..num_rows)
                .map(|i| {
                    // Read as unsigned big-endian, cast to i128 (valid since bits <= 125)
                    let bytes: [u8; 16] = src[i * 32 + 16..i * 32 + 32].try_into().unwrap();
                    u128::from_be_bytes(bytes) as i128
                })
                .collect();
            Ok(Arc::new(
                Decimal128Array::new(ScalarBuffer::from(values), nulls)
                    .with_precision_and_scale(DEC128_PREC, 0)?,
            ))
        }
        n if n <= DEC_256_MAX_BINARY_PREC => {
            // Big-endian u256 in all 32 bytes, stored as Decimal256 (i256)
            let values: Vec<ArrowI256> = (0..num_rows)
                .map(|i| {
                    let mut le = [0u8; 32];
                    for j in 0..32 {
                        le[j] = src[i * 32 + 31 - j];
                    }
                    ArrowI256::from_le_bytes(le)
                })
                .collect();
            Ok(Arc::new(
                Decimal256Array::new(ScalarBuffer::from(values), nulls)
                    .with_precision_and_scale(DEC256_PREC, 0)?,
            ))
        }
        n if n <= 256 => {
            // uint256 → Utf8 (too large for numeric types)
            let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 20);
            for i in 0..num_rows {
                if nulls.as_ref().is_some_and(|n| n.is_null(i)) {
                    builder.append_null();
                } else {
                    let bytes: [u8; 32] = src[i * 32..i * 32 + 32].try_into().unwrap();
                    let u = U256::from_be_bytes(bytes);
                    builder.append_value(u.to_string());
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        _ => Err(DataFusionError::Internal(format!(
            "unexpected uint bit width: {}",
            bits
        ))),
    }
}

/// Decode signed integer from 32-byte big-endian sign-extended slots.
fn decode_int_from_slots(
    src: &[u8],
    num_rows: usize,
    bits: usize,
    nulls: Option<NullBuffer>,
) -> Result<Arc<dyn Array>, DataFusionError> {
    match bits {
        8 => {
            let values: Vec<i8> = (0..num_rows).map(|i| src[i * 32 + 31] as i8).collect();
            Ok(Arc::new(Int8Array::new(ScalarBuffer::from(values), nulls)))
        }
        16 => {
            let values: Vec<i16> = (0..num_rows)
                .map(|i| i16::from_be_bytes(src[i * 32 + 30..i * 32 + 32].try_into().unwrap()))
                .collect();
            Ok(Arc::new(Int16Array::new(ScalarBuffer::from(values), nulls)))
        }
        n if n <= 32 => {
            let values: Vec<i32> = (0..num_rows)
                .map(|i| i32::from_be_bytes(src[i * 32 + 28..i * 32 + 32].try_into().unwrap()))
                .collect();
            Ok(Arc::new(Int32Array::new(ScalarBuffer::from(values), nulls)))
        }
        n if n <= 64 => {
            let values: Vec<i64> = (0..num_rows)
                .map(|i| i64::from_be_bytes(src[i * 32 + 24..i * 32 + 32].try_into().unwrap()))
                .collect();
            Ok(Arc::new(Int64Array::new(ScalarBuffer::from(values), nulls)))
        }
        n if n <= DEC_128_MAX_BINARY_PREC => {
            // Sign-extended big-endian i128 in bytes [16..32]
            let values: Vec<i128> = (0..num_rows)
                .map(|i| {
                    let bytes: [u8; 16] = src[i * 32 + 16..i * 32 + 32].try_into().unwrap();
                    i128::from_be_bytes(bytes)
                })
                .collect();
            Ok(Arc::new(
                Decimal128Array::new(ScalarBuffer::from(values), nulls)
                    .with_precision_and_scale(DEC128_PREC, 0)?,
            ))
        }
        n if n <= DEC_256_MAX_BINARY_PREC => {
            // Sign-extended big-endian i256 in all 32 bytes
            let values: Vec<ArrowI256> = (0..num_rows)
                .map(|i| {
                    let mut le = [0u8; 32];
                    for j in 0..32 {
                        le[j] = src[i * 32 + 31 - j];
                    }
                    ArrowI256::from_le_bytes(le)
                })
                .collect();
            Ok(Arc::new(
                Decimal256Array::new(ScalarBuffer::from(values), nulls)
                    .with_precision_and_scale(DEC256_PREC, 0)?,
            ))
        }
        n if n <= 256 => {
            // int256 → Utf8
            let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 20);
            for i in 0..num_rows {
                if nulls.as_ref().is_some_and(|n| n.is_null(i)) {
                    builder.append_null();
                } else {
                    let bytes: [u8; 32] = src[i * 32..i * 32 + 32].try_into().unwrap();
                    let s = I256::from_be_bytes(bytes);
                    builder.append_value(s.to_string());
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        _ => Err(DataFusionError::Internal(format!(
            "unexpected int bit width: {}",
            bits
        ))),
    }
}

// ============================================================
// Body decoding — fixed layout (all fields are 32-byte ABI slots)
// ============================================================

/// Decode body fields from a BinaryArray where all fields have fixed 32-byte ABI layout.
/// Each row's data contains `num_fields * 32` bytes with fields at offsets `0, 32, 64, ...`.
fn decode_body_fixed_layout(
    data_col: &BinaryArray,
    body_types: &[DynSolType],
) -> Result<Vec<Arc<dyn Array>>, DataFusionError> {
    let num_rows = data_col.len();
    let num_fields = body_types.len();
    let required_len = num_fields * 32;

    // Pre-compute row validity: non-null data with sufficient length
    let mut valid = BooleanBufferBuilder::new(num_rows);
    for i in 0..num_rows {
        let is_valid = !data_col.is_null(i) && data_col.value(i).len() >= required_len;
        valid.append(is_valid);
    }
    let validity = valid.finish();

    let mut columns = Vec::with_capacity(num_fields);
    for (field_idx, ty) in body_types.iter().enumerate() {
        let abi_offset = field_idx * 32;
        let col = decode_body_field(data_col, &validity, abi_offset, ty, num_rows)?;
        columns.push(col);
    }

    Ok(columns)
}

/// Decode a single body field from the 32-byte ABI slot at `abi_offset` in each row.
fn decode_body_field(
    data_col: &BinaryArray,
    validity: &datafusion::arrow::buffer::BooleanBuffer,
    abi_offset: usize,
    ty: &DynSolType,
    num_rows: usize,
) -> Result<Arc<dyn Array>, DataFusionError> {
    match ty {
        DynSolType::Address => {
            let mut dst = Vec::with_capacity(num_rows * 20);
            let mut null_bits = BooleanBufferBuilder::new(num_rows);
            for i in 0..num_rows {
                if validity.value(i) {
                    let row = data_col.value(i);
                    dst.extend_from_slice(&row[abi_offset + 12..abi_offset + 32]);
                    null_bits.append(true);
                } else {
                    dst.extend_from_slice(&[0u8; 20]);
                    null_bits.append(false);
                }
            }
            Ok(Arc::new(FixedSizeBinaryArray::new(
                20,
                Buffer::from(dst),
                Some(NullBuffer::new(null_bits.finish())),
            )))
        }
        DynSolType::Bool => {
            let mut bits = BooleanBufferBuilder::new(num_rows);
            let mut null_bits = BooleanBufferBuilder::new(num_rows);
            for i in 0..num_rows {
                if validity.value(i) {
                    let row = data_col.value(i);
                    bits.append(row[abi_offset + 31] != 0);
                    null_bits.append(true);
                } else {
                    bits.append(false);
                    null_bits.append(false);
                }
            }
            Ok(Arc::new(BooleanArray::new(
                bits.finish(),
                Some(NullBuffer::new(null_bits.finish())),
            )))
        }
        DynSolType::Uint(bit_width) => {
            decode_body_uint(data_col, validity, abi_offset, *bit_width, num_rows)
        }
        DynSolType::Int(bit_width) => {
            decode_body_int(data_col, validity, abi_offset, *bit_width, num_rows)
        }
        DynSolType::FixedBytes(n) => {
            let n = *n;
            let mut dst = Vec::with_capacity(num_rows * n);
            let mut null_bits = BooleanBufferBuilder::new(num_rows);
            for i in 0..num_rows {
                if validity.value(i) {
                    let row = data_col.value(i);
                    dst.extend_from_slice(&row[abi_offset..abi_offset + n]);
                    null_bits.append(true);
                } else {
                    dst.resize(dst.len() + n, 0);
                    null_bits.append(false);
                }
            }
            Ok(Arc::new(FixedSizeBinaryArray::new(
                n as i32,
                Buffer::from(dst),
                Some(NullBuffer::new(null_bits.finish())),
            )))
        }
        _ => Err(DataFusionError::Internal(format!(
            "unexpected fixed-layout body type: {}",
            ty
        ))),
    }
}

/// Decode unsigned int body field from 32-byte ABI slots.
fn decode_body_uint(
    data_col: &BinaryArray,
    validity: &datafusion::arrow::buffer::BooleanBuffer,
    abi_offset: usize,
    bits: usize,
    num_rows: usize,
) -> Result<Arc<dyn Array>, DataFusionError> {
    // Helper: read the 32-byte slot from row i
    macro_rules! slot {
        ($i:expr) => {
            &data_col.value($i)[abi_offset..abi_offset + 32]
        };
    }

    match bits {
        8 => {
            let mut values = Vec::with_capacity(num_rows);
            let mut null_bits = BooleanBufferBuilder::new(num_rows);
            for i in 0..num_rows {
                if validity.value(i) {
                    values.push(slot!(i)[31]);
                    null_bits.append(true);
                } else {
                    values.push(0);
                    null_bits.append(false);
                }
            }
            Ok(Arc::new(UInt8Array::new(
                ScalarBuffer::from(values),
                Some(NullBuffer::new(null_bits.finish())),
            )))
        }
        16 => {
            let mut values = Vec::with_capacity(num_rows);
            let mut null_bits = BooleanBufferBuilder::new(num_rows);
            for i in 0..num_rows {
                if validity.value(i) {
                    let s = slot!(i);
                    values.push(u16::from_be_bytes(s[30..32].try_into().unwrap()));
                    null_bits.append(true);
                } else {
                    values.push(0);
                    null_bits.append(false);
                }
            }
            Ok(Arc::new(UInt16Array::new(
                ScalarBuffer::from(values),
                Some(NullBuffer::new(null_bits.finish())),
            )))
        }
        n if n <= 32 => {
            let mut values = Vec::with_capacity(num_rows);
            let mut null_bits = BooleanBufferBuilder::new(num_rows);
            for i in 0..num_rows {
                if validity.value(i) {
                    let s = slot!(i);
                    values.push(u32::from_be_bytes(s[28..32].try_into().unwrap()));
                    null_bits.append(true);
                } else {
                    values.push(0);
                    null_bits.append(false);
                }
            }
            Ok(Arc::new(UInt32Array::new(
                ScalarBuffer::from(values),
                Some(NullBuffer::new(null_bits.finish())),
            )))
        }
        n if n <= 64 => {
            let mut values = Vec::with_capacity(num_rows);
            let mut null_bits = BooleanBufferBuilder::new(num_rows);
            for i in 0..num_rows {
                if validity.value(i) {
                    let s = slot!(i);
                    values.push(u64::from_be_bytes(s[24..32].try_into().unwrap()));
                    null_bits.append(true);
                } else {
                    values.push(0);
                    null_bits.append(false);
                }
            }
            Ok(Arc::new(UInt64Array::new(
                ScalarBuffer::from(values),
                Some(NullBuffer::new(null_bits.finish())),
            )))
        }
        n if n <= DEC_128_MAX_BINARY_PREC => {
            let mut values = Vec::with_capacity(num_rows);
            let mut null_bits = BooleanBufferBuilder::new(num_rows);
            for i in 0..num_rows {
                if validity.value(i) {
                    let s = slot!(i);
                    let bytes: [u8; 16] = s[16..32].try_into().unwrap();
                    values.push(u128::from_be_bytes(bytes) as i128);
                    null_bits.append(true);
                } else {
                    values.push(0);
                    null_bits.append(false);
                }
            }
            Ok(Arc::new(
                Decimal128Array::new(
                    ScalarBuffer::from(values),
                    Some(NullBuffer::new(null_bits.finish())),
                )
                .with_precision_and_scale(DEC128_PREC, 0)?,
            ))
        }
        n if n <= DEC_256_MAX_BINARY_PREC => {
            let mut values = Vec::with_capacity(num_rows);
            let mut null_bits = BooleanBufferBuilder::new(num_rows);
            for i in 0..num_rows {
                if validity.value(i) {
                    let s = slot!(i);
                    let mut le = [0u8; 32];
                    for j in 0..32 {
                        le[j] = s[31 - j];
                    }
                    values.push(ArrowI256::from_le_bytes(le));
                    null_bits.append(true);
                } else {
                    values.push(ArrowI256::ZERO);
                    null_bits.append(false);
                }
            }
            Ok(Arc::new(
                Decimal256Array::new(
                    ScalarBuffer::from(values),
                    Some(NullBuffer::new(null_bits.finish())),
                )
                .with_precision_and_scale(DEC256_PREC, 0)?,
            ))
        }
        n if n <= 256 => {
            // uint256 → Utf8
            let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 20);
            for i in 0..num_rows {
                if validity.value(i) {
                    let s = slot!(i);
                    let bytes: [u8; 32] = s.try_into().unwrap();
                    builder.append_value(U256::from_be_bytes(bytes).to_string());
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        _ => Err(DataFusionError::Internal(format!(
            "unexpected uint bit width: {}",
            bits
        ))),
    }
}

/// Decode signed int body field from 32-byte ABI slots.
fn decode_body_int(
    data_col: &BinaryArray,
    validity: &datafusion::arrow::buffer::BooleanBuffer,
    abi_offset: usize,
    bits: usize,
    num_rows: usize,
) -> Result<Arc<dyn Array>, DataFusionError> {
    macro_rules! slot {
        ($i:expr) => {
            &data_col.value($i)[abi_offset..abi_offset + 32]
        };
    }

    match bits {
        8 => {
            let mut values = Vec::with_capacity(num_rows);
            let mut null_bits = BooleanBufferBuilder::new(num_rows);
            for i in 0..num_rows {
                if validity.value(i) {
                    values.push(slot!(i)[31] as i8);
                    null_bits.append(true);
                } else {
                    values.push(0);
                    null_bits.append(false);
                }
            }
            Ok(Arc::new(Int8Array::new(
                ScalarBuffer::from(values),
                Some(NullBuffer::new(null_bits.finish())),
            )))
        }
        16 => {
            let mut values = Vec::with_capacity(num_rows);
            let mut null_bits = BooleanBufferBuilder::new(num_rows);
            for i in 0..num_rows {
                if validity.value(i) {
                    let s = slot!(i);
                    values.push(i16::from_be_bytes(s[30..32].try_into().unwrap()));
                    null_bits.append(true);
                } else {
                    values.push(0);
                    null_bits.append(false);
                }
            }
            Ok(Arc::new(Int16Array::new(
                ScalarBuffer::from(values),
                Some(NullBuffer::new(null_bits.finish())),
            )))
        }
        n if n <= 32 => {
            let mut values = Vec::with_capacity(num_rows);
            let mut null_bits = BooleanBufferBuilder::new(num_rows);
            for i in 0..num_rows {
                if validity.value(i) {
                    let s = slot!(i);
                    values.push(i32::from_be_bytes(s[28..32].try_into().unwrap()));
                    null_bits.append(true);
                } else {
                    values.push(0);
                    null_bits.append(false);
                }
            }
            Ok(Arc::new(Int32Array::new(
                ScalarBuffer::from(values),
                Some(NullBuffer::new(null_bits.finish())),
            )))
        }
        n if n <= 64 => {
            let mut values = Vec::with_capacity(num_rows);
            let mut null_bits = BooleanBufferBuilder::new(num_rows);
            for i in 0..num_rows {
                if validity.value(i) {
                    let s = slot!(i);
                    values.push(i64::from_be_bytes(s[24..32].try_into().unwrap()));
                    null_bits.append(true);
                } else {
                    values.push(0);
                    null_bits.append(false);
                }
            }
            Ok(Arc::new(Int64Array::new(
                ScalarBuffer::from(values),
                Some(NullBuffer::new(null_bits.finish())),
            )))
        }
        n if n <= DEC_128_MAX_BINARY_PREC => {
            let mut values = Vec::with_capacity(num_rows);
            let mut null_bits = BooleanBufferBuilder::new(num_rows);
            for i in 0..num_rows {
                if validity.value(i) {
                    let s = slot!(i);
                    let bytes: [u8; 16] = s[16..32].try_into().unwrap();
                    values.push(i128::from_be_bytes(bytes));
                    null_bits.append(true);
                } else {
                    values.push(0);
                    null_bits.append(false);
                }
            }
            Ok(Arc::new(
                Decimal128Array::new(
                    ScalarBuffer::from(values),
                    Some(NullBuffer::new(null_bits.finish())),
                )
                .with_precision_and_scale(DEC128_PREC, 0)?,
            ))
        }
        n if n <= DEC_256_MAX_BINARY_PREC => {
            let mut values = Vec::with_capacity(num_rows);
            let mut null_bits = BooleanBufferBuilder::new(num_rows);
            for i in 0..num_rows {
                if validity.value(i) {
                    let s = slot!(i);
                    let mut le = [0u8; 32];
                    for j in 0..32 {
                        le[j] = s[31 - j];
                    }
                    values.push(ArrowI256::from_le_bytes(le));
                    null_bits.append(true);
                } else {
                    values.push(ArrowI256::ZERO);
                    null_bits.append(false);
                }
            }
            Ok(Arc::new(
                Decimal256Array::new(
                    ScalarBuffer::from(values),
                    Some(NullBuffer::new(null_bits.finish())),
                )
                .with_precision_and_scale(DEC256_PREC, 0)?,
            ))
        }
        n if n <= 256 => {
            // int256 → Utf8
            let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 20);
            for i in 0..num_rows {
                if validity.value(i) {
                    let s = slot!(i);
                    let bytes: [u8; 32] = s.try_into().unwrap();
                    builder.append_value(I256::from_be_bytes(bytes).to_string());
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        _ => Err(DataFusionError::Internal(format!(
            "unexpected int bit width: {}",
            bits
        ))),
    }
}

// ============================================================
// Body decoding — row-by-row fallback for dynamic types
// ============================================================

/// Fallback: decode body fields row-by-row using alloy's ABI decoder.
/// Used for events with dynamic body types (string, bytes, arrays).
fn decode_body_row_by_row(
    data_col: &BinaryArray,
    body_types: &[DynSolType],
    body_tuple: &DynSolType,
    event_name: &str,
) -> Result<Vec<Arc<dyn Array>>, DataFusionError> {
    let num_rows = data_col.len();

    // Build a StructBuilder for the body fields only
    let fields: Vec<_> = body_types
        .iter()
        .enumerate()
        .map(|(i, ty)| {
            Ok(datafusion::arrow::datatypes::Field::new(
                format!("f{i}"),
                sol_to_arrow_type(ty)?,
                true,
            ))
        })
        .collect::<Result<Vec<_>, DataFusionError>>()?;
    let fields = Fields::from(fields);
    let mut builder = StructBuilder::from_fields(fields, num_rows);

    for i in 0..num_rows {
        let row_data = if data_col.is_null(i) {
            None
        } else {
            Some(data_col.value(i))
        };

        let values = row_data
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "expected non-null log data for event decoding".to_string(),
                )
            })
            .and_then(|data| {
                body_tuple.abi_decode_sequence(data).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "failed to decode data field with {} bytes: {}",
                        data.len(),
                        e,
                    ))
                })
            })
            .and_then(|decoded| match decoded {
                DynSolValue::Tuple(values) => Ok(values),
                _ => unreachable!(),
            });

        match values {
            Ok(values) => {
                for (j, value) in values.into_iter().enumerate() {
                    let field_builder = &mut builder.field_builders_mut()[j];
                    append_sol_value_to_builder(field_builder, value)?;
                }
            }
            Err(e) => {
                trace!(
                    "failed to decode event '{}{}', filling with nulls. Error: {}",
                    event_name, body_tuple, e
                );
                for (j, ty) in body_types.iter().enumerate() {
                    let arrow_ty = sol_to_arrow_type(ty)?;
                    let field_builder = &mut builder.field_builders_mut()[j];
                    append_null_value_to_builder(field_builder, &arrow_ty)?;
                }
            }
        }
        builder.append(true);
    }

    let struct_array = builder.finish();
    let columns: Vec<Arc<dyn Array>> = (0..struct_array.num_columns())
        .map(|i| struct_array.column(i).clone())
        .collect();
    Ok(columns)
}

#[cfg(test)]
mod tests {
    use alloy::hex;
    use datafusion::arrow::array::FixedSizeBinaryBuilder;

    use super::*;

    #[test]
    fn vectorized_address_topic() {
        // Two addresses in topic format (left-padded to 32 bytes)
        let mut builder = FixedSizeBinaryBuilder::new(32);
        builder
            .append_value(hex!(
                "000000000000000000000000e43ca1dee3f0fc1e2df73a0745674545f11a59f5"
            ))
            .unwrap();
        builder
            .append_value(hex!(
                "00000000000000000000000010bff0723fa78a7c31260a9cbe7aa6ff470905d1"
            ))
            .unwrap();
        let col = builder.finish();

        let result = decode_topic_vectorized(&col, &DynSolType::Address).unwrap();
        let fsb = result
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();

        assert_eq!(fsb.value_length(), 20);
        assert_eq!(
            fsb.value(0),
            hex!("e43ca1dee3f0fc1e2df73a0745674545f11a59f5")
        );
        assert_eq!(
            fsb.value(1),
            hex!("10bff0723fa78a7c31260a9cbe7aa6ff470905d1")
        );
    }

    #[test]
    fn vectorized_bool_topic() {
        let mut builder = FixedSizeBinaryBuilder::new(32);
        builder
            .append_value(hex!(
                "0000000000000000000000000000000000000000000000000000000000000001"
            ))
            .unwrap();
        builder
            .append_value(hex!(
                "0000000000000000000000000000000000000000000000000000000000000000"
            ))
            .unwrap();
        let col = builder.finish();

        let result = decode_topic_vectorized(&col, &DynSolType::Bool).unwrap();
        let ba = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(ba.value(0));
        assert!(!ba.value(1));
    }

    #[test]
    fn vectorized_uint64_topic() {
        let mut builder = FixedSizeBinaryBuilder::new(32);
        builder
            .append_value(hex!(
                "000000000000000000000000000000000000000000000000000000003b9aca00"
            ))
            .unwrap();
        let col = builder.finish();

        let result = decode_topic_vectorized(&col, &DynSolType::Uint(64)).unwrap();
        let ua = result.as_any().downcast_ref::<UInt64Array>().unwrap();
        assert_eq!(ua.value(0), 1_000_000_000u64);
    }

    #[test]
    fn vectorized_null_topic() {
        let mut builder = FixedSizeBinaryBuilder::new(32);
        builder
            .append_value(hex!(
                "000000000000000000000000e43ca1dee3f0fc1e2df73a0745674545f11a59f5"
            ))
            .unwrap();
        builder.append_null();
        let col = builder.finish();

        let result = decode_topic_vectorized(&col, &DynSolType::Address).unwrap();
        let fsb = result
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert!(!fsb.is_null(0));
        assert!(fsb.is_null(1));
    }

    #[test]
    fn vectorized_empty_topic() {
        let mut builder = FixedSizeBinaryBuilder::new(32);
        let col = builder.finish();

        let result = decode_topic_vectorized(&col, &DynSolType::Address).unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn fixed_layout_body_uint256_as_string() {
        use datafusion::arrow::array::{BinaryBuilder, StringArray};

        let mut builder = BinaryBuilder::new();
        // uint256 value = 42
        builder.append_value(hex!(
            "000000000000000000000000000000000000000000000000000000000000002a"
        ));
        let data = builder.finish();

        let body_types = vec![DynSolType::Uint(256)];
        let columns = decode_body_fixed_layout(&data, &body_types).unwrap();

        assert_eq!(columns.len(), 1);
        let sa = columns[0].as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(sa.value(0), "42");
    }

    #[test]
    fn fixed_layout_body_multiple_fields() {
        use datafusion::arrow::array::BinaryBuilder;

        let mut builder = BinaryBuilder::new();
        // address (20 bytes, left-padded) + uint64
        builder.append_value(hex!(
            "000000000000000000000000e43ca1dee3f0fc1e2df73a0745674545f11a59f5\
             000000000000000000000000000000000000000000000000000000003b9aca00"
        ));
        let data = builder.finish();

        let body_types = vec![DynSolType::Address, DynSolType::Uint(64)];
        let columns = decode_body_fixed_layout(&data, &body_types).unwrap();

        assert_eq!(columns.len(), 2);
        let addr = columns[0]
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(
            addr.value(0),
            hex!("e43ca1dee3f0fc1e2df73a0745674545f11a59f5")
        );

        let val = columns[1].as_any().downcast_ref::<UInt64Array>().unwrap();
        assert_eq!(val.value(0), 1_000_000_000u64);
    }

    #[test]
    fn fixed_layout_body_null_data() {
        use datafusion::arrow::array::BinaryBuilder;

        let mut builder = BinaryBuilder::new();
        builder.append_null();
        let data = builder.finish();

        let body_types = vec![DynSolType::Uint(64)];
        let columns = decode_body_fixed_layout(&data, &body_types).unwrap();

        assert_eq!(columns.len(), 1);
        let ua = columns[0].as_any().downcast_ref::<UInt64Array>().unwrap();
        assert!(ua.is_null(0));
    }

    /// Run the old row-by-row decode path using Event::decode_topic/decode_body
    /// with a StructBuilder, exactly as the original `decode()` function did.
    fn decode_row_by_row(
        event: &Event,
        topic1: &Bytes32ArrayType,
        topic2: &Bytes32ArrayType,
        topic3: &Bytes32ArrayType,
        data: &BinaryArray,
    ) -> Arc<dyn Array> {
        use itertools::izip;

        let fields = event.fields().unwrap();
        let mut builder = StructBuilder::from_fields(fields, topic1.len());
        for (t1, t2, t3, d) in izip!(topic1, topic2, topic3, data) {
            event.decode_topic(&mut builder, 1, t1).unwrap();
            event.decode_topic(&mut builder, 2, t2).unwrap();
            event.decode_topic(&mut builder, 3, t3).unwrap();
            event.decode_body(&mut builder, d).unwrap();
            builder.append(true);
        }
        Arc::new(builder.finish())
    }

    /// Helper: assert every column of two StructArrays is identical.
    #[track_caller]
    fn assert_structs_eq(old: &Arc<dyn Array>, new: &Arc<dyn Array>) {
        let old_s = old.as_any().downcast_ref::<StructArray>().unwrap();
        let new_s = new.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(old_s.num_columns(), new_s.num_columns(), "column count");
        assert_eq!(old_s.len(), new_s.len(), "row count");
        for col in 0..old_s.num_columns() {
            assert_eq!(
                old_s.column(col).as_ref(),
                new_s.column(col).as_ref(),
                "mismatch in column {} ({})",
                col,
                old_s.fields()[col].name()
            );
        }
    }

    /// Compare vectorized vs row-by-row for Uniswap V3 Swap (10,000 rows).
    /// 2 address topics + 5 mixed-type body fields: int256, int256, uint160,
    /// uint128, int24. Covers Decimal256, Int32, address, and Utf8 string
    /// representation of large ints.
    #[test]
    fn vectorized_matches_row_by_row_swap_10k() {
        use alloy::{hex::FromHex as _, primitives::B256};
        use datafusion::{
            arrow::array::{BinaryBuilder, FixedSizeBinaryBuilder},
            scalar::ScalarValue,
        };

        const SIG: &str = "Swap(address indexed sender,address indexed recipient,int256 amount0,int256 amount1,uint160 sqrtPriceX96,uint128 liquidity,int24 tick)";
        const NUM_ROWS: usize = 10_000;

        #[rustfmt::skip]
        const CSV: [(&str, &str, &str); 5] = [
            ("0x000000000000000000000000e43ca1dee3f0fc1e2df73a0745674545f11a59f5",
             "0x00000000000000000000000010bff0723fa78a7c31260a9cbe7aa6ff470905d1",
             "0x000000000000000000000000000000000000000000000000000000003b9aca00\
              fffffffffffffffffffffffffffffffffffffffffffffffffbbe45052a28056300\
              0000000000000000000000000000000000446e1e52218e5b236f8735ca89440000\
              000000000000000000000000000000000000000000005ca05c4528d95aa2000000\
              000000000000000000000000000000000000000000000000000002fb65"),
            ("0x0000000000000000000000001111111254eeb25477b68fb85ed929f73a960582",
             "0x0000000000000000000000001111111254eeb25477b68fb85ed929f73a960582",
             "0x0000000000000000000000000000000000000000000000000000000095ed66b5\
              fffffffffffffffffffffffffffffffffffffffffffffffff54af50c6b89978100\
              0000000000000000000000000000000000446e00ba767226fd3681ec499dd50000\
              000000000000000000000000000000000000000000005ca05c4528d95aa2000000\
              000000000000000000000000000000000000000000000000000002fb65"),
            ("0x000000000000000000000000def1c0ded9bec7f1a1670819833240f027b25eff",
             "0x0000000000000000000000002bf39a1004ff433938a5f933a44b8dad377937f6",
             "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffff486d4238\
              0000000000000000000000000000000000000000000000000d1f893781823b9f00\
              0000000000000000000000000000000000446e24fadfd4b87fa4adb40e37820000\
              000000000000000000000000000000000000000000005ca05c4528d95aa2000000\
              000000000000000000000000000000000000000000000000000002fb65"),
            ("0x0000000000000000000000003fc91a3afd70395cd496c647d5a6cc9d4b2b7fad",
             "0x000000000000000000000000e1525583c72de1a3dada24f761007ba8a560e220",
             "0x000000000000000000000000000000000000000000000000000000011303cef5\
              ffffffffffffffffffffffffffffffffffffffffffffffffec5c0f7fe74d8dd700\
              0000000000000000000000000000000000446deeb2b3c9adc67ccca1b5a8b90000\
              000000000000000000000000000000000000000000005ca05c4528d95aa2000000\
              000000000000000000000000000000000000000000000000000002fb65"),
            ("0x000000000000000000000000767c8bb1574bee5d4fe35e27e0003c89d43c5121",
             "0x0000000000000000000000002d722c96f79d149dd21e9ef36f93fc12906ce9f8",
             "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffdd404a44ec\
              0000000000000000000000000000000000000000000000027c2bb0000000000000\
              00000000000000000000000000000000004474ca36a10f972d98019c42e0620000\
              000000000000000000000000000000000000000000005cbb7a5b248d7479000000\
              000000000000000000000000000000000000000000000000000002fb6d"),
        ];

        // Pre-parse the hex data once
        let parsed: Vec<_> = CSV
            .iter()
            .map(|(t1, t2, d)| {
                (
                    B256::from_hex(t1).unwrap(),
                    B256::from_hex(t2).unwrap(),
                    alloy::primitives::Bytes::from_hex(d).unwrap().to_vec(),
                )
            })
            .collect();

        let event = Event::try_from(&ScalarValue::new_utf8(SIG)).unwrap();

        let mut t1b = FixedSizeBinaryBuilder::with_capacity(NUM_ROWS, 32);
        let mut t2b = FixedSizeBinaryBuilder::with_capacity(NUM_ROWS, 32);
        let mut t3b = FixedSizeBinaryBuilder::with_capacity(NUM_ROWS, 32);
        let mut db = BinaryBuilder::with_capacity(NUM_ROWS, NUM_ROWS * 160);
        for i in 0..NUM_ROWS {
            let (t1, t2, d) = &parsed[i % parsed.len()];
            // Sprinkle nulls: every 97th topic1, every 131st data
            if i % 97 == 96 {
                t1b.append_null();
            } else {
                t1b.append_value(t1.as_slice()).unwrap();
            }
            t2b.append_value(t2.as_slice()).unwrap();
            t3b.append_null();
            if i % 131 == 130 {
                db.append_null();
            } else {
                db.append_value(d);
            }
        }
        let topic1 = t1b.finish();
        let topic2 = t2b.finish();
        let topic3 = t3b.finish();
        let data = db.finish();

        let old = decode_row_by_row(&event, &topic1, &topic2, &topic3, &data);
        let new = decode_columnar(&event, &topic1, &topic2, &topic3, &data).unwrap();
        assert_structs_eq(&old, &new);
    }

    /// Compare vectorized vs row-by-row for Transfer (10,000 rows).
    /// 2 address topics + 1 uint256 body → Utf8.
    #[test]
    fn vectorized_matches_row_by_row_transfer_10k() {
        use alloy::{hex::FromHex as _, primitives::B256};
        use datafusion::{
            arrow::array::{BinaryBuilder, FixedSizeBinaryBuilder},
            scalar::ScalarValue,
        };

        const SIG: &str = "Transfer(address indexed from, address indexed to, uint256 value)";
        const NUM_ROWS: usize = 10_000;

        let base: [(&str, &str, &str); 3] = [
            (
                "0x000000000000000000000000aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "0x000000000000000000000000bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "0x0000000000000000000000000000000000000000000000000000000000000064",
            ),
            (
                "0x000000000000000000000000cccccccccccccccccccccccccccccccccccccccc",
                "0x000000000000000000000000dddddddddddddddddddddddddddddddddddddddd",
                "0x00000000000000000000000000000000000000000000000000000000000003e8",
            ),
            (
                "0x000000000000000000000000e43ca1dee3f0fc1e2df73a0745674545f11a59f5",
                "0x00000000000000000000000010bff0723fa78a7c31260a9cbe7aa6ff470905d1",
                "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            ),
        ];

        let parsed: Vec<_> = base
            .iter()
            .map(|(t1, t2, d)| {
                (
                    B256::from_hex(t1).unwrap(),
                    B256::from_hex(t2).unwrap(),
                    alloy::primitives::Bytes::from_hex(d).unwrap().to_vec(),
                )
            })
            .collect();

        let event = Event::try_from(&ScalarValue::new_utf8(SIG)).unwrap();

        let mut t1b = FixedSizeBinaryBuilder::with_capacity(NUM_ROWS, 32);
        let mut t2b = FixedSizeBinaryBuilder::with_capacity(NUM_ROWS, 32);
        let mut t3b = FixedSizeBinaryBuilder::with_capacity(NUM_ROWS, 32);
        let mut db = BinaryBuilder::with_capacity(NUM_ROWS, NUM_ROWS * 32);
        for i in 0..NUM_ROWS {
            let (t1, t2, d) = &parsed[i % parsed.len()];
            t1b.append_value(t1.as_slice()).unwrap();
            if i % 83 == 82 {
                t2b.append_null();
            } else {
                t2b.append_value(t2.as_slice()).unwrap();
            }
            t3b.append_null();
            if i % 151 == 150 {
                db.append_null();
            } else {
                db.append_value(d);
            }
        }
        let topic1 = t1b.finish();
        let topic2 = t2b.finish();
        let topic3 = t3b.finish();
        let data = db.finish();

        let old = decode_row_by_row(&event, &topic1, &topic2, &topic3, &data);
        let new = decode_columnar(&event, &topic1, &topic2, &topic3, &data).unwrap();
        assert_structs_eq(&old, &new);
    }

    /// Compare vectorized vs row-by-row for indexed reference types (10,000 rows).
    /// string/bytes topics stored as keccak256 hash + uint256 body.
    #[test]
    fn vectorized_matches_row_by_row_indexed_reference_types_10k() {
        use datafusion::{
            arrow::array::{BinaryBuilder, FixedSizeBinaryBuilder},
            scalar::ScalarValue,
        };

        const SIG: &str = "DataStored(string indexed key, bytes indexed data, uint256 value)";
        const NUM_ROWS: usize = 10_000;

        let event = Event::try_from(&ScalarValue::new_utf8(SIG)).unwrap();

        let hashes: [[u8; 32]; 3] = [
            hex!("a2e0a8d4e5b3c2e1e5c8d0f2a1b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1"),
            hex!("b3f1c9e5d7a8f2b4c6e0d8f1a3b5c7e9d1f3a5b7c9e1f3a5b7c9e1f3a5b7c9e1"),
            hex!("1111111111111111111111111111111111111111111111111111111111111111"),
        ];
        let values: [[u8; 32]; 3] = [
            hex!("000000000000000000000000000000000000000000000000000000000000002a"),
            hex!("00000000000000000000000000000000000000000000000000000000deadbeef"),
            hex!("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
        ];

        let mut t1b = FixedSizeBinaryBuilder::with_capacity(NUM_ROWS, 32);
        let mut t2b = FixedSizeBinaryBuilder::with_capacity(NUM_ROWS, 32);
        let mut t3b = FixedSizeBinaryBuilder::with_capacity(NUM_ROWS, 32);
        let mut db = BinaryBuilder::with_capacity(NUM_ROWS, NUM_ROWS * 32);
        for i in 0..NUM_ROWS {
            t1b.append_value(hashes[i % hashes.len()]).unwrap();
            t2b.append_value(hashes[(i + 1) % hashes.len()]).unwrap();
            t3b.append_null();
            if i % 113 == 112 {
                db.append_null();
            } else {
                db.append_value(values[i % values.len()]);
            }
        }
        let topic1 = t1b.finish();
        let topic2 = t2b.finish();
        let topic3 = t3b.finish();
        let data = db.finish();

        let old = decode_row_by_row(&event, &topic1, &topic2, &topic3, &data);
        let new = decode_columnar(&event, &topic1, &topic2, &topic3, &data).unwrap();
        assert_structs_eq(&old, &new);
    }

    /// Event with a dynamic `string` body field — uses row-by-row fallback.
    #[test]
    fn vectorized_matches_row_by_row_string_body_10k() {
        use datafusion::{
            arrow::array::{BinaryBuilder, FixedSizeBinaryBuilder},
            scalar::ScalarValue,
        };

        // NameRegistered(address indexed owner, string name, uint256 cost)
        const SIG: &str = "NameRegistered(address indexed owner, string name, uint256 cost)";
        const NUM_ROWS: usize = 10_000;

        let event = Event::try_from(&ScalarValue::new_utf8(SIG)).unwrap();

        // ABI-encode body: string "hello" + uint256(42)
        // string is dynamic: offset(32 bytes) + uint256(32 bytes) + length(32 bytes) + data(32 bytes padded)
        let body_data = hex!(
            "0000000000000000000000000000000000000000000000000000000000000040" // offset to string
            "000000000000000000000000000000000000000000000000000000000000002a" // cost = 42
            "0000000000000000000000000000000000000000000000000000000000000005" // string length = 5
            "68656c6c6f000000000000000000000000000000000000000000000000000000" // "hello" padded
        );

        let owner = hex!("000000000000000000000000aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

        let mut t1b = FixedSizeBinaryBuilder::with_capacity(NUM_ROWS, 32);
        let mut t2b = FixedSizeBinaryBuilder::with_capacity(NUM_ROWS, 32);
        let mut t3b = FixedSizeBinaryBuilder::with_capacity(NUM_ROWS, 32);
        let mut db = BinaryBuilder::with_capacity(NUM_ROWS, NUM_ROWS * body_data.len());
        for i in 0..NUM_ROWS {
            t1b.append_value(owner).unwrap();
            t2b.append_null();
            t3b.append_null();
            if i % 200 == 199 {
                db.append_null();
            } else {
                db.append_value(body_data);
            }
        }
        let topic1 = t1b.finish();
        let topic2 = t2b.finish();
        let topic3 = t3b.finish();
        let data = db.finish();

        let old = decode_row_by_row(&event, &topic1, &topic2, &topic3, &data);
        let new = decode_columnar(&event, &topic1, &topic2, &topic3, &data).unwrap();
        assert_structs_eq(&old, &new);
    }

    /// Event with `bytes` body field — dynamic type, uses row-by-row fallback.
    #[test]
    fn vectorized_matches_row_by_row_bytes_body_10k() {
        use datafusion::{
            arrow::array::{BinaryBuilder, FixedSizeBinaryBuilder},
            scalar::ScalarValue,
        };

        // DataStored(address indexed sender, bytes payload)
        const SIG: &str = "DataStored(address indexed sender, bytes payload)";
        const NUM_ROWS: usize = 10_000;

        let event = Event::try_from(&ScalarValue::new_utf8(SIG)).unwrap();

        // ABI-encode body: bytes with 6 bytes of data
        let body_data = hex!(
            "0000000000000000000000000000000000000000000000000000000000000020" // offset to bytes
            "0000000000000000000000000000000000000000000000000000000000000006" // length = 6
            "deadbeef0102000000000000000000000000000000000000000000000000000000" // data padded
        );

        let sender = hex!("000000000000000000000000bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");

        let mut t1b = FixedSizeBinaryBuilder::with_capacity(NUM_ROWS, 32);
        let mut t2b = FixedSizeBinaryBuilder::with_capacity(NUM_ROWS, 32);
        let mut t3b = FixedSizeBinaryBuilder::with_capacity(NUM_ROWS, 32);
        let mut db = BinaryBuilder::with_capacity(NUM_ROWS, NUM_ROWS * body_data.len());
        for i in 0..NUM_ROWS {
            t1b.append_value(sender).unwrap();
            t2b.append_null();
            t3b.append_null();
            if i % 300 == 299 {
                db.append_null();
            } else {
                db.append_value(body_data);
            }
        }
        let topic1 = t1b.finish();
        let topic2 = t2b.finish();
        let topic3 = t3b.finish();
        let data = db.finish();

        let old = decode_row_by_row(&event, &topic1, &topic2, &topic3, &data);
        let new = decode_columnar(&event, &topic1, &topic2, &topic3, &data).unwrap();
        assert_structs_eq(&old, &new);
    }

    /// Event with mixed fixed and dynamic body fields.
    /// Tests that the fallback path handles a mix of types correctly.
    #[test]
    fn vectorized_matches_row_by_row_mixed_body_10k() {
        use datafusion::{
            arrow::array::{BinaryBuilder, FixedSizeBinaryBuilder},
            scalar::ScalarValue,
        };

        // OrderFilled(address indexed maker, uint256 amount, string memo, bool success)
        const SIG: &str =
            "OrderFilled(address indexed maker, uint256 amount, string memo, bool success)";
        const NUM_ROWS: usize = 10_000;

        let event = Event::try_from(&ScalarValue::new_utf8(SIG)).unwrap();

        // ABI-encode body: uint256(1000) + string offset + bool(true) + string "ok"
        // Layout: amount(32) + offset_to_memo(32) + success(32) + memo_length(32) + memo_data(32)
        let body_data = hex!(
            "00000000000000000000000000000000000000000000000000000000000003e8" // amount = 1000
            "0000000000000000000000000000000000000000000000000000000000000060" // offset to memo
            "0000000000000000000000000000000000000000000000000000000000000001" // success = true
            "0000000000000000000000000000000000000000000000000000000000000002" // memo length = 2
            "6f6b000000000000000000000000000000000000000000000000000000000000" // "ok" padded
        );

        let maker = hex!("000000000000000000000000cccccccccccccccccccccccccccccccccccccccc");

        let mut t1b = FixedSizeBinaryBuilder::with_capacity(NUM_ROWS, 32);
        let mut t2b = FixedSizeBinaryBuilder::with_capacity(NUM_ROWS, 32);
        let mut t3b = FixedSizeBinaryBuilder::with_capacity(NUM_ROWS, 32);
        let mut db = BinaryBuilder::with_capacity(NUM_ROWS, NUM_ROWS * body_data.len());
        for i in 0..NUM_ROWS {
            t1b.append_value(maker).unwrap();
            t2b.append_null();
            t3b.append_null();
            if i % 150 == 149 {
                db.append_null();
            } else {
                db.append_value(body_data);
            }
        }
        let topic1 = t1b.finish();
        let topic2 = t2b.finish();
        let topic3 = t3b.finish();
        let data = db.finish();

        let old = decode_row_by_row(&event, &topic1, &topic2, &topic3, &data);
        let new = decode_columnar(&event, &topic1, &topic2, &topic3, &data).unwrap();
        assert_structs_eq(&old, &new);
    }
}
