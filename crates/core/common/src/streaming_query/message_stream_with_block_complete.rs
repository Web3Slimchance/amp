use std::{
    pin::Pin,
    task::{Context, Poll},
};

use arrow::{array::RecordBatch, error::ArrowError};
use datasets_raw::arrow::DataType;
use futures::{Stream, ready};

use super::QueryMessage;
use crate::plan_visitors::WatermarkColumn;

/// A stream adapter that enriches a `QueryMessage` stream with `Watermark` messages.
///
/// This stream wraps a QueryMessage stream and:
/// - Validates that Data batches are ordered by the active watermark column
/// - Tracks the last watermark value emitted
/// - Splits batches on watermark boundaries and emits Watermark messages
/// - Clears state on MicrobatchEnd and reorgs
pub struct MessageStreamWithWatermark<S> {
    inner: S,
    watermark_column: WatermarkColumn,
    /// The last watermark value we've been emitting data for
    last_watermark_value: Option<u64>,
    /// Buffer for partial batch data that needs to be emitted
    pending_batch: Option<RecordBatch>,
    /// Watermark value to complete when emitting the pending batch
    pending_watermark: Option<u64>,
}

/// Error type for [`MessageStreamWithWatermark`] stream items.
pub type MessageStreamError = Box<dyn std::error::Error + Sync + Send + 'static>;

impl<S> MessageStreamWithWatermark<S>
where
    S: Stream<Item = Result<QueryMessage, MessageStreamError>>,
{
    pub fn new(inner: S, watermark_column: WatermarkColumn) -> Self {
        Self {
            inner,
            watermark_column,
            last_watermark_value: None,
            pending_batch: None,
            pending_watermark: None,
        }
    }
}

impl<S> Stream for MessageStreamWithWatermark<S>
where
    S: Stream<Item = Result<QueryMessage, MessageStreamError>> + Unpin,
{
    type Item = Result<QueryMessage, MessageStreamError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        // First, emit any pending watermark message, then pending batch
        if let Some(watermark) = this.pending_watermark.take() {
            return Poll::Ready(Some(Ok(QueryMessage::Watermark(watermark))));
        }

        if let Some(batch) = this.pending_batch.take() {
            return Poll::Ready(Some(Ok(QueryMessage::Data(batch))));
        }

        // Poll the inner stream
        let message = match ready!(Pin::new(&mut this.inner).poll_next(cx)) {
            Some(Ok(message)) => message,
            Some(Err(e)) => return Poll::Ready(Some(Err(e))),
            None => return Poll::Ready(None),
        };

        match message {
            QueryMessage::MicrobatchStart { range, is_reorg } => {
                // Clear state on reorgs
                if is_reorg {
                    this.last_watermark_value = None;
                }
                Poll::Ready(Some(Ok(QueryMessage::MicrobatchStart { range, is_reorg })))
            }
            QueryMessage::Data(batch) => {
                match process_data_batch(batch, this.last_watermark_value, this.watermark_column) {
                    Ok(BatchProcessResult::PassThrough(batch)) => {
                        Poll::Ready(Some(Ok(QueryMessage::Data(batch))))
                    }
                    Ok(BatchProcessResult::Split {
                        current_batch,
                        completed_watermark,
                        next_batch,
                        next_watermark,
                    }) => {
                        this.last_watermark_value = Some(next_watermark);
                        this.pending_batch = Some(next_batch);

                        match current_batch {
                            None => {
                                Poll::Ready(Some(Ok(QueryMessage::Watermark(completed_watermark))))
                            }
                            Some(batch) => {
                                this.pending_watermark = Some(completed_watermark);
                                Poll::Ready(Some(Ok(QueryMessage::Data(batch))))
                            }
                        }
                    }
                    Ok(BatchProcessResult::UpdateWatermark(batch, watermark)) => {
                        this.last_watermark_value = Some(watermark);
                        Poll::Ready(Some(Ok(QueryMessage::Data(batch))))
                    }
                    Err(e) => Poll::Ready(Some(Err(e.into()))),
                }
            }
            QueryMessage::MicrobatchEnd(range) => {
                // Clear state on microbatch end
                this.last_watermark_value = None;
                Poll::Ready(Some(Ok(QueryMessage::MicrobatchEnd(range))))
            }
            QueryMessage::Watermark(block_num) => Poll::Ready(Some(Err(format!(
                "Unexpected Watermark message from inner stream: {}",
                block_num
            )
            .into()))),
        }
    }
}

enum BatchProcessResult {
    /// Pass the batch through unchanged
    PassThrough(RecordBatch),
    /// Update the tracked watermark value and pass through
    UpdateWatermark(RecordBatch, u64),
    /// Split the batch on watermark boundary
    Split {
        current_batch: Option<RecordBatch>,
        completed_watermark: u64,
        next_batch: RecordBatch,
        next_watermark: u64,
    },
}

fn process_data_batch(
    batch: RecordBatch,
    last_watermark_value: Option<u64>,
    watermark_column: WatermarkColumn,
) -> Result<BatchProcessResult, ProcessDataBatchError> {
    let watermark_array = batch.column_by_name(watermark_column.column_name());

    let Some(watermark_array) = watermark_array else {
        // No watermark column, pass through unchanged
        return Ok(BatchProcessResult::PassThrough(batch));
    };

    // Extract watermark values from the array
    let watermarks = extract_watermark_values(watermark_array)
        .map_err(ProcessDataBatchError::ExtractWatermarkValues)?;

    if watermarks.is_empty() {
        return Ok(BatchProcessResult::PassThrough(batch));
    }

    // Always validate ordering
    validate_watermark_ordering(&watermarks)
        .map_err(ProcessDataBatchError::ValidateWatermarkOrdering)?;

    let first = watermarks[0];
    let last = watermarks[watermarks.len() - 1];

    match last_watermark_value {
        None => {
            // First batch, just track the watermark (no splitting on first batch)
            Ok(BatchProcessResult::UpdateWatermark(batch, last))
        }
        Some(current) => {
            if first == current {
                if last == current {
                    // All rows have the same watermark as before
                    Ok(BatchProcessResult::PassThrough(batch))
                } else {
                    // Batch spans multiple watermark values, need to split
                    let split_index = watermarks.iter().position(|&n| n > current).unwrap(); // Safe: we know last > current from condition above
                    let (current_batch, next_batch) = split_record_batch(&batch, split_index)
                        .map_err(ProcessDataBatchError::SplitRecordBatch)?;

                    Ok(BatchProcessResult::Split {
                        current_batch: Some(current_batch),
                        completed_watermark: current,
                        next_batch,
                        next_watermark: last,
                    })
                }
            } else if first > current {
                // New watermark value started - emit entire batch as-is
                Ok(BatchProcessResult::Split {
                    current_batch: None,
                    completed_watermark: current,
                    next_batch: batch,
                    next_watermark: last,
                })
            } else {
                // Watermark went backwards - this can happen during reorgs
                // Since we clear state on reorgs, this should be handled naturally
                Ok(BatchProcessResult::UpdateWatermark(batch, last))
            }
        }
    }
}

/// Errors that occur when processing a data batch for watermark alignment.
#[derive(Debug, thiserror::Error)]
pub enum ProcessDataBatchError {
    #[error("Failed to extract watermark values")]
    ExtractWatermarkValues(#[source] ExtractWatermarkValuesError),

    #[error("Failed to validate watermark ordering")]
    ValidateWatermarkOrdering(#[source] ValidateWatermarkOrderingError),

    #[error("Failed to split record batch")]
    SplitRecordBatch(#[source] SplitRecordBatchError),
}

fn extract_watermark_values(
    array: &dyn arrow::array::Array,
) -> Result<Vec<u64>, ExtractWatermarkValuesError> {
    use arrow::{array::*, datatypes::DataType};

    match array.data_type() {
        DataType::UInt64 => {
            let uint64_array = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or(ExtractWatermarkValuesError::Downcast)?;
            uint64_array
                .iter()
                .collect::<Option<Vec<_>>>()
                .ok_or(ExtractWatermarkValuesError::NullValues)
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _) => {
            let ts_array = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or(ExtractWatermarkValuesError::Downcast)?;
            ts_array
                .iter()
                .map(|v| {
                    v.map(|nanos| nanos as u64)
                        .ok_or(ExtractWatermarkValuesError::NullValues)
                })
                .collect()
        }
        _ => Err(ExtractWatermarkValuesError::UnsupportedType(
            array.data_type().clone(),
        )),
    }
}

/// Errors that occur when extracting watermark values from an array.
#[derive(Debug, thiserror::Error)]
pub enum ExtractWatermarkValuesError {
    #[error("Failed to downcast watermark array")]
    Downcast,

    #[error("Found null values in watermark column")]
    NullValues,

    #[error("Unsupported watermark column type: {0}")]
    UnsupportedType(DataType),
}

fn validate_watermark_ordering(values: &[u64]) -> Result<(), ValidateWatermarkOrderingError> {
    for window in values.windows(2) {
        if window[0] > window[1] {
            return Err(ValidateWatermarkOrderingError {
                previous: window[0],
                current: window[1],
            });
        }
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
#[error("Watermark values not ordered: {previous} > {current}")]
pub struct ValidateWatermarkOrderingError {
    previous: u64,
    current: u64,
}

fn split_record_batch(
    batch: &RecordBatch,
    split_index: usize,
) -> Result<(RecordBatch, RecordBatch), SplitRecordBatchError> {
    if split_index == 0 {
        return Err(SplitRecordBatchError::SplitIndexZero);
    }

    if split_index >= batch.num_rows() {
        return Err(SplitRecordBatchError::SplitIndexOutOfBounds(split_index));
    }

    let first_batch_arrays: Vec<_> = batch
        .columns()
        .iter()
        .map(|array| array.slice(0, split_index))
        .collect();

    let second_batch_arrays: Vec<_> = batch
        .columns()
        .iter()
        .map(|array| array.slice(split_index, batch.num_rows() - split_index))
        .collect();

    let first_batch = RecordBatch::try_new(batch.schema(), first_batch_arrays)
        .map_err(SplitRecordBatchError::CreateRecordBatch)?;
    let second_batch = RecordBatch::try_new(batch.schema(), second_batch_arrays)
        .map_err(SplitRecordBatchError::CreateRecordBatch)?;

    Ok((first_batch, second_batch))
}

/// Errors that occur when splitting a record batch at a given index
///
/// This error type is used by `split_record_batch()`.
#[derive(Debug, thiserror::Error)]
pub enum SplitRecordBatchError {
    /// Split index is 0, cannot split
    ///
    /// This occurs when the split index is 0, cannot split.
    #[error("Split index is 0, cannot split")]
    SplitIndexZero,

    /// Split index is out of bounds
    ///
    /// This occurs when the split index is out of bounds.
    #[error("Split index is out of bounds: {0}")]
    SplitIndexOutOfBounds(usize),

    /// Failed to create RecordBatch
    ///
    /// This occurs when the RecordBatch cannot be created.
    #[error("Failed to create RecordBatch")]
    CreateRecordBatch(#[source] ArrowError),
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::UInt64Array,
        datatypes::{DataType, Field, Schema},
    };
    use datasets_common::block_num::RESERVED_BLOCK_NUM_COLUMN_NAME;
    use futures::{StreamExt as _, stream};

    use super::*;
    use crate::BlockRange;

    fn create_test_batch(block_nums: Vec<u64>, data: Vec<u64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(RESERVED_BLOCK_NUM_COLUMN_NAME, DataType::UInt64, false),
            Field::new("data", DataType::UInt64, false),
        ]));

        let block_array = Arc::new(UInt64Array::from(block_nums));
        let data_array = Arc::new(UInt64Array::from(data));

        RecordBatch::try_new(schema, vec![block_array, data_array]).unwrap()
    }

    fn create_test_range(start: u64, end: u64) -> BlockRange {
        BlockRange {
            numbers: start..=end,
            network: "test".parse().expect("valid network id"),
            hash: [0u8; 32].into(),
            prev_hash: [0u8; 32].into(),
            timestamp: None,
        }
    }

    async fn collect_messages(
        messages: Vec<Result<QueryMessage, MessageStreamError>>,
    ) -> Vec<Result<QueryMessage, MessageStreamError>> {
        let input_stream = stream::iter(messages);
        let mut aligned_stream =
            MessageStreamWithWatermark::new(input_stream, WatermarkColumn::BlockNum);

        let mut results = Vec::new();
        while let Some(msg) = aligned_stream.next().await {
            results.push(msg);
        }
        results
    }

    fn expect_data_blocks(msg: &Result<QueryMessage, MessageStreamError>) -> Vec<u64> {
        if let Ok(QueryMessage::Data(batch)) = msg {
            extract_watermark_values(
                batch
                    .column_by_name(RESERVED_BLOCK_NUM_COLUMN_NAME)
                    .unwrap(),
            )
            .unwrap()
        } else {
            panic!("Expected Data message");
        }
    }

    fn microbatch(start: u64, end: u64) -> QueryMessage {
        QueryMessage::MicrobatchStart {
            range: create_test_range(start, end),
            is_reorg: false,
        }
    }

    fn microbatch_reorg(start: u64, end: u64) -> QueryMessage {
        QueryMessage::MicrobatchStart {
            range: create_test_range(start, end),
            is_reorg: true,
        }
    }

    #[tokio::test]
    async fn test_single_block_batch() {
        let messages = vec![
            Ok(microbatch(100, 100)),
            Ok(QueryMessage::Data(create_test_batch(
                vec![100, 100, 100],
                vec![1, 2, 3],
            ))),
            Ok(QueryMessage::MicrobatchEnd(create_test_range(100, 100))),
        ];

        let results = collect_messages(messages).await;
        assert_eq!(results.len(), 3); // No Watermark for single block
    }

    #[tokio::test]
    async fn test_split_at_block_boundary() {
        let messages = vec![
            Ok(microbatch(100, 101)),
            Ok(QueryMessage::Data(create_test_batch(
                vec![100, 100],
                vec![1, 2],
            ))),
            Ok(QueryMessage::Data(create_test_batch(
                vec![100, 101, 101],
                vec![3, 4, 5],
            ))), // spans blocks
            Ok(QueryMessage::MicrobatchEnd(create_test_range(100, 101))),
        ];

        let results = collect_messages(messages).await;
        assert_eq!(results.len(), 6);
        assert_eq!(expect_data_blocks(&results[2]), vec![100]); // Split portion
        matches!(results[3], Ok(QueryMessage::Watermark(100)));
        assert_eq!(expect_data_blocks(&results[4]), vec![101, 101]); // Remainder
    }

    #[tokio::test]
    async fn test_new_block_transition_no_current_data() {
        let messages = vec![
            Ok(microbatch(100, 101)),
            Ok(QueryMessage::Data(create_test_batch(
                vec![100, 100],
                vec![1, 2],
            ))),
            Ok(QueryMessage::Data(create_test_batch(
                vec![101, 101],
                vec![3, 4],
            ))), // new block
            Ok(QueryMessage::MicrobatchEnd(create_test_range(100, 101))),
        ];

        let results = collect_messages(messages).await;
        assert_eq!(results.len(), 5);
        matches!(results[2], Ok(QueryMessage::Watermark(100))); // No empty batch emitted
    }

    #[tokio::test]
    async fn test_reorg_clears_state() {
        let messages = vec![
            Ok(microbatch(100, 100)),
            Ok(QueryMessage::Data(create_test_batch(
                vec![100, 100],
                vec![1, 2],
            ))),
            Ok(microbatch_reorg(99, 99)),
            Ok(QueryMessage::Data(create_test_batch(
                vec![99, 99],
                vec![3, 4],
            ))), // goes backwards
            Ok(QueryMessage::MicrobatchEnd(create_test_range(99, 99))),
        ];

        let results = collect_messages(messages).await;
        assert_eq!(results.len(), 5); // No Watermark during reorg
    }

    #[tokio::test]
    async fn test_three_blocks_in_batch() {
        let messages = vec![
            Ok(microbatch(100, 102)),
            Ok(QueryMessage::Data(create_test_batch(vec![100], vec![1]))),
            Ok(QueryMessage::Data(create_test_batch(
                vec![100, 101, 102, 102],
                vec![2, 3, 4, 5],
            ))), // 3 blocks
            Ok(QueryMessage::MicrobatchEnd(create_test_range(100, 102))),
        ];

        let results = collect_messages(messages).await;
        assert_eq!(results.len(), 6);
        assert_eq!(expect_data_blocks(&results[2]), vec![100]); // Split portion
        matches!(results[3], Ok(QueryMessage::Watermark(100)));
        assert_eq!(expect_data_blocks(&results[4]), vec![101, 102, 102]); // Remainder (not split again)
    }

    #[tokio::test]
    async fn test_ordering_validation() {
        let messages = vec![
            Ok(microbatch(99, 101)),
            Ok(QueryMessage::Data(create_test_batch(
                vec![100, 99, 101],
                vec![1, 2, 3],
            ))), // unordered
        ];

        let results = collect_messages(messages).await;
        assert_eq!(results.len(), 2);
        assert!(results[1].is_err()); // Error due to bad ordering
    }

    #[tokio::test]
    async fn test_no_block_num_column() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "data",
            DataType::UInt64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(UInt64Array::from(vec![1, 2, 3]))]).unwrap();

        let messages = vec![
            Ok(microbatch(100, 100)),
            Ok(QueryMessage::Data(batch)),
            Ok(QueryMessage::MicrobatchEnd(create_test_range(100, 100))),
        ];

        let results = collect_messages(messages).await;
        assert_eq!(results.len(), 3); // Should pass through unchanged
    }

    #[tokio::test]
    async fn test_unexpected_block_complete_from_inner() {
        let messages = vec![Ok(QueryMessage::Watermark(100))]; // Unexpected from inner

        let results = collect_messages(messages).await;
        assert_eq!(results.len(), 1);
        assert!(results[0].is_err()); // Should be an error
    }

    #[tokio::test]
    async fn test_same_block_passthrough() {
        let messages = vec![
            Ok(microbatch(100, 100)),
            Ok(QueryMessage::Data(create_test_batch(
                vec![100, 100],
                vec![1, 2],
            ))), // sets last_block_num = 100
            Ok(QueryMessage::Data(create_test_batch(
                vec![100, 100],
                vec![3, 4],
            ))), // same block, should pass through
            Ok(QueryMessage::MicrobatchEnd(create_test_range(100, 100))),
        ];

        let results = collect_messages(messages).await;
        assert_eq!(results.len(), 4); // No Watermark, just passthrough
        assert_eq!(expect_data_blocks(&results[1]), vec![100, 100]);
        assert_eq!(expect_data_blocks(&results[2]), vec![100, 100]); // Passed through unchanged
    }

    #[tokio::test]
    async fn test_block_number_goes_backwards() {
        let messages = vec![
            Ok(microbatch(100, 100)),
            Ok(QueryMessage::Data(create_test_batch(
                vec![100, 100],
                vec![1, 2],
            ))), // sets last_block_num = 100
            Ok(QueryMessage::Data(create_test_batch(
                vec![99, 99],
                vec![3, 4],
            ))), // goes backwards (not during reorg)
            Ok(QueryMessage::MicrobatchEnd(create_test_range(99, 100))),
        ];

        let results = collect_messages(messages).await;
        assert_eq!(results.len(), 4); // Should handle backwards gracefully (UpdateWatermark)
        assert_eq!(expect_data_blocks(&results[2]), vec![99, 99]); // Backwards batch passed through
    }
}
