use std::{sync::Arc, time::Duration};

use datafusion::parquet::file::properties::WriterProperties as ParquetWriterProperties;

use super::{
    collector::CollectorProperties,
    compaction::{
        algorithm::SegmentSizeLimit, compactor::CompactorProperties, config::ParquetConfig,
    },
};

/// Parquet writing and compaction configuration resolved from [`ParquetConfig`].
#[derive(Debug, Clone)]
pub struct WriterProperties {
    /// Low-level Parquet encoding settings (compression, bloom filters, etc.).
    pub parquet: ParquetWriterProperties,
    /// Compaction algorithm and scheduling configuration.
    pub compactor: CompactorProperties,
    /// Garbage-collection scheduling configuration.
    pub collector: CollectorProperties,
    /// Target segment size limit that triggers file splits.
    pub partition: SegmentSizeLimit,
    /// Maximum in-memory cache size in bytes for Parquet footer metadata.
    pub cache_size_mb: usize,
    /// Maximum uncompressed size of a single row group before flushing.
    pub max_row_group_bytes: usize,
    /// Wall-clock duration after which an open segment is flushed.
    pub segment_flush_interval: Duration,
}

pub fn parquet_opts(config: impl Into<ParquetConfig>) -> Arc<WriterProperties> {
    let config = config.into();
    // We have not done our own benchmarking, but the default 1_000_000 value for this adds about a
    // megabyte of storage per column, per row group. This analysis by InfluxData suggests that
    // smaller NDV values may be equally effective:
    // https://www.influxdata.com/blog/using-parquets-bloom-filters/
    let bloom_filter_ndv = 10_000;

    // For DataFusion defaults, see `ParquetOptions` here:
    // https://github.com/apache/arrow-datafusion/blob/main/datafusion/common/src/config.rs
    //
    // Note: We could set `sorting_columns` for columns like `block_num` and `ordinal`. However,
    // Datafusion doesn't actually read that metadata info anywhere and just reiles on the
    // `file_sort_order` set on the reader configuration.
    let parquet = ParquetWriterProperties::builder()
        .set_compression(config.compression)
        .set_bloom_filter_ndv(bloom_filter_ndv)
        .set_bloom_filter_enabled(config.bloom_filters)
        .build();

    let collector = CollectorProperties::from(&config);
    let compactor = CompactorProperties::from(&config);
    let partition = SegmentSizeLimit::from(&config.target_size);
    let cache_size_mb = (config.cache_size_mb * 1024 * 1024) as usize;
    let max_row_group_bytes = (config.max_row_group_mb * 1024 * 1024) as usize;

    let segment_flush_interval = config.segment_flush_interval_secs.clone().into();

    WriterProperties {
        parquet,
        compactor,
        collector,
        partition,
        cache_size_mb,
        max_row_group_bytes,
        segment_flush_interval,
    }
    .into()
}
