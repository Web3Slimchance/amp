//! System column constants shared across all dataset types.

use crate::block_num::RESERVED_BLOCK_NUM_COLUMN_NAME;

/// Reserved column name for the block timestamp in dataset tables.
pub const RESERVED_TS_COLUMN_NAME: &str = "_ts";

/// All reserved system column names, in the order they appear in output schemas.
pub const WATERMARK_COLUMN_NAMES: &[&str] =
    &[RESERVED_BLOCK_NUM_COLUMN_NAME, RESERVED_TS_COLUMN_NAME];
