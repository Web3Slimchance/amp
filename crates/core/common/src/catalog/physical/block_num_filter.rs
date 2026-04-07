//! `_block_num` filter analysis for segment-level pruning.
//!
//! Uses DataFusion's `ExprSimplifier` with interval guarantees to determine
//! whether a predicate can possibly be satisfied by a given block range.

use std::sync::Arc;

use datafusion::{
    common::{DFSchema, ScalarValue, tree_node::TreeNode},
    logical_expr::{
        interval_arithmetic::{Interval, NullableInterval},
        simplify::SimplifyContext,
    },
    optimizer::simplify_expressions::ExprSimplifier,
    prelude::Expr,
};
use datasets_common::block_num::RESERVED_BLOCK_NUM_COLUMN_NAME;

/// Returns `true` if `expr` references the `_block_num` column anywhere.
///
/// Used by `supports_filters_pushdown` to decide which filters to mark
/// `Inexact` so they reach `scan()`. Any filter mentioning `_block_num` is
/// worth pushing down — the simplifier will conservatively keep it if it
/// can't prove unsatisfiability.
pub fn references_block_num(expr: &Expr) -> bool {
    let mut found = false;
    let _ = expr.apply(|node| {
        if matches!(node, Expr::Column(c) if c.name == RESERVED_BLOCK_NUM_COLUMN_NAME) {
            found = true;
            Ok(datafusion::common::tree_node::TreeNodeRecursion::Stop)
        } else {
            Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
        }
    });
    found
}

/// Returns `false` only when the filters are **provably unsatisfiable** for a
/// segment covering the block range `[start, end]`.
///
/// A `true` return does **not** guarantee matching rows exist — it means the
/// simplifier could not rule the segment out. Callers must tolerate false
/// positives (segments kept unnecessarily) but will never see false negatives
/// (segments pruned that contained matching rows).
///
/// Internally, this feeds the segment's `_block_num` range as a
/// [`NullableInterval`] guarantee into DataFusion's [`ExprSimplifier`]. If any
/// filter simplifies to the literal `false`, the segment is provably empty.
/// Filters the simplifier cannot fully evaluate (e.g. arithmetic expressions,
/// references to other columns) are conservatively treated as satisfiable.
pub fn filters_maybe_satisfiable_for_range(
    filters: &[Expr],
    schema: &Arc<DFSchema>,
    start: u64,
    end: u64,
) -> bool {
    if filters.is_empty() {
        return true;
    }

    let interval = NullableInterval::NotNull {
        values: match Interval::try_new(
            ScalarValue::UInt64(Some(start)),
            ScalarValue::UInt64(Some(end)),
        ) {
            Ok(iv) => iv,
            Err(_) => return true, // can't build interval → don't prune
        },
    };

    let guarantees = vec![(
        Expr::Column(RESERVED_BLOCK_NUM_COLUMN_NAME.into()),
        interval,
    )];

    let context = SimplifyContext::default().with_schema(Arc::clone(schema));
    let simplifier = ExprSimplifier::new(context).with_guarantees(guarantees);

    for filter in filters {
        if let Ok(Expr::Literal(ScalarValue::Boolean(Some(false)), _)) =
            simplifier.simplify(filter.clone())
        {
            return false;
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::{
        common::{DFSchema, ToDFSchema as _},
        prelude::{col, lit},
    };

    use super::*;

    fn test_schema() -> Arc<DFSchema> {
        Arc::new(
            Schema::new(vec![
                Field::new("_block_num", DataType::UInt64, false),
                Field::new("gas_used", DataType::UInt64, false),
            ])
            .to_dfschema()
            .unwrap(),
        )
    }

    /// Mirrors the real pipeline: only filters that pass `references_block_num`
    /// (i.e. that `supports_filters_pushdown` would mark `Inexact`) reach the
    /// simplifier.
    fn sat(filters: &[Expr], start: u64, end: u64) -> bool {
        let pushed: Vec<Expr> = filters
            .iter()
            .filter(|f| references_block_num(f))
            .cloned()
            .collect();
        filters_maybe_satisfiable_for_range(&pushed, &test_schema(), start, end)
    }

    // -----------------------------------------------------------------------
    // references_block_num — gate tests
    // -----------------------------------------------------------------------

    #[test]
    fn gate_accepts_simple_comparison() {
        assert!(references_block_num(&col("_block_num").gt(lit(100u64))));
    }

    #[test]
    fn gate_accepts_conjunction() {
        let expr = col("_block_num")
            .gt_eq(lit(10u64))
            .and(col("_block_num").lt(lit(20u64)));
        assert!(references_block_num(&expr));
    }

    #[test]
    fn gate_accepts_not() {
        assert!(references_block_num(&Expr::Not(Box::new(
            col("_block_num").gt(lit(100u64))
        ))));
    }

    #[test]
    fn gate_accepts_arithmetic() {
        assert!(references_block_num(
            &(col("_block_num") + lit(10u64)).gt(lit(100u64))
        ));
    }

    #[test]
    fn gate_accepts_or() {
        let expr = col("_block_num")
            .gt(lit(10u64))
            .or(col("_block_num").lt(lit(5u64)));
        assert!(references_block_num(&expr));
    }

    #[test]
    fn gate_accepts_mixed_conjunction() {
        // _block_num > 10 AND gas_used < 100 — references _block_num
        let expr = col("_block_num")
            .gt(lit(10u64))
            .and(col("gas_used").lt(lit(100u64)));
        assert!(references_block_num(&expr));
    }

    #[test]
    fn gate_rejects_non_block_num() {
        assert!(!references_block_num(&col("gas_used").gt(lit(100u64))));
    }

    // -----------------------------------------------------------------------
    // Full pipeline: gate + simplifier — basic comparisons
    // -----------------------------------------------------------------------

    #[test]
    fn gt_prunes_below() {
        assert!(!sat(&[col("_block_num").gt(lit(100u64))], 0, 50));
    }

    #[test]
    fn gt_keeps_overlap() {
        assert!(sat(&[col("_block_num").gt(lit(100u64))], 0, 150));
    }

    #[test]
    fn gt_keeps_above() {
        assert!(sat(&[col("_block_num").gt(lit(100u64))], 200, 300));
    }

    // -----------------------------------------------------------------------
    // range conjunctions
    // -----------------------------------------------------------------------

    #[test]
    fn range_keeps_overlap() {
        let filters = vec![
            col("_block_num")
                .gt_eq(lit(10u64))
                .and(col("_block_num").lt_eq(lit(20u64))),
        ];
        assert!(sat(&filters, 15, 25));
    }

    #[test]
    fn range_prunes_disjoint() {
        let filters = vec![
            col("_block_num")
                .gt_eq(lit(10u64))
                .and(col("_block_num").lt_eq(lit(20u64))),
        ];
        assert!(!sat(&filters, 25, 35));
    }

    // -----------------------------------------------------------------------
    // equality
    // -----------------------------------------------------------------------

    #[test]
    fn eq_keeps_containing_range() {
        assert!(sat(&[col("_block_num").eq(lit(42u64))], 40, 50));
    }

    #[test]
    fn eq_prunes_non_containing_range() {
        assert!(!sat(&[col("_block_num").eq(lit(42u64))], 50, 60));
    }

    // -----------------------------------------------------------------------
    // NOT / negation — gate now accepts, simplifier evaluates
    // -----------------------------------------------------------------------

    #[test]
    fn not_prunes_when_inner_is_always_true() {
        // NOT (_block_num > 100), segment [200, 300]
        // inner is always true → NOT is always false → prune
        assert!(!sat(
            &[Expr::Not(Box::new(col("_block_num").gt(lit(100u64))))],
            200,
            300
        ));
    }

    #[test]
    fn not_keeps_when_inner_is_always_false() {
        // NOT (_block_num > 100), segment [0, 50]
        // inner is always false → NOT is always true → keep
        assert!(sat(
            &[Expr::Not(Box::new(col("_block_num").gt(lit(100u64))))],
            0,
            50
        ));
    }

    #[test]
    fn not_keeps_partial_overlap() {
        assert!(sat(
            &[Expr::Not(Box::new(col("_block_num").gt(lit(100u64))))],
            50,
            150
        ));
    }

    // -----------------------------------------------------------------------
    // arithmetic — gate accepts, simplifier is conservative
    // -----------------------------------------------------------------------

    #[test]
    fn arithmetic_conservative_no_prune() {
        // _block_num + 10 > 100 with [0, 50] — could prune but DF v52 can't
        // prove it. Conservative: keep the segment.
        assert!(sat(
            &[(col("_block_num") + lit(10u64)).gt(lit(100u64))],
            0,
            50
        ));
    }

    #[test]
    fn arithmetic_keeps_overlap() {
        assert!(sat(
            &[(col("_block_num") + lit(10u64)).gt(lit(100u64))],
            85,
            120
        ));
    }

    // -----------------------------------------------------------------------
    // non-block-num filters (gate rejects → no pruning)
    // -----------------------------------------------------------------------

    #[test]
    fn non_block_num_filter_never_prunes() {
        assert!(sat(&[col("gas_used").gt(lit(0u64))], 0, 50));
    }

    #[test]
    fn empty_filters_never_prunes() {
        assert!(sat(&[], 0, 50));
    }

    // -----------------------------------------------------------------------
    // mixed filters — gate keeps block_num refs, drops the rest
    // -----------------------------------------------------------------------

    #[test]
    fn mixed_filters_prune_on_block_num() {
        // _block_num > 100 passes gate, gas_used > 0 is dropped by gate.
        // The surviving _block_num filter prunes [0, 50].
        let filters = vec![
            col("_block_num").gt(lit(100u64)),
            col("gas_used").gt(lit(0u64)),
        ];
        assert!(!sat(&filters, 0, 50));
    }
}
