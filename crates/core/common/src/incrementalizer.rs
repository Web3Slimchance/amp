use std::sync::Arc;

use datafusion::{
    catalog::TableProvider,
    common::{
        DFSchemaRef, JoinType,
        tree_node::{Transformed, TreeNode as _, TreeNodeRecursion, TreeNodeRewriter},
    },
    datasource::{TableType, source_as_provider},
    error::DataFusionError,
    logical_expr::{
        EmptyRelation, Join as JoinStruct, LogicalPlan, TableScan, Union as UnionStruct,
    },
    prelude::{Expr, col},
    sql::TableReference,
};
use thiserror::Error;
use tracing::instrument;

use crate::{
    catalog::physical::snapshot::QueryableSnapshot,
    plan_visitors::{NonIncrementalOp, WatermarkColumn},
    udfs::block_num::is_block_num_udf,
};

/// Whether to match the pre-propagation `block_num()` UDF only, or also the
/// post-propagation column reference form.
///
/// - `Udf`: only `block_num()` / `col("block_num()")` — for validating raw user queries.
/// - `Propagated`: also `col("_block_num")` (or `col("_ts")`, depending on the active
///   watermark) — for the incrementalizer which runs after the propagator has replaced
///   the UDF with the watermark column.
#[derive(Clone, Copy)]
pub enum WatermarkForm {
    Udf,
    Propagated(WatermarkColumn),
}

impl WatermarkForm {
    pub fn matches(self, expr: &Expr) -> bool {
        match self {
            WatermarkForm::Udf => is_block_num_udf(expr),
            WatermarkForm::Propagated(wm) => {
                matches!(expr, Expr::Column(c) if c.name == wm.column_name())
            }
        }
    }
}

/// Assuming that output table has been synced up to `start - 1`, and that the input tables are immutable (all of our tables currently are), this will return the _incremental version_ of `plan` that computes the microbatch `[start, end]`.
///
/// The supported incremental operators are _linear operators_ (projection, filter, union) and inner joins.
///
/// Algorithm:
/// - Consider the path from a table scan to the root of the query plan. If there are only linear operators in that path, that table scan will be filtered by `start <= _block_num <= end`.
/// - If there are inner joins, then consider the first join in the path. The table is either on the left (L) or right (R) side of the join. The join will be rewritten according to the inner join update rule:
///
/// Δ(L⋈R)=(ΔL⋈R[t−1​])∪(L[t−1]​⋈ΔR)∪(ΔL⋈ΔR)
///
/// Where ΔT is is `T where start <= _block_num <= end`, and T[t−1​] is `T where _block_num < start`.
///
/// ## Special cases
/// - If a join as an `on` condition that is an inequality on `_block_num`, e.g. `l._block_num <= r._block_num`, then Δ(L⋈R)=(L[t−1]​⋈ΔR)∪(ΔL⋈ΔR), a term is optimized away.
///   This is because the inequality can be relaxed to `start <= r_block_num`. More generally, if the `on` clause can be relaxed by a lower start bound, we can push it down and potentially eliminate a term. This is not yet implemented.
/// - If a join has a `l._block_num = r._block_num` condition, then Δ(L⋈R)=ΔL⋈ΔR. These joins may stack with each other and with linear operators, or on top of output of general joins.
///
/// ## Further reading
/// - The inner join update formula is well-known and commonly implemented in incremental view maintenance systems. For one academic reference see:
/// - https://sigmodrecord.org/publications/sigmodRecord/2403/pdfs/20_dbsp-budiu.pdf
pub fn incrementalize_plan(
    plan: LogicalPlan,
    start: Expr,
    end: Expr,
    watermark: WatermarkColumn,
) -> Result<LogicalPlan, DataFusionError> {
    let mut incrementalizer = Incrementalizer::new(start, end, watermark);
    let plan = plan.rewrite(&mut incrementalizer)?.data;
    Ok(plan)
}

/// The range to scan, relative to a given `start` and `end` for the current microbatch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RelationRange {
    Delta,   // The range [start, end]
    History, // The range (, start)
}

/// The incrementalizer is essentially a pushdown rewriter that also applies the inner join update rule.
#[derive(Debug, Clone)]
struct Incrementalizer {
    /// Literal expression for the start of the microbatch range.
    start: Expr,
    /// Literal expression for the end of the microbatch range.
    end: Expr,

    /// Which watermark column to filter on (e.g. `_block_num` or `_ts`).
    watermark: WatermarkColumn,

    // Rewriter state: The range we are currently pushing down
    curr_range: RelationRange,
}

impl Incrementalizer {
    fn new(start: Expr, end: Expr, watermark: WatermarkColumn) -> Self {
        Self {
            start,
            end,
            watermark,
            curr_range: RelationRange::Delta,
        }
    }

    fn with_range(&self, range: RelationRange) -> Self {
        Self {
            curr_range: range,
            ..self.clone()
        }
    }
}

impl TreeNodeRewriter for Incrementalizer {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>, DataFusionError> {
        use LogicalPlan::*;
        use RelationRange::*;

        // The incrementalizer must run after watermark propagation, so `WatermarkForm::Propagated`
        match incremental_op_kind(&node, WatermarkForm::Propagated(self.watermark))
            .map_err(|e| DataFusionError::External(e.into()))?
        {
            IncrementalOpKind::Linear | IncrementalOpKind::WatermarkEqJoin => {
                // Linear ops and _block_num equality joins just push the current range
                // to both children via normal tree recursion. For WatermarkEqJoin this works
                // because _block_num equality guarantees temporal alignment:
                // Δ(L⋈R) = ΔL⋈ΔR and History(L⋈R) = History(L)⋈History(R).
                Ok(Transformed::no(node))
            }
            IncrementalOpKind::InnerJoin => {
                let LogicalPlan::Join(join) = node else {
                    unreachable!("IncrementalOpKind::InnerJoin only returned for Join nodes")
                };

                if self.curr_range == History {
                    return Err(DataFusionError::External(
                        "stacked joins not supported".into(),
                    ));
                }

                // Apply the inner join update rule: Δ(L⋈R) = (ΔL⋈R[t−1]) ∪ (L[t−1]⋈ΔR) ∪ (ΔL⋈ΔR)
                let left = join.left.as_ref();
                let right = join.right.as_ref();

                // Term 1: ΔL⋈R[t−1]
                let delta_left = left.clone().rewrite(&mut self.with_range(Delta))?.data;
                let history_right = right.clone().rewrite(&mut self.with_range(History))?.data;
                let term1 = create_join(delta_left, history_right, &join)?;

                // Term 2: L[t−1]⋈ΔR
                let history_left = left.clone().rewrite(&mut self.with_range(History))?.data;
                let delta_right = right.clone().rewrite(&mut self.with_range(Delta))?.data;
                let term2 = create_join(history_left, delta_right, &join)?;

                // Term 3: ΔL⋈ΔR
                let delta_left2 = left.clone().rewrite(&mut self.with_range(Delta))?.data;
                let delta_right2 = right.clone().rewrite(&mut self.with_range(Delta))?.data;
                let term3 = create_join(delta_left2, delta_right2, &join)?;

                // Create 3-way union
                // Use use a literal because constructors would wipe qualifiers
                let union = LogicalPlan::Union(UnionStruct {
                    schema: term1.schema().clone(),
                    inputs: vec![Arc::new(term1), Arc::new(term2), Arc::new(term3)],
                });

                // Jump prevents automatic recursion into union children (we've already rewritten them)
                Ok(Transformed::new(union, true, TreeNodeRecursion::Jump))
            }
            IncrementalOpKind::Table => match node {
                TableScan(table_scan) => {
                    let Ok(table_provider) = source_as_provider(&table_scan.source) else {
                        return Err(DataFusionError::External(
                            "TableSource was not DefaultTableSource".into(),
                        ));
                    };

                    validate_table_provider(table_provider.as_ref())?;

                    let constrained = constrain_by_range(
                        table_scan,
                        &self.start,
                        &self.end,
                        self.curr_range,
                        self.watermark,
                    )?;
                    Ok(Transformed::yes(TableScan(constrained)))
                }

                // A static value has always existed and is never updated.
                // Therefore, it has only history and no delta.
                Values(_) => match self.curr_range {
                    History => Ok(Transformed::no(node)),
                    Delta => Ok(Transformed::yes(EmptyRelation(empty_relation(
                        node.schema().clone(),
                    )))),
                },
                EmptyRelation(_) => Ok(Transformed::no(node)),

                // Panic: We only return `IncrementalOpKind::Table` for the above variants.
                _ => unreachable!("unhandled table node: {:?}", node),
            },
        }
    }

    fn f_up(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>, DataFusionError> {
        Ok(Transformed::no(node))
    }
}

/// Validates that a table provider is suitable for incremental scanning.
fn validate_table_provider(table_provider: &dyn TableProvider) -> Result<(), DataFusionError> {
    if !table_provider.as_any().is::<QueryableSnapshot>() {
        return Err(DataFusionError::External(
            format!(
                "Unsupported table provider in incremental scan: {:?}",
                table_provider
            )
            .into(),
        ));
    }

    Ok(())
}

/// Adds a watermark range filter to the table scan.
///
/// For a Delta range: `WHERE start <= <watermark> AND <watermark> <= end`
/// For a History range: `WHERE <watermark> < start`
///
/// This assumes that the watermark column has already been propagated and is therefore
/// present in the schema of `plan`.
#[instrument(skip_all, err)]
fn constrain_by_range(
    mut table_scan: TableScan,
    start: &Expr,
    end: &Expr,
    range: RelationRange,
    watermark: WatermarkColumn,
) -> Result<TableScan, ConstrainByRangeError> {
    if table_scan.source.table_type() != TableType::Base
        || table_scan.source.get_logical_plan().is_some()
    {
        // These should not exist in a streamable and optimized plan.
        return Err(ConstrainByRangeError(
            table_scan.table_name,
            table_scan.source.table_type(),
        ));
    }

    let wm_col = col(watermark.column_name());
    let predicate = match range {
        RelationRange::Delta => start
            .clone()
            .lt_eq(wm_col.clone())
            .and(wm_col.lt_eq(end.clone())),
        RelationRange::History => wm_col.lt(start.clone()),
    };

    table_scan.filters.push(predicate);
    Ok(table_scan)
}

/// Error when constraining a table scan by range
///
/// This occurs when attempting to constrain a table scan that is not a base table.
/// Base tables are required for range-based filtering to work correctly.
#[derive(Debug, thiserror::Error)]
#[error("non-base table found: {0:?}")]
struct ConstrainByRangeError(TableReference, TableType);

impl From<ConstrainByRangeError> for DataFusionError {
    fn from(e: ConstrainByRangeError) -> Self {
        DataFusionError::External(e.into())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum NonIncrementalQueryError {
    /// The plan node is invalid for any kind of query
    #[error("invalid operation: {0}")]
    Invalid(String),
    /// The plan node cannot be materialized incrementally due to the given operation
    #[error("query contains non-incremental operation: {0}")]
    NonIncremental(NonIncrementalOp),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncrementalOpKind {
    Linear,
    InnerJoin,
    /// An inner join with an equi-join condition on the watermark column (e.g.
    /// `l._block_num = r._block_num`). Acts like a linear operator: the range
    /// filter can be pushed to both children.
    WatermarkEqJoin,
    Table,
}

pub fn incremental_op_kind(
    node: &LogicalPlan,
    form: WatermarkForm,
) -> Result<IncrementalOpKind, NonIncrementalQueryError> {
    use IncrementalOpKind::*;
    use LogicalPlan::*;
    use NonIncrementalQueryError::*;

    match node {
        // Entirely unsupported operations.
        Dml(_) | Ddl(_) | Statement(_) | Copy(_) | Extension(_) => {
            Err(NonIncrementalQueryError::Invalid(node.to_string()))
        }

        // Stateless operators
        Projection(_) | Filter(_) | Union(_) | Unnest(_) | Repartition(_) | Subquery(_)
        | SubqueryAlias(_) | DescribeTable(_) | Explain(_) | Analyze(_) => Ok(Linear),

        // Tables
        TableScan(_) | Values(_) | EmptyRelation(_) => Ok(IncrementalOpKind::Table),

        // Joins
        Join(join) => match join.join_type {
            JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi => {
                let has_wm_eq = match form {
                    WatermarkForm::Udf => {
                        // Pre-propagation: check for _block_num (the only watermark
                        // column that existed in the original schema).
                        has_watermark_eq_condition(&join.on, WatermarkColumn::BlockNum)
                    }
                    WatermarkForm::Propagated(wm) => has_watermark_eq_condition(&join.on, wm),
                };
                if has_wm_eq {
                    Ok(WatermarkEqJoin)
                } else {
                    Ok(InnerJoin)
                }
            }

            // Outer and anti joins are not incremental
            JoinType::Left
            | JoinType::Right
            | JoinType::Full
            | JoinType::LeftAnti
            | JoinType::RightAnti
            | JoinType::LeftMark
            | JoinType::RightMark => Err(NonIncremental(NonIncrementalOp::Join(join.join_type))),
        },

        // Operations that are supported only in batch queries
        Limit(_) => Err(NonIncremental(NonIncrementalOp::Limit)),

        Aggregate(agg) => {
            // An aggregate is incrementally valid when _block_num is the first group-by key.
            // This also handles DISTINCT ON (_block_num, ...) which DataFusion's optimizer
            // rewrites to an Aggregate with _block_num as the first group-by key.
            let first_group_ok = agg.group_expr.first().is_some_and(|e| form.matches(e));
            if first_group_ok {
                Ok(Linear)
            } else {
                Err(NonIncremental(NonIncrementalOp::Aggregate))
            }
        }
        Distinct(distinct) => {
            use datafusion::logical_expr::Distinct as DistinctEnum;
            match distinct {
                // SELECT DISTINCT ON (_block_num, ...) is locally incrementalizable:
                // since on_expr starts with _block_num and microbatches never overlap in
                // _block_num values, deduplication can be applied per microbatch.
                // Also accepts block_num() UDF which will be replaced during propagation.
                DistinctEnum::On(on) => {
                    let first_on_ok = on.on_expr.first().is_some_and(|e| form.matches(e));
                    if first_on_ok {
                        Ok(Linear)
                    } else {
                        Err(NonIncremental(NonIncrementalOp::Distinct))
                    }
                }
                DistinctEnum::All(_) => Err(NonIncremental(NonIncrementalOp::Distinct)),
            }
        }
        Sort(_) => Err(NonIncremental(NonIncrementalOp::Sort)),
        Window(_) => Err(NonIncremental(NonIncrementalOp::Window)),
        RecursiveQuery(_) => Err(NonIncremental(NonIncrementalOp::RecursiveQuery)),
    }
}

/// Returns true if any equi-join condition pair equates the watermark column on both sides.
fn has_watermark_eq_condition(on: &[(Expr, Expr)], watermark: WatermarkColumn) -> bool {
    let col_name = watermark.column_name();
    on.iter().any(|(l, r)| {
        matches!(l, Expr::Column(c) if c.name == col_name)
            && matches!(r, Expr::Column(c) if c.name == col_name)
    })
}

fn empty_relation(schema: DFSchemaRef) -> EmptyRelation {
    EmptyRelation {
        produce_one_row: false,
        schema,
    }
}

/// Helper function to recreate a join with new children but preserving the original join configuration.
fn create_join(
    left: LogicalPlan,
    right: LogicalPlan,
    original: &JoinStruct,
) -> Result<LogicalPlan, DataFusionError> {
    JoinStruct::try_new(
        Arc::new(left),
        Arc::new(right),
        original.on.clone(),
        original.filter.clone(),
        original.join_type,
        original.join_constraint,
        original.null_equality,
    )
    .map(LogicalPlan::Join)
}
