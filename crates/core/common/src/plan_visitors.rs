use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
};

use datafusion::{
    arrow::datatypes::{DataType, SchemaRef, TimeUnit},
    common::{
        Column, JoinType, ScalarValue, plan_err,
        tree_node::{Transformed, TreeNode as _, TreeNodeRecursion, TreeNodeRewriter},
    },
    error::DataFusionError,
    functions::core::expr_fn::greatest,
    functions_aggregate::first_last::first_value,
    logical_expr::{
        Aggregate as AggregateStruct, Join as JoinStruct, LogicalPlan, LogicalPlanBuilder, Sort,
        SubqueryAlias as SubqueryAliasStruct, Union as UnionStruct,
    },
    physical_plan::ExecutionPlan,
    prelude::{Expr, col, lit},
    sql::{TableReference, utils::UNNEST_PLACEHOLDER},
};
use datasets_common::{
    block_num::RESERVED_BLOCK_NUM_COLUMN_NAME, network_id::NetworkId,
    watermark_columns::RESERVED_TS_COLUMN_NAME,
};

use crate::{
    incrementalizer::{
        BlockNumForm, IncrementalOpKind, NonIncrementalQueryError, incremental_op_kind,
    },
    udfs::{
        block_num::{BLOCK_NUM_UDF_SCHEMA_NAME, is_block_num_udf},
        ts::{TS_UDF_SCHEMA_NAME, is_ts_udf},
    },
};

// ── Watermark column definition ──────────────────────────────────────────

/// Old EVM datasets have `timestamp` instead of `_ts`. During propagation and
/// schema inference, `timestamp` is accepted as an equivalent of `_ts`.
///
/// This should be removed entirely once all raw datasets have been migrated to include `_ts`.
const TS_COMPAT_COLUMN_NAME: &str = "timestamp";

/// A watermark column that gets propagated through the query plan.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum WatermarkColumn {
    BlockNum,
    Ts,
}

impl WatermarkColumn {
    /// All watermark column variants.
    pub const ALL: &[Self] = &[Self::BlockNum, Self::Ts];

    /// Returns the watermark columns present in every one of the given schemas.
    pub fn supported_by_all(source_schemas: impl IntoIterator<Item = SchemaRef>) -> Vec<Self> {
        let schemas: Vec<_> = source_schemas.into_iter().collect();
        Self::ALL
            .iter()
            .filter(|wm| schemas.iter().all(|s| wm.is_present_in(s)))
            .copied()
            .collect()
    }

    /// Checks if this watermark column (or an equivalent) is present in the schema.
    ///
    /// For `_ts`, a `timestamp` column with `Timestamp(Nanosecond, "+00:00")`
    /// is accepted as an equivalent — old EVM datasets have `timestamp` but
    /// not `_ts`.
    fn is_present_in(&self, schema: &datafusion::arrow::datatypes::Schema) -> bool {
        use datafusion::common::DFSchema;

        let has_field = |name: &str| schema.fields().iter().any(|f| f.name() == name);
        let has_field_with_type = |name: &str, dt: &DataType| {
            schema.fields().iter().any(|f| {
                f.name() == name && DFSchema::datatype_is_logically_equal(f.data_type(), dt)
            })
        };
        match self {
            Self::BlockNum => has_field(RESERVED_BLOCK_NUM_COLUMN_NAME),
            Self::Ts => {
                has_field(RESERVED_TS_COLUMN_NAME)
                    || has_field_with_type(TS_COMPAT_COLUMN_NAME, &self.data_type())
            }
        }
    }

    /// Reserved column name, e.g. `"_block_num"` or `"_ts"`.
    pub fn column_name(&self) -> &'static str {
        match self {
            Self::BlockNum => RESERVED_BLOCK_NUM_COLUMN_NAME,
            Self::Ts => RESERVED_TS_COLUMN_NAME,
        }
    }

    /// Arrow data type of the column.
    fn data_type(&self) -> DataType {
        match self {
            Self::BlockNum => DataType::UInt64,
            Self::Ts => DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
        }
    }

    /// Default literal for `EmptyRelation` / `Values` nodes.
    fn default_literal(&self) -> Expr {
        match self {
            Self::BlockNum => lit(0u64),
            Self::Ts => lit(ScalarValue::TimestampNanosecond(
                Some(0),
                Some("+00:00".into()),
            )),
        }
    }

    /// The schema name DataFusion assigns to a bare UDF call, e.g. `"block_num()"`.
    fn udf_schema_name(&self) -> &'static str {
        match self {
            Self::BlockNum => BLOCK_NUM_UDF_SCHEMA_NAME,
            Self::Ts => TS_UDF_SCHEMA_NAME,
        }
    }

    /// Construct an Arrow [`Field`](datafusion::arrow::datatypes::Field) for this column.
    fn field(&self) -> datafusion::arrow::datatypes::Field {
        datafusion::arrow::datatypes::Field::new(self.column_name(), self.data_type(), false)
    }

    /// Resolve this watermark column in a table schema, returning the expression
    /// to use. For `_ts`, falls back to `col("timestamp").alias("_ts")` when
    /// `_ts` is absent but `timestamp` is present (old EVM datasets).
    fn resolve_in_schema(
        &self,
        schema: &datafusion::arrow::datatypes::SchemaRef,
        table_name: &datafusion::sql::TableReference,
    ) -> Result<Expr, DataFusionError> {
        let name = self.column_name();
        if schema.fields().iter().any(|f| f.name() == name) {
            return Ok(col(name));
        }
        if matches!(self, Self::Ts)
            && schema
                .fields()
                .iter()
                .any(|f| f.name() == TS_COMPAT_COLUMN_NAME)
        {
            return Ok(col(TS_COMPAT_COLUMN_NAME)
                .alias_qualified(Some(table_name.clone()), RESERVED_TS_COLUMN_NAME));
        }
        Err(df_err(format!(
            "Table {table_name} is missing column {name}"
        )))
    }

    /// Returns `true` if `expr` is the sentinel UDF for this watermark column.
    fn is_udf(&self, expr: &Expr) -> bool {
        match self {
            Self::BlockNum => is_block_num_udf(expr),
            Self::Ts => is_ts_udf(expr),
        }
    }
}

/// Aliases with a name starting with `_` are always forbidden, since underscore-prefixed
/// names are reserved for special columns.
pub fn forbid_underscore_prefixed_aliases(plan: &LogicalPlan) -> Result<(), DataFusionError> {
    plan.apply(|node| {
        node.apply_expressions(|expr| {
            expr.apply(|e| {
                if let Expr::Alias(alias) = e
                    && alias.name.starts_with('_')
                // DF built-in we want to allow
                    && !alias.name.starts_with(UNNEST_PLACEHOLDER)
                {
                    return plan_err!(
                        "expression contains a column alias starting with '_': '{}'. \
                         Underscore-prefixed names are reserved. Please rename your column",
                        alias.name
                    );
                }
                Ok(TreeNodeRecursion::Continue)
            })
        })?;
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(())
}

/// Ensures that there are no duplicate field names in the plan's schema.
/// This includes fields that are qualified with different table names.
/// For example, `table1.column` and `table2.column` would be considered duplicates
/// because they both refer to `column`.
pub fn forbid_duplicate_field_names(
    physical_plan: &Arc<dyn ExecutionPlan>,
    logical_plan: &LogicalPlan,
) -> Result<(), DataFusionError> {
    let schema = physical_plan.schema();
    let mut duplicates: Vec<Vec<Column>> = Vec::new();
    let mut seen = BTreeSet::new();
    for field in schema.fields() {
        let name = field.name();
        if !seen.insert(name.as_str()) {
            let sources = logical_plan.schema().columns_with_unqualified_name(name);
            duplicates.push(sources);
        }
    }

    if !duplicates.is_empty() {
        return plan_err!(
            "Duplicate field names detected in plan schema: [{}]. Please alias your columns to be unique.",
            duplicates
                .into_iter()
                .map(|cols| {
                    cols.into_iter()
                        .map(|c| c.to_string())
                        .collect::<Vec<_>>()
                        .join(" and ")
                })
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    Ok(())
}

/// Computes `greatest(left.<column>, right.<column>)` for a join.
fn greatest_col_for_join(join: &JoinStruct, column_name: &str) -> Result<Expr, DataFusionError> {
    let left_schema = join.left.schema();
    let right_schema = join.right.schema();

    let left_col = left_schema
        .iter()
        .find(|(_, field)| field.name() == column_name)
        .map(|(qualifier, _)| col(Column::new(qualifier.cloned(), column_name)))
        .ok_or_else(|| df_err(format!("Left side of join missing {column_name}")))?;

    let right_col = right_schema
        .iter()
        .find(|(_, field)| field.name() == column_name)
        .map(|(qualifier, _)| col(Column::new(qualifier.cloned(), column_name)))
        .ok_or_else(|| df_err(format!("Right side of join missing {column_name}")))?;

    Ok(greatest(vec![left_col, right_col]))
}

// ── Watermark column propagator ──────────────────────────────────────────

/// Rewriter that propagates a single watermark column through the logical plan.
///
/// Parameterized by [`WatermarkColumn`] so the same logic handles both
/// `_block_num` and `_ts` without duplication.
struct WatermarkColumnPropagator {
    column: WatermarkColumn,
    /// The expression being bubbled up to be applied in the next projection as:
    /// `<expr> as <column_name>`.
    next_expr: Option<Expr>,
}

impl WatermarkColumnPropagator {
    fn new(column: WatermarkColumn) -> Self {
        Self {
            column,
            next_expr: None,
        }
    }

    fn col(&self) -> Expr {
        col(self.column.column_name())
    }
}

impl TreeNodeRewriter for WatermarkColumnPropagator {
    type Node = LogicalPlan;

    fn f_up(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>, DataFusionError> {
        use LogicalPlan::*;

        let column_name = self.column.column_name();
        let is_udf = |e: &Expr| self.column.is_udf(e);

        // Step 1: Replace the sentinel UDF in all expressions of this node using
        // the currently accumulated expression.
        //
        // `next_expr` is `None` at initialization, leaf nodes set it unconditionally.
        // The unwrap_or fallback is only reached for leaf nodes themselves (where
        // the UDF cannot appear, so replacement is a no-op) and post-Aggregate
        // nodes where we skip propagation.
        let col_expr = {
            let raw = self.next_expr.clone().unwrap_or_else(|| self.col());
            if raw != self.col() {
                raw.alias(column_name)
            } else {
                raw
            }
        };
        // Output-column positions (Projection.expr, DistinctOn.select_expr) use
        // `replace_udf_select`: when the UDF IS the entire expression it gets
        // aliased as udf_schema_name (preserving the user's output column
        // name); when the UDF is nested inside a larger expression the alias
        // would be swallowed by the outer expression anyway, so the plain
        // unaliased replacement is used there.
        //
        // All other positions (sort keys, Aggregate group keys, Filter …) always
        // use the unaliased replacement via `replace_udf`.
        use datafusion::logical_expr::Distinct as DistinctKind;
        let udf_schema_name = self.column.udf_schema_name();
        let select_replacement = col_expr.clone().alias(udf_schema_name);
        let replace_udf = |e: Expr, repl: &Expr| {
            e.transform(|e| {
                if is_udf(&e) {
                    Ok(Transformed::yes(repl.clone()))
                } else {
                    Ok(Transformed::no(e))
                }
            })
        };
        let replace_udf_select = |e: Expr| -> Result<Transformed<Expr>, DataFusionError> {
            if is_udf(&e) {
                Ok(Transformed::yes(select_replacement.clone()))
            } else {
                replace_udf(e, &col_expr)
            }
        };
        let (was_replaced, node) = match node {
            Distinct(DistinctKind::On(mut on)) => {
                // on_expr / sort_expr are sort keys — unaliased.
                // select_expr is named output — top-level-aware alias.
                let mut changed = false;
                for e in std::mem::take(&mut on.on_expr) {
                    let t = replace_udf(e, &col_expr)?;
                    changed |= t.transformed;
                    on.on_expr.push(t.data);
                }
                for e in std::mem::take(&mut on.select_expr) {
                    let t = replace_udf_select(e)?;
                    changed |= t.transformed;
                    on.select_expr.push(t.data);
                }
                if let Some(sort_exprs) = on.sort_expr.take() {
                    let mut new_sort = Vec::with_capacity(sort_exprs.len());
                    for mut s in sort_exprs {
                        let t = replace_udf(s.expr, &col_expr)?;
                        changed |= t.transformed;
                        s.expr = t.data;
                        new_sort.push(s);
                    }
                    on.sort_expr = Some(new_sort);
                }
                let node = if changed {
                    Distinct(DistinctKind::On(on)).recompute_schema()?
                } else {
                    Distinct(DistinctKind::On(on))
                };
                (changed, node)
            }
            node => {
                let is_projection = matches!(node, Projection(_));
                let r = node.map_expressions(|e| {
                    if is_projection {
                        replace_udf_select(e)
                    } else {
                        replace_udf(e, &col_expr)
                    }
                })?;
                let was = r.transformed;
                let node = if was {
                    r.data.recompute_schema()?
                } else {
                    r.data
                };
                (was, node)
            }
        };

        // Step 2: Handle actual propagation of the watermark column value.
        match node {
            Projection(mut projection) => {
                // Check against bare watermark column references when projecting over joins.
                let input_qualifiers: BTreeSet<&TableReference> = projection
                    .input
                    .schema()
                    .iter()
                    .filter_map(|(q, _)| q)
                    .collect();
                if input_qualifiers.len() > 1 {
                    for expr in projection.expr.iter() {
                        if matches!(expr, Expr::Column(c) if c.name == column_name) {
                            return plan_err!(
                                "selecting `{}` from a multi-table context (e.g. a join) is ambiguous. \
                                 Use the `{}` function instead to get the correct value, \
                                 or use explicit column names instead of `*` to omit `{}`",
                                column_name,
                                udf_schema_name,
                                column_name
                            );
                        }
                    }
                }

                // Consume next_expr: reset to col() so parent nodes see a simple
                // column reference from this projection's output.
                self.next_expr = Some(self.col());

                // In the trivial single-table case (`col_expr` is just
                // `col("<column_name>")`), skip auto-prepend when the projection
                // already contains the column.  For joins the expr is non-trivial
                // (e.g. `greatest(...)`) so we always prepend and let DF report
                // any ambiguity.
                if col_expr == self.col()
                    && projection
                        .expr
                        .iter()
                        .any(|e| matches!(e, Expr::Column(c) if c.name == column_name))
                {
                    return Ok(Transformed::new_transformed(
                        LogicalPlan::Projection(projection),
                        was_replaced,
                    ));
                }

                // Auto-prepend the watermark column expression.
                projection.expr.insert(0, col_expr);
                projection.schema = prepend_watermark_field(&projection.schema, &self.column);
                Ok(Transformed::yes(LogicalPlan::Projection(projection)))
            }

            // Rebuild union schemas to match their child projections
            Union(union) => {
                // Sanity check
                if self.next_expr != Some(self.col()) {
                    return Err(df_err(format!(
                        "unexpected `next_expr` for {}: {:?}",
                        column_name, self.next_expr
                    )));
                }

                Ok(Transformed::yes(Union(UnionStruct::try_new(union.inputs)?)))
            }

            Join(ref join) => {
                self.next_expr = Some(greatest_col_for_join(join, column_name)?);
                Ok(Transformed::new_transformed(node, was_replaced))
            }

            TableScan(ref scan) => {
                // We run this before optimizations, so we can assume the projection to be empty
                if scan.projection.is_some() {
                    return Err(df_err(format!("Scan should not have projection: {scan:?}")));
                }
                let resolved = self
                    .column
                    .resolve_in_schema(&scan.source.schema(), &scan.table_name)?;

                // If the column was resolved via a compat alias (e.g. timestamp→_ts),
                // wrap the scan in a Projection so the alias is visible to parent
                // nodes (especially Joins which inspect child schemas directly).
                if resolved != self.col() {
                    let mut exprs: Vec<Expr> = node.schema().iter().map(Expr::from).collect();
                    exprs.push(resolved.clone());
                    let projected = LogicalPlanBuilder::from(node).project(exprs)?.build()?;
                    self.next_expr = Some(self.col());
                    Ok(Transformed::yes(projected))
                } else {
                    self.next_expr = Some(self.col());
                    Ok(Transformed::new_transformed(node, was_replaced))
                }
            }

            // Constants are formally produced "before the first watermark", but
            // using watermark 0 should be ok in practice.
            EmptyRelation(_) | Values(_) => {
                self.next_expr = Some(self.column.default_literal());
                Ok(Transformed::new_transformed(node, was_replaced))
            }

            // SubqueryAlias caches its schema — we need to rebuild it to reflect
            // schema changes in its input.
            SubqueryAlias(subquery_alias) => {
                let rebuilt =
                    SubqueryAliasStruct::try_new(subquery_alias.input, subquery_alias.alias)?;
                Ok(Transformed::yes(LogicalPlan::SubqueryAlias(rebuilt)))
            }

            // These nodes do not cache schema and are not leaves. UDF calls in their
            // expressions (e.g. `WHERE block_num() > 100`) are handled by the
            // replacement above.
            Filter(_) | Repartition(_) | Subquery(_) | Explain(_) | Analyze(_)
            | DescribeTable(_) | Unnest(_) => Ok(Transformed::new_transformed(node, was_replaced)),

            // DISTINCT ON has a cached schema field that must be rebuilt to include
            // the watermark column after propagation.
            Distinct(distinct) => {
                match distinct {
                    DistinctKind::On(mut on) => {
                        // Consume next_expr.
                        self.next_expr = Some(self.col());

                        // In the trivial single-table case, skip auto-prepend when
                        // the column is already in the select list.
                        if col_expr == self.col()
                            && on
                                .select_expr
                                .iter()
                                .any(|e| matches!(e, Expr::Column(c) if c.name == column_name))
                        {
                            return Ok(Transformed::new_transformed(
                                LogicalPlan::Distinct(DistinctKind::On(on)),
                                was_replaced,
                            ));
                        }

                        // Prepend watermark column to select_expr and rebuild the cached schema.
                        on.select_expr.insert(0, col_expr);
                        on.schema = prepend_watermark_field(&on.schema, &self.column);
                        Ok(Transformed::yes(LogicalPlan::Distinct(DistinctKind::On(
                            on,
                        ))))
                    }
                    // Distinct All is transparent.
                    all @ DistinctKind::All(_) => Ok(Transformed::new_transformed(
                        LogicalPlan::Distinct(all),
                        was_replaced,
                    )),
                }
            }

            // GROUP BY <system_col>, ... — if the column is already a group key it
            // appears naturally in the aggregate's output schema.
            // If it is NOT a group key (e.g. `_ts` when the user only grouped by
            // `_block_num`), add `first_value(<col>)` as an aggregate expression
            // so the column survives past the aggregate.
            Aggregate(agg) => {
                let in_output = agg.schema.fields().iter().any(|f| f.name() == column_name);
                if in_output {
                    self.next_expr = Some(self.col());
                    Ok(Transformed::new_transformed(Aggregate(agg), was_replaced))
                } else {
                    // The column is not a group key. Add first_value(<col>) AS <col>
                    // to the aggregate so it appears in the output.
                    let first_val = first_value(col_expr.clone(), vec![]).alias(column_name);
                    let mut aggr_expr = agg.aggr_expr.to_vec();
                    aggr_expr.push(first_val);
                    let new_agg =
                        AggregateStruct::try_new(agg.input, agg.group_expr.to_vec(), aggr_expr)?;
                    self.next_expr = Some(self.col());
                    Ok(Transformed::yes(Aggregate(new_agg)))
                }
            }

            // These nodes are transparent: the watermark column passes through unchanged.
            Sort(_) | Limit(_) => Ok(Transformed::new_transformed(node, was_replaced)),

            // Unsupported for watermark column propagation.
            Window(_) | RecursiveQuery(_) | Statement(_) | Dml(_) | Ddl(_) | Copy(_)
            | Extension(_) => {
                plan_err!(
                    "{}() is not supported in this query",
                    self.column.udf_schema_name().trim_end_matches("()")
                )
            }
        }
    }
}

// ── Public propagation API ───────────────────────────────────────────────

/// Propagate the given watermark columns through the logical plan.
pub fn propagate_watermark_columns(
    plan: LogicalPlan,
    columns: &[WatermarkColumn],
) -> Result<LogicalPlan, DataFusionError> {
    // The transformation relies on `forbid_underscore_prefixed_aliases` to prevent
    // conflicts between user-selected columns and the propagated watermark columns.
    forbid_underscore_prefixed_aliases(&plan)?;
    // Propagate in reverse order so the first column in the slice ends up at position 0.
    let mut plan = plan;
    for wm in columns.iter().rev() {
        let mut propagator = WatermarkColumnPropagator::new(*wm);
        plan = plan.rewrite(&mut propagator).map(|t| t.data)?;
    }
    Ok(plan)
}

/// Remove the given columns from the plan's output by adding a projection that
/// selects all columns except those named in `columns_to_remove`.
pub fn unproject_columns(
    plan: LogicalPlan,
    columns_to_remove: &[&str],
) -> Result<LogicalPlan, DataFusionError> {
    let fields = plan.schema().fields();
    if !fields
        .iter()
        .any(|f| columns_to_remove.contains(&f.name().as_str()))
    {
        return Ok(plan);
    }
    let expr = plan
        .schema()
        .iter()
        .filter(|(_, field)| !columns_to_remove.contains(&field.name().as_str()))
        .map(Expr::from)
        .collect::<Vec<_>>();

    LogicalPlanBuilder::from(plan).project(expr)?.build()
}

/// Returns `true` if the plan contains any `block_num()` UDF calls.
pub fn plan_has_block_num_udf(plan: &LogicalPlan) -> bool {
    plan_has_udf(plan, is_block_num_udf)
}

/// Returns `true` if the plan contains any `ts()` UDF calls.
pub fn plan_has_ts_udf(plan: &LogicalPlan) -> bool {
    plan_has_udf(plan, is_ts_udf)
}

fn plan_has_udf(plan: &LogicalPlan, is_udf: fn(&Expr) -> bool) -> bool {
    let mut found = false;
    let _ = plan.apply(|node| {
        node.apply_expressions(|expr| {
            expr.apply(|e| {
                if is_udf(e) {
                    found = true;
                    return Ok(TreeNodeRecursion::Stop);
                }
                Ok(TreeNodeRecursion::Continue)
            })
        })?;
        if found {
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    });
    found
}

// ── Incrementality checking ──────────────────────────────────────────────

/// Reasons why a logical plan cannot be materialized incrementally
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NonIncrementalOp {
    /// Limit requires counting rows across batches
    Limit,
    /// Aggregations need state management
    Aggregate,
    /// Distinct operations need global deduplication
    Distinct,
    /// Outer joins are not incremental
    Join(JoinType),
    /// Sorts require seeing all data
    Sort,
    /// Window functions often require sorting and state
    Window,
    /// Recursive queries are inherently stateful
    RecursiveQuery,
    /// Stacked (nested) inner joins are not yet supported for incremental processing
    StackedJoins,
}

impl fmt::Display for NonIncrementalOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use NonIncrementalOp::*;
        match self {
            Limit => write!(f, "Limit"),
            Aggregate => write!(f, "Aggregate"),
            Distinct => write!(f, "Distinct"),
            Join(join_type) => write!(f, "Join({})", join_type),
            Sort => write!(f, "Sort"),
            Window => write!(f, "Window"),
            RecursiveQuery => write!(f, "RecursiveQuery"),
            StackedJoins => write!(f, "StackedJoins"),
        }
    }
}

/// Returns `Ok(())` if the given logical plan can be synced incrementally, `Err` otherwise.
pub fn is_incremental(plan: &LogicalPlan) -> Result<(), NonIncrementalQueryError> {
    let mut err: Option<NonIncrementalQueryError> = None;

    // Check individual nodes for unsupported operations.
    plan.exists(|node| match incremental_op_kind(node, BlockNumForm::Udf) {
        Ok(_) => Ok(false),
        Err(e) => {
            err = Some(e);
            Ok(true)
        }
    })
    .map_err(|df_err| NonIncrementalQueryError::Invalid(df_err.to_string()))?;

    if let Some(err) = err {
        return Err(err);
    }

    // Detect stacked (nested) joins. The incrementalizer rewrites join children
    // with History range, causing a nested join to fail at runtime. Detect this
    // structurally: if any join node has a descendant that is also an incremental join,
    // it's stacked. We check all descendants (not just direct children) because linear
    // nodes like SubqueryAlias or Projection can sit between two joins without breaking
    // the nesting relationship.
    let has_stacked_joins = plan
        .exists(|node| {
            let LogicalPlan::Join(join) = node else {
                return Ok(false);
            };
            if !matches!(
                incremental_op_kind(node, BlockNumForm::Udf),
                Ok(IncrementalOpKind::InnerJoin)
            ) {
                return Ok(false);
            }
            let child_has_join = |child: &LogicalPlan| {
                child.exists(|desc| {
                    Ok(matches!(
                        incremental_op_kind(desc, BlockNumForm::Udf),
                        Ok(IncrementalOpKind::InnerJoin)
                    ))
                })
            };
            Ok(child_has_join(&join.left)? || child_has_join(&join.right)?)
        })
        .map_err(|err| NonIncrementalQueryError::Invalid(err.to_string()))?;

    if has_stacked_joins {
        return Err(NonIncrementalQueryError::NonIncremental(
            NonIncrementalOp::StackedJoins,
        ));
    }

    Ok(())
}

// ── Table reference extraction ───────────────────────────────────────────

pub fn extract_table_references_from_plan(
    plan: &LogicalPlan,
) -> Result<Vec<TableReference>, DataFusionError> {
    let mut refs = BTreeSet::new();

    plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            refs.insert(scan.table_name.clone());
        }

        Ok(TreeNodeRecursion::Continue)
    })?;

    Ok(refs.into_iter().collect())
}

/// Information about a cross-network join detected in the logical plan.
///
/// A cross-network join occurs when the left and right inputs of a join reference
/// tables from different blockchain networks. This is not supported for streaming
/// queries because blocks from different chains cannot be synchronized.
#[derive(Debug, Clone)]
pub struct CrossNetworkJoinInfo {
    /// Networks involved in the cross-network join
    pub networks: BTreeSet<datasets_common::network_id::NetworkId>,
}

impl fmt::Display for CrossNetworkJoinInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "join across multiple networks: {:?}", self.networks)
    }
}

/// Checks if any join in the logical plan crosses multiple networks.
///
/// Streaming queries cannot join tables from different networks because blocks
/// from different chains cannot be synchronized. Returns the first cross-network
/// join found, or `None` if all joins operate within a single network.
pub fn find_cross_network_join(
    plan: &LogicalPlan,
    catalog: &crate::catalog::physical::Catalog,
) -> Result<Option<CrossNetworkJoinInfo>, DataFusionError> {
    let table_to_networks: BTreeMap<TableReference, BTreeSet<NetworkId>> = catalog
        .entries()
        .iter()
        .map(|(physical_table, sql_schema_name)| {
            let table_ref = TableReference::Partial {
                schema: Arc::from(&**sql_schema_name),
                table: Arc::from(physical_table.table_name().as_str()),
            };
            (table_ref, physical_table.networks().clone())
        })
        .collect();

    let reference_networks =
        |subtree: &LogicalPlan| -> Result<BTreeSet<NetworkId>, DataFusionError> {
            let table_refs = extract_table_references_from_plan(subtree)?;
            Ok(table_refs
                .into_iter()
                .filter_map(|table_ref| table_to_networks.get(&table_ref))
                .flatten()
                .cloned()
                .collect())
        };

    let mut cross_network_join: Option<CrossNetworkJoinInfo> = None;

    plan.apply(|node| {
        if cross_network_join.is_some() {
            return Ok(TreeNodeRecursion::Stop);
        }

        if let LogicalPlan::Join(join) = node {
            let mut networks: BTreeSet<NetworkId> = Default::default();
            networks.extend(reference_networks(&join.left)?);
            networks.extend(reference_networks(&join.right)?);

            if networks.len() > 1 {
                cross_network_join = Some(CrossNetworkJoinInfo { networks });
                return Ok(TreeNodeRecursion::Stop);
            }
        }

        Ok(TreeNodeRecursion::Continue)
    })?;

    Ok(cross_network_join)
}

/// Collects the schemas of all TableScan sources in the plan.
pub fn source_schemas(plan: &LogicalPlan) -> Vec<SchemaRef> {
    let mut schemas = Vec::new();
    let _ = plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            schemas.push(scan.source.schema());
        }
        Ok(TreeNodeRecursion::Continue)
    });
    schemas
}

pub fn order_by_block_num(plan: LogicalPlan) -> LogicalPlan {
    let sort = Sort {
        expr: vec![col(RESERVED_BLOCK_NUM_COLUMN_NAME).sort(true, false)],
        input: Arc::new(plan),
        fetch: None,
    };
    LogicalPlan::Sort(sort)
}

/// Prepend a watermark column field to a DFSchema (idempotent).
pub fn prepend_watermark_field(
    schema: &datafusion::common::DFSchema,
    wm: &WatermarkColumn,
) -> Arc<datafusion::common::DFSchema> {
    use datafusion::arrow::datatypes::Fields;

    // Do nothing if a field with the same name is already present. Note that this
    // is not redundant with `DFSchema::merge`, because that will consider
    // different qualifiers as different fields even if the name is the same.
    if schema.fields().iter().any(|f| f.name() == wm.column_name()) {
        return Arc::new(schema.clone());
    }

    let mut new_schema = datafusion::common::DFSchema::from_unqualified_fields(
        Fields::from(vec![wm.field()]),
        Default::default(),
    )
    .unwrap();
    new_schema.merge(schema);
    new_schema.into()
}

/// Prepend both watermark column fields (`_block_num`, `_ts`) to a DFSchema.
///
/// The result has column order `[_block_num, _ts, ...existing_fields]`.
pub fn prepend_watermark_column_fields(
    schema: &datafusion::common::DFSchema,
) -> Arc<datafusion::common::DFSchema> {
    // Prepend in reverse order so the final result is [_block_num, _ts, ...]
    let schema = prepend_watermark_field(schema, &WatermarkColumn::Ts);
    prepend_watermark_field(&schema, &WatermarkColumn::BlockNum)
}

fn df_err(msg: String) -> DataFusionError {
    DataFusionError::External(msg.into())
}

#[cfg(test)]
mod tests;
