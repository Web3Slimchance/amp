//! Filter pushthrough for `evm_decode` / `evm_decode_log`.
//!
//! When a query filters on a decoded field that maps to an indexed event
//! parameter, this rewrite derives an equivalent filter on the raw topic
//! column. The derived filter is added conjunctively (AND) with the original
//! so that downstream pushdown optimisations can push it to the scan level.
//!
//! Implemented as a DataFusion [`OptimizerRule`] so it participates in the
//! standard optimiser pipeline. DataFusion's built-in `FilterPushDown` rule
//! then pushes the derived predicates toward the scan.

use std::sync::Arc;

use alloy::dyn_abi::DynSolType;
use datafusion::{
    common::tree_node::Transformed,
    error::DataFusionError,
    logical_expr::{BinaryExpr, Filter, LogicalPlan, Operator, Projection, SubqueryAlias},
    optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule},
    prelude::Expr,
    scalar::ScalarValue,
};

use crate::udfs::evm_common::Event;

// ---------------------------------------------------------------------------
// OptimizerRule
// ---------------------------------------------------------------------------

/// Derives topic-column predicates from filters on `evm_decode` output fields
/// that correspond to indexed event parameters.
#[derive(Debug)]
pub struct FilterPushthroughDecodeRule;

/// A derived topic predicate, tagged with where it should be placed.
enum Derived {
    /// The evm_decode call is inline in the filter expression — the derived
    /// predicate references columns visible at the current filter level and
    /// can simply be ANDed onto the filter predicate.
    Local(Expr),
    /// The evm_decode call lives inside a child Projection behind a column
    /// alias.  The derived predicate references topic columns that are only
    /// visible *below* that projection, so it must be injected as a new
    /// Filter between the Projection and its input.
    Deep { col_name: String, predicate: Expr },
}

impl OptimizerRule for FilterPushthroughDecodeRule {
    fn name(&self) -> &str {
        "filter_pushthrough_decode"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        node: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        let LogicalPlan::Filter(ref filter) = node else {
            return Ok(Transformed::no(node));
        };

        let derivations = derive_topic_predicates(&filter.predicate, &filter.input);
        if derivations.is_empty() {
            return Ok(Transformed::no(node));
        }

        let mut new_predicate = filter.predicate.clone();
        let mut new_input: LogicalPlan = (*filter.input).clone();

        for d in derivations {
            match d {
                Derived::Local(pred) => {
                    new_predicate = new_predicate.and(pred);
                }
                Derived::Deep {
                    col_name,
                    predicate,
                } => {
                    new_input =
                        inject_filter_below_decode_projection(new_input, &col_name, predicate);
                }
            }
        }

        let new_filter = LogicalPlan::Filter(Filter::try_new(new_predicate, Arc::new(new_input))?);
        Ok(Transformed::yes(new_filter))
    }
}

// ---------------------------------------------------------------------------
// Predicate derivation
// ---------------------------------------------------------------------------

/// Walks the predicate expression tree and collects derived topic predicates.
fn derive_topic_predicates(predicate: &Expr, input: &Arc<LogicalPlan>) -> Vec<Derived> {
    let mut derived = Vec::new();
    collect_derived(predicate, input, &mut derived);
    derived
}

fn collect_derived(expr: &Expr, input: &Arc<LogicalPlan>, out: &mut Vec<Derived>) {
    match expr {
        // Recurse into AND conjunctions.
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            collect_derived(&binary.left, input, out);
            collect_derived(&binary.right, input, out);
        }

        // Equality: get_field(evm_decode(...), field) = literal
        Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
            if let Some(d) = try_derive_eq(&binary.left, &binary.right, input) {
                out.push(d);
            } else if let Some(d) = try_derive_eq(&binary.right, &binary.left, input) {
                out.push(d);
            }
        }

        // IN list: get_field(evm_decode(...), field) IN (lit, lit, ...)
        Expr::InList(in_list) if !in_list.negated => {
            if let Some(d) = try_derive_in_list(&in_list.expr, &in_list.list, input) {
                out.push(d);
            }
        }

        _ => {}
    }
}

/// Attempts to derive a `topic_col = encoded_literal` predicate.
fn try_derive_eq(
    field_expr: &Expr,
    literal_expr: &Expr,
    input: &Arc<LogicalPlan>,
) -> Option<Derived> {
    let resolved = resolve_get_field_to_topic(field_expr, input)?;
    let literal_bytes = extract_literal_bytes(literal_expr)?;

    if !is_value_type(&resolved.sol_type) {
        return None;
    }

    let encoded = encode_to_topic(&resolved.sol_type, &literal_bytes).ok()?;
    let encoded_literal = ScalarValue::FixedSizeBinary(32, Some(encoded.to_vec()));

    let predicate = Expr::BinaryExpr(BinaryExpr::new(
        Box::new(resolved.topic_col_expr),
        Operator::Eq,
        Box::new(Expr::Literal(encoded_literal, None)),
    ));

    Some(match resolved.col_name {
        None => Derived::Local(predicate),
        Some(col_name) => Derived::Deep {
            col_name,
            predicate,
        },
    })
}

/// Attempts to derive a `topic_col IN (enc1, enc2, ...)` predicate.
fn try_derive_in_list(
    field_expr: &Expr,
    list: &[Expr],
    input: &Arc<LogicalPlan>,
) -> Option<Derived> {
    let resolved = resolve_get_field_to_topic(field_expr, input)?;

    if !is_value_type(&resolved.sol_type) {
        return None;
    }

    let mut encoded_list = Vec::with_capacity(list.len());
    for item in list {
        let bytes = extract_literal_bytes(item)?;
        let encoded = encode_to_topic(&resolved.sol_type, &bytes).ok()?;
        encoded_list.push(Expr::Literal(
            ScalarValue::FixedSizeBinary(32, Some(encoded.to_vec())),
            None,
        ));
    }

    let predicate = Expr::InList(datafusion::logical_expr::expr::InList::new(
        Box::new(resolved.topic_col_expr),
        encoded_list,
        false,
    ));

    Some(match resolved.col_name {
        None => Derived::Local(predicate),
        Some(col_name) => Derived::Deep {
            col_name,
            predicate,
        },
    })
}

// ---------------------------------------------------------------------------
// Expression pattern matching
// ---------------------------------------------------------------------------

/// Result of resolving a `get_field(evm_decode_log(...), field)` expression.
struct ResolvedTopic {
    /// The expression referencing the topic column (e.g. `Column("topic2")`).
    topic_col_expr: Expr,
    /// The Solidity type of the indexed parameter.
    sol_type: DynSolType,
    /// If the evm_decode was resolved through a column alias in a child
    /// projection, this is the alias name.  `None` when the evm_decode call
    /// appears directly in the filter expression.
    col_name: Option<String>,
}

/// Resolves a `get_field(evm_decode_log(...), field_name)` expression.
///
/// Handles both:
/// - Direct inline `evm_decode_log(...)` references in the expression.
/// - Column references that resolve to `evm_decode_log(...)` via a child
///   projection.
fn resolve_get_field_to_topic(expr: &Expr, input: &Arc<LogicalPlan>) -> Option<ResolvedTopic> {
    let Expr::ScalarFunction(get_field_sf) = expr else {
        return None;
    };
    if get_field_sf.func.name() != "get_field" || get_field_sf.args.len() < 2 {
        return None;
    }

    let field_name = match &get_field_sf.args[1] {
        Expr::Literal(ScalarValue::Utf8(Some(name)), _) => name.as_str(),
        _ => return None,
    };

    let struct_expr = &get_field_sf.args[0];

    // Case 1: direct evm_decode_log(...) call.
    if let Expr::ScalarFunction(sf) = struct_expr
        && let Some((topic_col_expr, sol_type)) = resolve_from_decode_call(sf, field_name)
    {
        return Some(ResolvedTopic {
            topic_col_expr,
            sol_type,
            col_name: None,
        });
    }

    // Case 2: column reference → resolve through the input plan.
    if let Expr::Column(col) = struct_expr
        && let Some(sf) = find_evm_decode_in_plan(&col.name, input)
        && let Some((topic_col_expr, sol_type)) = resolve_from_decode_call(&sf, field_name)
    {
        return Some(ResolvedTopic {
            topic_col_expr,
            sol_type,
            col_name: Some(col.name.clone()),
        });
    }

    None
}

/// Given a `ScalarFunction` that is an `evm_decode_log` / `evm_decode` call,
/// resolve which topic column corresponds to `field_name` and return the
/// topic column expression together with the Solidity type.
fn resolve_from_decode_call(
    sf: &datafusion::logical_expr::expr::ScalarFunction,
    field_name: &str,
) -> Option<(Expr, DynSolType)> {
    let name = sf.func.name();
    if name != "evm_decode_log" && name != "evm_decode" {
        return None;
    }

    // evm_decode_log(topic1, topic2, topic3, data, signature)
    if sf.args.len() != 5 {
        return None;
    }

    let sig_str = match &sf.args[4] {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _) => s,
        _ => return None,
    };

    let event: Event = sig_str.parse().ok()?;
    let (topic_index, sol_type) = event.find_indexed_param(field_name)?;

    // topic_index 0 → sf.args[0] (topic1), 1 → sf.args[1] (topic2), etc.
    let topic_col_expr: Expr = sf.args[topic_index].clone();
    let sol_type: DynSolType = sol_type.clone();
    Some((topic_col_expr, sol_type))
}

/// Walks the plan tree below a Filter to find a Projection expression
/// aliased to `col_name` that is an `evm_decode_log` / `evm_decode` call.
fn find_evm_decode_in_plan(
    col_name: &str,
    plan: &LogicalPlan,
) -> Option<datafusion::logical_expr::expr::ScalarFunction> {
    match plan {
        LogicalPlan::Projection(proj) => {
            for expr in &proj.expr {
                let (alias_name, inner) = match expr {
                    Expr::Alias(alias) => (alias.name.as_str(), alias.expr.as_ref()),
                    _ => continue,
                };
                if alias_name != col_name {
                    continue;
                }
                if let Expr::ScalarFunction(sf) = inner {
                    let n = sf.func.name();
                    if n == "evm_decode_log" || n == "evm_decode" {
                        return Some(sf.clone());
                    }
                }
            }
            None
        }
        LogicalPlan::SubqueryAlias(sq) => find_evm_decode_in_plan(col_name, &sq.input),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Deep-injection: place a derived predicate below a decode projection
// ---------------------------------------------------------------------------

/// Returns `true` if `proj` contains an `evm_decode_log` / `evm_decode`
/// expression aliased to `col_name`.
fn projection_has_decode_alias(proj: &Projection, col_name: &str) -> bool {
    proj.expr.iter().any(|e| {
        if let Expr::Alias(alias) = e
            && alias.name == col_name
            && let Expr::ScalarFunction(sf) = alias.expr.as_ref()
        {
            let n = sf.func.name();
            n == "evm_decode_log" || n == "evm_decode"
        } else {
            false
        }
    })
}

/// Walks through SubqueryAlias/Projection nodes and injects `predicate` as a
/// new Filter between the decode Projection and its input.  Returns the
/// modified plan.  If the target projection is not found, the plan is
/// returned unchanged (safety: no modification on mismatch).
fn inject_filter_below_decode_projection(
    plan: LogicalPlan,
    col_name: &str,
    predicate: Expr,
) -> LogicalPlan {
    match plan {
        LogicalPlan::Projection(proj) if projection_has_decode_alias(&proj, col_name) => {
            // Inject a Filter between the Projection and its input.
            match Filter::try_new(predicate, proj.input.clone()) {
                Ok(new_filter) => Projection::try_new(
                    proj.expr.clone(),
                    Arc::new(LogicalPlan::Filter(new_filter)),
                )
                .map(LogicalPlan::Projection)
                .unwrap_or(LogicalPlan::Projection(proj)),
                Err(_) => LogicalPlan::Projection(proj),
            }
        }
        LogicalPlan::SubqueryAlias(sq) => {
            let modified_inner =
                inject_filter_below_decode_projection((*sq.input).clone(), col_name, predicate);
            match SubqueryAlias::try_new(Arc::new(modified_inner), sq.alias.clone()) {
                Ok(new_sq) => LogicalPlan::SubqueryAlias(new_sq),
                Err(_) => LogicalPlan::SubqueryAlias(sq),
            }
        }
        other => other,
    }
}

// ---------------------------------------------------------------------------
// Literal extraction
// ---------------------------------------------------------------------------

/// Extracts raw bytes from a literal expression suitable for ABI encoding.
fn extract_literal_bytes(expr: &Expr) -> Option<Vec<u8>> {
    match expr {
        Expr::Literal(scalar, _) => extract_scalar_bytes(scalar),
        // Handle CAST(literal AS type) — DF may wrap literals in casts.
        Expr::Cast(cast) => extract_literal_bytes(&cast.expr),
        Expr::TryCast(cast) => extract_literal_bytes(&cast.expr),
        _ => None,
    }
}

fn extract_scalar_bytes(scalar: &ScalarValue) -> Option<Vec<u8>> {
    match scalar {
        ScalarValue::Binary(Some(b)) | ScalarValue::LargeBinary(Some(b)) => Some(b.clone()),
        ScalarValue::FixedSizeBinary(_, Some(b)) => Some(b.clone()),
        ScalarValue::Boolean(Some(b)) => Some(vec![*b as u8]),
        ScalarValue::UInt8(Some(n)) => Some(strip_leading_zeros(&n.to_be_bytes())),
        ScalarValue::UInt16(Some(n)) => Some(strip_leading_zeros(&n.to_be_bytes())),
        ScalarValue::UInt32(Some(n)) => Some(strip_leading_zeros(&n.to_be_bytes())),
        ScalarValue::UInt64(Some(n)) => Some(strip_leading_zeros(&n.to_be_bytes())),
        ScalarValue::Int8(Some(n)) => Some(n.to_be_bytes().to_vec()),
        ScalarValue::Int16(Some(n)) => Some(n.to_be_bytes().to_vec()),
        ScalarValue::Int32(Some(n)) => Some(n.to_be_bytes().to_vec()),
        ScalarValue::Int64(Some(n)) => Some(n.to_be_bytes().to_vec()),
        _ => None,
    }
}

/// Strips leading zero bytes so that `encode_to_topic` can left-pad
/// correctly for unsigned integers.
fn strip_leading_zeros(bytes: &[u8]) -> Vec<u8> {
    let first_nonzero = bytes.iter().position(|&b| b != 0).unwrap_or(bytes.len());
    if first_nonzero == bytes.len() {
        // Value is zero — keep one byte.
        vec![0]
    } else {
        bytes[first_nonzero..].to_vec()
    }
}

// ---------------------------------------------------------------------------
// ABI topic encoding
// ---------------------------------------------------------------------------

/// Returns `true` if the Solidity type is a value type that can be
/// meaningfully encoded into a 32-byte topic word. Reference types
/// (`string`, `bytes`, arrays, tuples) are hashed by Solidity, so the
/// original value cannot be reconstructed from the topic.
fn is_value_type(ty: &DynSolType) -> bool {
    matches!(
        ty,
        DynSolType::Address
            | DynSolType::Bool
            | DynSolType::Uint(_)
            | DynSolType::Int(_)
            | DynSolType::FixedBytes(_)
    )
}

/// Encodes a raw value into a 32-byte topic word following Solidity's
/// indexed-parameter encoding rules.
fn encode_to_topic(ty: &DynSolType, value: &[u8]) -> Result<[u8; 32], DataFusionError> {
    let mut word = [0u8; 32];
    match ty {
        DynSolType::Address => {
            // Addresses are 20 bytes, left-padded with 12 zero bytes.
            if value.len() > 20 {
                return Err(DataFusionError::Plan(format!(
                    "filter pushthrough: expected <=20 bytes for address, got {}",
                    value.len()
                )));
            }
            word[32 - value.len()..].copy_from_slice(value);
        }
        DynSolType::Bool => {
            word[31] = u8::from(!value.is_empty() && value[value.len() - 1] != 0);
        }
        DynSolType::Uint(_) => {
            // Big-endian, left-padded with zeros.
            if value.len() > 32 {
                return Err(DataFusionError::Plan(format!(
                    "filter pushthrough: value too large for uint ({}B)",
                    value.len()
                )));
            }
            word[32 - value.len()..].copy_from_slice(value);
        }
        DynSolType::Int(_) => {
            // Big-endian, sign-extended.
            if value.len() > 32 {
                return Err(DataFusionError::Plan(format!(
                    "filter pushthrough: value too large for int ({}B)",
                    value.len()
                )));
            }
            let sign_byte = if !value.is_empty() && value[0] & 0x80 != 0 {
                0xFF
            } else {
                0x00
            };
            word[..32 - value.len()].fill(sign_byte);
            word[32 - value.len()..].copy_from_slice(value);
        }
        DynSolType::FixedBytes(n) => {
            // Right-padded: value occupies the leftmost bytes.
            let n = *n;
            if value.len() > n {
                return Err(DataFusionError::Plan(format!(
                    "filter pushthrough: value too large for bytes{n} ({}B)",
                    value.len()
                )));
            }
            word[..value.len()].copy_from_slice(value);
        }
        _ => {
            return Err(DataFusionError::Plan(format!(
                "filter pushthrough: unsupported type for topic encoding: {ty:?}"
            )));
        }
    }
    Ok(word)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::{
        arrow::{
            array::RecordBatch,
            datatypes::{DataType, Field, Schema},
        },
        common::{Column, tree_node::TreeNode as _},
        datasource::{MemTable, provider_as_source},
        logical_expr::{
            LogicalPlan, LogicalPlanBuilder,
            expr::{InList, ScalarFunction as ScalarFunctionExpr},
        },
        prelude::Expr,
        scalar::ScalarValue,
    };

    use super::*;
    use crate::udfs::evm_decode_log::EvmDecodeLog;

    /// Applies the rule to a plan for testing.
    fn filter_pushthrough_decode(plan: LogicalPlan) -> Result<LogicalPlan, DataFusionError> {
        let rule = FilterPushthroughDecodeRule;
        plan.transform_up(|node| rule.rewrite(node, &NoopOptimizerConfig))
            .map(|t| t.data)
    }

    struct NoopOptimizerConfig;
    impl OptimizerConfig for NoopOptimizerConfig {
        fn query_execution_start_time(&self) -> chrono::DateTime<chrono::Utc> {
            chrono::Utc::now()
        }
        fn alias_generator(&self) -> &Arc<datafusion::common::alias::AliasGenerator> {
            unimplemented!()
        }
        fn options(&self) -> Arc<datafusion::config::ConfigOptions> {
            Arc::new(datafusion::config::ConfigOptions::default())
        }
    }

    /// Shorthand for creating an `Expr::Literal` in DF 52 (which requires metadata).
    fn scalar_lit(val: ScalarValue) -> Expr {
        Expr::Literal(val, None)
    }

    /// Helper: build a table-scan plan with the standard log columns.
    fn logs_scan() -> LogicalPlan {
        let schema = Arc::new(Schema::new(vec![
            Field::new("topic0", DataType::FixedSizeBinary(32), true),
            Field::new("topic1", DataType::FixedSizeBinary(32), true),
            Field::new("topic2", DataType::FixedSizeBinary(32), true),
            Field::new("topic3", DataType::FixedSizeBinary(32), true),
            Field::new("data", DataType::Binary, true),
            Field::new("address", DataType::FixedSizeBinary(20), true),
        ]));
        let batch = RecordBatch::new_empty(schema.clone());
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        LogicalPlanBuilder::scan("logs", provider_as_source(Arc::new(table)), None)
            .unwrap()
            .build()
            .unwrap()
    }

    /// Helper: create an `evm_decode_log(topic1, topic2, topic3, data, sig)` expression.
    fn evm_decode_expr(sig: &str) -> Expr {
        let udf: datafusion::logical_expr::ScalarUDF = EvmDecodeLog::new().into();
        Expr::ScalarFunction(ScalarFunctionExpr {
            func: Arc::new(udf),
            args: vec![
                Expr::Column(Column::new_unqualified("topic1")),
                Expr::Column(Column::new_unqualified("topic2")),
                Expr::Column(Column::new_unqualified("topic3")),
                Expr::Column(Column::new_unqualified("data")),
                Expr::Literal(ScalarValue::Utf8(Some(sig.to_string())), None),
            ],
        })
    }

    /// Helper: wrap `inner_expr["field_name"]` using get_field.
    fn get_field_expr(inner: Expr, field_name: &str) -> Expr {
        datafusion::functions::core::expr_fn::get_field(
            inner,
            ScalarValue::Utf8(Some(field_name.to_string())),
        )
    }

    const TRANSFER_SIG: &str = "Transfer(address indexed from, address indexed to, uint256 value)";

    // ------------------------------------------------------------------
    // Test 1: basic equality on indexed address field (to → topic2)
    // ------------------------------------------------------------------
    #[test]
    fn test_eq_indexed_address_to() {
        let scan = logs_scan();
        let dec = evm_decode_expr(TRANSFER_SIG);
        let plan =
            LogicalPlanBuilder::from(scan)
                .project(vec![dec.clone().alias("dec")])
                .unwrap()
                .filter(get_field_expr(dec.clone(), "to").eq(scalar_lit(
                    ScalarValue::FixedSizeBinary(20, Some(vec![0xba; 20])),
                )))
                .unwrap()
                .build()
                .unwrap();

        let rewritten = filter_pushthrough_decode(plan).unwrap();
        let plan_str = format!("{rewritten}");

        // The derived predicate should reference topic2 and contain the
        // left-padded 32-byte encoding of the address.
        assert!(
            plan_str.contains("topic2"),
            "expected topic2 in plan:\n{plan_str}"
        );
    }

    // ------------------------------------------------------------------
    // Test 2: equality on first indexed parameter (from → topic1)
    // ------------------------------------------------------------------
    #[test]
    fn test_eq_indexed_address_from() {
        let scan = logs_scan();
        let dec = evm_decode_expr(TRANSFER_SIG);
        let plan = LogicalPlanBuilder::from(scan)
            .project(vec![dec.clone().alias("dec")])
            .unwrap()
            .filter(get_field_expr(dec.clone(), "from").eq(scalar_lit(
                ScalarValue::FixedSizeBinary(20, Some(vec![0xab; 20])),
            )))
            .unwrap()
            .build()
            .unwrap();

        let rewritten = filter_pushthrough_decode(plan).unwrap();
        let plan_str = format!("{rewritten}");

        assert!(
            plan_str.contains("topic1"),
            "expected topic1 in plan:\n{plan_str}"
        );
    }

    // ------------------------------------------------------------------
    // Test 3: non-indexed field — no rewrite
    // ------------------------------------------------------------------
    #[test]
    fn test_non_indexed_field_no_rewrite() {
        let scan = logs_scan();
        let dec = evm_decode_expr(TRANSFER_SIG);
        let plan = LogicalPlanBuilder::from(scan)
            .project(vec![dec.clone().alias("dec")])
            .unwrap()
            .filter(
                get_field_expr(dec.clone(), "value")
                    .gt(scalar_lit(ScalarValue::UInt64(Some(1000)))),
            )
            .unwrap()
            .build()
            .unwrap();

        let original_str = format!("{plan}");
        let rewritten = filter_pushthrough_decode(plan).unwrap();
        let rewritten_str = format!("{rewritten}");

        assert_eq!(
            original_str, rewritten_str,
            "plan should not be modified for non-indexed field"
        );
    }

    // ------------------------------------------------------------------
    // Test 4: indexed reference type (string) — no rewrite
    // ------------------------------------------------------------------
    #[test]
    fn test_indexed_reference_type_no_rewrite() {
        let sig = "DataStored(string indexed key, uint256 value)";
        let scan = logs_scan();
        let dec = evm_decode_expr(sig);
        let plan = LogicalPlanBuilder::from(scan)
            .project(vec![dec.clone().alias("dec")])
            .unwrap()
            .filter(
                get_field_expr(dec.clone(), "key")
                    .eq(scalar_lit(ScalarValue::Utf8(Some("hello".to_string())))),
            )
            .unwrap()
            .build()
            .unwrap();

        let original_str = format!("{plan}");
        let rewritten = filter_pushthrough_decode(plan).unwrap();
        let rewritten_str = format!("{rewritten}");

        assert_eq!(
            original_str, rewritten_str,
            "plan should not be modified for indexed reference type"
        );
    }

    // ------------------------------------------------------------------
    // Test 5: multiple decoded-field filters (from AND to)
    // ------------------------------------------------------------------
    #[test]
    fn test_multiple_decoded_field_filters() {
        let scan = logs_scan();
        let dec = evm_decode_expr(TRANSFER_SIG);
        let plan = LogicalPlanBuilder::from(scan)
            .project(vec![dec.clone().alias("dec")])
            .unwrap()
            .filter(
                get_field_expr(dec.clone(), "from")
                    .eq(scalar_lit(ScalarValue::FixedSizeBinary(
                        20,
                        Some(vec![0xaa; 20]),
                    )))
                    .and(get_field_expr(dec.clone(), "to").eq(scalar_lit(
                        ScalarValue::FixedSizeBinary(20, Some(vec![0xbb; 20])),
                    ))),
            )
            .unwrap()
            .build()
            .unwrap();

        let rewritten = filter_pushthrough_decode(plan).unwrap();
        let plan_str = format!("{rewritten}");

        assert!(
            plan_str.contains("topic1"),
            "expected topic1 in plan:\n{plan_str}"
        );
        assert!(
            plan_str.contains("topic2"),
            "expected topic2 in plan:\n{plan_str}"
        );
    }

    // ------------------------------------------------------------------
    // Test 6: bool indexed parameter
    // ------------------------------------------------------------------
    #[test]
    fn test_bool_indexed_parameter() {
        let sig = "StatusChanged(bool indexed active, uint256 value)";
        let scan = logs_scan();
        let dec = evm_decode_expr(sig);
        let plan = LogicalPlanBuilder::from(scan)
            .project(vec![dec.clone().alias("dec")])
            .unwrap()
            .filter(
                get_field_expr(dec.clone(), "active")
                    .eq(scalar_lit(ScalarValue::Boolean(Some(true)))),
            )
            .unwrap()
            .build()
            .unwrap();

        let rewritten = filter_pushthrough_decode(plan).unwrap();
        let plan_str = format!("{rewritten}");

        // Derived predicate should encode true as ...0001.
        assert!(
            plan_str.contains("topic1"),
            "expected topic1 in plan:\n{plan_str}"
        );
    }

    // ------------------------------------------------------------------
    // Test 7: uint256 indexed parameter
    // ------------------------------------------------------------------
    #[test]
    fn test_uint256_indexed_parameter() {
        let sig = "TokenMinted(uint256 indexed id, address to)";
        let scan = logs_scan();
        let dec = evm_decode_expr(sig);
        let plan = LogicalPlanBuilder::from(scan)
            .project(vec![dec.clone().alias("dec")])
            .unwrap()
            .filter(get_field_expr(dec.clone(), "id").eq(scalar_lit(ScalarValue::UInt64(Some(42)))))
            .unwrap()
            .build()
            .unwrap();

        let rewritten = filter_pushthrough_decode(plan).unwrap();
        let plan_str = format!("{rewritten}");

        assert!(
            plan_str.contains("topic1"),
            "expected topic1 in plan:\n{plan_str}"
        );
    }

    // ------------------------------------------------------------------
    // Test 8: IN list
    // ------------------------------------------------------------------
    #[test]
    fn test_in_list() {
        let scan = logs_scan();
        let dec = evm_decode_expr(TRANSFER_SIG);
        let plan = LogicalPlanBuilder::from(scan)
            .project(vec![dec.clone().alias("dec")])
            .unwrap()
            .filter(Expr::InList(InList::new(
                Box::new(get_field_expr(dec.clone(), "to")),
                vec![
                    scalar_lit(ScalarValue::FixedSizeBinary(20, Some(vec![0xaa; 20]))),
                    scalar_lit(ScalarValue::FixedSizeBinary(20, Some(vec![0xbb; 20]))),
                ],
                false,
            )))
            .unwrap()
            .build()
            .unwrap();

        let rewritten = filter_pushthrough_decode(plan).unwrap();
        let plan_str = format!("{rewritten}");

        assert!(
            plan_str.contains("topic2"),
            "expected topic2 in plan:\n{plan_str}"
        );
        assert!(
            plan_str.contains("IN"),
            "expected IN list in plan:\n{plan_str}"
        );
    }

    // ------------------------------------------------------------------
    // ABI encoding unit tests
    // ------------------------------------------------------------------
    #[test]
    fn test_encode_address() {
        let addr = vec![0xba; 20];
        let word = encode_to_topic(&DynSolType::Address, &addr).unwrap();
        assert_eq!(&word[..12], &[0u8; 12], "first 12 bytes should be zero");
        assert_eq!(&word[12..], &addr[..], "last 20 bytes should be address");
    }

    #[test]
    fn test_encode_bool_true() {
        let word = encode_to_topic(&DynSolType::Bool, &[1]).unwrap();
        assert_eq!(word[31], 1);
        assert_eq!(&word[..31], &[0u8; 31]);
    }

    #[test]
    fn test_encode_bool_false() {
        let word = encode_to_topic(&DynSolType::Bool, &[0]).unwrap();
        assert_eq!(word, [0u8; 32]);
    }

    #[test]
    fn test_encode_uint() {
        // 42 = 0x2a
        let word = encode_to_topic(&DynSolType::Uint(256), &[0x2a]).unwrap();
        assert_eq!(&word[..31], &[0u8; 31]);
        assert_eq!(word[31], 0x2a);
    }

    #[test]
    fn test_encode_int_negative() {
        // -1 in int8 = 0xFF
        let word = encode_to_topic(&DynSolType::Int(8), &[0xFF]).unwrap();
        // Sign-extended: all bytes should be 0xFF.
        assert_eq!(word, [0xFF; 32]);
    }

    #[test]
    fn test_encode_fixed_bytes() {
        let val = vec![0xDE, 0xAD];
        let word = encode_to_topic(&DynSolType::FixedBytes(4), &val).unwrap();
        assert_eq!(&word[..2], &[0xDE, 0xAD]);
        assert_eq!(&word[2..], &[0u8; 30]);
    }

    // ------------------------------------------------------------------
    // Column-reference resolution through projection
    // ------------------------------------------------------------------
    #[test]
    fn test_column_ref_through_projection() {
        let scan = logs_scan();
        let dec = evm_decode_expr(TRANSFER_SIG);

        // Build: Filter(dec['to'] = addr) -> SubqueryAlias(pc) -> Projection(evm_decode as dec)
        let projection = LogicalPlanBuilder::from(scan)
            .project(vec![dec.alias("dec")])
            .unwrap()
            .alias("pc")
            .unwrap()
            .build()
            .unwrap();

        let dec_col = Expr::Column(Column::new(Some("pc"), "dec"));
        let plan = LogicalPlanBuilder::from(projection)
            .filter(
                get_field_expr(dec_col, "to").eq(scalar_lit(ScalarValue::FixedSizeBinary(
                    20,
                    Some(vec![0xba; 20]),
                ))),
            )
            .unwrap()
            .build()
            .unwrap();

        let rewritten = filter_pushthrough_decode(plan).unwrap();
        let plan_str = format!("{rewritten}");

        assert!(
            plan_str.contains("topic2"),
            "expected topic2 in plan:\n{plan_str}"
        );
    }

    // ------------------------------------------------------------------
    // Integration test: SQL → plan → optimize, verify topic predicate
    // ------------------------------------------------------------------
    #[tokio::test]
    async fn test_sql_explain_plan_contains_topic_predicate() {
        use datafusion::{logical_expr::ScalarUDF, prelude::SessionContext};

        use crate::udfs::evm_decode_log::EvmDecodeLog;

        // Set up a session with the logs table and evm_decode_log UDF.
        let schema = Arc::new(Schema::new(vec![
            Field::new("topic0", DataType::FixedSizeBinary(32), true),
            Field::new("topic1", DataType::FixedSizeBinary(32), true),
            Field::new("topic2", DataType::FixedSizeBinary(32), true),
            Field::new("topic3", DataType::FixedSizeBinary(32), true),
            Field::new("data", DataType::Binary, true),
            Field::new("address", DataType::FixedSizeBinary(20), true),
        ]));
        let batch = RecordBatch::new_empty(schema.clone());
        let table = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap());

        let ctx = SessionContext::new();
        ctx.add_optimizer_rule(Arc::new(FilterPushthroughDecodeRule));
        ctx.register_udf(ScalarUDF::from(EvmDecodeLog::new()));
        // Also register the deprecated "evm_decode" alias.
        ctx.register_udf(ScalarUDF::from(EvmDecodeLog::new().with_deprecated_name()));
        ctx.register_table("logs", table).unwrap();

        // Plan a query that filters on an indexed decoded field.
        let sql = "\
            SELECT pc.dec['to'] AS to_address \
            FROM ( \
                SELECT evm_decode(topic1, topic2, topic3, data, \
                    'Transfer(address indexed from, address indexed to, uint256 value)') AS dec \
                FROM logs \
            ) pc \
            WHERE pc.dec['to'] = X'bafead7c60ea473758ed6c6021505e8bbd7e8e5d'";

        let plan = ctx
            .sql(sql)
            .await
            .expect("should parse SQL")
            .logical_plan()
            .clone();

        // The rule is registered as an OptimizerRule, so DF's optimize
        // applies it alongside built-in passes (including FilterPushDown).
        let optimized = ctx.state().optimize(&plan).unwrap();

        let plan_str = format!("{}", optimized.display_indent());
        eprintln!("optimized plan:\n{plan_str}");

        // The derived topic2 predicate should appear in the optimized plan,
        // pushed down close to the scan.
        assert!(
            plan_str.contains("topic2"),
            "expected derived topic2 predicate in optimized plan:\n{plan_str}"
        );
    }
}
