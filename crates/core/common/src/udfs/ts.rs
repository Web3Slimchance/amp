use std::any::Any;

use datafusion::{
    arrow::datatypes::{DataType, TimeUnit},
    error::Result,
    logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility},
    prelude::Expr,
};

/// A planning-time sentinel UDF that gets replaced with the appropriate `_ts`
/// expression during `WatermarkColumnPropagator::f_up`.  Panics if it reaches execution.
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct TsUdf {
    signature: Signature,
}

impl Default for TsUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl TsUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![], Volatility::Stable),
        }
    }
}

const TS_UDF_NAME: &str = "ts";

impl ScalarUDFImpl for TsUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        TS_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Timestamp(
            TimeUnit::Nanosecond,
            Some("+00:00".into()),
        ))
    }

    fn invoke_with_args(&self, _: ScalarFunctionArgs) -> Result<ColumnarValue> {
        panic!("ts() is a planning-time sentinel; must be replaced before execution")
    }
}

/// The column name DataFusion assigns to a `ts()` call when used as an
/// Aggregate group key: `columnize_expr` converts the outer Projection's
/// ScalarFunction to `col("ts()")`.  `is_ts_udf` matches both forms.
pub(crate) const TS_UDF_SCHEMA_NAME: &str = "ts()";

pub(crate) fn is_ts_udf(expr: &Expr) -> bool {
    matches!(expr, Expr::ScalarFunction(f)
        if f.func.name() == TS_UDF_NAME && f.args.is_empty())
        || matches!(expr, Expr::Column(c)
            if c.relation.is_none() && c.name == TS_UDF_SCHEMA_NAME)
}
