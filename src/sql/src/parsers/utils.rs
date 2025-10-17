// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use chrono::Utc;
use datafusion::config::ConfigOptions;
use datafusion::error::Result as DfResult;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::context::SessionState;
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion_common::tree_node::{TreeNode, TreeNodeVisitor};
use datafusion_common::{DFSchema, ScalarValue};
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::simplify::SimplifyContext;
use datafusion_expr::{AggregateUDF, Expr, ScalarUDF, TableSource, WindowUDF};
use datafusion_sql::TableReference;
use datafusion_sql::planner::{ContextProvider, SqlToRel};
use datatypes::arrow::datatypes::DataType;
use datatypes::schema::{
    COLUMN_FULLTEXT_OPT_KEY_ANALYZER, COLUMN_FULLTEXT_OPT_KEY_BACKEND,
    COLUMN_FULLTEXT_OPT_KEY_CASE_SENSITIVE, COLUMN_FULLTEXT_OPT_KEY_FALSE_POSITIVE_RATE,
    COLUMN_FULLTEXT_OPT_KEY_GRANULARITY, COLUMN_SKIPPING_INDEX_OPT_KEY_FALSE_POSITIVE_RATE,
    COLUMN_SKIPPING_INDEX_OPT_KEY_GRANULARITY, COLUMN_SKIPPING_INDEX_OPT_KEY_TYPE,
};
use snafu::{ResultExt, ensure};
use sqlparser::dialect::Dialect;

use crate::error::{
    ConvertToLogicalExpressionSnafu, InvalidSqlSnafu, ParseSqlValueSnafu, Result,
    SimplificationSnafu,
};
use crate::parser::{ParseOptions, ParserContext};
use crate::statements::statement::Statement;

/// Check if the given SQL query is a TQL statement.
pub fn is_tql(dialect: &dyn Dialect, sql: &str) -> Result<bool> {
    let stmts = ParserContext::create_with_dialect(sql, dialect, ParseOptions::default())?;

    ensure!(
        stmts.len() == 1,
        InvalidSqlSnafu {
            msg: format!("Expect only one statement, found {}", stmts.len())
        }
    );
    let stmt = &stmts[0];
    match stmt {
        Statement::Tql(_) => Ok(true),
        _ => Ok(false),
    }
}

/// Convert a parser expression to a scalar value. This function will try the
/// best to resolve and reduce constants. Exprs like `1 + 1` or `now()` can be
/// handled properly.
///
/// if `require_now_expr` is true, it will ensure that the expression contains a `now()` function.
/// If the expression does not contain `now()`, it will return an error.
///
pub fn parser_expr_to_scalar_value_literal(
    expr: sqlparser::ast::Expr,
    require_now_expr: bool,
) -> Result<ScalarValue> {
    // 1. convert parser expr to logical expr
    let empty_df_schema = DFSchema::empty();
    let logical_expr = SqlToRel::new(&StubContextProvider::default())
        .sql_to_expr(expr, &empty_df_schema, &mut Default::default())
        .context(ConvertToLogicalExpressionSnafu)?;

    struct FindNow {
        found: bool,
    }

    impl TreeNodeVisitor<'_> for FindNow {
        type Node = Expr;
        fn f_down(
            &mut self,
            node: &Self::Node,
        ) -> DfResult<datafusion_common::tree_node::TreeNodeRecursion> {
            if let Expr::ScalarFunction(func) = node
                && func.name().to_lowercase() == "now"
            {
                if !func.args.is_empty() {
                    return Err(datafusion_common::DataFusionError::Plan(
                        "now() function should not have arguments".to_string(),
                    ));
                }
                self.found = true;
                return Ok(datafusion_common::tree_node::TreeNodeRecursion::Stop);
            }
            Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
        }
    }

    if require_now_expr {
        let have_now = {
            let mut visitor = FindNow { found: false };
            logical_expr.visit(&mut visitor).unwrap();
            visitor.found
        };
        if !have_now {
            return ParseSqlValueSnafu {
                msg: format!(
                    "expected now() expression, but not found in {}",
                    logical_expr
                ),
            }
            .fail();
        }
    }

    // 2. simplify logical expr
    let execution_props = ExecutionProps::new().with_query_execution_start_time(Utc::now());
    let info =
        SimplifyContext::new(&execution_props).with_schema(Arc::new(empty_df_schema.clone()));

    let simplifier = ExprSimplifier::new(info);

    // Coerce the logical expression so simplifier can handle it correctly. This is necessary for const eval with possible type mismatch. i.e.: `now() - now() + '15s'::interval` which is `TimestampNanosecond - TimestampNanosecond + IntervalMonthDayNano`.
    let logical_expr = simplifier
        .coerce(logical_expr, &empty_df_schema)
        .context(SimplificationSnafu)?;

    let simplified_expr = simplifier
        .simplify(logical_expr)
        .context(SimplificationSnafu)?;

    if let datafusion::logical_expr::Expr::Literal(lit, _) = simplified_expr {
        Ok(lit)
    } else {
        // Err(ParseSqlValue)
        ParseSqlValueSnafu {
            msg: format!("expected literal value, but found {:?}", simplified_expr),
        }
        .fail()
    }
}

/// Helper struct for [`parser_expr_to_scalar_value`].
struct StubContextProvider {
    state: SessionState,
}

impl Default for StubContextProvider {
    fn default() -> Self {
        Self {
            state: SessionStateBuilder::new()
                .with_config(Default::default())
                .with_runtime_env(Default::default())
                .with_default_features()
                .build(),
        }
    }
}

impl ContextProvider for StubContextProvider {
    fn get_table_source(&self, _name: TableReference) -> DfResult<Arc<dyn TableSource>> {
        unimplemented!()
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.state.scalar_functions().get(name).cloned()
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.state.aggregate_functions().get(name).cloned()
    }

    fn get_window_meta(&self, _name: &str) -> Option<Arc<WindowUDF>> {
        unimplemented!()
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        unimplemented!()
    }

    fn options(&self) -> &ConfigOptions {
        self.state.config_options()
    }

    fn udf_names(&self) -> Vec<String> {
        self.state.scalar_functions().keys().cloned().collect()
    }

    fn udaf_names(&self) -> Vec<String> {
        self.state.aggregate_functions().keys().cloned().collect()
    }

    fn udwf_names(&self) -> Vec<String> {
        self.state.window_functions().keys().cloned().collect()
    }
}

pub fn validate_column_fulltext_create_option(key: &str) -> bool {
    [
        COLUMN_FULLTEXT_OPT_KEY_ANALYZER,
        COLUMN_FULLTEXT_OPT_KEY_CASE_SENSITIVE,
        COLUMN_FULLTEXT_OPT_KEY_BACKEND,
        COLUMN_FULLTEXT_OPT_KEY_GRANULARITY,
        COLUMN_FULLTEXT_OPT_KEY_FALSE_POSITIVE_RATE,
    ]
    .contains(&key)
}

pub fn validate_column_skipping_index_create_option(key: &str) -> bool {
    [
        COLUMN_SKIPPING_INDEX_OPT_KEY_GRANULARITY,
        COLUMN_SKIPPING_INDEX_OPT_KEY_TYPE,
        COLUMN_SKIPPING_INDEX_OPT_KEY_FALSE_POSITIVE_RATE,
    ]
    .contains(&key)
}

/// Convert an [`IntervalMonthDayNano`] to a [`Duration`].
#[cfg(feature = "enterprise")]
pub fn convert_month_day_nano_to_duration(
    interval: arrow_buffer::IntervalMonthDayNano,
) -> Result<std::time::Duration> {
    let months: i64 = interval.months.into();
    let days: i64 = interval.days.into();
    let months_in_seconds: i64 = months * 60 * 60 * 24 * 3044 / 1000;
    let days_in_seconds: i64 = days * 60 * 60 * 24;
    let seconds_from_nanos = interval.nanoseconds / 1_000_000_000;
    let total_seconds = months_in_seconds + days_in_seconds + seconds_from_nanos;

    let mut nanos_remainder = interval.nanoseconds % 1_000_000_000;
    let mut adjusted_seconds = total_seconds;

    if nanos_remainder < 0 {
        nanos_remainder += 1_000_000_000;
        adjusted_seconds -= 1;
    }

    snafu::ensure!(
        adjusted_seconds >= 0,
        crate::error::InvalidIntervalSnafu {
            reason: "must be a positive interval",
        }
    );

    // Cast safety: `adjusted_seconds` is guaranteed to be non-negative before.
    let adjusted_seconds = adjusted_seconds as u64;
    // Cast safety: `nanos_remainder` is smaller than 1_000_000_000 which
    // is checked above.
    let nanos_remainder = nanos_remainder as u32;

    Ok(std::time::Duration::new(adjusted_seconds, nanos_remainder))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::DateTime;
    use datafusion::functions::datetime::expr_fn::now;
    use datafusion_expr::lit;
    use datatypes::arrow::datatypes::TimestampNanosecondType;

    use super::*;

    /// Keep this test to make sure we are using datafusion's `ExprSimplifier` correctly.
    #[test]
    fn test_simplifier() {
        let now_time = DateTime::from_timestamp(61, 0).unwrap();
        let lit_now = lit(ScalarValue::new_timestamp::<TimestampNanosecondType>(
            now_time.timestamp_nanos_opt(),
            None,
        ));
        let testcases = vec![
            (now(), lit_now),
            (now() - now(), lit(ScalarValue::DurationNanosecond(Some(0)))),
            (
                now() + lit(ScalarValue::new_interval_dt(0, 1500)),
                lit(ScalarValue::new_timestamp::<TimestampNanosecondType>(
                    Some(62500000000),
                    None,
                )),
            ),
            (
                now() - (now() + lit(ScalarValue::new_interval_dt(0, 1500))),
                lit(ScalarValue::DurationNanosecond(Some(-1500000000))),
            ),
            // this one failed if type is not coerced
            (
                now() - now() + lit(ScalarValue::new_interval_dt(0, 1500)),
                lit(ScalarValue::new_interval_mdn(0, 0, 1500000000)),
            ),
            (
                lit(ScalarValue::new_interval_mdn(
                    0,
                    0,
                    61 * 86400 * 1_000_000_000,
                )),
                lit(ScalarValue::new_interval_mdn(
                    0,
                    0,
                    61 * 86400 * 1_000_000_000,
                )),
            ),
        ];

        let execution_props = ExecutionProps::new().with_query_execution_start_time(now_time);
        let info = SimplifyContext::new(&execution_props).with_schema(Arc::new(DFSchema::empty()));

        let simplifier = ExprSimplifier::new(info);
        for (expr, expected) in testcases {
            let expr_name = expr.schema_name().to_string();
            let expr = simplifier.coerce(expr, &DFSchema::empty()).unwrap();

            let simplified_expr = simplifier.simplify(expr).unwrap();
            assert_eq!(
                simplified_expr, expected,
                "Failed to simplify expression: {expr_name}"
            );
        }
    }
}
