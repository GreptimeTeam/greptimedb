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
use datafusion::execution::context::SessionState;
use datafusion::execution::SessionStateBuilder;
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion_common::{DFSchema, ScalarValue};
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::simplify::SimplifyContext;
use datafusion_expr::{AggregateUDF, ScalarUDF, TableSource, WindowUDF};
use datafusion_sql::planner::{ContextProvider, SqlToRel};
use datafusion_sql::TableReference;
use datatypes::arrow::datatypes::DataType;
use datatypes::schema::{
    COLUMN_FULLTEXT_OPT_KEY_ANALYZER, COLUMN_FULLTEXT_OPT_KEY_BACKEND,
    COLUMN_FULLTEXT_OPT_KEY_CASE_SENSITIVE, COLUMN_FULLTEXT_OPT_KEY_FALSE_POSITIVE_RATE,
    COLUMN_FULLTEXT_OPT_KEY_GRANULARITY, COLUMN_SKIPPING_INDEX_OPT_KEY_FALSE_POSITIVE_RATE,
    COLUMN_SKIPPING_INDEX_OPT_KEY_GRANULARITY, COLUMN_SKIPPING_INDEX_OPT_KEY_TYPE,
};
use snafu::ResultExt;

use crate::error::{
    ConvertToLogicalExpressionSnafu, ParseSqlValueSnafu, Result, SimplificationSnafu,
};

/// Convert a parser expression to a scalar value. This function will try the
/// best to resolve and reduce constants. Exprs like `1 + 1` or `now()` can be
/// handled properly.
pub fn parser_expr_to_scalar_value_literal(expr: sqlparser::ast::Expr) -> Result<ScalarValue> {
    // 1. convert parser expr to logical expr
    let empty_df_schema = DFSchema::empty();
    let logical_expr = SqlToRel::new(&StubContextProvider::default())
        .sql_to_expr(expr.into(), &empty_df_schema, &mut Default::default())
        .context(ConvertToLogicalExpressionSnafu)?;

    // 2. simplify logical expr
    let execution_props = ExecutionProps::new().with_query_execution_start_time(Utc::now());
    let info = SimplifyContext::new(&execution_props).with_schema(Arc::new(empty_df_schema));
    let simplified_expr = ExprSimplifier::new(info)
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
