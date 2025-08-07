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

use std::fmt;
use std::fmt::Formatter;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{AsArray, BooleanArray};
use common_function::scalars::matches_term::MatchesTermFinder;
use datafusion::config::ConfigOptions;
use datafusion::error::Result as DfResult;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::ScalarValue;
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::expressions::Literal;
use datafusion_physical_expr::{PhysicalExpr, ScalarFunctionExpr};

/// A physical expression that uses a pre-compiled term finder for the `matches_term` function.
///
/// This expression optimizes the `matches_term` function by pre-compiling the term
/// when the term is a constant value. This avoids recompiling the term for each row
/// during execution.
#[derive(Debug)]
pub struct PreCompiledMatchesTermExpr {
    /// The text column expression to search in
    text: Arc<dyn PhysicalExpr>,
    /// The constant term to search for
    term: String,
    /// The pre-compiled term finder
    finder: MatchesTermFinder,

    /// No used but show how index tokenizes the term basically.
    /// Not precise due to column options is unknown but for debugging purpose in most cases it's enough.
    probes: Vec<String>,
}

impl fmt::Display for PreCompiledMatchesTermExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "MatchesConstTerm({}, term: \"{}\", probes: {:?})",
            self.text, self.term, self.probes
        )
    }
}

impl Hash for PreCompiledMatchesTermExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.text.hash(state);
        self.term.hash(state);
    }
}

impl PartialEq for PreCompiledMatchesTermExpr {
    fn eq(&self, other: &Self) -> bool {
        self.text.eq(&other.text) && self.term.eq(&other.term)
    }
}

impl Eq for PreCompiledMatchesTermExpr {}

impl PhysicalExpr for PreCompiledMatchesTermExpr {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(
        &self,
        _input_schema: &arrow_schema::Schema,
    ) -> datafusion::error::Result<arrow_schema::DataType> {
        Ok(arrow_schema::DataType::Boolean)
    }

    fn nullable(&self, input_schema: &arrow_schema::Schema) -> datafusion::error::Result<bool> {
        self.text.nullable(input_schema)
    }

    fn evaluate(
        &self,
        batch: &common_recordbatch::DfRecordBatch,
    ) -> datafusion::error::Result<ColumnarValue> {
        let num_rows = batch.num_rows();

        let text_value = self.text.evaluate(batch)?;
        let array = text_value.into_array(num_rows)?;
        let str_array = array.as_string::<i32>();

        let mut result = BooleanArray::builder(num_rows);
        for text in str_array {
            match text {
                Some(text) => {
                    result.append_value(self.finder.find(text));
                }
                None => {
                    result.append_null();
                }
            }
        }

        Ok(ColumnarValue::Array(Arc::new(result.finish())))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.text]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::error::Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(PreCompiledMatchesTermExpr {
            text: children[0].clone(),
            term: self.term.clone(),
            finder: self.finder.clone(),
            probes: self.probes.clone(),
        }))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

/// Optimizer rule that pre-compiles constant term in `matches_term` function.
///
/// This optimizer looks for `matches_term` function calls where the second argument
/// (the term to match) is a constant value. When found, it replaces the function
/// call with a specialized `PreCompiledMatchesTermExpr` that uses a pre-compiled
/// term finder.
///
/// Example:
/// ```sql
/// -- Before optimization:
/// matches_term(text_column, 'constant_term')
///
/// -- After optimization:
/// PreCompiledMatchesTermExpr(text_column, 'constant_term')
/// ```
///
/// This optimization improves performance by:
/// 1. Pre-compiling the term once instead of for each row
/// 2. Using a specialized expression that avoids function call overhead
#[derive(Debug)]
pub struct MatchesConstantTermOptimizer;

impl PhysicalOptimizerRule for MatchesConstantTermOptimizer {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let res = plan
            .transform_down(&|plan: Arc<dyn ExecutionPlan>| {
                if let Some(filter) = plan.as_any().downcast_ref::<FilterExec>() {
                    let pred = filter.predicate().clone();
                    let new_pred = pred.transform_down(&|expr: Arc<dyn PhysicalExpr>| {
                        if let Some(func) = expr.as_any().downcast_ref::<ScalarFunctionExpr>() {
                            if !func.name().eq_ignore_ascii_case("matches_term") {
                                return Ok(Transformed::no(expr));
                            }
                            let args = func.args();
                            if args.len() != 2 {
                                return Ok(Transformed::no(expr));
                            }

                            if let Some(lit) = args[1].as_any().downcast_ref::<Literal>() {
                                if let ScalarValue::Utf8(Some(term)) = lit.value() {
                                    let finder = MatchesTermFinder::new(term);

                                    // For debugging purpose. Not really precise but enough for most cases.
                                    let probes = term
                                        .split(|c: char| !c.is_alphanumeric() && c != '_')
                                        .filter(|s| !s.is_empty())
                                        .map(|s| s.to_string())
                                        .collect();

                                    let expr = PreCompiledMatchesTermExpr {
                                        text: args[0].clone(),
                                        term: term.to_string(),
                                        finder,
                                        probes,
                                    };

                                    return Ok(Transformed::yes(Arc::new(expr)));
                                }
                            }
                        }

                        Ok(Transformed::no(expr))
                    })?;

                    if new_pred.transformed {
                        let exec = FilterExec::try_new(new_pred.data, filter.input().clone())?
                            .with_default_selectivity(filter.default_selectivity())?
                            .with_projection(filter.projection().cloned())?;
                        return Ok(Transformed::yes(Arc::new(exec) as _));
                    }
                }

                Ok(Transformed::no(plan))
            })?
            .data;

        Ok(res)
    }

    fn name(&self) -> &str {
        "MatchesConstantTerm"
    }

    fn schema_check(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use catalog::memory::MemoryCatalogManager;
    use catalog::RegisterTableRequest;
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use common_function::scalars::matches_term::MatchesTermFunction;
    use common_function::scalars::udf::create_udf;
    use common_function::state::FunctionState;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::get_plan_string;
    use datafusion_common::{Column, DFSchema};
    use datafusion_expr::expr::ScalarFunction;
    use datafusion_expr::{Expr, Literal, ScalarUDF};
    use datafusion_physical_expr::{create_physical_expr, ScalarFunctionExpr};
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use session::context::QueryContext;
    use table::metadata::{TableInfoBuilder, TableMetaBuilder};
    use table::test_util::EmptyTable;

    use super::*;
    use crate::parser::QueryLanguageParser;
    use crate::{QueryEngineFactory, QueryEngineRef};

    fn create_test_batch() -> RecordBatch {
        let schema = Schema::new(vec![Field::new("text", DataType::Utf8, true)]);

        let text_array = StringArray::from(vec![
            Some("hello world"),
            Some("greeting"),
            Some("hello there"),
            None,
        ]);

        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(text_array) as ArrayRef]).unwrap()
    }

    fn create_test_engine() -> QueryEngineRef {
        let table_name = "test".to_string();
        let columns = vec![
            ColumnSchema::new(
                "text".to_string(),
                ConcreteDataType::string_datatype(),
                false,
            ),
            ColumnSchema::new(
                "timestamp".to_string(),
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
        ];

        let schema = Arc::new(datatypes::schema::Schema::new(columns));
        let table_meta = TableMetaBuilder::empty()
            .schema(schema)
            .primary_key_indices(vec![])
            .value_indices(vec![0])
            .next_column_id(2)
            .build()
            .unwrap();
        let table_info = TableInfoBuilder::default()
            .name(&table_name)
            .meta(table_meta)
            .build()
            .unwrap();
        let table = EmptyTable::from_table_info(&table_info);
        let catalog_list = MemoryCatalogManager::with_default_setup();
        assert!(catalog_list
            .register_table_sync(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name,
                table_id: 1024,
                table,
            })
            .is_ok());
        QueryEngineFactory::new(
            catalog_list,
            None,
            None,
            None,
            None,
            false,
            Default::default(),
        )
        .query_engine()
    }

    fn matches_term_udf() -> Arc<ScalarUDF> {
        Arc::new(create_udf(
            Arc::new(MatchesTermFunction),
            QueryContext::arc(),
            Arc::new(FunctionState::default()),
        ))
    }

    #[test]
    fn test_matches_term_optimization() {
        let batch = create_test_batch();

        // Create a predicate with a constant pattern
        let predicate = create_physical_expr(
            &Expr::ScalarFunction(ScalarFunction::new_udf(
                matches_term_udf(),
                vec![Expr::Column(Column::from_name("text")), "hello".lit()],
            )),
            &DFSchema::try_from(batch.schema().clone()).unwrap(),
            &Default::default(),
        )
        .unwrap();

        let input = DataSourceExec::from_data_source(
            MemorySourceConfig::try_new(&[vec![batch.clone()]], batch.schema(), None).unwrap(),
        );
        let filter = FilterExec::try_new(predicate, input).unwrap();

        // Apply the optimizer
        let optimizer = MatchesConstantTermOptimizer;
        let optimized_plan = optimizer
            .optimize(Arc::new(filter), &Default::default())
            .unwrap();

        let optimized_filter = optimized_plan
            .as_any()
            .downcast_ref::<FilterExec>()
            .unwrap();
        let predicate = optimized_filter.predicate();

        // The predicate should be a PreCompiledMatchesTermExpr
        assert!(
            std::any::TypeId::of::<PreCompiledMatchesTermExpr>() == predicate.as_any().type_id()
        );
    }

    #[test]
    fn test_matches_term_no_optimization() {
        let batch = create_test_batch();

        // Create a predicate with a non-constant pattern
        let predicate = create_physical_expr(
            &Expr::ScalarFunction(ScalarFunction::new_udf(
                matches_term_udf(),
                vec![
                    Expr::Column(Column::from_name("text")),
                    Expr::Column(Column::from_name("text")),
                ],
            )),
            &DFSchema::try_from(batch.schema().clone()).unwrap(),
            &Default::default(),
        )
        .unwrap();

        let input = DataSourceExec::from_data_source(
            MemorySourceConfig::try_new(&[vec![batch.clone()]], batch.schema(), None).unwrap(),
        );
        let filter = FilterExec::try_new(predicate, input).unwrap();

        let optimizer = MatchesConstantTermOptimizer;
        let optimized_plan = optimizer
            .optimize(Arc::new(filter), &Default::default())
            .unwrap();

        let optimized_filter = optimized_plan
            .as_any()
            .downcast_ref::<FilterExec>()
            .unwrap();
        let predicate = optimized_filter.predicate();

        // The predicate should still be a ScalarFunctionExpr
        assert!(std::any::TypeId::of::<ScalarFunctionExpr>() == predicate.as_any().type_id());
    }

    #[tokio::test]
    async fn test_matches_term_optimization_from_sql() {
        let sql = "WITH base AS (
        SELECT text, timestamp FROM test 
        WHERE MATCHES_TERM(text, 'hello wo_rld') 
        AND timestamp > '2025-01-01 00:00:00'
    ),
    subquery1 AS (
        SELECT * FROM base 
        WHERE MATCHES_TERM(text, 'world')
    ),
    subquery2 AS (
        SELECT * FROM test 
        WHERE MATCHES_TERM(text, 'greeting') 
        AND timestamp < '2025-01-02 00:00:00'
    ),
    union_result AS (
        SELECT * FROM subquery1 
        UNION ALL 
        SELECT * FROM subquery2
    ),
    joined_data AS (
        SELECT a.text, a.timestamp, b.text as other_text 
        FROM union_result a 
        JOIN test b ON a.timestamp = b.timestamp 
        WHERE MATCHES_TERM(a.text, 'there')
    )
    SELECT text, other_text 
    FROM joined_data 
    WHERE MATCHES_TERM(text, '42') 
    AND MATCHES_TERM(other_text, 'foo')";

        let query_ctx = QueryContext::arc();

        let stmt = QueryLanguageParser::parse_sql(sql, &query_ctx).unwrap();
        let engine = create_test_engine();
        let logical_plan = engine
            .planner()
            .plan(&stmt, query_ctx.clone())
            .await
            .unwrap();

        let engine_ctx = engine.engine_context(query_ctx);
        let state = engine_ctx.state();

        let analyzed_plan = state
            .analyzer()
            .execute_and_check(logical_plan.clone(), state.config_options(), |_, _| {})
            .unwrap();

        let optimized_plan = state
            .optimizer()
            .optimize(analyzed_plan, state, |_, _| {})
            .unwrap();

        let physical_plan = state
            .query_planner()
            .create_physical_plan(&optimized_plan, state)
            .await
            .unwrap();

        let plan_str = get_plan_string(&physical_plan).join("\n");
        assert!(plan_str.contains("MatchesConstTerm(text@0, term: \"foo\", probes: [\"foo\"]"));
        assert!(plan_str.contains(
            "MatchesConstTerm(text@0, term: \"hello wo_rld\", probes: [\"hello\", \"wo_rld\"]"
        ));
        assert!(plan_str.contains("MatchesConstTerm(text@0, term: \"world\", probes: [\"world\"]"));
        assert!(plan_str
            .contains("MatchesConstTerm(text@0, term: \"greeting\", probes: [\"greeting\"]"));
        assert!(plan_str.contains("MatchesConstTerm(text@0, term: \"there\", probes: [\"there\"]"));
        assert!(plan_str.contains("MatchesConstTerm(text@0, term: \"42\", probes: [\"42\"]"));
        assert!(!plan_str.contains("matches_term"))
    }
}
