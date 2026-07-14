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

use datafusion::datasource::DefaultTableSource;
use datafusion_common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion, TreeNodeVisitor,
};
use datafusion_common::{Column, Result as DataFusionResult, ScalarValue};
use datafusion_expr::expr::{AggregateFunction, WindowFunction};
use datafusion_expr::utils::COUNT_STAR_EXPANSION;
use datafusion_expr::{Expr, LogicalPlan, WindowFunctionDefinition, col, lit};
use datafusion_optimizer::AnalyzerRule;
use datafusion_optimizer::utils::NamePreserver;
use datafusion_sql::TableReference;
use table::table::adapter::DfTableProviderAdapter;

/// A replacement to DataFusion's [`CountWildcardRule`]. This rule
/// would prefer to use TIME INDEX for counting wildcard as it's
/// faster to read comparing to PRIMARY KEYs.
///
/// [`CountWildcardRule`]: datafusion::optimizer::analyzer::CountWildcardRule
#[derive(Debug)]
pub struct CountWildcardToTimeIndexRule;

impl AnalyzerRule for CountWildcardToTimeIndexRule {
    fn name(&self) -> &str {
        "count_wildcard_to_time_index_rule"
    }

    fn analyze(
        &self,
        plan: LogicalPlan,
        _config: &datafusion::config::ConfigOptions,
    ) -> DataFusionResult<LogicalPlan> {
        plan.transform_down_with_subqueries(&Self::analyze_internal)
            .data()
    }
}

impl CountWildcardToTimeIndexRule {
    fn analyze_internal(plan: LogicalPlan) -> DataFusionResult<Transformed<LogicalPlan>> {
        let name_preserver = NamePreserver::new(&plan);
        let new_arg = if let Some(time_index) = Self::try_find_time_index_col(&plan) {
            vec![col(time_index)]
        } else {
            vec![lit(COUNT_STAR_EXPANSION)]
        };
        plan.map_expressions(|expr| {
            let original_name = name_preserver.save(&expr);
            let transformed_expr = expr.transform_up(|expr| match expr {
                Expr::WindowFunction(mut window_function)
                    if Self::is_count_star_window_aggregate(&window_function) =>
                {
                    window_function.params.args.clone_from(&new_arg);
                    Ok(Transformed::yes(Expr::WindowFunction(window_function)))
                }
                Expr::AggregateFunction(mut aggregate_function)
                    if Self::is_count_star_aggregate(&aggregate_function) =>
                {
                    aggregate_function.params.args.clone_from(&new_arg);
                    Ok(Transformed::yes(Expr::AggregateFunction(
                        aggregate_function,
                    )))
                }
                _ => Ok(Transformed::no(expr)),
            })?;
            Ok(transformed_expr.update_data(|data| original_name.restore(data)))
        })
    }

    fn try_find_time_index_col(plan: &LogicalPlan) -> Option<Column> {
        let mut finder = TimeIndexFinder::default();
        // Safety: `TimeIndexFinder` won't throw error.
        plan.visit(&mut finder).unwrap();
        let col = finder.into_column();

        // The resolved time index must be present and non-nullable in the
        // immediate input schema. Schema-changing nodes can otherwise expose
        // a nullable field with the same name as the source time index.
        if let Some(col) = &col {
            // if more than one input, we give up and just use `count(1)`
            if plan.inputs().len() > 1 {
                return None;
            }
            let Some(input) = plan.inputs().first().copied() else {
                return None;
            };
            let Ok((_, field)) = input.schema().qualified_field_from_column(col) else {
                return None;
            };
            if field.is_nullable() {
                return None;
            }
        }

        col
    }
}

/// Utility functions from the original rule.
impl CountWildcardToTimeIndexRule {
    #[expect(deprecated)]
    fn args_at_most_wildcard_or_literal_one(args: &[Expr]) -> bool {
        match args {
            [] => true,
            [Expr::Literal(ScalarValue::Int64(Some(v)), _)] => *v == 1,
            [Expr::Wildcard { .. }] => true,
            _ => false,
        }
    }

    fn is_count_star_aggregate(aggregate_function: &AggregateFunction) -> bool {
        let args = &aggregate_function.params.args;
        matches!(aggregate_function,
            AggregateFunction {
                func,
                ..
            } if func.name() == "count" && Self::args_at_most_wildcard_or_literal_one(args))
    }

    fn is_count_star_window_aggregate(window_function: &WindowFunction) -> bool {
        let args = &window_function.params.args;
        matches!(window_function.fun,
                WindowFunctionDefinition::AggregateUDF(ref udaf)
                    if udaf.name() == "count" && Self::args_at_most_wildcard_or_literal_one(args))
    }
}

#[derive(Default)]
struct TimeIndexFinder {
    time_index_col: Option<String>,
    table_alias: Option<TableReference>,
    has_projection: bool,
}

impl TreeNodeVisitor<'_> for TimeIndexFinder {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: &Self::Node) -> DataFusionResult<TreeNodeRecursion> {
        // A projection may rename or replace the time index. We only know the
        // source metadata, so do not rewrite through an unproven projection.
        if matches!(node, LogicalPlan::Projection(_)) {
            self.has_projection = true;
        }

        if let LogicalPlan::SubqueryAlias(subquery_alias) = node {
            self.table_alias = Some(subquery_alias.alias.clone());
        }

        if let LogicalPlan::TableScan(table_scan) = &node
            && let Some(source) = table_scan
                .source
                .as_any()
                .downcast_ref::<DefaultTableSource>()
            && let Some(adapter) = source
                .table_provider
                .as_any()
                .downcast_ref::<DfTableProviderAdapter>()
        {
            let table_info = adapter.table().table_info();
            self.table_alias
                .get_or_insert(table_scan.table_name.clone());
            self.time_index_col = table_info
                .meta
                .schema
                .timestamp_column()
                .map(|c| c.name.clone());

            return Ok(TreeNodeRecursion::Stop);
        }

        if node.inputs().len() > 1 {
            // if more than one input, we give up and just use `count(1)`
            return Ok(TreeNodeRecursion::Stop);
        }

        Ok(TreeNodeRecursion::Continue)
    }

    fn f_up(&mut self, _node: &Self::Node) -> DataFusionResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Stop)
    }
}

impl TimeIndexFinder {
    fn into_column(self) -> Option<Column> {
        (!self.has_projection)
            .then(|| {
                self.time_index_col
                    .map(|c| Column::new(self.table_alias, c))
            })
            .flatten()
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use common_catalog::consts::DEFAULT_CATALOG_NAME;
    use common_error::ext::{BoxedError, ErrorExt, StackError};
    use common_error::status_code::StatusCode;
    use common_recordbatch::{RecordBatch, SendableRecordBatchStream};
    use datafusion::functions_aggregate::count::count_all;
    use datafusion::functions_aggregate::min_max::max;
    use datafusion_common::Column;
    use datafusion_expr::LogicalPlanBuilder;
    use datafusion_sql::TableReference;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema, SchemaBuilder};
    use datatypes::vectors::{Int64Vector, TimestampMillisecondVector, VectorRef};
    use store_api::data_source::DataSource;
    use store_api::storage::ScanRequest;
    use table::metadata::{FilterPushDownType, TableInfoBuilder, TableMetaBuilder, TableType};
    use table::table::numbers::NumbersTable;
    use table::test_util::MemTable;
    use table::{Table, TableRef};

    use super::*;

    #[test]
    fn uppercase_table_name() {
        let numbers_table = NumbersTable::table_with_name(0, "AbCdE".to_string());
        let table_source = Arc::new(DefaultTableSource::new(Arc::new(
            DfTableProviderAdapter::new(numbers_table),
        )));

        let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
            .unwrap()
            .aggregate(Vec::<Expr>::new(), vec![count_all()])
            .unwrap()
            .alias(r#""FgHiJ""#)
            .unwrap()
            .build()
            .unwrap();

        let mut finder = TimeIndexFinder::default();
        plan.visit(&mut finder).unwrap();

        assert_eq!(finder.table_alias, Some(TableReference::bare("FgHiJ")));
        assert!(finder.time_index_col.is_none());
    }

    #[test]
    fn bare_table_name_time_index() {
        let table_ref = TableReference::bare("multi_partitioned_test_1");
        let table =
            build_time_index_table("multi_partitioned_test_1", "public", DEFAULT_CATALOG_NAME);
        let table_source = Arc::new(DefaultTableSource::new(Arc::new(
            DfTableProviderAdapter::new(table),
        )));

        let plan =
            LogicalPlanBuilder::scan_with_filters(table_ref.clone(), table_source, None, vec![])
                .unwrap()
                .aggregate(Vec::<Expr>::new(), vec![count_all()])
                .unwrap()
                .build()
                .unwrap();

        let time_index = CountWildcardToTimeIndexRule::try_find_time_index_col(&plan);
        assert_eq!(
            time_index,
            Some(Column::new(Some(table_ref), "greptime_timestamp"))
        );
    }

    #[test]
    fn schema_qualified_table_name_time_index() {
        let table_ref = TableReference::partial("telemetry_events", "multi_partitioned_test_1");
        let table = build_time_index_table(
            "multi_partitioned_test_1",
            "telemetry_events",
            DEFAULT_CATALOG_NAME,
        );
        let table_source = Arc::new(DefaultTableSource::new(Arc::new(
            DfTableProviderAdapter::new(table),
        )));

        let plan =
            LogicalPlanBuilder::scan_with_filters(table_ref.clone(), table_source, None, vec![])
                .unwrap()
                .aggregate(Vec::<Expr>::new(), vec![count_all()])
                .unwrap()
                .build()
                .unwrap();

        let time_index = CountWildcardToTimeIndexRule::try_find_time_index_col(&plan);
        assert_eq!(
            time_index,
            Some(Column::new(Some(table_ref), "greptime_timestamp"))
        );
    }

    #[test]
    fn fully_qualified_table_name_time_index() {
        let table_ref = TableReference::full(
            "telemetry_catalog",
            "telemetry_events",
            "multi_partitioned_test_1",
        );
        let table = build_time_index_table(
            "multi_partitioned_test_1",
            "telemetry_events",
            "telemetry_catalog",
        );
        let table_source = Arc::new(DefaultTableSource::new(Arc::new(
            DfTableProviderAdapter::new(table),
        )));

        let plan =
            LogicalPlanBuilder::scan_with_filters(table_ref.clone(), table_source, None, vec![])
                .unwrap()
                .aggregate(Vec::<Expr>::new(), vec![count_all()])
                .unwrap()
                .build()
                .unwrap();

        let time_index = CountWildcardToTimeIndexRule::try_find_time_index_col(&plan);
        assert_eq!(
            time_index,
            Some(Column::new(Some(table_ref), "greptime_timestamp"))
        );
    }

    #[test]
    fn qp_026_count_wildcard_shape_matrix() {
        let config = datafusion::config::ConfigOptions::default();

        let direct = CountWildcardToTimeIndexRule
            .analyze(count_star(qp_026_source_plan("source")), &config)
            .unwrap();
        assert_count_argument_column(&direct, "source", "ts");

        let simple_alias = count_star(
            LogicalPlanBuilder::from(qp_026_source_plan("source"))
                .alias("projected")
                .unwrap()
                .build()
                .unwrap(),
        );
        let simple_alias = CountWildcardToTimeIndexRule
            .analyze(simple_alias, &config)
            .unwrap();
        assert_count_argument_column(&simple_alias, "projected", "ts");

        let nested_rename = count_star(
            LogicalPlanBuilder::from(qp_026_source_plan("source"))
                .project(vec![col("ts").alias("renamed")])
                .unwrap()
                .alias("projected")
                .unwrap()
                .build()
                .unwrap(),
        );
        let nested_rename = CountWildcardToTimeIndexRule
            .analyze(nested_rename, &config)
            .unwrap();
        assert_count_argument_literal_one(&nested_rename);

        let nested_rename_with_payload_reorder = count_star(
            LogicalPlanBuilder::from(qp_026_source_plan("source"))
                .project(vec![col("payload"), col("ts").alias("renamed")])
                .unwrap()
                .alias("projected")
                .unwrap()
                .build()
                .unwrap(),
        );
        let nested_rename_with_payload_reorder = CountWildcardToTimeIndexRule
            .analyze(nested_rename_with_payload_reorder, &config)
            .unwrap();
        assert_count_argument_literal_one(&nested_rename_with_payload_reorder);

        let multi_input = count_star(
            LogicalPlanBuilder::from(qp_026_source_plan("left"))
                .cross_join(qp_026_source_plan("right"))
                .unwrap()
                .build()
                .unwrap(),
        );
        let multi_input = CountWildcardToTimeIndexRule
            .analyze(multi_input, &config)
            .unwrap();
        assert_count_argument_literal_one(&multi_input);
    }

    #[test]
    fn qp_026_projection_name_collision_falls_back_to_literal_one() {
        let before = count_star(
            LogicalPlanBuilder::from(qp_026_source_plan("source"))
                .project(vec![col("payload").alias("ts")])
                .unwrap()
                .alias("projected")
                .unwrap()
                .build()
                .unwrap(),
        );

        let aggregate = aggregate_plan(&before);
        let field = aggregate
            .input
            .schema()
            .qualified_field_with_name(Some(&TableReference::bare("projected")), "ts")
            .unwrap();
        assert!(field.1.is_nullable());

        let after = CountWildcardToTimeIndexRule
            .analyze(before, &datafusion::config::ConfigOptions::default())
            .unwrap();
        assert_count_argument_literal_one(&after);
    }

    #[test]
    fn qp_026_inner_aggregate_nullable_time_index_name_falls_back_to_literal_one() {
        let before = count_star(
            LogicalPlanBuilder::from(qp_026_source_plan("source"))
                .aggregate(Vec::<Expr>::new(), vec![max(col("payload")).alias("ts")])
                .unwrap()
                .alias("aggregated")
                .unwrap()
                .build()
                .unwrap(),
        );

        let aggregate = aggregate_plan(&before);
        let field = aggregate
            .input
            .schema()
            .qualified_field_with_name(Some(&TableReference::bare("aggregated")), "ts")
            .unwrap();
        assert!(field.1.is_nullable());

        let after = CountWildcardToTimeIndexRule
            .analyze(before, &datafusion::config::ConfigOptions::default())
            .unwrap();
        assert_count_argument_literal_one(&after);
    }

    fn qp_026_source_plan(table_name: &str) -> LogicalPlan {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new("payload", ConcreteDataType::int64_datatype(), true),
        ]));
        let columns: Vec<VectorRef> = vec![
            Arc::new(TimestampMillisecondVector::from_slice([1, 2, 3])),
            Arc::new(Int64Vector::from(vec![Some(10), None, Some(30)])),
        ];
        let table = MemTable::table(
            table_name,
            RecordBatch::new(schema, columns).expect("QP-026 test record batch must be valid"),
        );
        let source = Arc::new(DefaultTableSource::new(Arc::new(
            DfTableProviderAdapter::new(table),
        )));
        LogicalPlanBuilder::scan_with_filters(table_name, source, None, vec![])
            .unwrap()
            .build()
            .unwrap()
    }

    fn count_star(input: LogicalPlan) -> LogicalPlan {
        LogicalPlanBuilder::from(input)
            .aggregate(Vec::<Expr>::new(), vec![count_all()])
            .unwrap()
            .build()
            .unwrap()
    }

    fn count_aggregate(plan: &LogicalPlan) -> &AggregateFunction {
        let LogicalPlan::Aggregate(aggregate) = plan else {
            panic!("expected aggregate plan, got {plan:?}");
        };
        assert_eq!(1, aggregate.aggr_expr.len());
        let expr = unwrap_aliases(&aggregate.aggr_expr[0]);
        let Expr::AggregateFunction(count) = expr else {
            panic!("expected count aggregate, got {:?}", aggregate.aggr_expr[0]);
        };
        assert_eq!("count", count.func.name());
        count
    }

    fn unwrap_aliases(expr: &Expr) -> &Expr {
        match expr {
            Expr::Alias(alias) => unwrap_aliases(alias.expr.as_ref()),
            expr => expr,
        }
    }

    fn assert_count_argument_column(plan: &LogicalPlan, relation: &str, name: &str) {
        let count = count_aggregate(plan);
        let [Expr::Column(column)] = count.params.args.as_slice() else {
            panic!(
                "expected one column count argument, got {:?}",
                count.params.args
            );
        };
        assert_eq!(Some(TableReference::bare(relation)), column.relation);
        assert_eq!(name, column.name);
    }

    fn assert_count_argument_literal_one(plan: &LogicalPlan) {
        let count = count_aggregate(plan);
        assert!(matches!(
            count.params.args.as_slice(),
            [Expr::Literal(ScalarValue::Int64(Some(1)), _)]
        ));
    }

    fn aggregate_plan(plan: &LogicalPlan) -> &datafusion_expr::logical_plan::Aggregate {
        let LogicalPlan::Aggregate(aggregate) = plan else {
            panic!("expected aggregate plan, got {plan:?}");
        };
        aggregate
    }

    fn build_time_index_table(table_name: &str, schema_name: &str, catalog_name: &str) -> TableRef {
        let column_schemas = vec![
            ColumnSchema::new(
                "greptime_timestamp",
                ConcreteDataType::timestamp_nanosecond_datatype(),
                false,
            )
            .with_time_index(true),
        ];
        let schema = SchemaBuilder::try_from_columns(column_schemas)
            .unwrap()
            .build()
            .unwrap();
        let meta = TableMetaBuilder::new_external_table()
            .schema(Arc::new(schema))
            .next_column_id(1)
            .build()
            .unwrap();
        let info = TableInfoBuilder::new(table_name.to_string(), meta)
            .table_id(1)
            .table_version(0)
            .catalog_name(catalog_name)
            .schema_name(schema_name)
            .table_type(TableType::Base)
            .build()
            .unwrap();
        let data_source = Arc::new(DummyDataSource);
        Arc::new(Table::new(
            Arc::new(info),
            FilterPushDownType::Unsupported,
            data_source,
        ))
    }

    struct DummyDataSource;

    impl DataSource for DummyDataSource {
        fn get_stream(
            &self,
            _request: ScanRequest,
        ) -> Result<SendableRecordBatchStream, BoxedError> {
            Err(BoxedError::new(DummyDataSourceError))
        }
    }

    #[derive(Debug)]
    struct DummyDataSourceError;

    impl std::fmt::Display for DummyDataSourceError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "dummy data source error")
        }
    }

    impl std::error::Error for DummyDataSourceError {}

    impl StackError for DummyDataSourceError {
        fn debug_fmt(&self, _: usize, _: &mut Vec<String>) {}

        fn next(&self) -> Option<&dyn StackError> {
            None
        }
    }

    impl ErrorExt for DummyDataSourceError {
        fn status_code(&self) -> StatusCode {
            StatusCode::Internal
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }
}
