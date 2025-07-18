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

use datafusion::datasource::DefaultTableSource;
use datafusion::functions_aggregate::expr_fn::avg;
use datafusion::functions_aggregate::min_max::min;
use datafusion_common::JoinType;
use datafusion_expr::{col, lit, Expr, LogicalPlanBuilder};
use datafusion_sql::TableReference;
use table::table::adapter::DfTableProviderAdapter;
use table::table::numbers::NumbersTable;

use super::*;

/// test plan like:
/// ```
/// Aggregate: min(t.number)
///  Projection: t.number
/// ```
/// which means aggr introduce new column requirements that shouldn't be updated in lower projection
///
/// this help test expand need actually add new column requirements
/// because `Sort`/`Limit` doesn't introduce new column requirements
/// only `Aggregate` does
#[test]
fn expand_step_aggr_with_not_in_scope_proj() {
    // TODO(discord9): change to partitioned table
    let numbers_table = NumbersTable::table(0);
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(numbers_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .project(vec![Expr::Column(Column::new(
            Some(TableReference::bare("t")),
            "number",
        ))])
        .unwrap()
        .aggregate(Vec::<Expr>::new(), vec![min(col("number"))])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    println!("result: {result}");
    let expected = [
        "Projection: min(t.number)",
        "  MergeScan [is_placeholder=false]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn expand_step_proj_then_sort() {
    let numbers_table = NumbersTable::table(0);
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(numbers_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .project(vec![Expr::Column(Column::new(
            Some(TableReference::bare("t")),
            "number",
        ))])
        .unwrap()
        .aggregate(Vec::<Expr>::new(), vec![min(col("number"))])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    let expected = [
        "Projection: min(t.number)",
        "  MergeScan [is_placeholder=false]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[ignore = "Projection is disabled for https://github.com/apache/arrow-datafusion/issues/6489"]
#[test]
fn transform_simple_projection_filter() {
    let numbers_table = NumbersTable::table(0);
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(numbers_table),
    )));

    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .filter(col("number").lt(lit(10)))
        .unwrap()
        .project(vec![col("number")])
        .unwrap()
        .distinct()
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    let expected = [
        "Distinct:",
        "  MergeScan [is_placeholder=false]",
        "    Distinct:",
        "      Projection: t.number",
        "        Filter: t.number < Int32(10)",
        "          TableScan: t",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn transform_aggregator() {
    let numbers_table = NumbersTable::table(0);
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(numbers_table),
    )));

    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .aggregate(Vec::<Expr>::new(), vec![avg(col("number"))])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    let expected = "Projection: avg(t.number)\
        \n  MergeScan [is_placeholder=false]";
    assert_eq!(expected, result.to_string());
}

#[test]
fn transform_distinct_order() {
    let numbers_table = NumbersTable::table(0);
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(numbers_table),
    )));

    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .distinct()
        .unwrap()
        .sort(vec![col("number").sort(true, false)])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    let expected = ["Projection: t.number", "  MergeScan [is_placeholder=false]"].join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn transform_single_limit() {
    let numbers_table = NumbersTable::table(0);
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(numbers_table),
    )));

    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .limit(0, Some(1))
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    let expected = "Projection: t.number\
        \n  MergeScan [is_placeholder=false]";
    assert_eq!(expected, result.to_string());
}

#[test]
fn transform_unalighed_join_with_alias() {
    let left = NumbersTable::table(0);
    let right = NumbersTable::table(1);
    let left_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(left),
    )));
    let right_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(right),
    )));

    let right_plan = LogicalPlanBuilder::scan_with_filters("t", right_source, None, vec![])
        .unwrap()
        .alias("right")
        .unwrap()
        .build()
        .unwrap();

    let plan = LogicalPlanBuilder::scan_with_filters("t", left_source, None, vec![])
        .unwrap()
        .join_on(
            right_plan,
            JoinType::LeftSemi,
            vec![col("t.number").eq(col("right.number"))],
        )
        .unwrap()
        .limit(0, Some(1))
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    let expected = [
        "Limit: skip=0, fetch=1",
        "  LeftSemi Join:  Filter: t.number = right.number",
        "    Projection: t.number",
        "      MergeScan [is_placeholder=false]",
        "    SubqueryAlias: right",
        "      Projection: t.number",
        "        MergeScan [is_placeholder=false]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}
