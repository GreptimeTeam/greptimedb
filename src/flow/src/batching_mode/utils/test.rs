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

use common_recordbatch::RecordBatch;
use common_time::Timestamp;
use datafusion_common::tree_node::TreeNode as _;
use datafusion_expr::GroupingSet;
use datatypes::prelude::{ConcreteDataType, MutableVector, Scalar, ScalarVectorBuilder, VectorRef};
use datatypes::schema::{ColumnSchema, Schema};
use datatypes::timestamp::TimestampMillisecond;
use datatypes::vectors::TimestampMillisecondVectorBuilder;
use pretty_assertions::assert_eq;
use query::query_engine::DefaultSerializer;
use session::context::QueryContext;
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};
use table::test_util::MemTable;

use super::*;
use crate::batching_mode::state::FilterExprInfo;
use crate::test_utils::create_test_query_engine;

fn u32_table(table_name: &str, columns: Vec<&str>, rows: usize) -> TableRef {
    let column_schemas = columns
        .iter()
        .map(|name| ColumnSchema::new(*name, ConcreteDataType::uint32_datatype(), false))
        .collect::<Vec<_>>();
    let vectors = columns
        .iter()
        .map(|_| Arc::new(<u32 as Scalar>::VectorType::from_vec(vec![1; rows])) as VectorRef)
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(column_schemas));
    let recordbatch = RecordBatch::new(schema, vectors).unwrap();
    MemTable::table(table_name, recordbatch)
}

fn single_row_u32_table(table_name: &str, columns: Vec<&str>) -> TableRef {
    u32_table(table_name, columns, 1)
}

fn empty_u32_table(table_name: &str, columns: Vec<&str>) -> TableRef {
    u32_table(table_name, columns, 0)
}

fn time_window_u32_table(table_name: &str) -> TableRef {
    let schema = Arc::new(Schema::new(vec![
        ColumnSchema::new(
            "time_window",
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        )
        .with_time_index(true),
        ColumnSchema::new("number", ConcreteDataType::uint32_datatype(), true),
    ]));

    let mut time_window_builder = TimestampMillisecondVectorBuilder::with_capacity(1);
    time_window_builder.push(Some(TimestampMillisecond::new(0)));
    let recordbatch = RecordBatch::new(
        schema,
        vec![
            time_window_builder.to_vector_cloned(),
            Arc::new(<u32 as Scalar>::VectorType::from_vec(vec![1])) as VectorRef,
        ],
    )
    .unwrap();
    MemTable::table(table_name, recordbatch)
}

fn assert_same_logical_plan(actual: &LogicalPlan, expected: &LogicalPlan) {
    assert_eq!(
        format!("{}", expected.display_indent()),
        format!("{}", actual.display_indent())
    );
}

fn test_sink_scan(sink_table: TableRef, sink_table_name: &TableName) -> LogicalPlan {
    let table_provider = Arc::new(DfTableProviderAdapter::new(sink_table));
    let table_source = Arc::new(DefaultTableSource::new(table_provider));
    LogicalPlan::TableScan(
        TableScan::try_new(
            TableReference::Full {
                catalog: sink_table_name[0].clone().into(),
                schema: sink_table_name[1].clone().into(),
                table: sink_table_name[2].clone().into(),
            },
            table_source,
            None,
            vec![],
            None,
        )
        .unwrap(),
    )
}

fn expected_left_join_rewrite(
    delta_plan: &LogicalPlan,
    sink_table: TableRef,
    sink_table_name: &TableName,
    delta_selected_exprs: Vec<Expr>,
    sink_selected_exprs: Vec<Expr>,
    join_keys: (Vec<Column>, Vec<Column>),
    projection_exprs: Vec<Expr>,
) -> LogicalPlan {
    expected_left_join_rewrite_with_sink_filter(
        delta_plan,
        sink_table,
        sink_table_name,
        delta_selected_exprs,
        sink_selected_exprs,
        None,
        join_keys,
        projection_exprs,
    )
}

#[allow(clippy::too_many_arguments)]
fn expected_left_join_rewrite_with_sink_filter(
    delta_plan: &LogicalPlan,
    sink_table: TableRef,
    sink_table_name: &TableName,
    delta_selected_exprs: Vec<Expr>,
    sink_selected_exprs: Vec<Expr>,
    sink_filter: Option<Expr>,
    join_keys: (Vec<Column>, Vec<Column>),
    projection_exprs: Vec<Expr>,
) -> LogicalPlan {
    let delta_alias = "__flow_delta";
    let sink_alias = "__flow_sink";
    let delta_selected = LogicalPlanBuilder::from(delta_plan.clone())
        .project(delta_selected_exprs)
        .unwrap()
        .alias(delta_alias)
        .unwrap()
        .build()
        .unwrap();
    let sink_scan = test_sink_scan(sink_table, sink_table_name);
    let sink_input = if let Some(predicate) = sink_filter {
        LogicalPlanBuilder::from(sink_scan)
            .filter(predicate)
            .unwrap()
            .build()
            .unwrap()
    } else {
        sink_scan
    };
    let sink_selected = LogicalPlanBuilder::from(sink_input)
        .project(sink_selected_exprs)
        .unwrap()
        .alias(sink_alias)
        .unwrap()
        .build()
        .unwrap();
    let joined = LogicalPlanBuilder::from(delta_selected)
        .join_detailed(
            sink_selected,
            JoinType::Left,
            join_keys,
            None,
            NullEquality::NullEqualsNull,
        )
        .unwrap()
        .build()
        .unwrap();
    LogicalPlanBuilder::from(joined)
        .project(projection_exprs)
        .unwrap()
        .build()
        .unwrap()
}

fn max_merge_expr(field_name: &str) -> Expr {
    let left = qualified_col("__flow_delta", field_name);
    let right = qualified_col("__flow_sink", field_name);
    when(is_null(right.clone()), left.clone())
        .when(left.clone().gt_eq(right.clone()), left)
        .otherwise(right)
        .unwrap()
        .alias(field_name)
}

fn sum_merge_expr(field_name: &str) -> Expr {
    let left = qualified_col("__flow_delta", field_name);
    let right = qualified_col("__flow_sink", field_name);
    when(is_null(left.clone()), right.clone())
        .when(is_null(right.clone()), left.clone())
        .otherwise(binary_expr(left, Operator::Plus, right))
        .unwrap()
        .alias(field_name)
}

async fn analyze_test_sql(sql: &str) -> IncrementalAggregateAnalysis {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let plan = sql_to_df_plan(ctx, query_engine, sql, false).await.unwrap();
    analyze_incremental_aggregate_plan(&plan).unwrap().unwrap()
}

async fn analyze_grouping_set_plan(
    make_grouping_set: impl FnOnce(Expr) -> GroupingSet,
) -> IncrementalAggregateAnalysis {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let plan = sql_to_df_plan(
        ctx,
        query_engine,
        "SELECT sum(number) AS number, ts FROM numbers_with_ts GROUP BY ts",
        false,
    )
    .await
    .unwrap();

    let LogicalPlan::Projection(projection) = plan else {
        panic!("expected projection over aggregate")
    };
    let LogicalPlan::Aggregate(aggregate) = projection.input.as_ref() else {
        panic!("expected aggregate below projection")
    };
    let group_expr = aggregate.group_expr[0].clone();
    let grouping_set_aggregate = datafusion_expr::logical_plan::Aggregate::try_new(
        aggregate.input.clone(),
        vec![Expr::GroupingSet(make_grouping_set(group_expr))],
        aggregate.aggr_expr.clone(),
    )
    .unwrap();
    let plan = LogicalPlan::Aggregate(grouping_set_aggregate);
    analyze_incremental_aggregate_plan(&plan).unwrap().unwrap()
}

fn assert_unsupported(analysis: &IncrementalAggregateAnalysis, reason: &str) {
    assert!(
        analysis
            .unsupported_exprs
            .iter()
            .any(|expr| expr.contains(reason)),
        "expected unsupported reason containing {reason:?}, got {:?}",
        analysis.unsupported_exprs
    );
    assert!(
        analysis.merge_columns.is_empty(),
        "unsupported analysis should disable merge columns"
    );
}

/// test if uppercase are handled correctly(with quote)
#[tokio::test]
async fn test_sql_plan_convert() {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let old = r#"SELECT "NUMBER" FROM "UPPERCASE_NUMBERS_WITH_TS""#;
    let new = sql_to_df_plan(ctx.clone(), query_engine.clone(), old, false)
        .await
        .unwrap();
    let new_sql = df_plan_to_sql(&new).unwrap();

    assert_eq!(
        r#"SELECT `UPPERCASE_NUMBERS_WITH_TS`.`NUMBER` FROM `UPPERCASE_NUMBERS_WITH_TS`"#,
        new_sql
    );
}

#[tokio::test]
async fn test_add_filter() {
    let testcases = vec![
        (
            "SELECT number FROM numbers_with_ts GROUP BY number",
            "SELECT numbers_with_ts.number FROM numbers_with_ts WHERE (number > 4) GROUP BY numbers_with_ts.number",
        ),
        (
            "SELECT number FROM numbers_with_ts WHERE number < 2 OR number >10",
            "SELECT numbers_with_ts.number FROM numbers_with_ts WHERE ((numbers_with_ts.number < 2) OR (numbers_with_ts.number > 10)) AND (number > 4)",
        ),
        (
            "SELECT date_bin('5 minutes', ts) as time_window FROM numbers_with_ts GROUP BY time_window",
            "SELECT date_bin('5 minutes', numbers_with_ts.ts) AS time_window FROM numbers_with_ts WHERE (number > 4) GROUP BY date_bin('5 minutes', numbers_with_ts.ts)",
        ),
        // subquery
        (
            "SELECT number, time_window FROM (SELECT number, date_bin('5 minutes', ts) as time_window FROM numbers_with_ts GROUP BY time_window, number);",
            "SELECT numbers_with_ts.number, time_window FROM (SELECT numbers_with_ts.number, date_bin('5 minutes', numbers_with_ts.ts) AS time_window FROM numbers_with_ts WHERE (number > 4) GROUP BY date_bin('5 minutes', numbers_with_ts.ts), numbers_with_ts.number)",
        ),
        // complex subquery without alias
        (
            "SELECT sum(number), number, date_bin('5 minutes', ts) as time_window, bucket_name FROM (SELECT number, ts, case when number < 5 THEN 'bucket_0_5' when number >= 5 THEN 'bucket_5_inf' END as bucket_name FROM numbers_with_ts) GROUP BY number, time_window, bucket_name;",
            "SELECT sum(numbers_with_ts.number), numbers_with_ts.number, date_bin('5 minutes', numbers_with_ts.ts) AS time_window, bucket_name FROM (SELECT numbers_with_ts.number, numbers_with_ts.ts, CASE WHEN (numbers_with_ts.number < 5) THEN 'bucket_0_5' WHEN (numbers_with_ts.number >= 5) THEN 'bucket_5_inf' END AS bucket_name FROM numbers_with_ts WHERE (number > 4)) GROUP BY numbers_with_ts.number, date_bin('5 minutes', numbers_with_ts.ts), bucket_name",
        ),
        // complex subquery alias
        (
            "SELECT sum(number), number, date_bin('5 minutes', ts) as time_window, bucket_name FROM (SELECT number, ts, case when number < 5 THEN 'bucket_0_5' when number >= 5 THEN 'bucket_5_inf' END as bucket_name FROM numbers_with_ts) as cte WHERE number > 1 GROUP BY number, time_window, bucket_name;",
            "SELECT sum(cte.number), cte.number, date_bin('5 minutes', cte.ts) AS time_window, cte.bucket_name FROM (SELECT numbers_with_ts.number, numbers_with_ts.ts, CASE WHEN (numbers_with_ts.number < 5) THEN 'bucket_0_5' WHEN (numbers_with_ts.number >= 5) THEN 'bucket_5_inf' END AS bucket_name FROM numbers_with_ts WHERE (number > 4)) AS cte WHERE (cte.number > 1) GROUP BY cte.number, date_bin('5 minutes', cte.ts), cte.bucket_name",
        ),
    ];
    use datafusion_expr::{col, lit};
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();

    for (before, after) in testcases {
        let sql = before;
        let plan = sql_to_df_plan(ctx.clone(), query_engine.clone(), sql, false)
            .await
            .unwrap();

        let mut add_filter = AddFilterRewriter::new(col("number").gt(lit(4u32)));
        let plan = plan.rewrite(&mut add_filter).unwrap().data;
        let new_sql = df_plan_to_sql(&plan).unwrap();
        assert_eq!(after, new_sql);
    }
}

#[tokio::test]
async fn test_add_auto_column_rewriter() {
    let testcases = vec![
        // add update_at
        (
            "SELECT number FROM numbers_with_ts",
            Ok("SELECT numbers_with_ts.number, now() AS ts FROM numbers_with_ts"),
            vec![
                ColumnSchema::new("number", ConcreteDataType::int32_datatype(), true),
                ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                )
                .with_time_index(true),
            ],
        ),
        // add ts placeholder
        (
            "SELECT number FROM numbers_with_ts",
            Ok(
                "SELECT numbers_with_ts.number, CAST('1970-01-01 00:00:00' AS TIMESTAMP) AS __ts_placeholder FROM numbers_with_ts",
            ),
            vec![
                ColumnSchema::new("number", ConcreteDataType::int32_datatype(), true),
                ColumnSchema::new(
                    AUTO_CREATED_PLACEHOLDER_TS_COL,
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                )
                .with_time_index(true),
            ],
        ),
        // no modify
        (
            "SELECT number, ts FROM numbers_with_ts",
            Ok("SELECT numbers_with_ts.number, numbers_with_ts.ts FROM numbers_with_ts"),
            vec![
                ColumnSchema::new("number", ConcreteDataType::int32_datatype(), true),
                ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                )
                .with_time_index(true),
            ],
        ),
        // add update_at and ts placeholder
        (
            "SELECT number FROM numbers_with_ts",
            Ok(
                "SELECT numbers_with_ts.number, now() AS update_at, CAST('1970-01-01 00:00:00' AS TIMESTAMP) AS __ts_placeholder FROM numbers_with_ts",
            ),
            vec![
                ColumnSchema::new("number", ConcreteDataType::int32_datatype(), true),
                ColumnSchema::new(
                    "update_at",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                ColumnSchema::new(
                    AUTO_CREATED_PLACEHOLDER_TS_COL,
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                )
                .with_time_index(true),
            ],
        ),
        // add ts placeholder
        (
            "SELECT number, ts FROM numbers_with_ts",
            Ok(
                "SELECT numbers_with_ts.number, numbers_with_ts.ts AS update_at, CAST('1970-01-01 00:00:00' AS TIMESTAMP) AS __ts_placeholder FROM numbers_with_ts",
            ),
            vec![
                ColumnSchema::new("number", ConcreteDataType::int32_datatype(), true),
                ColumnSchema::new(
                    "update_at",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                ColumnSchema::new(
                    AUTO_CREATED_PLACEHOLDER_TS_COL,
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                )
                .with_time_index(true),
            ],
        ),
        // add update_at after time index column
        (
            "SELECT number, ts FROM numbers_with_ts",
            Ok(
                "SELECT numbers_with_ts.number, numbers_with_ts.ts, now() AS update_atat FROM numbers_with_ts",
            ),
            vec![
                ColumnSchema::new("number", ConcreteDataType::int32_datatype(), true),
                ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                )
                .with_time_index(true),
                ColumnSchema::new(
                    // name is irrelevant for update_at column
                    "update_atat",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
            ],
        ),
        // error datatype mismatch
        (
            "SELECT number, ts FROM numbers_with_ts",
            Err(
                "Expect the last column in table to be timestamp column, found column atat with type Int8",
            ),
            vec![
                ColumnSchema::new("number", ConcreteDataType::int32_datatype(), true),
                ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                )
                .with_time_index(true),
                ColumnSchema::new(
                    // name is irrelevant for update_at column
                    "atat",
                    ConcreteDataType::int8_datatype(),
                    false,
                ),
            ],
        ),
        // error datatype mismatch on second last column
        (
            "SELECT number FROM numbers_with_ts",
            Err(
                "Expect the second last column in the table to be timestamp column, found column ts with type Int8",
            ),
            vec![
                ColumnSchema::new("number", ConcreteDataType::int32_datatype(), true),
                ColumnSchema::new("ts", ConcreteDataType::int8_datatype(), false),
                ColumnSchema::new(
                    // name is irrelevant for update_at column
                    "atat",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                )
                .with_time_index(true),
            ],
        ),
    ];

    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    for (before, after, column_schemas) in testcases {
        let schema = Arc::new(Schema::new(column_schemas));
        let mut add_auto_column_rewriter = ColumnMatcherRewriter::new(schema, Vec::new(), false);

        let plan = sql_to_df_plan(ctx.clone(), query_engine.clone(), before, false)
            .await
            .unwrap();
        let new_sql = (|| {
            let plan = plan
                .rewrite(&mut add_auto_column_rewriter)
                .map_err(|e| e.to_string())?
                .data;
            df_plan_to_sql(&plan).map_err(|e| e.to_string())
        })();
        match (after, new_sql.clone()) {
            (Ok(after), Ok(new_sql)) => assert_eq!(after, new_sql),
            (Err(expected), Err(real_err_msg)) => assert!(
                real_err_msg.contains(expected),
                "expected: {expected}, real: {real_err_msg}"
            ),
            _ => panic!("expected: {:?}, real: {:?}", after, new_sql),
        }
    }
}

#[tokio::test]
async fn test_find_group_by_exprs() {
    let testcases = vec![
        (
            "SELECT arrow_cast(date_bin(INTERVAL '1 MINS', numbers_with_ts.ts), 'Timestamp(Second, None)') AS ts FROM numbers_with_ts GROUP BY ts;",
            vec!["ts"],
        ),
        (
            "SELECT number FROM numbers_with_ts GROUP BY number",
            vec!["number"],
        ),
        (
            "SELECT date_bin('5 minutes', ts) as time_window FROM numbers_with_ts GROUP BY time_window",
            vec!["time_window"],
        ),
        // subquery
        (
            "SELECT number, time_window FROM (SELECT number, date_bin('5 minutes', ts) as time_window FROM numbers_with_ts GROUP BY time_window, number);",
            vec!["time_window", "number"],
        ),
        // complex subquery without alias
        (
            "SELECT sum(number), number, date_bin('5 minutes', ts) as time_window, bucket_name FROM (SELECT number, ts, case when number < 5 THEN 'bucket_0_5' when number >= 5 THEN 'bucket_5_inf' END as bucket_name FROM numbers_with_ts) GROUP BY number, time_window, bucket_name;",
            vec!["number", "time_window", "bucket_name"],
        ),
        // complex subquery alias
        (
            "SELECT sum(number), number, date_bin('5 minutes', ts) as time_window, bucket_name FROM (SELECT number, ts, case when number < 5 THEN 'bucket_0_5' when number >= 5 THEN 'bucket_5_inf' END as bucket_name FROM numbers_with_ts) as cte GROUP BY number, time_window, bucket_name;",
            vec!["number", "time_window", "bucket_name"],
        ),
    ];

    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    for (sql, expected) in testcases {
        // need to be unoptimize for better readiability
        let plan = sql_to_df_plan(ctx.clone(), query_engine.clone(), sql, false)
            .await
            .unwrap();
        let mut group_by_exprs = FindGroupByFinalName::default();
        plan.visit(&mut group_by_exprs).unwrap();
        let expected: HashSet<String> = expected.into_iter().map(|s| s.to_string()).collect();
        assert_eq!(
            expected,
            group_by_exprs.get_group_expr_names().unwrap_or_default()
        );
    }
}

#[tokio::test]
async fn test_analyze_incremental_aggregate_plan() {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let testcases: Vec<(&str, IncrementalAggregateMergeOp, &str)> = vec![
        (
            "SELECT sum(number) AS number, ts FROM numbers_with_ts GROUP BY ts",
            IncrementalAggregateMergeOp::Sum,
            "number",
        ),
        (
            "SELECT count(number) AS number, ts FROM numbers_with_ts GROUP BY ts",
            IncrementalAggregateMergeOp::Sum,
            "number",
        ),
        (
            "SELECT min(number) AS number, ts FROM numbers_with_ts GROUP BY ts",
            IncrementalAggregateMergeOp::Min,
            "number",
        ),
        (
            "SELECT max(number) AS number, ts FROM numbers_with_ts GROUP BY ts",
            IncrementalAggregateMergeOp::Max,
            "number",
        ),
        (
            "SELECT bit_and(number) AS number, ts FROM numbers_with_ts GROUP BY ts",
            IncrementalAggregateMergeOp::BitAnd,
            "number",
        ),
        (
            "SELECT bit_or(number) AS number, ts FROM numbers_with_ts GROUP BY ts",
            IncrementalAggregateMergeOp::BitOr,
            "number",
        ),
        (
            "SELECT bit_xor(number) AS number, ts FROM numbers_with_ts GROUP BY ts",
            IncrementalAggregateMergeOp::BitXor,
            "number",
        ),
        (
            "SELECT bool_and(number > 5) AS cond, ts FROM numbers_with_ts GROUP BY ts",
            IncrementalAggregateMergeOp::BoolAnd,
            "cond",
        ),
        (
            "SELECT bool_or(number > 5) AS cond, ts FROM numbers_with_ts GROUP BY ts",
            IncrementalAggregateMergeOp::BoolOr,
            "cond",
        ),
    ];

    for (sql, expected_op, expected_field_name) in testcases {
        let plan = sql_to_df_plan(ctx.clone(), query_engine.clone(), sql, false)
            .await
            .unwrap();

        let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
        assert!(analysis.unsupported_exprs.is_empty());
        assert!(analysis.group_key_names.contains(&"ts".to_string()));
        assert_eq!(analysis.merge_columns.len(), 1);
        assert_eq!(
            analysis.merge_columns[0].output_field_name,
            expected_field_name
        );
        assert_eq!(analysis.merge_columns[0].merge_op, expected_op);
    }
}

#[tokio::test]
async fn test_analyze_incremental_aggregate_plan_keeps_aliases_for_multiple_aggregates() {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let sql = "SELECT max(number) AS max_number, min(number) AS min_number, ts FROM numbers_with_ts GROUP BY ts";
    let plan = sql_to_df_plan(ctx, query_engine, sql, false).await.unwrap();

    let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
    assert!(analysis.unsupported_exprs.is_empty());
    assert!(analysis.group_key_names.contains(&"ts".to_string()));
    assert_eq!(analysis.merge_columns.len(), 2);
    assert!(analysis.merge_columns.iter().any(|merge_col| {
        merge_col.output_field_name == "max_number"
            && merge_col.merge_op == IncrementalAggregateMergeOp::Max
    }));
    assert!(analysis.merge_columns.iter().any(|merge_col| {
        merge_col.output_field_name == "min_number"
            && merge_col.merge_op == IncrementalAggregateMergeOp::Min
    }));
}

#[tokio::test]
async fn test_analyze_incremental_aggregate_plan_allows_auto_created_sink_columns() {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let sql = format!(
        "SELECT max(number) AS total, ts, now() AS {}, CAST('1970-01-01 00:00:00' AS TIMESTAMP) AS {} FROM numbers_with_ts GROUP BY ts",
        AUTO_CREATED_UPDATE_AT_TS_COL, AUTO_CREATED_PLACEHOLDER_TS_COL
    );
    let plan = sql_to_df_plan(ctx, query_engine, &sql, false)
        .await
        .unwrap();

    let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
    assert!(
        analysis.unsupported_exprs.is_empty(),
        "auto-created sink columns should not disable incremental analysis: {:?}",
        analysis.unsupported_exprs
    );
    assert!(
        analysis
            .literal_columns
            .iter()
            .any(|name| name == AUTO_CREATED_UPDATE_AT_TS_COL)
    );
    assert!(
        analysis
            .literal_columns
            .iter()
            .any(|name| name == AUTO_CREATED_PLACEHOLDER_TS_COL)
    );
    assert_eq!(analysis.merge_columns.len(), 1);
    assert_eq!(analysis.merge_columns[0].output_field_name, "total");
    assert_eq!(
        analysis.merge_columns[0].merge_op,
        IncrementalAggregateMergeOp::Max
    );
}

#[tokio::test]
async fn test_analyze_incremental_aggregate_plan_allows_where_before_aggregate() {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let sql = "SELECT sum(number) AS number, ts FROM numbers_with_ts WHERE number > 10 GROUP BY ts";
    let plan = sql_to_df_plan(ctx, query_engine, sql, false).await.unwrap();

    let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
    assert!(analysis.unsupported_exprs.is_empty());
    assert!(analysis.group_key_names.contains(&"ts".to_string()));
    assert_eq!(analysis.merge_columns.len(), 1);
    assert_eq!(analysis.merge_columns[0].output_field_name, "number");
    assert_eq!(
        analysis.merge_columns[0].merge_op,
        IncrementalAggregateMergeOp::Sum
    );
}

#[tokio::test]
async fn test_analyze_incremental_aggregate_plan_allows_alias_wrapped_scan() {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let testcases = [
        "SELECT sum(n.number) AS number, n.ts FROM numbers_with_ts AS n GROUP BY n.ts",
        "SELECT sum(n.number) AS number, n.ts FROM numbers_with_ts AS n WHERE n.number > 10 GROUP BY n.ts",
    ];

    for sql in testcases {
        let plan = sql_to_df_plan(ctx.clone(), query_engine.clone(), sql, false)
            .await
            .unwrap();
        let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
        assert!(
            analysis.unsupported_exprs.is_empty(),
            "alias-wrapped table scan should be supported for SQL {sql}: {:?}",
            analysis.unsupported_exprs
        );
        assert!(analysis.group_key_names.contains(&"ts".to_string()));
        assert_eq!(analysis.merge_columns.len(), 1);
        assert_eq!(analysis.merge_columns[0].output_field_name, "number");
        assert_eq!(
            analysis.merge_columns[0].merge_op,
            IncrementalAggregateMergeOp::Sum
        );
    }
}

#[tokio::test]
async fn test_rewrite_incremental_aggregate_allows_alias_wrapped_scan() {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let sql = "SELECT max(n.number) AS number, n.ts FROM numbers_with_ts AS n GROUP BY n.ts";
    let plan = sql_to_df_plan(ctx, query_engine, sql, false).await.unwrap();
    let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
    assert!(analysis.unsupported_exprs.is_empty());

    let rewritten = rewrite_incremental_aggregate_with_sink_merge(
        &plan,
        &analysis,
        single_row_u32_table("alias_wrapped_sink", vec!["ts", "number"]),
        &[
            "greptime".to_string(),
            "public".to_string(),
            "alias_wrapped_sink".to_string(),
        ],
        None,
    )
    .await
    .unwrap();

    let rewritten_fields = rewritten
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    assert_eq!(rewritten_fields, analysis.output_field_names);
}

#[tokio::test]
async fn test_analyze_incremental_aggregate_plan_rejects_having_filter() {
    let sql =
        "SELECT sum(number) AS number, ts FROM numbers_with_ts GROUP BY ts HAVING sum(number) > 10";
    let analysis = analyze_test_sql(sql).await;
    assert_unsupported(&analysis, "post-aggregate filter");
}

#[tokio::test]
async fn test_analyze_incremental_aggregate_plan_allows_aggregate_filter() {
    let sql = "SELECT sum(number) FILTER (WHERE number > 10) AS number, ts FROM numbers_with_ts GROUP BY ts";
    let analysis = analyze_test_sql(sql).await;

    assert!(analysis.unsupported_exprs.is_empty());
    assert!(analysis.group_key_names.contains(&"ts".to_string()));
    assert_eq!(analysis.merge_columns.len(), 1);
    assert_eq!(analysis.merge_columns[0].output_field_name, "number");
    assert_eq!(
        analysis.merge_columns[0].merge_op,
        IncrementalAggregateMergeOp::Sum
    );
}

#[tokio::test]
async fn test_analyze_incremental_aggregate_plan_rejects_aggregate_order_by() {
    let sql = "SELECT sum(number ORDER BY ts) AS number, ts FROM numbers_with_ts GROUP BY ts";
    let analysis = analyze_test_sql(sql).await;
    assert_unsupported(&analysis, "aggregate ORDER BY");
}

#[tokio::test]
async fn test_analyze_incremental_aggregate_plan_rejects_sort_above_aggregate() {
    let sql =
        "SELECT sum(number) AS number, ts FROM numbers_with_ts GROUP BY ts ORDER BY number DESC";
    let analysis = analyze_test_sql(sql).await;
    assert_unsupported(&analysis, "post-aggregate plan shape");
}

#[tokio::test]
async fn test_analyze_incremental_aggregate_plan_rejects_limit_above_aggregate() {
    let sql = "SELECT max(number) AS number, ts FROM numbers_with_ts GROUP BY ts LIMIT 1";
    let analysis = analyze_test_sql(sql).await;
    assert_unsupported(&analysis, "post-aggregate plan shape");
}

#[tokio::test]
async fn test_analyze_incremental_aggregate_plan_rejects_distinct_above_aggregate() {
    let sql = "SELECT DISTINCT max(number) AS number, ts FROM numbers_with_ts GROUP BY ts";
    let analysis = analyze_test_sql(sql).await;
    assert_unsupported(&analysis, "post-aggregate plan shape");
}

#[tokio::test]
async fn test_analyze_incremental_aggregate_plan_rejects_nested_aggregates() {
    let sql = "SELECT sum(cnt) AS total FROM (SELECT count(*) AS cnt, ts FROM numbers_with_ts GROUP BY ts) s";
    let analysis = analyze_test_sql(sql).await;
    assert_unsupported(&analysis, "aggregate input plan shape");
}

#[tokio::test]
async fn test_analyze_incremental_aggregate_plan_rejects_union_aggregate_branches() {
    let sql = "SELECT sum(number) AS number, ts FROM numbers_with_ts GROUP BY ts UNION ALL SELECT max(number) AS number, ts FROM numbers_with_ts GROUP BY ts";
    let analysis = analyze_test_sql(sql).await;
    assert_unsupported(&analysis, "post-aggregate plan shape");
}

#[tokio::test]
async fn test_analyze_incremental_aggregate_plan_rejects_window_above_aggregate() {
    let sql = "SELECT sum(number) AS number, ts, row_number() OVER (ORDER BY sum(number)) AS rn FROM numbers_with_ts GROUP BY ts";
    let analysis = analyze_test_sql(sql).await;
    assert_unsupported(&analysis, "post-aggregate plan shape");
}

#[tokio::test]
async fn test_analyze_incremental_aggregate_plan_rejects_join_below_aggregate() {
    let sql = "SELECT sum(lhs.number) AS number, lhs.ts FROM numbers_with_ts AS lhs JOIN numbers_with_ts AS rhs ON lhs.ts = rhs.ts GROUP BY lhs.ts";
    let analysis = analyze_test_sql(sql).await;
    assert_unsupported(&analysis, "aggregate input plan shape");
}

#[tokio::test]
async fn test_analyze_incremental_aggregate_plan_rejects_grouping_sets() {
    let analysis =
        analyze_grouping_set_plan(|expr| GroupingSet::GroupingSets(vec![vec![expr]])).await;
    assert_unsupported(&analysis, "GROUPING SETS/CUBE/ROLLUP");
}

#[tokio::test]
async fn test_analyze_incremental_aggregate_plan_rejects_cube() {
    let analysis = analyze_grouping_set_plan(|expr| GroupingSet::Cube(vec![expr])).await;
    assert_unsupported(&analysis, "GROUPING SETS/CUBE/ROLLUP");
}

#[tokio::test]
async fn test_analyze_incremental_aggregate_plan_rejects_rollup() {
    let analysis = analyze_grouping_set_plan(|expr| GroupingSet::Rollup(vec![expr])).await;
    assert_unsupported(&analysis, "GROUPING SETS/CUBE/ROLLUP");
}

#[tokio::test]
async fn test_analyze_incremental_aggregate_plan_preserves_raw_aggregate_name() {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let sql = "SELECT max(number), ts FROM numbers_with_ts GROUP BY ts";
    let plan = sql_to_df_plan(ctx, query_engine, sql, false).await.unwrap();

    let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
    assert!(analysis.unsupported_exprs.is_empty());
    assert_eq!(analysis.merge_columns.len(), 1);
    assert_eq!(
        analysis.merge_columns[0].output_field_name,
        "max(numbers_with_ts.number)"
    );
    assert_eq!(
        analysis.merge_columns[0].merge_op,
        IncrementalAggregateMergeOp::Max
    );
}

#[tokio::test]
async fn test_analyze_incremental_aggregate_plan_rejects_wrapper_aliased_as_raw_name() {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let sql = r#"SELECT COALESCE(max(number), 0) AS "max(numbers_with_ts.number)", ts FROM numbers_with_ts GROUP BY ts"#;
    let plan = sql_to_df_plan(ctx, query_engine, sql, false).await.unwrap();

    let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
    assert!(
        !analysis.unsupported_exprs.is_empty(),
        "wrapper aliased to a raw aggregate field name must not bypass analysis"
    );
    assert!(analysis.merge_columns.is_empty());
}

#[tokio::test]
async fn test_analyze_incremental_aggregate_plan_supports_count_star() {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let sql = "SELECT count(*) AS wildcard, ts FROM numbers_with_ts GROUP BY ts";
    let plan = sql_to_df_plan(ctx, query_engine, sql, false).await.unwrap();

    let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
    assert!(analysis.unsupported_exprs.is_empty());
    assert_eq!(analysis.merge_columns.len(), 1);
    assert_eq!(analysis.merge_columns[0].output_field_name, "wildcard");
    assert_eq!(
        analysis.merge_columns[0].merge_op,
        IncrementalAggregateMergeOp::Sum
    );
}

#[tokio::test]
async fn test_analyze_incremental_aggregate_plan_supports_aggregate_input_exprs() {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let testcases = [
        "SELECT sum(abs(number)) AS sum_abs, ts FROM numbers_with_ts GROUP BY ts",
        "SELECT sum(CASE WHEN number > 5 THEN 1 ELSE 0 END) AS above_five, ts FROM numbers_with_ts GROUP BY ts",
    ];

    for sql in testcases {
        let plan = sql_to_df_plan(ctx.clone(), query_engine.clone(), sql, false)
            .await
            .unwrap();
        let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
        assert!(
            analysis.unsupported_exprs.is_empty(),
            "aggregate input expressions should be mergeable for SQL: {sql}"
        );
        assert_eq!(analysis.merge_columns.len(), 1);
        assert_eq!(
            analysis.merge_columns[0].merge_op,
            IncrementalAggregateMergeOp::Sum
        );
    }
}

#[tokio::test]
async fn test_analyze_incremental_aggregate_plan_rejects_output_expr_wrappers() {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let testcases = [
        "SELECT abs(sum(number)) AS abs_sum, ts FROM numbers_with_ts GROUP BY ts",
        "SELECT max(number) - min(number) AS maxmin, ts FROM numbers_with_ts GROUP BY ts",
        "SELECT count(number) + 123 AS total_count, ts FROM numbers_with_ts GROUP BY ts",
        "SELECT sum(CASE WHEN number > 5 THEN 1 ELSE 0 END) / count(number) AS ratio, ts FROM numbers_with_ts GROUP BY ts",
    ];

    for sql in testcases {
        let plan = sql_to_df_plan(ctx.clone(), query_engine.clone(), sql, false)
            .await
            .unwrap();
        let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
        assert!(
            !analysis.unsupported_exprs.is_empty(),
            "aggregate output wrappers should be rejected for SQL: {sql}"
        );
    }
}

#[tokio::test]
async fn test_analyze_incremental_aggregate_plan_allows_literal_outputs() {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let sql = "SELECT max(number) AS number, ts, 42 AS lit FROM numbers_with_ts GROUP BY ts";
    let plan = sql_to_df_plan(ctx.clone(), query_engine.clone(), sql, false)
        .await
        .unwrap();

    let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
    assert!(analysis.unsupported_exprs.is_empty());
    assert_eq!(analysis.literal_columns, vec!["lit".to_string()]);
    assert_eq!(
        analysis.output_field_names,
        vec!["number".to_string(), "ts".to_string(), "lit".to_string()]
    );

    let sink_table_name = [
        "greptime".to_string(),
        "public".to_string(),
        "numbers_with_ts".to_string(),
    ];
    let (sink_table, _) = get_table_info_df_schema(
        query_engine.engine_state().catalog_manager().clone(),
        sink_table_name.clone(),
    )
    .await
    .unwrap();
    let rewritten = rewrite_incremental_aggregate_with_sink_merge(
        &plan,
        &analysis,
        sink_table.clone(),
        &sink_table_name,
        None,
    )
    .await
    .unwrap();

    let rewritten_fields = rewritten
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    assert_eq!(rewritten_fields, analysis.output_field_names);
    let expected = expected_left_join_rewrite(
        &plan,
        sink_table,
        &sink_table_name,
        vec![
            unqualified_col("ts"),
            unqualified_col("number"),
            unqualified_col("lit"),
        ],
        vec![unqualified_col("ts"), unqualified_col("number")],
        (
            vec![qualified_column("__flow_delta", "ts")],
            vec![qualified_column("__flow_sink", "ts")],
        ),
        vec![
            max_merge_expr("number"),
            qualified_col("__flow_delta", "ts").alias("ts"),
            qualified_col("__flow_delta", "lit").alias("lit"),
        ],
    );
    assert_same_logical_plan(&rewritten, &expected);
}

#[tokio::test]
async fn test_analyze_incremental_aggregate_plan_allows_unaliased_literal_output() {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let sql = "SELECT 42, max(number) AS number, ts FROM numbers_with_ts GROUP BY ts";
    let plan = sql_to_df_plan(ctx, query_engine, sql, false).await.unwrap();

    let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
    assert!(analysis.unsupported_exprs.is_empty());
    assert_eq!(analysis.literal_columns.len(), 1);
    assert_eq!(analysis.output_field_names[0], analysis.literal_columns[0]);
    assert_eq!(analysis.output_field_names[1], "number");
    assert_eq!(analysis.output_field_names[2], "ts");
}

#[tokio::test]
async fn test_analyze_incremental_aggregate_plan_allows_string_literal_output() {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let sql = "SELECT max(number) AS number, ts, 'hello' AS label FROM numbers_with_ts GROUP BY ts";
    let plan = sql_to_df_plan(ctx, query_engine, sql, false).await.unwrap();

    let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
    assert!(analysis.unsupported_exprs.is_empty());
    assert_eq!(analysis.literal_columns, vec!["label".to_string()]);
    assert_eq!(
        analysis.output_field_names,
        vec!["number".to_string(), "ts".to_string(), "label".to_string()]
    );
}

#[tokio::test]
async fn test_rewrite_incremental_aggregate_preserves_non_identifier_aliases() {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let sql = "SELECT max(number) AS \"max value\", number, 42 AS \"literal value\" FROM numbers_with_ts GROUP BY number";
    let plan = sql_to_df_plan(ctx, query_engine, sql, false).await.unwrap();
    let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
    assert!(analysis.unsupported_exprs.is_empty());
    assert_eq!(
        analysis.output_field_names,
        vec!["max value", "number", "literal value"]
    );

    let sink_table = single_row_u32_table("non_identifier_alias_sink", vec!["number", "max value"]);
    let rewritten = rewrite_incremental_aggregate_with_sink_merge(
        &plan,
        &analysis,
        sink_table,
        &[
            "greptime".to_string(),
            "public".to_string(),
            "non_identifier_alias_sink".to_string(),
        ],
        None,
    )
    .await
    .unwrap();

    assert_eq!(
        rewritten
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>(),
        vec![
            "max value".to_string(),
            "number".to_string(),
            "literal value".to_string()
        ]
    );
}

#[tokio::test]
async fn test_analyze_incremental_aggregate_plan_rejects_uncovered_outputs() {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let sql =
        "SELECT sum(number) AS total, number + 1 AS bucket FROM numbers_with_ts GROUP BY number";
    let plan = sql_to_df_plan(ctx, query_engine, sql, false).await.unwrap();

    let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
    assert!(
        analysis
            .unsupported_exprs
            .iter()
            .any(|expr| expr.contains("unsupported output field: bucket")),
        "non-literal extra output should be rejected: {:?}",
        analysis.unsupported_exprs
    );
}

#[tokio::test]
async fn test_analyze_incremental_aggregate_plan_rejects_computed_group_key_shadow() {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let sql =
        "SELECT number + 1 AS number, sum(number) AS total FROM numbers_with_ts GROUP BY number";
    let plan = sql_to_df_plan(ctx, query_engine, sql, false).await.unwrap();

    let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
    assert_unsupported(&analysis, "not a transparent group expression");
}

#[tokio::test]
async fn test_datafusion_rejects_duplicate_output_names() {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let sql = "SELECT max(number) AS x, min(number) AS x, ts FROM numbers_with_ts GROUP BY ts";
    let err = sql_to_df_plan(ctx, query_engine, sql, false)
        .await
        .unwrap_err();
    let err = format!("{err:?}");
    assert!(
        err.contains("Projections require unique expression names"),
        "DataFusion should reject duplicate output aliases before incremental analysis: {err}"
    );
}

#[tokio::test]
async fn test_analyze_incremental_aggregate_plan_rejects_same_aggregate_multiple_aliases() {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let sql = "SELECT sum(number) AS a, sum(number) AS b, ts FROM numbers_with_ts GROUP BY ts";
    let plan = sql_to_df_plan(ctx, query_engine, sql, false).await.unwrap();

    let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
    assert!(
        analysis
            .unsupported_exprs
            .iter()
            .any(|expr| expr.contains("same aggregate output")
                && expr.contains("a")
                && expr.contains("b")),
        "same aggregate with multiple aliases should be unsupported until explicit reproduction is implemented: {:?}",
        analysis.unsupported_exprs
    );
    assert!(analysis.merge_columns.is_empty());
}

#[test]
fn test_qualified_col_preserves_non_identifier_field_name() {
    let expr = qualified_col("__flow_delta", "max(numbers_with_ts.number)");
    let Expr::Column(column) = expr else {
        panic!("expected column expression");
    };
    assert_eq!(column.name, "max(numbers_with_ts.number)");
    assert_eq!(column.relation.unwrap().to_string(), "__flow_delta");
}

#[tokio::test]
async fn test_analyze_incremental_aggregate_plan_multiple_group_keys() {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let sql = "SELECT sum(number) AS total, ts, number AS bucket FROM numbers_with_ts GROUP BY ts, number";
    let plan = sql_to_df_plan(ctx, query_engine, sql, false).await.unwrap();

    let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
    assert!(analysis.unsupported_exprs.is_empty());
    assert!(analysis.group_key_names.contains(&"ts".to_string()));
    assert!(analysis.group_key_names.contains(&"bucket".to_string()));
    assert_eq!(analysis.group_key_names.len(), 2);
    assert_eq!(analysis.merge_columns.len(), 1);
    assert_eq!(analysis.merge_columns[0].output_field_name, "total");
    assert_eq!(
        analysis.merge_columns[0].merge_op,
        IncrementalAggregateMergeOp::Sum
    );
}

#[tokio::test]
async fn test_analyze_incremental_aggregate_plan_rejects_avg() {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let sql = "SELECT avg(number) AS avg_num, ts FROM numbers_with_ts GROUP BY ts";
    let plan = sql_to_df_plan(ctx, query_engine, sql, false).await.unwrap();

    let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
    assert!(!analysis.unsupported_exprs.is_empty());
}

#[tokio::test]
async fn test_analyze_incremental_aggregate_plan_rejects_distinct() {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let sql = "SELECT count(distinct number) AS number, ts FROM numbers_with_ts GROUP BY ts";
    let plan = sql_to_df_plan(ctx, query_engine, sql, false).await.unwrap();

    let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
    assert!(!analysis.unsupported_exprs.is_empty());
}

#[tokio::test]
async fn test_analyze_incremental_aggregate_plan_rejects_coalesce_wrapped_aggregate() {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    // COALESCE wraps the aggregate output — the wrapper is not merge-transparent,
    // so the analyzer should mark the aggregate as unsupported rather than
    // attempting an unsafe incremental rewrite.
    let sql =
        "SELECT COALESCE(max(number), 0) AS coalesced_max, ts FROM numbers_with_ts GROUP BY ts";
    let plan = sql_to_df_plan(ctx, query_engine, sql, false).await.unwrap();
    let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
    // Non-transparent wrapper → alias unresolvable → unsupported
    assert!(
        !analysis.unsupported_exprs.is_empty(),
        "COALESCE-wrapped aggregate should be unsupported"
    );
    assert!(
        analysis.merge_columns.is_empty(),
        "COALESCE-wrapped aggregate should have no merge columns"
    );
}

#[tokio::test]
async fn test_rewrite_incremental_aggregate_with_left_join() {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let sql = "SELECT max(number) AS number, ts FROM numbers_with_ts GROUP BY ts";
    let plan = sql_to_df_plan(ctx.clone(), query_engine.clone(), sql, false)
        .await
        .unwrap();
    let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
    let sink_table_name = [
        "greptime".to_string(),
        "public".to_string(),
        "numbers_with_ts".to_string(),
    ];
    let (sink_table, _) = get_table_info_df_schema(
        query_engine.engine_state().catalog_manager().clone(),
        sink_table_name.clone(),
    )
    .await
    .unwrap();

    let rewritten = rewrite_incremental_aggregate_with_sink_merge(
        &plan,
        &analysis,
        sink_table.clone(),
        &sink_table_name,
        None,
    )
    .await
    .unwrap();

    let expected = expected_left_join_rewrite(
        &plan,
        sink_table,
        &sink_table_name,
        vec![unqualified_col("ts"), unqualified_col("number")],
        vec![unqualified_col("ts"), unqualified_col("number")],
        (
            vec![qualified_column("__flow_delta", "ts")],
            vec![qualified_column("__flow_sink", "ts")],
        ),
        vec![
            max_merge_expr("number"),
            qualified_col("__flow_delta", "ts").alias("ts"),
        ],
    );
    assert_same_logical_plan(&rewritten, &expected);
}

#[tokio::test]
async fn test_rewrite_incremental_aggregate_filters_sink_dirty_time_window() {
    // This verifies the rewrite placement when callers supply an already
    // inferred sink dirty-window predicate. The task-level inference rules are
    // covered by `infer_sink_time_window_filter_col` tests in task.rs.
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let sql = "SELECT max(number) AS number, date_bin(INTERVAL '1 second', ts) AS time_window FROM numbers_with_ts GROUP BY time_window";
    let plan = sql_to_df_plan(ctx.clone(), query_engine.clone(), sql, false)
        .await
        .unwrap();
    let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
    let sink_table = time_window_u32_table("time_window_sink");
    let sink_table_name = [
        "greptime".to_string(),
        "public".to_string(),
        "time_window_sink".to_string(),
    ];
    let dirty_filter = FilterExprInfo {
        expr: unqualified_col("ts"),
        col_name: "ts".to_string(),
        time_ranges: vec![(
            Timestamp::new_millisecond(0),
            Timestamp::new_millisecond(1000),
        )],
        window_size: chrono::Duration::seconds(1),
    };
    let sink_filter = dirty_filter
        .predicate_for_col("time_window")
        .unwrap()
        .unwrap();

    let rewritten = rewrite_incremental_aggregate_with_sink_merge(
        &plan,
        &analysis,
        sink_table.clone(),
        &sink_table_name,
        Some(sink_filter.clone()),
    )
    .await
    .unwrap();

    let expected = expected_left_join_rewrite_with_sink_filter(
        &plan,
        sink_table,
        &sink_table_name,
        vec![unqualified_col("time_window"), unqualified_col("number")],
        vec![unqualified_col("time_window"), unqualified_col("number")],
        Some(sink_filter),
        (
            vec![qualified_column("__flow_delta", "time_window")],
            vec![qualified_column("__flow_sink", "time_window")],
        ),
        vec![
            max_merge_expr("number"),
            qualified_col("__flow_delta", "time_window").alias("time_window"),
        ],
    );
    assert_same_logical_plan(&rewritten, &expected);
}

#[tokio::test]
async fn test_analyze_incremental_aggregate_plan_rejects_global_aggregate() {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let testcases = [
        "SELECT max(number) AS number FROM numbers_with_ts",
        "SELECT max(number) AS number, 42 AS lit FROM numbers_with_ts",
        "SELECT count(*) AS cnt, sum(number) AS total FROM numbers_with_ts",
    ];

    for sql in testcases {
        let plan = sql_to_df_plan(ctx.clone(), query_engine.clone(), sql, false)
            .await
            .unwrap();
        let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
        assert_unsupported(&analysis, "global aggregate");
    }
}

#[tokio::test]
async fn test_rewrite_incremental_aggregate_rejects_empty_group_keys() {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let sql = "SELECT max(number) AS number FROM numbers_with_ts";
    let plan = sql_to_df_plan(ctx, query_engine, sql, false).await.unwrap();
    let analysis = IncrementalAggregateAnalysis {
        group_key_names: vec![],
        merge_columns: vec![IncrementalAggregateMergeColumn::new(
            "number".to_string(),
            IncrementalAggregateMergeOp::Max,
        )],
        literal_columns: vec![],
        output_field_names: vec!["number".to_string()],
        unsupported_exprs: vec![],
    };

    let sink_table = single_row_u32_table("global_guard_sink", vec!["number"]);
    let sink_table_name = [
        "greptime".to_string(),
        "public".to_string(),
        "global_guard_sink".to_string(),
    ];
    let err = rewrite_incremental_aggregate_with_sink_merge(
        &plan,
        &analysis,
        sink_table,
        &sink_table_name,
        None,
    )
    .await
    .unwrap_err();
    let err = format!("{err:?}");
    assert!(
        err.contains("global aggregate query is not supported"),
        "rewrite should defensively reject empty group keys: {err}"
    );
}

#[tokio::test]
async fn test_rewrite_incremental_aggregate_preserves_raw_aggregate_field_name() {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let sql = "SELECT max(number), number FROM numbers_with_ts GROUP BY number";
    let plan = sql_to_df_plan(ctx, query_engine, sql, false).await.unwrap();
    let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
    assert!(analysis.unsupported_exprs.is_empty());

    let raw_field_name = "max(numbers_with_ts.number)";
    let sink_table = single_row_u32_table("raw_aggregate_sink", vec!["number", raw_field_name]);
    let sink_table_name = [
        "greptime".to_string(),
        "public".to_string(),
        "raw_aggregate_sink".to_string(),
    ];
    let rewritten = rewrite_incremental_aggregate_with_sink_merge(
        &plan,
        &analysis,
        sink_table.clone(),
        &sink_table_name,
        None,
    )
    .await
    .unwrap();

    let rewritten_fields = rewritten
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    assert!(rewritten_fields.contains(&raw_field_name.to_string()));
    let expected = expected_left_join_rewrite(
        &plan,
        sink_table,
        &sink_table_name,
        vec![unqualified_col("number"), unqualified_col(raw_field_name)],
        vec![unqualified_col("number"), unqualified_col(raw_field_name)],
        (
            vec![qualified_column("__flow_delta", "number")],
            vec![qualified_column("__flow_sink", "number")],
        ),
        vec![
            max_merge_expr(raw_field_name),
            qualified_col("__flow_delta", "number").alias("number"),
        ],
    );
    assert_same_logical_plan(&rewritten, &expected);
}

#[tokio::test]
async fn test_null_cast() {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let sql = "SELECT NULL::DOUBLE FROM numbers_with_ts";
    let plan = sql_to_df_plan(ctx, query_engine.clone(), sql, false)
        .await
        .unwrap();

    let _sub_plan = DFLogicalSubstraitConvertor {}
        .encode(&plan, DefaultSerializer)
        .unwrap();
}

#[tokio::test]
async fn test_analyze_incremental_aggregate_plan_rejects_cast_wrapped_alias() {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let testcases = [
        "SELECT CAST(sum(number) AS BIGINT) AS total, ts FROM numbers_with_ts GROUP BY ts",
        "SELECT TRY_CAST(sum(number) AS BIGINT) AS total, ts FROM numbers_with_ts GROUP BY ts",
    ];

    for sql in testcases {
        let plan = sql_to_df_plan(ctx.clone(), query_engine.clone(), sql, false)
            .await
            .unwrap();
        let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
        assert!(
            !analysis.unsupported_exprs.is_empty(),
            "CAST/TryCast-wrapped aggregate output should be unsupported for SQL: {sql}"
        );
    }
}
