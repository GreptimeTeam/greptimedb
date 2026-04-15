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

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Int64Array, TimestampMillisecondArray};
use arrow::datatypes::{Field, Schema};
use common_time::Timestamp;
use common_time::timestamp::TimeUnit as TimestampUnit;
use datafusion::functions_aggregate::count::count_udaf;
use datafusion_common::utils::expr::COUNT_STAR_EXPANSION;
use datafusion_expr::expr::{AggregateFunction, AggregateFunctionParams};
use datafusion_expr::{Expr, LogicalPlan};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::aggregate::{AggregateExprBuilder, AggregateFunctionExpr};
use datafusion_physical_expr::expressions::{Column, Literal, PhysicalSortExpr};
use datatypes::arrow::array::AsArray;
use datatypes::arrow::datatypes::{DataType, TimeUnit};
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema as GreptimeSchema};
use datatypes::value::Value;
use session::context::QueryContext;
use store_api::scan_stats::{
    RegionScanColumnStats as RegionScanColumnInputStats,
    RegionScanFileStats as RegionScanFileInputStats, RegionScanStats as RegionScanInputStats,
};
use table::metadata::{TableInfoBuilder, TableMetaBuilder};
use table::test_util::EmptyTable;

use super::StatsAgg;
use super::check::{RejectReason, RewriteCheck, is_supported_aggregate_name};
use super::split::{
    FileStatsRequirement, StatsAggExt, has_partition_expr_mismatch, partial_state_from_stats,
    split_count_field_files, split_count_star_files, split_min_max_field_files, split_time_files,
};
use crate::parser::QueryLanguageParser;
use crate::tests::new_query_engine_with_table;

fn test_timestamp(value: i64) -> Timestamp {
    Timestamp::new(value, TimestampUnit::Millisecond)
}

fn field_stats(
    exact_non_null_rows: Option<usize>,
    min_value: Option<Value>,
    max_value: Option<Value>,
) -> HashMap<String, RegionScanColumnInputStats> {
    HashMap::from([(
        "value".to_string(),
        RegionScanColumnInputStats {
            min_value,
            max_value,
            exact_non_null_rows,
        },
    )])
}

fn build_test_aggr_expr(
    distinct: bool,
    ignore_nulls: bool,
    order_by: bool,
) -> datafusion_common::Result<AggregateFunctionExpr> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        true,
    )]));
    let args = vec![Arc::new(Column::new("value", 0)) as Arc<dyn PhysicalExpr>];
    let mut builder = AggregateExprBuilder::new(Arc::new((*count_udaf()).clone()), args)
        .schema(schema)
        .alias("count(value)");

    if distinct {
        builder = builder.with_distinct(true);
    }
    if ignore_nulls {
        builder = builder.ignore_nulls();
    }
    if order_by {
        builder = builder.order_by(vec![PhysicalSortExpr {
            expr: Arc::new(Column::new("value", 0)),
            options: Default::default(),
        }]);
    }

    builder.build()
}

fn build_count_star_aggr_expr() -> datafusion_common::Result<AggregateFunctionExpr> {
    let schema = Arc::new(Schema::empty());
    let args = vec![Arc::new(Literal::new(COUNT_STAR_EXPANSION)) as Arc<dyn PhysicalExpr>];

    AggregateExprBuilder::new(Arc::new((*count_udaf()).clone()), args)
        .schema(schema)
        .alias("count(*)")
        .build()
}

fn new_test_engine() -> crate::QueryEngineRef {
    let columns = vec![
        ColumnSchema::new(
            "ts",
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        ),
        ColumnSchema::new("value", ConcreteDataType::int64_datatype(), true),
    ];
    let schema = Arc::new(GreptimeSchema::new(columns));
    let table_meta = TableMetaBuilder::empty()
        .schema(schema)
        .primary_key_indices(vec![0])
        .value_indices(vec![1])
        .next_column_id(1024)
        .build()
        .unwrap();
    let table_info = TableInfoBuilder::new("test", table_meta).build().unwrap();
    let table = EmptyTable::from_table_info(&table_info);

    new_query_engine_with_table(table)
}

async fn parse_sql_to_plan(sql: &str) -> LogicalPlan {
    let query_ctx = QueryContext::arc();
    let stmt = QueryLanguageParser::parse_sql(sql, &query_ctx).unwrap();
    new_test_engine()
        .planner()
        .plan(&stmt, query_ctx)
        .await
        .unwrap()
}

#[test]
fn test_supported_aggregate_names() {
    assert!(is_supported_aggregate_name("min"));
    assert!(is_supported_aggregate_name("MAX"));
    assert!(is_supported_aggregate_name("count"));
    assert!(!is_supported_aggregate_name("sum"));
    assert!(!is_supported_aggregate_name("avg(value)"));
}

#[test]
fn test_file_stats_requirement_matrix() {
    let count_star = StatsAgg::CountStar;
    assert_eq!(
        count_star.file_stats_requirement(),
        FileStatsRequirement::FileExactRowCount
    );

    let time_min = StatsAgg::MinTimeIndex {
        arg_type: DataType::Timestamp(TimeUnit::Millisecond, None),
    };
    assert_eq!(
        time_min.file_stats_requirement(),
        FileStatsRequirement::FileTimeRange
    );

    let field_count = StatsAgg::CountField {
        column_name: "value".to_string(),
        arg_type: DataType::Int64,
    };
    assert_eq!(
        field_count.file_stats_requirement(),
        FileStatsRequirement::RowGroupNullCount
    );
}

#[test]
fn test_count_star_file_stats_eligibility() {
    let stats = RegionScanInputStats {
        files: vec![
            RegionScanFileInputStats {
                file_ordinal: 0,
                exact_num_rows: None,
                time_range: Some((test_timestamp(10), test_timestamp(20))),
                field_stats: HashMap::new(),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 1,
                exact_num_rows: Some(42),
                time_range: Some((test_timestamp(30), test_timestamp(40))),
                field_stats: HashMap::new(),
                partition_expr_matches_region: false,
            },
            RegionScanFileInputStats {
                file_ordinal: 2,
                exact_num_rows: Some(7),
                time_range: Some((test_timestamp(50), test_timestamp(60))),
                field_stats: HashMap::new(),
                partition_expr_matches_region: true,
            },
        ],
    };

    assert!(StatsAgg::CountStar.has_stats_files(&stats));
}

#[test]
fn test_split_count_star_files() {
    let stats = RegionScanInputStats {
        files: vec![
            RegionScanFileInputStats {
                file_ordinal: 0,
                exact_num_rows: Some(3),
                time_range: Some((test_timestamp(10), test_timestamp(20))),
                field_stats: HashMap::new(),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 1,
                exact_num_rows: None,
                time_range: Some((test_timestamp(30), test_timestamp(40))),
                field_stats: HashMap::new(),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 2,
                exact_num_rows: Some(5),
                time_range: Some((test_timestamp(50), test_timestamp(60))),
                field_stats: HashMap::new(),
                partition_expr_matches_region: false,
            },
        ],
    };

    let split = split_count_star_files(&stats);
    assert_eq!(split.stats_file_ordinals, vec![0]);
    assert_eq!(split.scan_file_ordinals, vec![1, 2]);
    assert_eq!(split.stats, 3);
}

#[test]
fn test_split_count_star_files_keeps_zero_row_files_stats_eligible() {
    let stats = RegionScanInputStats {
        files: vec![
            RegionScanFileInputStats {
                file_ordinal: 0,
                exact_num_rows: Some(0),
                time_range: None,
                field_stats: HashMap::new(),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 1,
                exact_num_rows: None,
                time_range: None,
                field_stats: HashMap::new(),
                partition_expr_matches_region: true,
            },
        ],
    };

    let split = split_count_star_files(&stats);
    assert_eq!(split.stats_file_ordinals, vec![0]);
    assert_eq!(split.scan_file_ordinals, vec![1]);
    assert_eq!(split.stats, 0);
}

#[test]
fn test_supported_aggregate_rejects_distinct_ignore_nulls_and_order_by_shapes() {
    let distinct = build_test_aggr_expr(true, false, false).unwrap();
    let ignore_nulls = build_test_aggr_expr(false, true, false).unwrap();
    let order_by = build_test_aggr_expr(false, false, true).unwrap();

    assert!(matches!(
        RewriteCheck::check_agg_shape(&distinct),
        Err(RejectReason::UnsupportedAggregate)
    ));
    assert!(matches!(
        RewriteCheck::check_agg_shape(&ignore_nulls),
        Err(RejectReason::UnsupportedAggregate)
    ));
    assert!(matches!(
        RewriteCheck::check_agg_shape(&order_by),
        Err(RejectReason::UnsupportedAggregate)
    ));
}

#[test]
fn test_count_star_expansion_is_treated_as_count_star() {
    let expr = build_count_star_aggr_expr().unwrap();

    assert_eq!(expr.fun().name(), "count");
    assert_eq!(expr.expressions().len(), 1);
    assert!(
        expr.expressions()[0]
            .as_any()
            .downcast_ref::<Literal>()
            .is_some_and(|lit| lit.value() == &COUNT_STAR_EXPANSION)
    );
}

#[tokio::test]
async fn test_sql_count_star_is_planned_with_count_star_expansion() {
    let plan = parse_sql_to_plan("select count(*) from test").await;
    let LogicalPlan::Projection(projection) = plan else {
        panic!("expected projection over aggregate plan, got {plan:?}");
    };
    let LogicalPlan::Aggregate(aggregate) = projection.input.as_ref() else {
        panic!("expected aggregate input, got {:?}", projection.input);
    };

    assert_eq!(aggregate.aggr_expr.len(), 1);
    let Expr::AggregateFunction(AggregateFunction {
        func,
        params: AggregateFunctionParams { args, .. },
    }) = &aggregate.aggr_expr[0]
    else {
        panic!(
            "expected aggregate function expr, got {:?}",
            aggregate.aggr_expr[0]
        );
    };

    assert_eq!(func.name(), "count");
    assert_eq!(args.len(), 1);
    assert!(matches!(
        &args[0],
        Expr::Literal(value, _) if value == &COUNT_STAR_EXPANSION
    ));
}

#[test]
fn test_partition_expr_mismatch_detection() {
    let stats = RegionScanInputStats {
        files: vec![
            RegionScanFileInputStats {
                file_ordinal: 0,
                exact_num_rows: Some(1),
                time_range: Some((test_timestamp(10), test_timestamp(20))),
                field_stats: HashMap::new(),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 1,
                exact_num_rows: Some(2),
                time_range: Some((test_timestamp(30), test_timestamp(40))),
                field_stats: HashMap::new(),
                partition_expr_matches_region: false,
            },
        ],
    };

    assert!(has_partition_expr_mismatch(Some(&stats)));
}

#[test]
fn test_time_file_stats_eligibility() {
    let stats = RegionScanInputStats {
        files: vec![
            RegionScanFileInputStats {
                file_ordinal: 0,
                exact_num_rows: Some(3),
                time_range: None,
                field_stats: HashMap::new(),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 1,
                exact_num_rows: None,
                time_range: Some((test_timestamp(30), test_timestamp(40))),
                field_stats: HashMap::new(),
                partition_expr_matches_region: true,
            },
        ],
    };

    assert!(
        StatsAgg::MinTimeIndex {
            arg_type: DataType::Timestamp(TimeUnit::Millisecond, None),
        }
        .has_stats_files(&stats)
    );
}

#[test]
fn test_split_time_files() {
    let stats = RegionScanInputStats {
        files: vec![
            RegionScanFileInputStats {
                file_ordinal: 0,
                exact_num_rows: Some(3),
                time_range: Some((test_timestamp(50), test_timestamp(70))),
                field_stats: HashMap::new(),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 1,
                exact_num_rows: Some(4),
                time_range: None,
                field_stats: HashMap::new(),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 2,
                exact_num_rows: Some(5),
                time_range: Some((test_timestamp(10), test_timestamp(20))),
                field_stats: HashMap::new(),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 3,
                exact_num_rows: Some(6),
                time_range: Some((test_timestamp(5), test_timestamp(100))),
                field_stats: HashMap::new(),
                partition_expr_matches_region: false,
            },
        ],
    };

    let split = split_time_files(&stats);
    assert_eq!(split.stats_file_ordinals, vec![0, 2]);
    assert_eq!(split.scan_file_ordinals, vec![1, 3]);
    assert_eq!(split.stats.min, Some(test_timestamp(10)));
    assert_eq!(split.stats.max, Some(test_timestamp(70)));
}

#[test]
fn test_count_field_file_stats_eligibility() {
    let stats = RegionScanInputStats {
        files: vec![
            RegionScanFileInputStats {
                file_ordinal: 0,
                exact_num_rows: Some(3),
                time_range: Some((test_timestamp(10), test_timestamp(20))),
                field_stats: HashMap::new(),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 1,
                exact_num_rows: Some(5),
                time_range: Some((test_timestamp(30), test_timestamp(40))),
                field_stats: field_stats(Some(4), Some(Value::Int64(1)), Some(Value::Int64(9))),
                partition_expr_matches_region: true,
            },
        ],
    };

    assert!(
        StatsAgg::CountField {
            column_name: "value".to_string(),
            arg_type: DataType::Int64,
        }
        .has_stats_files(&stats)
    );
}

#[test]
fn test_split_count_field_files() {
    let stats = RegionScanInputStats {
        files: vec![
            RegionScanFileInputStats {
                file_ordinal: 0,
                exact_num_rows: Some(3),
                time_range: Some((test_timestamp(10), test_timestamp(20))),
                field_stats: field_stats(Some(2), Some(Value::Int64(1)), Some(Value::Int64(3))),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 1,
                exact_num_rows: Some(4),
                time_range: Some((test_timestamp(30), test_timestamp(40))),
                field_stats: HashMap::new(),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 2,
                exact_num_rows: Some(5),
                time_range: Some((test_timestamp(50), test_timestamp(60))),
                field_stats: field_stats(Some(4), Some(Value::Int64(5)), Some(Value::Int64(8))),
                partition_expr_matches_region: true,
            },
        ],
    };

    let split = split_count_field_files(&stats, "value");
    assert_eq!(split.stats_file_ordinals, vec![0, 2]);
    assert_eq!(split.scan_file_ordinals, vec![1]);
    assert_eq!(split.stats, 6);
}

#[test]
fn test_min_max_field_stats_eligibility() {
    let stats = RegionScanInputStats {
        files: vec![
            RegionScanFileInputStats {
                file_ordinal: 0,
                exact_num_rows: Some(3),
                time_range: Some((test_timestamp(10), test_timestamp(20))),
                field_stats: field_stats(Some(2), Some(Value::Int64(1)), Some(Value::Int64(3))),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 1,
                exact_num_rows: Some(4),
                time_range: Some((test_timestamp(30), test_timestamp(40))),
                field_stats: HashMap::new(),
                partition_expr_matches_region: false,
            },
        ],
    };

    assert!(
        StatsAgg::MinField {
            column_name: "value".to_string(),
            arg_type: DataType::Int64,
        }
        .has_stats_files(&stats)
    );
}

#[test]
fn test_split_min_max_field_files() {
    let stats = RegionScanInputStats {
        files: vec![
            RegionScanFileInputStats {
                file_ordinal: 0,
                exact_num_rows: Some(3),
                time_range: Some((test_timestamp(10), test_timestamp(20))),
                field_stats: field_stats(Some(2), Some(Value::Int64(4)), Some(Value::Int64(9))),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 1,
                exact_num_rows: Some(4),
                time_range: Some((test_timestamp(30), test_timestamp(40))),
                field_stats: HashMap::new(),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 2,
                exact_num_rows: Some(5),
                time_range: Some((test_timestamp(50), test_timestamp(60))),
                field_stats: field_stats(Some(4), Some(Value::Int64(1)), Some(Value::Int64(7))),
                partition_expr_matches_region: true,
            },
        ],
    };

    let split = split_min_max_field_files(&stats, "value");
    assert_eq!(split.stats_file_ordinals, vec![0, 2]);
    assert_eq!(split.scan_file_ordinals, vec![1]);
    assert_eq!(split.stats.min, Some(Value::Int64(1)));
    assert_eq!(split.stats.max, Some(Value::Int64(9)));
}

#[test]
fn test_partial_state_from_stats_count_star() {
    let aggregate = StatsAgg::CountStar;
    let stats = RegionScanInputStats {
        files: vec![
            RegionScanFileInputStats {
                file_ordinal: 0,
                exact_num_rows: Some(3),
                time_range: Some((test_timestamp(10), test_timestamp(20))),
                field_stats: HashMap::new(),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 1,
                exact_num_rows: Some(4),
                time_range: Some((test_timestamp(30), test_timestamp(40))),
                field_stats: HashMap::new(),
                partition_expr_matches_region: true,
            },
        ],
    };

    let value = partial_state_from_stats(&aggregate, &stats)
        .unwrap()
        .unwrap();
    let array = value.to_array().unwrap();
    let struct_array = array.as_struct();
    let count_values = struct_array
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(count_values.value(0), 7);
}

#[test]
fn test_partial_state_from_stats_count_field() {
    let aggregate = StatsAgg::CountField {
        column_name: "value".to_string(),
        arg_type: DataType::Int64,
    };
    let stats = RegionScanInputStats {
        files: vec![
            RegionScanFileInputStats {
                file_ordinal: 0,
                exact_num_rows: Some(3),
                time_range: Some((test_timestamp(10), test_timestamp(20))),
                field_stats: field_stats(Some(2), Some(Value::Int64(1)), Some(Value::Int64(3))),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 1,
                exact_num_rows: Some(4),
                time_range: Some((test_timestamp(30), test_timestamp(40))),
                field_stats: field_stats(Some(4), Some(Value::Int64(5)), Some(Value::Int64(9))),
                partition_expr_matches_region: true,
            },
        ],
    };

    let value = partial_state_from_stats(&aggregate, &stats)
        .unwrap()
        .unwrap();
    let array = value.to_array().unwrap();
    let struct_array = array.as_struct();
    let count_values = struct_array
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(count_values.value(0), 6);
}

#[test]
fn test_partial_state_from_stats_min_time() {
    let aggregate = StatsAgg::MinTimeIndex {
        arg_type: DataType::Timestamp(TimeUnit::Millisecond, None),
    };
    let stats = RegionScanInputStats {
        files: vec![
            RegionScanFileInputStats {
                file_ordinal: 0,
                exact_num_rows: Some(3),
                time_range: Some((test_timestamp(50), test_timestamp(70))),
                field_stats: HashMap::new(),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 1,
                exact_num_rows: Some(5),
                time_range: Some((test_timestamp(10), test_timestamp(20))),
                field_stats: HashMap::new(),
                partition_expr_matches_region: true,
            },
        ],
    };

    let value = partial_state_from_stats(&aggregate, &stats)
        .unwrap()
        .unwrap();
    let array = value.to_array().unwrap();
    let struct_array = array.as_struct();
    let ts_values = struct_array
        .column(0)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();
    assert_eq!(ts_values.value(0), 10);
}

#[test]
fn test_partial_state_from_stats_max_field() {
    let aggregate = StatsAgg::MaxField {
        column_name: "value".to_string(),
        arg_type: DataType::Int64,
    };
    let stats = RegionScanInputStats {
        files: vec![
            RegionScanFileInputStats {
                file_ordinal: 0,
                exact_num_rows: Some(3),
                time_range: Some((test_timestamp(10), test_timestamp(20))),
                field_stats: field_stats(Some(2), Some(Value::Int64(1)), Some(Value::Int64(3))),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 1,
                exact_num_rows: Some(4),
                time_range: Some((test_timestamp(30), test_timestamp(40))),
                field_stats: field_stats(Some(4), Some(Value::Int64(5)), Some(Value::Int64(9))),
                partition_expr_matches_region: true,
            },
        ],
    };

    let value = partial_state_from_stats(&aggregate, &stats)
        .unwrap()
        .unwrap();
    let array = value.to_array().unwrap();
    let struct_array = array.as_struct();
    let max_values = struct_array
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(max_values.value(0), 9);
}
