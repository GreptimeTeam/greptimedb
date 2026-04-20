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
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{Int64Array, TimestampMillisecondArray};
use arrow::datatypes::{Field, Schema};
use common_error::ext::BoxedError;
use common_recordbatch::{
    EmptyRecordBatchStream, RecordBatch as CommonRecordBatch, RecordBatches,
    SendableRecordBatchStream,
};
use common_time::Timestamp;
use common_time::timestamp::TimeUnit as TimestampUnit;
use datafusion::execution::context::SessionContext;
use datafusion::functions_aggregate::count::count_udaf;
use datafusion::functions_aggregate::min_max::{max_udaf, min_udaf};
use datafusion::physical_plan::displayable;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
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
use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder, RegionMetadataRef};
use store_api::region_engine::{
    PrepareRequest, QueryScanContext, RegionScanner, ScannerProperties,
};
use store_api::scan_stats::{
    RegionScanColumnStats as RegionScanColumnInputStats,
    RegionScanFileStats as RegionScanFileInputStats, RegionScanStats as RegionScanInputStats,
};
use store_api::storage::{FileId, RegionId, ScanRequest};
use table::metadata::{TableInfoBuilder, TableMetaBuilder};
use table::table::scan::RegionScanExec;
use table::test_util::EmptyTable;

use super::check::{RejectReason, RewriteCheck, is_supported_aggregate_name};
use super::split::{
    FileStatsRequirement, StatsAggExt, common_stats_file_ordinals, filter_stats_by_file_ordinals,
    has_partition_expr_mismatch, partial_state_from_stats, split_count_field_files,
    split_count_star_files, split_min_max_field_files, split_time_files,
};
use super::{AggregateStats, StatsAgg};
use crate::parser::QueryLanguageParser;
use crate::tests::new_query_engine_with_table;

fn test_timestamp(value: i64) -> Timestamp {
    Timestamp::new(value, TimestampUnit::Millisecond)
}

fn test_file_id(seed: usize) -> FileId {
    FileId::parse_str(&format!("00000000-0000-0000-0000-{:012x}", seed + 1)).unwrap()
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

#[derive(Debug)]
struct StatsRecordingScanner {
    schema: datatypes::schema::SchemaRef,
    metadata: RegionMetadataRef,
    properties: ScannerProperties,
    base_stats: RegionScanInputStats,
    excluded_count: Arc<AtomicUsize>,
    file_batches: Vec<(usize, CommonRecordBatch)>,
    excluded_file_ordinals: Vec<usize>,
}

impl StatsRecordingScanner {
    fn new(
        schema: datatypes::schema::SchemaRef,
        metadata: RegionMetadataRef,
        base_stats: RegionScanInputStats,
        excluded_count: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            schema,
            metadata,
            properties: ScannerProperties::default().with_append_mode(true),
            base_stats,
            excluded_count,
            file_batches: Vec::new(),
            excluded_file_ordinals: Vec::new(),
        }
    }

    fn with_file_batches(mut self, file_batches: Vec<(usize, CommonRecordBatch)>) -> Self {
        self.file_batches = file_batches;
        self
    }
}

impl DisplayAs for StatsRecordingScanner {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StatsRecordingScanner")
    }
}

impl RegionScanner for StatsRecordingScanner {
    fn name(&self) -> &str {
        "StatsRecordingScanner"
    }

    fn properties(&self) -> &ScannerProperties {
        &self.properties
    }

    fn schema(&self) -> datatypes::schema::SchemaRef {
        self.schema.clone()
    }

    fn metadata(&self) -> RegionMetadataRef {
        self.metadata.clone()
    }

    fn prepare(&mut self, request: PrepareRequest) -> Result<(), common_error::ext::BoxedError> {
        request.validate()?;
        if let Some(excluded_file_ordinals) = request.excluded_file_ordinals.as_ref() {
            self.excluded_count
                .store(excluded_file_ordinals.len(), Ordering::Relaxed);
            self.excluded_file_ordinals = excluded_file_ordinals.clone();
        }
        self.properties.prepare(request);
        Ok(())
    }

    fn scan_partition(
        &self,
        _: &QueryScanContext,
        _: &ExecutionPlanMetricsSet,
        _: usize,
    ) -> Result<SendableRecordBatchStream, common_error::ext::BoxedError> {
        let batches = self
            .file_batches
            .iter()
            .filter(|(file_ordinal, _)| !self.excluded_file_ordinals.contains(file_ordinal))
            .map(|(_, batch)| batch.clone())
            .collect::<Vec<_>>();

        if batches.is_empty() {
            Ok(Box::pin(EmptyRecordBatchStream::new(self.schema.clone())))
        } else {
            Ok(RecordBatches::try_new(self.schema.clone(), batches)
                .map_err(BoxedError::new)?
                .as_stream())
        }
    }

    fn has_predicate_without_region(&self) -> bool {
        false
    }

    fn scan_input_stats(
        &self,
    ) -> Result<Option<RegionScanInputStats>, common_error::ext::BoxedError> {
        Ok(Some(filter_stats_by_file_ordinals(
            &self.base_stats,
            &self
                .base_stats
                .files
                .iter()
                .filter_map(|file| {
                    (!self.excluded_file_ordinals.contains(&file.file_ordinal))
                        .then_some(file.file_ordinal)
                })
                .collect::<Vec<_>>(),
        )))
    }

    fn add_dyn_filter_to_predicate(&mut self, _: Vec<Arc<dyn PhysicalExpr>>) -> Vec<bool> {
        Vec::new()
    }

    fn set_logical_region(&mut self, logical_region: bool) {
        self.properties.set_logical_region(logical_region);
    }
}

fn test_region_metadata() -> RegionMetadataRef {
    let mut builder = RegionMetadataBuilder::new(RegionId::new(1024, 1));
    builder
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            semantic_type: api::v1::SemanticType::Timestamp,
            column_id: 1,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new("value", ConcreteDataType::int64_datatype(), true),
            semantic_type: api::v1::SemanticType::Field,
            column_id: 2,
        })
        .primary_key(vec![]);
    Arc::new(builder.build().unwrap())
}

fn scan_batch(
    schema: datatypes::schema::SchemaRef,
    timestamps: Vec<i64>,
    values: Vec<Option<i64>>,
) -> CommonRecordBatch {
    let df_record_batch = datatypes::arrow::record_batch::RecordBatch::try_new(
        schema.arrow_schema().clone(),
        vec![
            Arc::new(TimestampMillisecondArray::from_iter_values(timestamps)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .unwrap();

    CommonRecordBatch::from_df_record_batch(schema, df_record_batch)
}

fn build_test_aggr_expr(
    distinct: bool,
    ignore_nulls: bool,
    order_by: bool,
) -> datafusion_common::Result<AggregateFunctionExpr> {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("value", DataType::Int64, true),
    ]));
    let args = vec![Arc::new(Column::new("value", 1)) as Arc<dyn PhysicalExpr>];
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

fn build_min_field_aggr_expr_with_ignore_nulls(
    ignore_nulls: bool,
) -> datafusion_common::Result<AggregateFunctionExpr> {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("value", DataType::Int64, true),
    ]));
    let args = vec![Arc::new(Column::new("value", 1)) as Arc<dyn PhysicalExpr>];
    let mut builder = AggregateExprBuilder::new(Arc::new((*min_udaf()).clone()), args)
        .schema(schema)
        .alias("min(value)");
    if ignore_nulls {
        builder = builder.ignore_nulls();
    }

    builder.build()
}

fn build_max_field_aggr_expr_with_ignore_nulls(
    ignore_nulls: bool,
) -> datafusion_common::Result<AggregateFunctionExpr> {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("value", DataType::Int64, true),
    ]));
    let args = vec![Arc::new(Column::new("value", 1)) as Arc<dyn PhysicalExpr>];
    let mut builder = AggregateExprBuilder::new(Arc::new((*max_udaf()).clone()), args)
        .schema(schema)
        .alias("max(value)");
    if ignore_nulls {
        builder = builder.ignore_nulls();
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
                file_id: test_file_id(0),
                exact_num_rows: None,
                time_range: Some((test_timestamp(10), test_timestamp(20))),
                field_stats: HashMap::new(),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 1,
                file_id: test_file_id(1),
                exact_num_rows: Some(42),
                time_range: Some((test_timestamp(30), test_timestamp(40))),
                field_stats: HashMap::new(),
                partition_expr_matches_region: false,
            },
            RegionScanFileInputStats {
                file_ordinal: 2,
                file_id: test_file_id(2),
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
                file_id: test_file_id(0),
                exact_num_rows: Some(3),
                time_range: Some((test_timestamp(10), test_timestamp(20))),
                field_stats: HashMap::new(),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 1,
                file_id: test_file_id(1),
                exact_num_rows: None,
                time_range: Some((test_timestamp(30), test_timestamp(40))),
                field_stats: HashMap::new(),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 2,
                file_id: test_file_id(2),
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
                file_id: test_file_id(0),
                exact_num_rows: Some(0),
                time_range: None,
                field_stats: HashMap::new(),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 1,
                file_id: test_file_id(1),
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
fn test_supported_aggregate_shape_allows_ignore_nulls_for_count_min_max() {
    let distinct = build_test_aggr_expr(true, false, false).unwrap();
    let count_ignore_nulls = build_test_aggr_expr(false, true, false).unwrap();
    let min_ignore_nulls = build_min_field_aggr_expr_with_ignore_nulls(true).unwrap();
    let max_ignore_nulls = build_max_field_aggr_expr_with_ignore_nulls(true).unwrap();
    let order_by = build_test_aggr_expr(false, false, true).unwrap();

    let reject_reason = |expr: AggregateFunctionExpr| {
        let schema = execution_test_schema();
        let region_scan = Arc::new(
            RegionScanExec::new(
                Box::new(StatsRecordingScanner::new(
                    schema.clone(),
                    test_region_metadata(),
                    execution_test_stats(),
                    Arc::new(AtomicUsize::new(0)),
                )),
                ScanRequest::default(),
                None,
            )
            .unwrap(),
        );
        let aggregate = Arc::new(
            AggregateExec::try_new(
                AggregateMode::Partial,
                PhysicalGroupBy::new_single(vec![]),
                vec![Arc::new(expr)],
                vec![None],
                region_scan.clone(),
                schema.arrow_schema().clone(),
            )
            .unwrap(),
        );
        let check = RewriteCheck::new(aggregate.as_ref(), region_scan.as_ref());
        check.skip_reason().unwrap()
    };

    assert!(matches!(
        RewriteCheck::check_agg_shape(&distinct),
        Err(RejectReason::UnsupportedAggregate)
    ));
    assert!(reject_reason(count_ignore_nulls).is_none());
    assert!(reject_reason(min_ignore_nulls).is_none());
    assert!(reject_reason(max_ignore_nulls).is_none());
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
                file_id: test_file_id(0),
                exact_num_rows: Some(1),
                time_range: Some((test_timestamp(10), test_timestamp(20))),
                field_stats: HashMap::new(),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 1,
                file_id: test_file_id(1),
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
                file_id: test_file_id(0),
                exact_num_rows: Some(3),
                time_range: None,
                field_stats: HashMap::new(),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 1,
                file_id: test_file_id(1),
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
                file_id: test_file_id(0),
                exact_num_rows: Some(3),
                time_range: Some((test_timestamp(50), test_timestamp(70))),
                field_stats: HashMap::new(),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 1,
                file_id: test_file_id(1),
                exact_num_rows: Some(4),
                time_range: None,
                field_stats: HashMap::new(),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 2,
                file_id: test_file_id(2),
                exact_num_rows: Some(5),
                time_range: Some((test_timestamp(10), test_timestamp(20))),
                field_stats: HashMap::new(),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 3,
                file_id: test_file_id(3),
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
                file_id: test_file_id(0),
                exact_num_rows: Some(3),
                time_range: Some((test_timestamp(10), test_timestamp(20))),
                field_stats: HashMap::new(),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 1,
                file_id: test_file_id(1),
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
                file_id: test_file_id(0),
                exact_num_rows: Some(3),
                time_range: Some((test_timestamp(10), test_timestamp(20))),
                field_stats: field_stats(Some(2), Some(Value::Int64(1)), Some(Value::Int64(3))),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 1,
                file_id: test_file_id(1),
                exact_num_rows: Some(4),
                time_range: Some((test_timestamp(30), test_timestamp(40))),
                field_stats: HashMap::new(),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 2,
                file_id: test_file_id(2),
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
                file_id: test_file_id(0),
                exact_num_rows: Some(3),
                time_range: Some((test_timestamp(10), test_timestamp(20))),
                field_stats: field_stats(Some(2), Some(Value::Int64(1)), Some(Value::Int64(3))),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 1,
                file_id: test_file_id(1),
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
                file_id: test_file_id(0),
                exact_num_rows: Some(3),
                time_range: Some((test_timestamp(10), test_timestamp(20))),
                field_stats: field_stats(Some(2), Some(Value::Int64(4)), Some(Value::Int64(9))),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 1,
                file_id: test_file_id(1),
                exact_num_rows: Some(4),
                time_range: Some((test_timestamp(30), test_timestamp(40))),
                field_stats: HashMap::new(),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 2,
                file_id: test_file_id(2),
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
fn test_common_stats_file_ordinals_intersects_supported_aggregates() {
    let stats = RegionScanInputStats {
        files: vec![
            RegionScanFileInputStats {
                file_ordinal: 0,
                file_id: test_file_id(0),
                exact_num_rows: Some(3),
                time_range: Some((test_timestamp(10), test_timestamp(20))),
                field_stats: field_stats(Some(2), Some(Value::Int64(4)), Some(Value::Int64(9))),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 1,
                file_id: test_file_id(1),
                exact_num_rows: Some(4),
                time_range: Some((test_timestamp(30), test_timestamp(40))),
                field_stats: HashMap::new(),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 2,
                file_id: test_file_id(2),
                exact_num_rows: Some(5),
                time_range: Some((test_timestamp(50), test_timestamp(60))),
                field_stats: field_stats(Some(4), Some(Value::Int64(1)), Some(Value::Int64(7))),
                partition_expr_matches_region: true,
            },
        ],
    };

    let aggregates = vec![
        StatsAgg::CountStar,
        StatsAgg::CountField {
            column_name: "value".to_string(),
            arg_type: DataType::Int64,
        },
        StatsAgg::MaxField {
            column_name: "value".to_string(),
            arg_type: DataType::Int64,
        },
    ];

    assert_eq!(common_stats_file_ordinals(&aggregates, &stats), vec![0, 2]);
}

#[test]
fn test_common_stats_file_ordinals_returns_only_shared_stats_eligible_files() {
    let stats = RegionScanInputStats {
        files: vec![
            RegionScanFileInputStats {
                file_ordinal: 0,
                file_id: test_file_id(0),
                exact_num_rows: Some(3),
                time_range: Some((test_timestamp(10), test_timestamp(20))),
                field_stats: field_stats(Some(2), Some(Value::Int64(4)), Some(Value::Int64(9))),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 1,
                file_id: test_file_id(1),
                exact_num_rows: Some(4),
                time_range: Some((test_timestamp(30), test_timestamp(40))),
                field_stats: HashMap::new(),
                partition_expr_matches_region: true,
            },
        ],
    };

    let aggregates = vec![
        StatsAgg::CountStar,
        StatsAgg::CountField {
            column_name: "value".to_string(),
            arg_type: DataType::Int64,
        },
    ];

    assert_eq!(common_stats_file_ordinals(&aggregates, &stats), vec![0]);
}

#[test]
fn test_filter_stats_by_file_ordinals_keeps_only_selected_files() {
    let stats = RegionScanInputStats {
        files: vec![
            RegionScanFileInputStats {
                file_ordinal: 0,
                file_id: test_file_id(0),
                exact_num_rows: Some(3),
                time_range: Some((test_timestamp(10), test_timestamp(20))),
                field_stats: HashMap::new(),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 1,
                file_id: test_file_id(1),
                exact_num_rows: Some(4),
                time_range: Some((test_timestamp(30), test_timestamp(40))),
                field_stats: HashMap::new(),
                partition_expr_matches_region: true,
            },
        ],
    };

    let filtered = filter_stats_by_file_ordinals(&stats, &[1]);
    assert_eq!(filtered.files.len(), 1);
    assert_eq!(filtered.files[0].file_ordinal, 1);
}

#[test]
fn test_partial_state_from_stats_count_star() {
    let aggregate = StatsAgg::CountStar;
    let stats = RegionScanInputStats {
        files: vec![
            RegionScanFileInputStats {
                file_ordinal: 0,
                file_id: test_file_id(0),
                exact_num_rows: Some(3),
                time_range: Some((test_timestamp(10), test_timestamp(20))),
                field_stats: HashMap::new(),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 1,
                file_id: test_file_id(1),
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
                file_id: test_file_id(0),
                exact_num_rows: Some(3),
                time_range: Some((test_timestamp(10), test_timestamp(20))),
                field_stats: field_stats(Some(2), Some(Value::Int64(1)), Some(Value::Int64(3))),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 1,
                file_id: test_file_id(1),
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
                file_id: test_file_id(0),
                exact_num_rows: Some(3),
                time_range: Some((test_timestamp(50), test_timestamp(70))),
                field_stats: HashMap::new(),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 1,
                file_id: test_file_id(1),
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
                file_id: test_file_id(0),
                exact_num_rows: Some(3),
                time_range: Some((test_timestamp(10), test_timestamp(20))),
                field_stats: field_stats(Some(2), Some(Value::Int64(1)), Some(Value::Int64(3))),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 1,
                file_id: test_file_id(1),
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

#[test]
fn test_optimizer_rewrites_into_final_union_partial_scan_and_stats() {
    let schema = Arc::new(GreptimeSchema::new(vec![
        ColumnSchema::new(
            "ts",
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        ),
        ColumnSchema::new("value", ConcreteDataType::int64_datatype(), true),
    ]));
    let metadata = test_region_metadata();
    let excluded_count = Arc::new(AtomicUsize::new(0));
    let base_stats = RegionScanInputStats {
        files: vec![
            RegionScanFileInputStats {
                file_ordinal: 0,
                file_id: test_file_id(0),
                exact_num_rows: Some(3),
                time_range: Some((test_timestamp(10), test_timestamp(20))),
                field_stats: field_stats(Some(2), Some(Value::Int64(1)), Some(Value::Int64(3))),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 1,
                file_id: test_file_id(1),
                exact_num_rows: Some(4),
                time_range: Some((test_timestamp(30), test_timestamp(40))),
                field_stats: HashMap::new(),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 2,
                file_id: test_file_id(2),
                exact_num_rows: Some(5),
                time_range: Some((test_timestamp(50), test_timestamp(60))),
                field_stats: field_stats(Some(4), Some(Value::Int64(5)), Some(Value::Int64(9))),
                partition_expr_matches_region: true,
            },
        ],
    };
    let scanner = Box::new(StatsRecordingScanner::new(
        schema.clone(),
        metadata,
        base_stats,
        excluded_count.clone(),
    ));
    let scan = Arc::new(RegionScanExec::new(scanner, ScanRequest::default(), None).unwrap());
    let aggr_expr = Arc::new(build_test_aggr_expr(false, false, false).unwrap());
    let plan: Arc<dyn ExecutionPlan> = Arc::new(
        AggregateExec::try_new(
            AggregateMode::Single,
            PhysicalGroupBy::new_single(vec![]),
            vec![aggr_expr],
            vec![None],
            scan,
            schema.arrow_schema().clone(),
        )
        .unwrap(),
    );

    let optimized = AggregateStats::do_optimize(plan).unwrap();
    let final_agg = optimized.as_any().downcast_ref::<AggregateExec>().unwrap();
    assert_eq!(final_agg.mode(), &AggregateMode::Final);

    let coalesce = final_agg
        .input()
        .as_any()
        .downcast_ref::<CoalescePartitionsExec>()
        .unwrap();
    let union = coalesce
        .input()
        .as_any()
        .downcast_ref::<UnionExec>()
        .unwrap();
    assert_eq!(union.inputs().len(), 2);

    let partial_agg = union.inputs()[0]
        .as_any()
        .downcast_ref::<AggregateExec>()
        .unwrap();
    assert_eq!(partial_agg.mode(), &AggregateMode::Partial);
    let partial_scan = partial_agg
        .input()
        .as_any()
        .downcast_ref::<RegionScanExec>()
        .unwrap();
    let remaining = partial_scan.scan_input_stats().unwrap().unwrap();
    assert_eq!(
        remaining
            .files
            .iter()
            .map(|file| file.file_ordinal)
            .collect::<Vec<_>>(),
        vec![1]
    );
    assert_eq!(excluded_count.load(Ordering::Relaxed), 2);

    let explain = displayable(optimized.as_ref()).indent(true).to_string();
    assert!(
        explain.contains("aggregate_stats: rewritten=true"),
        "explain: {explain}"
    );
    assert!(explain.contains("stats_files=2"), "explain: {explain}");
    assert!(explain.contains(&test_file_id(0).to_string()), "explain: {explain}");
    assert!(explain.contains(&test_file_id(2).to_string()), "explain: {explain}");
}

#[test]
fn test_optimizer_rewrites_final_partial_plan_by_replacing_partial_input() {
    let schema = Arc::new(GreptimeSchema::new(vec![
        ColumnSchema::new(
            "ts",
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        ),
        ColumnSchema::new("value", ConcreteDataType::int64_datatype(), true),
    ]));
    let metadata = test_region_metadata();
    let excluded_count = Arc::new(AtomicUsize::new(0));
    let base_stats = RegionScanInputStats {
        files: vec![
            RegionScanFileInputStats {
                file_ordinal: 0,
                file_id: test_file_id(0),
                exact_num_rows: Some(3),
                time_range: Some((test_timestamp(10), test_timestamp(20))),
                field_stats: field_stats(Some(2), Some(Value::Int64(1)), Some(Value::Int64(3))),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 1,
                file_id: test_file_id(1),
                exact_num_rows: Some(4),
                time_range: Some((test_timestamp(30), test_timestamp(40))),
                field_stats: HashMap::new(),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 2,
                file_id: test_file_id(2),
                exact_num_rows: Some(5),
                time_range: Some((test_timestamp(50), test_timestamp(60))),
                field_stats: field_stats(Some(4), Some(Value::Int64(5)), Some(Value::Int64(9))),
                partition_expr_matches_region: true,
            },
        ],
    };
    let scanner = Box::new(StatsRecordingScanner::new(
        schema.clone(),
        metadata,
        base_stats,
        excluded_count.clone(),
    ));
    let scan = Arc::new(RegionScanExec::new(scanner, ScanRequest::default(), None).unwrap());
    let aggr_expr = Arc::new(build_test_aggr_expr(false, false, false).unwrap());
    let partial: Arc<dyn ExecutionPlan> = Arc::new(
        AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::new_single(vec![]),
            vec![aggr_expr.clone()],
            vec![None],
            scan,
            schema.arrow_schema().clone(),
        )
        .unwrap(),
    );
    let plan: Arc<dyn ExecutionPlan> = Arc::new(
        AggregateExec::try_new(
            AggregateMode::Final,
            PhysicalGroupBy::new_single(vec![]),
            vec![aggr_expr],
            vec![None],
            Arc::new(CoalescePartitionsExec::new(partial.clone())),
            partial.schema(),
        )
        .unwrap(),
    );

    let optimized = AggregateStats::do_optimize(plan).unwrap();
    let final_agg = optimized.as_any().downcast_ref::<AggregateExec>().unwrap();
    assert_eq!(final_agg.mode(), &AggregateMode::Final);

    let coalesce = final_agg
        .input()
        .as_any()
        .downcast_ref::<CoalescePartitionsExec>()
        .unwrap();
    let union = coalesce
        .input()
        .as_any()
        .downcast_ref::<UnionExec>()
        .unwrap();
    assert_eq!(union.inputs().len(), 2);

    let partial_agg = union.inputs()[0]
        .as_any()
        .downcast_ref::<AggregateExec>()
        .unwrap();
    assert_eq!(partial_agg.mode(), &AggregateMode::Partial);
    let partial_scan = partial_agg
        .input()
        .as_any()
        .downcast_ref::<RegionScanExec>()
        .unwrap();
    let remaining = partial_scan.scan_input_stats().unwrap().unwrap();
    assert_eq!(
        remaining
            .files
            .iter()
            .map(|file| file.file_ordinal)
            .collect::<Vec<_>>(),
        vec![1]
    );
    assert_eq!(excluded_count.load(Ordering::Relaxed), 2);
}

#[derive(Clone, Copy)]
enum ExecutionAggExprCase {
    CountValue { ignore_nulls: bool },
    CountStar,
    MinValue { ignore_nulls: bool },
    MaxValue { ignore_nulls: bool },
}

impl ExecutionAggExprCase {
    fn build(self) -> AggregateFunctionExpr {
        match self {
            ExecutionAggExprCase::CountValue { ignore_nulls } => {
                build_test_aggr_expr(false, ignore_nulls, false).unwrap()
            }
            ExecutionAggExprCase::CountStar => build_count_star_aggr_expr().unwrap(),
            ExecutionAggExprCase::MinValue { ignore_nulls } => {
                build_min_field_aggr_expr_with_ignore_nulls(ignore_nulls).unwrap()
            }
            ExecutionAggExprCase::MaxValue { ignore_nulls } => {
                build_max_field_aggr_expr_with_ignore_nulls(ignore_nulls).unwrap()
            }
        }
    }
}

fn execution_test_schema() -> datatypes::schema::SchemaRef {
    Arc::new(GreptimeSchema::new(vec![
        ColumnSchema::new(
            "ts",
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        )
        .with_time_index(true),
        ColumnSchema::new("value", ConcreteDataType::int64_datatype(), true),
    ]))
}

fn execution_test_stats() -> RegionScanInputStats {
    RegionScanInputStats {
        files: vec![
            RegionScanFileInputStats {
                file_ordinal: 0,
                file_id: test_file_id(0),
                exact_num_rows: Some(3),
                time_range: Some((test_timestamp(10), test_timestamp(12))),
                field_stats: field_stats(Some(2), Some(Value::Int64(1)), Some(Value::Int64(3))),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 1,
                file_id: test_file_id(1),
                exact_num_rows: Some(4),
                time_range: Some((test_timestamp(30), test_timestamp(33))),
                field_stats: HashMap::new(),
                partition_expr_matches_region: true,
            },
            RegionScanFileInputStats {
                file_ordinal: 2,
                file_id: test_file_id(2),
                exact_num_rows: Some(5),
                time_range: Some((test_timestamp(50), test_timestamp(54))),
                field_stats: field_stats(Some(4), Some(Value::Int64(7)), Some(Value::Int64(10))),
                partition_expr_matches_region: true,
            },
        ],
    }
}

fn execution_test_file_batches(
    schema: datatypes::schema::SchemaRef,
) -> Vec<(usize, CommonRecordBatch)> {
    vec![
        (
            0,
            scan_batch(
                schema.clone(),
                vec![10, 11, 12],
                vec![Some(1), None, Some(3)],
            ),
        ),
        (
            1,
            scan_batch(
                schema.clone(),
                vec![30, 31, 32, 33],
                vec![Some(4), Some(5), None, Some(6)],
            ),
        ),
        (
            2,
            scan_batch(
                schema,
                vec![50, 51, 52, 53, 54],
                vec![Some(7), Some(8), Some(9), Some(10), None],
            ),
        ),
    ]
}

fn build_execution_test_plan(
    schema: datatypes::schema::SchemaRef,
    metadata: RegionMetadataRef,
    base_stats: RegionScanInputStats,
    file_batches: Vec<(usize, CommonRecordBatch)>,
    aggr_expr: AggregateFunctionExpr,
    excluded_count: Arc<AtomicUsize>,
) -> Arc<dyn ExecutionPlan> {
    let scanner = Box::new(
        StatsRecordingScanner::new(schema.clone(), metadata, base_stats, excluded_count)
            .with_file_batches(file_batches),
    );
    let scan = Arc::new(RegionScanExec::new(scanner, ScanRequest::default(), None).unwrap());
    let aggr_expr = Arc::new(aggr_expr);
    let partial: Arc<dyn ExecutionPlan> = Arc::new(
        AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::new_single(vec![]),
            vec![aggr_expr.clone()],
            vec![None],
            scan,
            schema.arrow_schema().clone(),
        )
        .unwrap(),
    );

    Arc::new(
        AggregateExec::try_new(
            AggregateMode::Final,
            PhysicalGroupBy::new_single(vec![]),
            vec![aggr_expr],
            vec![None],
            Arc::new(CoalescePartitionsExec::new(partial.clone())),
            partial.schema(),
        )
        .unwrap(),
    )
}

fn optimized_plan_uses_stats_union(plan: &Arc<dyn ExecutionPlan>) -> bool {
    let Some(final_agg) = plan.as_any().downcast_ref::<AggregateExec>() else {
        return false;
    };

    let input = final_agg.input();
    if let Some(coalesce) = input.as_any().downcast_ref::<CoalescePartitionsExec>() {
        return coalesce
            .input()
            .as_any()
            .downcast_ref::<UnionExec>()
            .is_some();
    }

    input.as_any().downcast_ref::<UnionExec>().is_some()
}

#[tokio::test]
async fn test_optimizer_execution_matrix() {
    struct Case {
        name: &'static str,
        expr: ExecutionAggExprCase,
        expect_rewrite: bool,
        expected_excluded_count: usize,
        expected: String,
    }

    let cases = [
        Case {
            name: "count value",
            expr: ExecutionAggExprCase::CountValue {
                ignore_nulls: false,
            },
            expect_rewrite: true,
            expected_excluded_count: 2,
            expected: [
                "+--------------+",
                "| count(value) |",
                "+--------------+",
                "| 9            |",
                "+--------------+",
            ]
            .join("\n"),
        },
        Case {
            name: "count value ignore nulls",
            expr: ExecutionAggExprCase::CountValue { ignore_nulls: true },
            expect_rewrite: true,
            expected_excluded_count: 2,
            expected: [
                "+--------------+",
                "| count(value) |",
                "+--------------+",
                "| 9            |",
                "+--------------+",
            ]
            .join("\n"),
        },
        Case {
            name: "count star",
            expr: ExecutionAggExprCase::CountStar,
            expect_rewrite: true,
            expected_excluded_count: 3,
            expected: [
                "+----------+",
                "| count(*) |",
                "+----------+",
                "| 12       |",
                "+----------+",
            ]
            .join("\n"),
        },
        Case {
            name: "min value",
            expr: ExecutionAggExprCase::MinValue {
                ignore_nulls: false,
            },
            expect_rewrite: true,
            expected_excluded_count: 2,
            expected: [
                "+------------+",
                "| min(value) |",
                "+------------+",
                "| 1          |",
                "+------------+",
            ]
            .join("\n"),
        },
        Case {
            name: "min value ignore nulls",
            expr: ExecutionAggExprCase::MinValue { ignore_nulls: true },
            expect_rewrite: true,
            expected_excluded_count: 2,
            expected: [
                "+------------+",
                "| min(value) |",
                "+------------+",
                "| 1          |",
                "+------------+",
            ]
            .join("\n"),
        },
        Case {
            name: "max value",
            expr: ExecutionAggExprCase::MaxValue {
                ignore_nulls: false,
            },
            expect_rewrite: true,
            expected_excluded_count: 2,
            expected: [
                "+------------+",
                "| max(value) |",
                "+------------+",
                "| 10         |",
                "+------------+",
            ]
            .join("\n"),
        },
        Case {
            name: "max value ignore nulls",
            expr: ExecutionAggExprCase::MaxValue { ignore_nulls: true },
            expect_rewrite: true,
            expected_excluded_count: 2,
            expected: [
                "+------------+",
                "| max(value) |",
                "+------------+",
                "| 10         |",
                "+------------+",
            ]
            .join("\n"),
        },
    ];

    let schema = execution_test_schema();
    let metadata = test_region_metadata();
    let base_stats = execution_test_stats();
    let file_batches = execution_test_file_batches(schema.clone());

    for case in cases {
        let unoptimized = build_execution_test_plan(
            schema.clone(),
            metadata.clone(),
            base_stats.clone(),
            file_batches.clone(),
            case.expr.build(),
            Arc::new(AtomicUsize::new(0)),
        );
        let unoptimized_result =
            datafusion::physical_plan::collect(unoptimized, SessionContext::default().task_ctx())
                .await
                .unwrap();

        let optimized_excluded_count = Arc::new(AtomicUsize::new(0));
        let optimized = AggregateStats::do_optimize(build_execution_test_plan(
            schema.clone(),
            metadata.clone(),
            base_stats.clone(),
            file_batches.clone(),
            case.expr.build(),
            optimized_excluded_count.clone(),
        ))
        .unwrap();
        let optimized_result = datafusion::physical_plan::collect(
            optimized.clone(),
            SessionContext::default().task_ctx(),
        )
        .await
        .unwrap();

        let unoptimized_pretty = arrow::util::pretty::pretty_format_batches(&unoptimized_result)
            .unwrap()
            .to_string();
        let optimized_pretty = arrow::util::pretty::pretty_format_batches(&optimized_result)
            .unwrap()
            .to_string();

        assert_eq!(
            unoptimized_pretty,
            case.expected.as_str(),
            "case: {}",
            case.name
        );
        assert_eq!(
            optimized_pretty,
            case.expected.as_str(),
            "case: {}",
            case.name
        );
        assert_eq!(
            optimized_plan_uses_stats_union(&optimized),
            case.expect_rewrite,
            "case: {}",
            case.name
        );
        assert_eq!(
            optimized_excluded_count.load(Ordering::Relaxed),
            case.expected_excluded_count,
            "case: {}",
            case.name
        );
    }
}

#[test]
fn test_optimizer_observability_distinguishes_rewrite_hit_and_miss() {
    let schema = execution_test_schema();
    let metadata = test_region_metadata();
    let base_stats = execution_test_stats();
    let file_batches = execution_test_file_batches(schema.clone());
    let hit_excluded_count = Arc::new(AtomicUsize::new(0));

    let hit_plan = build_execution_test_plan(
        schema.clone(),
        metadata.clone(),
        base_stats.clone(),
        file_batches.clone(),
        ExecutionAggExprCase::CountValue {
            ignore_nulls: false,
        }
        .build(),
        hit_excluded_count.clone(),
    );
    let optimized_hit = AggregateStats::do_optimize(hit_plan).unwrap();
    assert!(optimized_plan_uses_stats_union(&optimized_hit));
    assert_eq!(hit_excluded_count.load(Ordering::Relaxed), 2);

    let hit_explain = displayable(optimized_hit.as_ref()).indent(true).to_string();
    assert!(hit_explain.contains("aggregate_stats: rewritten=true"));
    assert!(hit_explain.contains("stats_files=2"));
    assert!(hit_explain.contains(&test_file_id(0).to_string()));
    assert!(hit_explain.contains(&test_file_id(2).to_string()));

    let miss_excluded_count = Arc::new(AtomicUsize::new(0));
    let miss_scanner = Box::new(
        StatsRecordingScanner::new(
            schema.clone(),
            metadata,
            base_stats,
            miss_excluded_count.clone(),
        )
        .with_file_batches(file_batches),
    );
    let miss_scan = Arc::new(RegionScanExec::new(miss_scanner, ScanRequest::default(), None).unwrap());
    let miss_aggr_expr = Arc::new(build_test_aggr_expr(true, false, false).unwrap());
    let miss_plan: Arc<dyn ExecutionPlan> = Arc::new(
        AggregateExec::try_new(
            AggregateMode::Single,
            PhysicalGroupBy::new_single(vec![]),
            vec![miss_aggr_expr],
            vec![None],
            miss_scan,
            schema.arrow_schema().clone(),
        )
        .unwrap(),
    );

    let optimized_miss = AggregateStats::do_optimize(miss_plan).unwrap();
    assert!(!optimized_plan_uses_stats_union(&optimized_miss));
    assert_eq!(miss_excluded_count.load(Ordering::Relaxed), 0);

    let miss_explain = displayable(optimized_miss.as_ref()).indent(true).to_string();
    assert!(!miss_explain.contains("aggregate_stats: rewritten=true"));
}
