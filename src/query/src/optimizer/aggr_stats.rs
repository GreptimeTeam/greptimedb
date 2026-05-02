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

use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{DataFusionError, Result};
use table::table::scan::RegionScanExec;

use crate::optimizer::aggr_stats::stat_scan::StatsScanExec;
use crate::optimizer::aggr_stats::support_aggr::{
    SupportStatAggr, support_stat_aggr_from_aggr_expr,
};

pub(crate) mod stat_scan;
pub(crate) mod support_aggr;

/// Physical optimizer scaffold for aggregate-stats runtime rewrite.
///
/// This pass only owns query-shape eligibility and rewrite shape.
/// Runtime stats lookup/classification is handled during execution.
#[derive(Debug, Default)]
pub struct AggrStatsPhysicalRule;

impl PhysicalOptimizerRule for AggrStatsPhysicalRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Self::rewrite_plan_shape(plan)
    }

    fn name(&self) -> &str {
        "aggr_stats_physical"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

impl AggrStatsPhysicalRule {
    fn rewrite_plan_shape(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(|plan| {
            let Some(rewrite_target) = RewriteTarget::extract(&plan) else {
                return Ok(Transformed::no(plan));
            };
            rewrite_target.rewrite().map(Transformed::yes)
        })
        .map(|res| res.data)
    }
}

/// TODO(discord9): support more kind of aggr
#[allow(unused)]
enum RewriteTarget<'a> {
    FinalOverPartial {
        final_exec: &'a AggregateExec,
        partial_exec: &'a AggregateExec,
        region_scan: &'a RegionScanExec,
        keep_coalesce: bool,
        aggr_exprs: Vec<SupportStatAggr>,
    },
}

#[allow(unused)]
impl<'a> RewriteTarget<'a> {
    fn is_supported_rewrite(&self) -> bool {
        let Self::FinalOverPartial {
            final_exec,
            partial_exec,
            region_scan,
            ..
        } = self;

        final_exec.group_expr().is_empty()
            && final_exec
                .aggr_expr()
                .iter()
                .all(|aggr| !aggr.is_distinct())
            && final_exec
                .filter_expr()
                .iter()
                .all(|filter| filter.is_none())
            && partial_exec
                .filter_expr()
                .iter()
                .all(|filter| filter.is_none())
            && region_scan.append_mode()
            && !region_scan
                .scanner()
                .lock()
                .unwrap()
                .has_predicate_without_region()
    }

    #[allow(unused)]
    fn extract(plan: &'a Arc<dyn ExecutionPlan>) -> Option<Self> {
        let aggregate_exec = plan.as_any().downcast_ref::<AggregateExec>()?;

        if !matches!(aggregate_exec.mode(), AggregateMode::Final) {
            return None;
        }

        let (input, keep_coalesce) = if let Some(coalesce) = aggregate_exec
            .input()
            .as_any()
            .downcast_ref::<CoalescePartitionsExec>(
        ) {
            (coalesce.input(), true)
        } else {
            (aggregate_exec.input(), false)
        };

        let partial_exec = input.as_any().downcast_ref::<AggregateExec>()?;
        if !matches!(partial_exec.mode(), AggregateMode::Partial) {
            return None;
        }

        let region_scan = partial_exec
            .input()
            .as_any()
            .downcast_ref::<RegionScanExec>()?;
        let aggr_exprs = aggregate_exec
            .aggr_expr()
            .iter()
            .map(|aggr_expr| {
                support_stat_aggr_from_aggr_expr(aggr_expr.as_ref(), &region_scan.time_index())
            })
            .try_collect()?;
        let zelf = Self::FinalOverPartial {
            final_exec: aggregate_exec,
            partial_exec,
            region_scan,
            keep_coalesce,
            aggr_exprs,
        };
        if zelf.is_supported_rewrite() {
            Some(zelf)
        } else {
            None
        }
    }

    fn rewrite(&self) -> Result<Arc<dyn ExecutionPlan>> {
        match self {
            Self::FinalOverPartial {
                final_exec,
                partial_exec,
                region_scan,
                keep_coalesce,
                aggr_exprs,
            } => {
                let requirements = aggr_exprs.clone();
                let stats_scan = Arc::new(StatsScanExec::new(
                    partial_exec.schema(),
                    requirements.clone(),
                    region_scan.scanner(),
                ));

                let fallback_scan = Arc::new(
                    region_scan
                        .with_stats_aware_skip_requirements(requirements)
                        .map_err(|error| DataFusionError::External(error.into()))?,
                );
                let fallback_partial = Arc::new(AggregateExec::try_new(
                    *partial_exec.mode(),
                    Arc::new(partial_exec.group_expr().clone()),
                    partial_exec.aggr_expr().to_vec(),
                    partial_exec.filter_expr().to_vec(),
                    fallback_scan,
                    partial_exec.input_schema(),
                )?);

                let union = UnionExec::try_new(vec![stats_scan, fallback_partial])?;
                let merge_input: Arc<dyn ExecutionPlan> =
                    if *keep_coalesce || union.properties().partitioning.partition_count() > 1 {
                        Arc::new(CoalescePartitionsExec::new(union))
                    } else {
                        union
                    };

                let final_aggregate = AggregateExec::try_new(
                    *final_exec.mode(),
                    Arc::new(final_exec.group_expr().clone()),
                    final_exec.aggr_expr().to_vec(),
                    final_exec.filter_expr().to_vec(),
                    merge_input,
                    final_exec.input_schema(),
                )?;

                Ok(Arc::new(final_aggregate))
            }
        }
    }

    fn first_stage_aggregate(&self) -> &'a AggregateExec {
        match self {
            RewriteTarget::FinalOverPartial { partial_exec, .. } => partial_exec,
        }
    }

    fn region_scan(&self) -> &'a RegionScanExec {
        match self {
            RewriteTarget::FinalOverPartial { region_scan, .. } => region_scan,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::{Arc, Mutex};

    use api::v1::SemanticType;
    use bytes::Bytes;
    use common_error::ext::BoxedError;
    use common_query::aggr_stats::StatsCandidateFile;
    use common_recordbatch::{
        EmptyRecordBatchStream, RecordBatch, RecordBatches, SendableRecordBatchStream,
    };
    use datafusion::functions_aggregate::average::avg_udaf;
    use datafusion::functions_aggregate::count::count_udaf;
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use datafusion::parquet::file::properties::WriterProperties;
    use datafusion::physical_plan::aggregates::PhysicalGroupBy;
    use datafusion::physical_plan::filter_pushdown::{
        ChildFilterPushdownResult, ChildPushdownResult, FilterPushdownPhase, PushedDown,
    };
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use datafusion::physical_plan::{DisplayAs, DisplayFormatType, collect};
    use datafusion::scalar::ScalarValue;
    use datafusion_expr::utils::COUNT_STAR_EXPANSION;
    use datafusion_physical_expr::aggregate::{AggregateExprBuilder, AggregateFunctionExpr};
    use datafusion_physical_expr::expressions::{
        Column as PhysicalColumn, DynamicFilterPhysicalExpr, Literal, lit,
    };
    use datatypes::arrow::array::Float64Array;
    use datatypes::arrow::datatypes::DataType;
    use datatypes::arrow::record_batch::RecordBatch as ArrowRecordBatch;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::prelude::VectorRef;
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::{Float64Vector, TimestampMillisecondVector};
    use futures::StreamExt;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::region_engine::{
        FileStatsItem, PrepareRequest, QueryScanContext, RegionScanner, RowGroupStatsItem,
        ScannerProperties, SendableFileStatsStream, SinglePartitionScanner,
        StatsAwareFallbackReason, StatsAwareFileDecision,
    };
    use store_api::storage::{RegionId, ScanRequest};

    use super::*;

    #[derive(Clone, Debug)]
    struct ScanTestFile {
        id: String,
        stats: FileStatsItem,
        batch: RecordBatch,
    }

    #[derive(Debug)]
    struct RecordingStatsScanner {
        schema: Arc<Schema>,
        metadata: store_api::metadata::RegionMetadataRef,
        properties: ScannerProperties,
        files: Vec<ScanTestFile>,
        scanned_file_ids: Arc<Mutex<Vec<String>>>,
        has_predicate_without_region: bool,
    }

    impl RecordingStatsScanner {
        fn new(
            schema: Arc<Schema>,
            metadata: store_api::metadata::RegionMetadataRef,
            files: Vec<ScanTestFile>,
            scanned_file_ids: Arc<Mutex<Vec<String>>>,
        ) -> Self {
            let total_rows = files.iter().map(|file| file.batch.num_rows()).sum();
            Self {
                schema,
                metadata,
                properties: ScannerProperties::default()
                    .with_append_mode(true)
                    .with_total_rows(total_rows),
                files,
                scanned_file_ids,
                has_predicate_without_region: false,
            }
        }

        fn with_predicate_without_region(mut self, has_predicate_without_region: bool) -> Self {
            self.has_predicate_without_region = has_predicate_without_region;
            self
        }

        fn stats_decision(&self, file: &ScanTestFile) -> StatsAwareFileDecision {
            let requirements = self.properties.stats_aware_skip_requirements();
            let Some(candidate) = StatsCandidateFile::from_file_stats(
                &file.stats,
                self.metadata.partition_expr.as_deref(),
                requirements,
                &self.metadata.schema,
            )
            .unwrap() else {
                return StatsAwareFileDecision::scan_fallback(
                    file.id.clone(),
                    StatsAwareFallbackReason::MissingOrUnusableRuntimeStats,
                );
            };

            let values = requirements
                .iter()
                .map(|requirement| candidate.stat_value(requirement).unwrap().unwrap())
                .collect();
            StatsAwareFileDecision::stats_hit(file.id.clone(), values)
        }

        fn should_skip_file(&self, file: &ScanTestFile) -> bool {
            let requirements = self.properties.stats_aware_skip_requirements();
            if requirements.is_empty() {
                return false;
            }

            self.properties
                .stats_aware_file_decision_state()
                .get_or_compute(file.id.clone(), || {
                    Ok::<_, std::convert::Infallible>(self.stats_decision(file))
                })
                .unwrap()
                .is_stats_hit()
        }
    }

    impl RegionScanner for RecordingStatsScanner {
        fn name(&self) -> &str {
            "RecordingStatsScanner"
        }

        fn properties(&self) -> &ScannerProperties {
            &self.properties
        }

        fn schema(&self) -> Arc<Schema> {
            self.schema.clone()
        }

        fn metadata(&self) -> store_api::metadata::RegionMetadataRef {
            self.metadata.clone()
        }

        fn prepare(&mut self, request: PrepareRequest) -> std::result::Result<(), BoxedError> {
            self.properties.prepare(request);
            Ok(())
        }

        fn scan_partition(
            &self,
            _ctx: &QueryScanContext,
            _metrics_set: &ExecutionPlanMetricsSet,
            _partition: usize,
        ) -> std::result::Result<SendableRecordBatchStream, BoxedError> {
            let batches = self
                .files
                .iter()
                .filter(|file| self.has_predicate_without_region || !self.should_skip_file(file))
                .map(|file| {
                    self.scanned_file_ids.lock().unwrap().push(file.id.clone());
                    file.batch.clone()
                })
                .collect::<Vec<_>>();

            Ok(RecordBatches::try_new(self.schema.clone(), batches)
                .unwrap()
                .as_stream())
        }

        fn scan_stats(
            &self,
            _ctx: &QueryScanContext,
        ) -> std::result::Result<SendableFileStatsStream, BoxedError> {
            Ok(Box::pin(futures::stream::iter(
                self.files.clone().into_iter().map(|file| Ok(file.stats)),
            )))
        }

        fn has_predicate_without_region(&self) -> bool {
            self.has_predicate_without_region
        }

        fn add_dyn_filter_to_predicate(
            &mut self,
            filter_exprs: Vec<Arc<dyn datafusion::physical_plan::PhysicalExpr>>,
        ) -> Vec<bool> {
            let supported = filter_exprs
                .into_iter()
                .map(|expr| {
                    (expr as Arc<dyn std::any::Any + Send + Sync + 'static>)
                        .downcast::<DynamicFilterPhysicalExpr>()
                        .is_ok()
                })
                .collect::<Vec<_>>();
            self.has_predicate_without_region |= supported.iter().any(|supported| *supported);
            supported
        }

        fn set_logical_region(&mut self, logical_region: bool) {
            self.properties.set_logical_region(logical_region);
        }
    }

    impl datafusion::physical_plan::DisplayAs for RecordingStatsScanner {
        fn fmt_as(
            &self,
            _t: datafusion::physical_plan::DisplayFormatType,
            f: &mut std::fmt::Formatter<'_>,
        ) -> std::fmt::Result {
            write!(f, "RecordingStatsScanner")
        }
    }

    fn build_count_expr(schema: arrow_schema::SchemaRef) -> Arc<AggregateFunctionExpr> {
        Arc::new(
            AggregateExprBuilder::new(count_udaf(), vec![Arc::new(PhysicalColumn::new("v0", 0))])
                .schema(schema)
                .alias("count(v0)")
                .build()
                .unwrap(),
        )
    }

    fn build_count_time_index_expr(schema: arrow_schema::SchemaRef) -> Arc<AggregateFunctionExpr> {
        Arc::new(
            AggregateExprBuilder::new(count_udaf(), vec![Arc::new(PhysicalColumn::new("ts", 1))])
                .schema(schema)
                .alias("count(ts)")
                .build()
                .unwrap(),
        )
    }

    fn build_count_star_expr(schema: arrow_schema::SchemaRef) -> Arc<AggregateFunctionExpr> {
        Arc::new(
            AggregateExprBuilder::new(
                count_udaf(),
                vec![Arc::new(Literal::new(COUNT_STAR_EXPANSION.clone()))],
            )
            .schema(schema)
            .alias("count(*)")
            .build()
            .unwrap(),
        )
    }

    fn build_count_null_expr(schema: arrow_schema::SchemaRef) -> Arc<AggregateFunctionExpr> {
        Arc::new(
            AggregateExprBuilder::new(
                count_udaf(),
                vec![Arc::new(Literal::new(ScalarValue::Null))],
            )
            .schema(schema)
            .alias("count(NULL)")
            .build()
            .unwrap(),
        )
    }

    fn build_dynamic_filter_expr() -> Arc<dyn datafusion::physical_plan::PhysicalExpr> {
        Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(PhysicalColumn::new("v0", 0))],
            lit(true),
        ))
    }

    fn build_avg_expr(schema: arrow_schema::SchemaRef) -> Arc<AggregateFunctionExpr> {
        Arc::new(
            AggregateExprBuilder::new(avg_udaf(), vec![Arc::new(PhysicalColumn::new("v0", 0))])
                .schema(schema)
                .alias("avg(v0)")
                .build()
                .unwrap(),
        )
    }

    fn group_by_v0() -> PhysicalGroupBy {
        PhysicalGroupBy::new_single(vec![(
            Arc::new(PhysicalColumn::new("v0", 0)),
            "v0".to_string(),
        )])
    }

    fn build_region_scan(append_mode: bool) -> Arc<RegionScanExec> {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new("v0", ConcreteDataType::float64_datatype(), true),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
        ]));
        let stream = Box::pin(EmptyRecordBatchStream::new(schema.clone()));

        let mut metadata_builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
        metadata_builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("v0", ConcreteDataType::float64_datatype(), true),
                semantic_type: SemanticType::Field,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                )
                .with_time_index(true),
                semantic_type: SemanticType::Timestamp,
                column_id: 2,
            })
            .primary_key(vec![]);
        let metadata = Arc::new(metadata_builder.build().unwrap());

        let scanner = Box::new(SinglePartitionScanner::new(
            stream,
            append_mode,
            metadata,
            None,
        ));
        Arc::new(RegionScanExec::new(scanner, ScanRequest::default(), None).unwrap())
    }

    fn build_region_metadata(
        partition_expr: Option<&str>,
    ) -> store_api::metadata::RegionMetadataRef {
        let mut metadata_builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
        metadata_builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("v0", ConcreteDataType::float64_datatype(), true),
                semantic_type: SemanticType::Field,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                )
                .with_time_index(true),
                semantic_type: SemanticType::Timestamp,
                column_id: 2,
            })
            .primary_key(vec![]);
        let mut metadata = metadata_builder.build().unwrap();
        metadata.set_partition_expr(partition_expr.map(str::to_string));
        Arc::new(metadata)
    }

    fn build_float_row_groups(chunks: &[Vec<Option<f64>>]) -> Vec<RowGroupStatsItem> {
        let arrow_schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "v0",
            DataType::Float64,
            true,
        )]));
        let mut buffer = Cursor::new(Vec::new());
        let props = WriterProperties::builder().build();
        let mut writer =
            ArrowWriter::try_new(&mut buffer, arrow_schema.clone(), Some(props)).unwrap();

        for chunk in chunks {
            let batch = ArrowRecordBatch::try_new(
                arrow_schema.clone(),
                vec![Arc::new(Float64Array::from(chunk.clone()))],
            )
            .unwrap();
            writer.write(&batch).unwrap();
        }
        writer.close().unwrap();

        let metadata = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(buffer.into_inner()))
            .unwrap()
            .metadata()
            .clone();
        metadata
            .row_groups()
            .iter()
            .enumerate()
            .map(|(row_group_index, metadata)| RowGroupStatsItem {
                row_group_index,
                metadata: Arc::new(metadata.clone()),
            })
            .collect()
    }

    fn build_scan_test_file(
        schema: Arc<Schema>,
        id: &str,
        partition_expr: &str,
        values: Vec<Option<f64>>,
        with_row_groups: bool,
        ts_start: i64,
    ) -> ScanTestFile {
        let batch = RecordBatch::new(
            schema,
            vec![
                Arc::new(Float64Vector::from(values.clone())) as VectorRef,
                Arc::new(TimestampMillisecondVector::from_values(
                    (0..values.len()).map(|offset| ts_start + offset as i64),
                )) as VectorRef,
            ],
        )
        .unwrap();

        ScanTestFile {
            id: id.to_string(),
            stats: FileStatsItem {
                file_id: id.to_string(),
                num_rows: Some(values.len() as u64),
                file_partition_expr: Some(partition_expr.to_string()),
                row_groups: if with_row_groups {
                    build_float_row_groups(&[values])
                } else {
                    vec![]
                },
            },
            batch,
        }
    }

    fn build_recording_region_scan(
        files: Vec<ScanTestFile>,
        scanned_file_ids: Arc<Mutex<Vec<String>>>,
    ) -> Arc<RegionScanExec> {
        build_recording_region_scan_with_predicate(files, scanned_file_ids, false)
    }

    fn build_recording_region_scan_with_predicate(
        files: Vec<ScanTestFile>,
        scanned_file_ids: Arc<Mutex<Vec<String>>>,
        has_predicate_without_region: bool,
    ) -> Arc<RegionScanExec> {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new("v0", ConcreteDataType::float64_datatype(), true),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
        ]));
        let metadata = build_region_metadata(Some("host = 'a'"));
        let scanner = Box::new(
            RecordingStatsScanner::new(schema, metadata, files, scanned_file_ids)
                .with_predicate_without_region(has_predicate_without_region),
        );
        Arc::new(RegionScanExec::new(scanner, ScanRequest::default(), None).unwrap())
    }

    fn build_final_over_partial_plan() -> Arc<dyn ExecutionPlan> {
        build_final_over_partial_plan_with(build_region_scan(true), build_count_expr, None)
    }

    fn build_final_over_partial_plan_with(
        region_scan: Arc<RegionScanExec>,
        build_aggr_expr: fn(arrow_schema::SchemaRef) -> Arc<AggregateFunctionExpr>,
        group_by: Option<PhysicalGroupBy>,
    ) -> Arc<dyn ExecutionPlan> {
        let input_schema = region_scan.schema();
        let aggr_expr = build_aggr_expr(input_schema.clone());
        let group_by = group_by.unwrap_or_default();

        let partial = Arc::new(
            AggregateExec::try_new(
                AggregateMode::Partial,
                group_by.clone(),
                vec![aggr_expr.clone()],
                vec![None],
                region_scan,
                input_schema.clone(),
            )
            .unwrap(),
        );

        let coalesce = Arc::new(CoalescePartitionsExec::new(partial));
        Arc::new(
            AggregateExec::try_new(
                AggregateMode::Final,
                group_by,
                vec![aggr_expr],
                vec![None],
                coalesce,
                input_schema,
            )
            .unwrap(),
        )
    }

    #[test]
    fn rewrite_builds_stats_scan_union_with_stats_aware_fallback() {
        let plan = build_final_over_partial_plan();
        let optimized = AggrStatsPhysicalRule
            .optimize(plan, &ConfigOptions::default())
            .unwrap();

        let final_exec = optimized.as_any().downcast_ref::<AggregateExec>().unwrap();
        assert!(matches!(final_exec.mode(), AggregateMode::Final));

        let coalesce = final_exec
            .input()
            .as_any()
            .downcast_ref::<CoalescePartitionsExec>()
            .unwrap();
        let union = coalesce
            .input()
            .as_any()
            .downcast_ref::<UnionExec>()
            .unwrap();
        let union_children = union.children();
        assert_eq!(union_children.len(), 2);

        let stats_scan = union_children[0]
            .as_any()
            .downcast_ref::<StatsScanExec>()
            .unwrap();
        assert_eq!(
            stats_scan.requirements(),
            &[SupportStatAggr::CountNonNull {
                column_name: "v0".to_string(),
            }]
        );

        let fallback_partial = union_children[1]
            .as_any()
            .downcast_ref::<AggregateExec>()
            .unwrap();
        assert!(matches!(fallback_partial.mode(), AggregateMode::Partial));

        let fallback_scan = fallback_partial
            .input()
            .as_any()
            .downcast_ref::<RegionScanExec>()
            .unwrap();
        assert_eq!(
            fallback_scan.stats_aware_skip_requirements(),
            &[SupportStatAggr::CountNonNull {
                column_name: "v0".to_string(),
            }]
        );
    }

    #[test]
    fn rewrite_ignores_unsupported_avg_aggregate() {
        let plan =
            build_final_over_partial_plan_with(build_region_scan(true), build_avg_expr, None);
        let optimized = AggrStatsPhysicalRule
            .optimize(plan, &ConfigOptions::default())
            .unwrap();

        assert_final_over_partial_without_union(&optimized);
    }

    #[test]
    fn rewrite_ignores_grouped_aggregate() {
        let plan = build_final_over_partial_plan_with(
            build_region_scan(true),
            build_count_expr,
            Some(group_by_v0()),
        );
        let optimized = AggrStatsPhysicalRule
            .optimize(plan, &ConfigOptions::default())
            .unwrap();

        assert_final_over_partial_without_union(&optimized);
    }

    #[test]
    fn rewrite_ignores_non_append_region_scan() {
        let plan =
            build_final_over_partial_plan_with(build_region_scan(false), build_count_expr, None);
        let optimized = AggrStatsPhysicalRule
            .optimize(plan, &ConfigOptions::default())
            .unwrap();

        assert_final_over_partial_without_union(&optimized);
    }

    #[test]
    fn rewrite_ignores_region_scan_with_predicate_without_region() {
        let region_scan = build_recording_region_scan_with_predicate(
            Vec::new(),
            Arc::new(Mutex::new(Vec::new())),
            true,
        );
        let plan = build_final_over_partial_plan_with(region_scan, build_count_expr, None);
        let optimized = AggrStatsPhysicalRule
            .optimize(plan, &ConfigOptions::default())
            .unwrap();

        assert_final_over_partial_without_union(&optimized);
    }

    #[test]
    fn rewrite_maps_count_star_to_count_rows() {
        let optimized = AggrStatsPhysicalRule
            .optimize(
                build_final_over_partial_plan_with(
                    build_region_scan(true),
                    build_count_star_expr,
                    None,
                ),
                &ConfigOptions::default(),
            )
            .unwrap();

        assert_rewritten_stats_requirement(&optimized, &[SupportStatAggr::CountRows]);
    }

    #[test]
    fn rewrite_maps_count_time_index_to_count_rows() {
        let optimized = AggrStatsPhysicalRule
            .optimize(
                build_final_over_partial_plan_with(
                    build_region_scan(true),
                    build_count_time_index_expr,
                    None,
                ),
                &ConfigOptions::default(),
            )
            .unwrap();

        assert_rewritten_stats_requirement(&optimized, &[SupportStatAggr::CountRows]);
    }

    #[test]
    fn rewrite_ignores_count_null_literal() {
        let plan = build_final_over_partial_plan_with(
            build_region_scan(true),
            build_count_null_expr,
            None,
        );
        let optimized = AggrStatsPhysicalRule
            .optimize(plan, &ConfigOptions::default())
            .unwrap();

        assert_final_over_partial_without_union(&optimized);
    }

    #[tokio::test]
    async fn rewrite_mixed_plan_returns_correct_result_and_scans_only_fallback_files() {
        let scanned_file_ids = Arc::new(Mutex::new(Vec::new()));
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new("v0", ConcreteDataType::float64_datatype(), true),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
        ]));
        let region_scan = build_recording_region_scan(
            vec![
                build_scan_test_file(
                    schema.clone(),
                    "eligible-a",
                    "host = 'a'",
                    vec![Some(1.0), None, Some(2.0)],
                    true,
                    0,
                ),
                build_scan_test_file(
                    schema.clone(),
                    "eligible-b",
                    "host = 'a'",
                    vec![Some(3.0), Some(4.0)],
                    true,
                    10,
                ),
                build_scan_test_file(
                    schema,
                    "fallback-c",
                    "host = 'a'",
                    vec![Some(5.0), None, Some(6.0)],
                    false,
                    20,
                ),
            ],
            scanned_file_ids.clone(),
        );

        let plan = build_final_over_partial_plan_with(region_scan, build_count_expr, None);
        let optimized = AggrStatsPhysicalRule
            .optimize(plan, &ConfigOptions::default())
            .unwrap();

        let batches = collect(
            optimized.clone(),
            Arc::new(datafusion::execution::TaskContext::default()),
        )
        .await
        .unwrap();
        assert_eq!(batches.len(), 1);
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), 6);

        let scanned = scanned_file_ids.lock().unwrap().clone();
        assert_eq!(scanned, vec!["fallback-c".to_string()]);

        let stats_scan = stats_scan_from_rewritten_plan(&optimized);
        let display_state = stats_scan.decision_snapshot();
        assert_eq!(display_state.files_total(), 3);
        assert_eq!(
            display_state.stats_hit_file_ids(),
            &["eligible-a".to_string(), "eligible-b".to_string()]
        );
        assert_eq!(
            display_state.fallback_file_ids(),
            &["fallback-c".to_string()]
        );

        let display_text = VerboseStatsScanDisplay(stats_scan).to_string();
        assert!(display_text.contains("files_total=3"));
        assert!(display_text.contains("stats_hit_files=[\"eligible-a\", \"eligible-b\"]"));
        assert!(display_text.contains("stats_fallback_files=[\"fallback-c\"]"));
    }

    #[tokio::test]
    async fn late_dynamic_filter_pushdown_disables_stats_paths() {
        let scanned_file_ids = Arc::new(Mutex::new(Vec::new()));
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new("v0", ConcreteDataType::float64_datatype(), true),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
        ]));
        let region_scan = build_recording_region_scan(
            vec![
                build_scan_test_file(
                    schema.clone(),
                    "eligible-a",
                    "host = 'a'",
                    vec![Some(1.0), None, Some(2.0)],
                    true,
                    0,
                ),
                build_scan_test_file(
                    schema.clone(),
                    "eligible-b",
                    "host = 'a'",
                    vec![Some(3.0), Some(4.0)],
                    true,
                    10,
                ),
                build_scan_test_file(
                    schema,
                    "fallback-c",
                    "host = 'a'",
                    vec![Some(5.0), None, Some(6.0)],
                    false,
                    20,
                ),
            ],
            scanned_file_ids.clone(),
        );

        let plan = build_final_over_partial_plan_with(region_scan, build_count_expr, None);
        let optimized = AggrStatsPhysicalRule
            .optimize(plan, &ConfigOptions::default())
            .unwrap();
        let fallback_scan = fallback_scan_from_rewritten_plan(&optimized);
        let filter = build_dynamic_filter_expr();

        let pushdown = fallback_scan
            .handle_child_pushdown_result(
                FilterPushdownPhase::Post,
                ChildPushdownResult {
                    parent_filters: vec![ChildFilterPushdownResult {
                        filter,
                        child_results: vec![],
                    }],
                    self_filters: vec![],
                },
                &ConfigOptions::default(),
            )
            .unwrap();

        assert!(matches!(pushdown.filters.as_slice(), [PushedDown::Yes]));
        assert!(pushdown.updated_node.is_some());
        assert!(
            fallback_scan
                .scanner()
                .lock()
                .unwrap()
                .has_predicate_without_region()
        );

        let stats_scan = stats_scan_from_rewritten_plan(&optimized);
        let stats_stream = stats_scan
            .execute(0, Arc::new(datafusion::execution::TaskContext::default()))
            .unwrap();
        let stats_batches = stats_stream
            .map(|batch| batch.unwrap())
            .collect::<Vec<_>>()
            .await;
        assert!(stats_batches.is_empty());

        let batches = collect(
            optimized.clone(),
            Arc::new(datafusion::execution::TaskContext::default()),
        )
        .await
        .unwrap();
        assert_eq!(batches.len(), 1);
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), 6);

        let scanned = scanned_file_ids.lock().unwrap().clone();
        assert_eq!(
            scanned,
            vec![
                "eligible-a".to_string(),
                "eligible-b".to_string(),
                "fallback-c".to_string()
            ]
        );

        let display_state = stats_scan.decision_snapshot();
        assert_eq!(display_state.files_total(), 0);
        assert!(display_state.stats_hit_file_ids().is_empty());
        assert!(display_state.fallback_file_ids().is_empty());
    }

    struct VerboseStatsScanDisplay<'a>(&'a StatsScanExec);

    impl std::fmt::Display for VerboseStatsScanDisplay<'_> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            self.0.fmt_as(DisplayFormatType::Verbose, f)
        }
    }

    fn stats_scan_from_rewritten_plan(plan: &Arc<dyn ExecutionPlan>) -> &StatsScanExec {
        let final_exec = plan.as_any().downcast_ref::<AggregateExec>().unwrap();
        let coalesce = final_exec
            .input()
            .as_any()
            .downcast_ref::<CoalescePartitionsExec>()
            .unwrap();
        let union = coalesce
            .input()
            .as_any()
            .downcast_ref::<UnionExec>()
            .unwrap();
        union.children()[0]
            .as_any()
            .downcast_ref::<StatsScanExec>()
            .unwrap()
    }

    fn fallback_scan_from_rewritten_plan(plan: &Arc<dyn ExecutionPlan>) -> &RegionScanExec {
        let final_exec = plan.as_any().downcast_ref::<AggregateExec>().unwrap();
        let coalesce = final_exec
            .input()
            .as_any()
            .downcast_ref::<CoalescePartitionsExec>()
            .unwrap();
        let union = coalesce
            .input()
            .as_any()
            .downcast_ref::<UnionExec>()
            .unwrap();
        let fallback_partial = union.children()[1]
            .as_any()
            .downcast_ref::<AggregateExec>()
            .unwrap();
        fallback_partial
            .input()
            .as_any()
            .downcast_ref::<RegionScanExec>()
            .unwrap()
    }

    fn assert_rewritten_stats_requirement(
        plan: &Arc<dyn ExecutionPlan>,
        expected: &[SupportStatAggr],
    ) {
        let stats_scan = stats_scan_from_rewritten_plan(plan);
        assert_eq!(stats_scan.requirements(), expected);

        let final_exec = plan.as_any().downcast_ref::<AggregateExec>().unwrap();
        let coalesce = final_exec
            .input()
            .as_any()
            .downcast_ref::<CoalescePartitionsExec>()
            .unwrap();
        let union = coalesce
            .input()
            .as_any()
            .downcast_ref::<UnionExec>()
            .unwrap();
        let union_children = union.children();

        let fallback_partial = union_children[1]
            .as_any()
            .downcast_ref::<AggregateExec>()
            .unwrap();
        let fallback_scan = fallback_partial
            .input()
            .as_any()
            .downcast_ref::<RegionScanExec>()
            .unwrap();
        assert_eq!(fallback_scan.stats_aware_skip_requirements(), expected);
    }

    fn assert_final_over_partial_without_union(plan: &Arc<dyn ExecutionPlan>) {
        let final_exec = plan.as_any().downcast_ref::<AggregateExec>().unwrap();
        assert!(matches!(final_exec.mode(), AggregateMode::Final));

        let coalesce = final_exec
            .input()
            .as_any()
            .downcast_ref::<CoalescePartitionsExec>()
            .unwrap();
        assert!(
            coalesce
                .input()
                .as_any()
                .downcast_ref::<UnionExec>()
                .is_none()
        );

        let partial_exec = coalesce
            .input()
            .as_any()
            .downcast_ref::<AggregateExec>()
            .unwrap();
        assert!(matches!(partial_exec.mode(), AggregateMode::Partial));

        let region_scan = partial_exec
            .input()
            .as_any()
            .downcast_ref::<RegionScanExec>()
            .unwrap();
        assert!(region_scan.stats_aware_skip_requirements().is_empty());
    }
}
