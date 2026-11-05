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
use datafusion_common::{DataFusionError, Result as DfResult};
use datatypes::series::frame::{FrameContent, FrameLabelMode, SeriesFrameDeclaration};
use promql::extension_plan::{RawSeriesSourceExec, SeriesRepartitionExec};

/// Ensures a multi-partition series-frame *root* is emitted through one output partition.
///
/// The client receives one stream. Some multi-partition frame roots need identities merged across
/// partitions; region per-series scans do not, because their scan contract assigns each identity to
/// one partition.
///
/// The view is the root of a query-engine execution, which in a distributed statement is one
/// region's own root as well: a region answers its part of the statement through the same engine,
/// so the same coalescing would otherwise apply to the frames it hands upstream.
///
/// This rule normalizes the plan's root and nothing else. A single-partition root is left alone:
/// one producer's finality is the whole answer when that producer is the entire output, and the
/// router's job is reconciling several producers rather than raw content as such. Frame edges
/// below the root stay the responsibility of the operator that builds them - `FusedSeries` inserts
/// its own owner where a consumer needs one - so this is not a guard against every frame-shaped
/// subplan an arbitrary caller might execute directly.
#[derive(Debug, Default)]
pub struct SeriesFrameOutput;

impl PhysicalOptimizerRule for SeriesFrameOutput {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let schema = plan.schema();
        if !schema
            .metadata()
            .keys()
            .any(|key| key.starts_with("greptime.series."))
        {
            return Ok(plan);
        }

        let declaration = SeriesFrameDeclaration::parse(schema.as_ref()).map_err(|error| {
            DataFusionError::Plan(format!("invalid series frame declaration: {error}"))
        })?;
        if plan.properties().output_partitioning().partition_count() <= 1 {
            return Ok(plan);
        }
        // A raw frame reaches the client through the same single stream, and ordinary batch
        // coalescing cannot produce it: a chunk's `is_last` closes only its own producer, so
        // coalescing would hand over several partial series under one identity. The router needs
        // explicit labels to check that one key has one binding; a key-only frame therefore stays
        // rejected rather than silently coalesced.
        //
        // Ordinary multi-partition roots need their identities merged, but a root whose producer
        // declared that each identity lives in one partition does not: its chain is the identity.
        if declaration.content() == FrameContent::Raw
            && declaration.labels() != FrameLabelMode::Explicit
        {
            return Err(DataFusionError::Plan(
                "a multi-partition raw series frame needs explicit labels to be merged into one \
                 output stream"
                    .into(),
            ));
        }

        let completion = plan
            .downcast_ref::<RawSeriesSourceExec>()
            .is_some_and(RawSeriesSourceExec::is_per_series_scan)
            || plan.downcast_ref::<SeriesRepartitionExec>().is_some()
            || plan
                .downcast_ref::<promql::extension_plan::FusedSeriesExec>()
                .is_some_and(|fused| fused.has_partition_disjoint_identities());
        if completion {
            Ok(Arc::new(SeriesRepartitionExec::try_new_complete_disjoint(
                plan, 1,
            )?))
        } else {
            Ok(Arc::new(SeriesRepartitionExec::try_new(plan, 1)?))
        }
    }

    fn name(&self) -> &str {
        "SeriesFrameOutput"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use api::v1::SemanticType;
    use common_error::ext::BoxedError;
    use common_recordbatch::{EmptyRecordBatchStream, SendableRecordBatchStream};
    use datafusion::arrow::datatypes::Schema;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::DFSchema;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::logical_expr::{EmptyRelation, LogicalPlan};
    use datafusion::physical_plan::PhysicalExpr;
    use datafusion::physical_plan::display::{DisplayAs, DisplayFormatType};
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, SchemaRef};
    use datatypes::series::SeriesOrdering;
    use datatypes::series::frame::raw_series_frame_schema;
    use datatypes::series::samples::{FieldSpec, SeriesSchema};
    use datatypes::series::time::{EvalGrid, GridSpec};
    use promql::extension_plan::{
        FusedOutputFormat, FusedSeries, FusedSeriesBinding, RawSeriesSource, SeriesRouting,
    };
    use promql::native::plan::{
        BoundInput, BoundInstant, InstantValue, NativeExpr, NativeNode, NativeTree, NativeType,
    };
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder, RegionMetadataRef};
    use store_api::region_engine::{
        BoundTag, PrepareRequest, QueryScanContext, RawLabelSource, RegionScanner,
        ScannerProperties,
    };
    use store_api::storage::{RegionId, ScanRequest};
    use table::table::scan::RegionScanExec;

    use super::*;

    fn series_schema() -> Arc<SeriesSchema> {
        Arc::new(SeriesSchema {
            fields: Arc::from([FieldSpec {
                name: Arc::from("value"),
            }]),
            eval_grid: None,
        })
    }

    fn raw_frame_schema() -> Arc<Schema> {
        raw_series_frame_schema(&series_schema(), FrameLabelMode::Explicit)
            .expect("a raw frame declares its layout")
    }

    /// A plan declaring `partitions` empty explicit-label raw-frame partitions.
    fn raw_plan(partitions: usize) -> Arc<dyn ExecutionPlan> {
        let schema = raw_frame_schema();
        let batch = RecordBatch::new_empty(Arc::clone(&schema));
        let batches = (0..partitions)
            .map(|_| vec![batch.clone()])
            .collect::<Vec<_>>();
        let source = MemorySourceConfig::try_new(&batches, schema, None)
            .expect("the fixture batches match the frame schema");
        Arc::new(DataSourceExec::new(Arc::new(source)))
    }

    fn optimized(plan: Arc<dyn ExecutionPlan>) -> DfResult<Arc<dyn ExecutionPlan>> {
        SeriesFrameOutput.optimize(plan, &ConfigOptions::default())
    }

    /// The region layout the raw-series fixtures scan: the tag `host`, the time index and one field.
    fn region_metadata() -> RegionMetadataRef {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1024, 1));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "host",
                    ConcreteDataType::string_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Tag,
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
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "value",
                    ConcreteDataType::float64_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Field,
                column_id: 3,
            })
            .primary_key(vec![1]);
        Arc::new(builder.build().expect("the fixture metadata is valid"))
    }

    /// The label binding a per-series scan of [`region_metadata`] publishes: every key column is
    /// bound, so one identity lives in one scan partition.
    fn per_series_labels() -> RawLabelSource {
        RawLabelSource::PrimaryKey {
            metric_name: Arc::from("cpu"),
            physical_metadata: region_metadata(),
            logical_tags: Arc::from([BoundTag {
                column_id: 1,
                name: Arc::from("host"),
            }]),
        }
    }

    /// The region's scanner, answering the raw-series path the planner selects.
    #[derive(Debug)]
    struct RawSeriesScanner {
        metadata: RegionMetadataRef,
        properties: ScannerProperties,
    }

    impl DisplayAs for RawSeriesScanner {
        fn fmt_as(
            &self,
            _format: DisplayFormatType,
            f: &mut std::fmt::Formatter<'_>,
        ) -> std::fmt::Result {
            write!(f, "RawSeriesScanner")
        }
    }

    impl RegionScanner for RawSeriesScanner {
        fn name(&self) -> &str {
            "RawSeriesScanner"
        }

        fn properties(&self) -> &ScannerProperties {
            &self.properties
        }

        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.metadata.schema)
        }

        fn metadata(&self) -> RegionMetadataRef {
            Arc::clone(&self.metadata)
        }

        fn prepare(&mut self, request: PrepareRequest) -> std::result::Result<(), BoxedError> {
            self.properties.prepare(request);
            Ok(())
        }

        fn scan_partition(
            &self,
            _ctx: &QueryScanContext,
            _metrics: &ExecutionPlanMetricsSet,
            _partition: usize,
        ) -> std::result::Result<SendableRecordBatchStream, BoxedError> {
            Ok(Box::pin(EmptyRecordBatchStream::new(Arc::clone(
                &self.metadata.schema,
            ))))
        }

        fn raw_series_schema(
            &self,
            _label_source: &RawLabelSource,
        ) -> std::result::Result<Option<Arc<SeriesSchema>>, BoxedError> {
            Ok(Some(series_schema()))
        }

        fn has_predicate_without_region(&self) -> bool {
            true
        }

        fn add_dyn_filter_to_predicate(
            &mut self,
            filter_exprs: Vec<Arc<dyn PhysicalExpr>>,
        ) -> Vec<bool> {
            vec![false; filter_exprs.len()]
        }

        fn set_logical_region(&mut self, logical_region: bool) {
            self.properties.set_logical_region(logical_region);
        }

        fn set_query_load_region_id(&mut self, region_id: RegionId) {
            self.properties.set_query_load_region_id(region_id);
        }
    }

    /// A region's native raw-series leaf over `partitions` scan partitions.
    fn series_scan(partitions: usize) -> Arc<dyn ExecutionPlan> {
        let scanner = RawSeriesScanner {
            metadata: region_metadata(),
            properties: ScannerProperties::new(vec![vec![]; partitions], false, 0),
        };
        let scan = Arc::new(
            RegionScanExec::new(Box::new(scanner), ScanRequest::default(), None)
                .expect("the fixture scanner scans"),
        );
        scan.try_to_series_scan(per_series_labels())
            .expect("the fixture binding is legal")
            .expect("the fixture scanner answers raw series")
    }

    /// A [`RawSeriesSourceExec`] root over `child`, as the region planner builds one.
    fn raw_source_root(child: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        RawSeriesSource::try_new(
            Arc::from("cpu"),
            vec!["host".to_string()],
            vec!["value".to_string()],
            LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: Arc::new(
                    DFSchema::try_from(raw_frame_schema()).expect("the frame is a legal schema"),
                ),
            }),
        )
        .expect("the source binds one metric")
        .to_per_series_execution_plan(child)
        .expect("the child is a raw frame of these fields")
    }

    /// A fused root running one sample selection over `input`, as bound by `labels`.
    ///
    /// The input must be a contiguous raw frame, which both [`series_scan`] and [`raw_plan`] are.
    fn fused_root_over(
        input: Arc<dyn ExecutionPlan>,
        labels: RawLabelSource,
    ) -> Arc<dyn ExecutionPlan> {
        let grid = Arc::new(
            EvalGrid::try_new(GridSpec::try_new(0, 1_000, 2).expect("the grid is valid"))
                .expect("the grid is materialized"),
        );
        let raw = NativeType::Samples(series_schema());
        let evaluated = NativeType::Samples(Arc::new(SeriesSchema {
            fields: series_schema().fields.clone(),
            eval_grid: Some(Arc::clone(&grid)),
        }));
        let tree = NativeTree::try_new(NativeNode {
            expr: NativeExpr::Instant {
                input: Box::new(NativeNode {
                    expr: NativeExpr::Input(BoundInput {
                        source_index: 0,
                        output: raw.clone(),
                        ordering: SeriesOrdering::Contiguous,
                    }),
                    output: raw,
                }),
                op: BoundInstant {
                    lookback_ms: 0,
                    offset_ms: 0,
                    at_ms: None,
                    value: InstantValue::Sample,
                    output_grid: Arc::clone(&grid),
                },
            },
            output: evaluated,
        })
        .expect("the tree is legal");
        let output_schema = Arc::new(
            DFSchema::try_from(
                FusedSeries::series_frame_schema(&tree).expect("the tree declares a frame"),
            )
            .expect("the frame is a legal schema"),
        );
        let fused = FusedSeries::try_new_with_format(
            Arc::from("cpu"),
            tree,
            FusedSeriesBinding {
                grid,
                metric_name: Some(Arc::from("cpu")),
                time_index: "time".to_string(),
                tags: vec!["host".to_string()],
            },
            vec![LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: Arc::new(
                    DFSchema::try_from(raw_frame_schema()).expect("the frame is a legal schema"),
                ),
            })],
            output_schema,
            FusedOutputFormat::SeriesFrame,
        )
        .expect("the fused node is planned");
        fused
            .to_execution_plan(vec![(input, labels, SeriesOrdering::Contiguous)])
            .expect("the fused node is physical")
    }

    /// A fused root running one sample selection over one per-series scan.
    fn fused_root(partitions: usize) -> Arc<dyn ExecutionPlan> {
        fused_root_over(series_scan(partitions), per_series_labels())
    }

    #[test]
    fn an_unmarked_raw_root_selects_merge_inputs_router() {
        let exec = optimized(raw_plan(3)).unwrap();
        let router = exec.downcast_ref::<SeriesRepartitionExec>().unwrap();
        assert_eq!(
            promql::extension_plan::IdentityCompletion::MergeInputs,
            router.identity_completion()
        );
    }

    #[test]
    fn a_multi_partition_raw_root_is_routed_into_one_output() {
        let plan = raw_plan(3);
        assert_eq!(3, plan.properties().output_partitioning().partition_count());

        let exec = optimized(plan).expect("an explicit-label raw root is supported");
        assert_eq!(
            1,
            exec.properties().output_partitioning().partition_count(),
            "the client sees one raw stream"
        );
        let router = exec
            .downcast_ref::<SeriesRepartitionExec>()
            .expect("the exit is the shared router");
        assert_eq!(
            promql::extension_plan::IdentityCompletion::MergeInputs,
            router.identity_completion(),
            "unproven memory roots use conservative merging"
        );
        assert_eq!(
            &SeriesRouting::RawPrimaryKey,
            router.routing_policy(),
            "raw identities meet at their complete primary key"
        );
        assert_eq!(
            raw_frame_schema(),
            exec.schema(),
            "the frame schema is kept"
        );
    }

    #[test]
    fn a_repartition_root_selects_complete_disjoint_router() {
        let source: Arc<dyn ExecutionPlan> =
            Arc::new(SeriesRepartitionExec::try_new(raw_plan(3), 2).unwrap());
        let exec = optimized(source).unwrap();
        let router = exec.downcast_ref::<SeriesRepartitionExec>().unwrap();
        assert_eq!(
            promql::extension_plan::IdentityCompletion::CompleteDisjoint,
            router.identity_completion()
        );
    }

    #[test]
    fn a_per_series_raw_root_selects_complete_disjoint_router() {
        let root = raw_source_root(series_scan(2));
        assert!(
            root.downcast_ref::<RawSeriesSourceExec>()
                .expect("the root is the region's raw source")
                .is_per_series_scan(),
            "the fixture root is the per-series case"
        );
        assert_eq!(2, root.properties().output_partitioning().partition_count());

        let exec = optimized(root).unwrap();
        let router = exec
            .downcast_ref::<SeriesRepartitionExec>()
            .expect("the root is routed");
        assert_eq!(
            promql::extension_plan::IdentityCompletion::CompleteDisjoint,
            router.identity_completion(),
            "a per-series scan owns each identity in one partition"
        );
    }

    #[test]
    fn a_raw_source_root_without_per_series_provenance_selects_merge_inputs_router() {
        let root = raw_source_root(raw_plan(2));
        assert!(
            !root
                .downcast_ref::<RawSeriesSourceExec>()
                .expect("the root is the region's raw source")
                .is_per_series_scan(),
            "the fixture root is a raw source over something that is not a per-series scan"
        );

        let exec = optimized(root).unwrap();
        let router = exec
            .downcast_ref::<SeriesRepartitionExec>()
            .expect("the root is routed");
        assert_eq!(
            promql::extension_plan::IdentityCompletion::MergeInputs,
            router.identity_completion(),
            "the node alone proves nothing about where a scan keeps its identities"
        );
    }

    #[test]
    fn a_fused_root_with_disjoint_identities_selects_complete_disjoint_router() {
        let root = fused_root(2);
        assert!(
            root.downcast_ref::<promql::extension_plan::FusedSeriesExec>()
                .expect("the root is the fused node")
                .has_partition_disjoint_identities(),
            "the fixture root is the fused case"
        );
        assert_eq!(2, root.properties().output_partitioning().partition_count());

        let exec = optimized(root).unwrap();
        let router = exec
            .downcast_ref::<SeriesRepartitionExec>()
            .expect("the root is routed");
        assert_eq!(
            promql::extension_plan::IdentityCompletion::CompleteDisjoint,
            router.identity_completion(),
            "a fused chain over a per-series scan owns each identity in one partition"
        );
    }

    #[test]
    fn a_fused_root_without_disjoint_identities_selects_merge_inputs_router() {
        let root = fused_root_over(raw_plan(2), RawLabelSource::Explicit);
        assert!(
            !root
                .downcast_ref::<promql::extension_plan::FusedSeriesExec>()
                .expect("the root is the fused node")
                .has_partition_disjoint_identities(),
            "explicit labels say nothing about where a source keeps its identities"
        );
        assert_eq!(2, root.properties().output_partitioning().partition_count());

        let exec = optimized(root).unwrap();
        let router = exec
            .downcast_ref::<SeriesRepartitionExec>()
            .expect("the root is routed");
        assert_eq!(
            promql::extension_plan::IdentityCompletion::MergeInputs,
            router.identity_completion(),
            "a fused node without proven identity placement merges its inputs"
        );
    }

    #[test]
    fn a_single_partition_raw_root_is_left_alone() {
        let plan = raw_plan(1);
        let exec = optimized(Arc::clone(&plan)).expect("nothing to normalize");
        assert!(
            Arc::ptr_eq(&plan, &exec),
            "one producer is already the whole answer"
        );
    }

    #[test]
    fn the_rule_does_not_wrap_its_own_output_again() {
        let routed = optimized(raw_plan(3)).unwrap();
        let again = optimized(Arc::clone(&routed)).unwrap();
        assert!(
            Arc::ptr_eq(&routed, &again),
            "the exit declares one partition"
        );
    }

    #[test]
    fn a_plan_without_a_frame_declaration_is_untouched() {
        let schema = Arc::new(Schema::empty());
        let batch = RecordBatch::new_empty(Arc::clone(&schema));
        let source =
            MemorySourceConfig::try_new(&[vec![batch.clone()], vec![batch]], schema, None).unwrap();
        let plan: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(source)));
        let exec = optimized(Arc::clone(&plan)).expect("unrelated plans pass through");
        assert!(Arc::ptr_eq(&plan, &exec));
    }
}
