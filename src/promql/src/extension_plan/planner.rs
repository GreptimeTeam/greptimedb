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

use async_trait::async_trait;
use common_query::series_frame::series_frame_ordering;
use datafusion::arrow::datatypes::Schema;
use datafusion::catalog::Session;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datatypes::series::SeriesOrdering;
use datatypes::series::frame::{FrameLabelMode, SeriesFrameDeclaration};
use store_api::region_engine::RawLabelSource;
use table::table::scan::{RegionScanExec, SeriesScanExec};

use crate::extension_plan::{
    Absent, EmptyMetric, FusedSeries, HistogramFold, InstantManipulate, RangeManipulate,
    RawSeriesSource, ScalarCalculate, SeriesDivide, SeriesNormalize, UnionDistinctOn,
};

pub struct PromExtensionPlanner;

#[async_trait]
impl ExtensionPlanner for PromExtensionPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session: &dyn Session,
        planning_ctx: &PhysicalPlanningContext,
    ) -> DfResult<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(node) = node.as_any().downcast_ref::<SeriesNormalize>() {
            Ok(Some(node.to_execution_plan(physical_inputs[0].clone())))
        } else if let Some(node) = node.as_any().downcast_ref::<InstantManipulate>() {
            Ok(Some(node.to_execution_plan(physical_inputs[0].clone())))
        } else if let Some(node) = node.as_any().downcast_ref::<RangeManipulate>() {
            Ok(Some(node.to_execution_plan(physical_inputs[0].clone())))
        } else if let Some(node) = node.as_any().downcast_ref::<SeriesDivide>() {
            Ok(Some(node.to_execution_plan(physical_inputs[0].clone())))
        } else if let Some(node) = node.as_any().downcast_ref::<EmptyMetric>() {
            Ok(Some(node.to_execution_plan(
                session,
                planner,
                planning_ctx,
            )?))
        } else if let Some(node) = node.as_any().downcast_ref::<ScalarCalculate>() {
            Ok(Some(node.to_execution_plan(physical_inputs[0].clone())?))
        } else if let Some(node) = node.as_any().downcast_ref::<HistogramFold>() {
            Ok(Some(node.to_execution_plan(physical_inputs[0].clone())))
        } else if let Some(node) = node.as_any().downcast_ref::<UnionDistinctOn>() {
            Ok(Some(node.to_execution_plan(
                physical_inputs[0].clone(),
                physical_inputs[1].clone(),
            )))
        } else if let Some(node) = node.as_any().downcast_ref::<Absent>() {
            Ok(Some(node.to_execution_plan(physical_inputs[0].clone())))
        } else if let Some(node) = node.as_any().downcast_ref::<FusedSeries>() {
            plan_fused_series(
                node,
                physical_inputs,
                session.config_options().execution.target_partitions,
            )
        } else if let Some(node) = node.as_any().downcast_ref::<RawSeriesSource>() {
            plan_raw_series_source(
                node,
                physical_inputs,
                session.config_options().execution.target_partitions,
            )
            .map(Some)
        } else {
            Ok(None)
        }
    }
}

/// The label `__name__` is reported under, which is a column of this node's output but not a tag
/// column of any region.
const METRIC_NAME: &str = "__name__";

/// Builds the physical node of a [`FusedSeries`] over the raw series its inputs can provide.
///
/// The node reads series frames. Below it is either the region scan itself, when the node runs
/// in the region it reads, or an input that already produces explicit-label frames, when the
/// node runs above the regions of several tables and each [`RawSeriesSource`] answered for its own.
/// Anything else in between - a filter, a projection that drops the frame, an exchange - produces a
/// different frame, or no frame at all, and this layer cannot look under it.
///
/// `Ok(None)` is therefore the answer for children that are neither: the logical plan is already
/// fixed and DataFusion reports an unplanned extension node normally. A region scan that is found
/// but refuses the raw path is a different case — the logical route chose this node for this scan
/// and nothing here can undo that choice, because the logical plan is not available — so it is
/// reported as an error that names the reason instead of silently producing no plan.
fn plan_fused_series(
    fused: &FusedSeries,
    physical_inputs: &[Arc<dyn ExecutionPlan>],
    target_partitions: usize,
) -> DfResult<Option<Arc<dyn ExecutionPlan>>> {
    if physical_inputs.len() != fused.children().len() {
        return Err(DataFusionError::Plan(
            "FusedSeries physical child count mismatch".into(),
        ));
    }
    if physical_inputs.is_empty() {
        return fused
            .to_execution_plan_with_parallelism(Vec::new(), target_partitions)
            .map(Some);
    }
    let explicit_inputs = physical_inputs
        .iter()
        .map(normalize_raw_union)
        .collect::<DfResult<Vec<_>>>()?;
    if let Some(inputs) = explicit_inputs
        .iter()
        .map(|input| {
            explicit_label_frame_ordering(input.schema().as_ref())
                .map(|ordering| (Arc::clone(input), RawLabelSource::Explicit, ordering))
        })
        .collect::<Option<Vec<_>>>()
    {
        return fused
            .to_execution_plan_with_parallelism(inputs, target_partitions)
            .map(Some);
    }
    if physical_inputs
        .iter()
        .any(|input| input.downcast_ref::<RegionScanExec>().is_none())
    {
        return Ok(None);
    }

    let binding = fused.binding();
    let Some(metric_name) = binding.metric_name.as_deref() else {
        return Err(DataFusionError::Plan(
            "the FusedSeries node binds no metric name, so the region scan below it cannot be \
             asked for the series of one metric"
                .to_string(),
        ));
    };
    let source_fields = super::fused_series::tree_source_fields(fused.tree())?;
    if source_fields.len() != physical_inputs.len() {
        return Err(DataFusionError::Plan(
            "FusedSeries physical plan source count mismatch".into(),
        ));
    }
    let mut series_inputs = Vec::with_capacity(physical_inputs.len());
    for (physical, field_names) in physical_inputs.iter().zip(source_fields) {
        let Some(scan) = physical.downcast_ref::<RegionScanExec>() else {
            return Ok(None);
        };
        let series_scan = series_scan(
            scan,
            metric_name,
            &binding.tags,
            &field_names,
            target_partitions,
        )?;
        series_inputs.push((
            series_scan.clone() as Arc<dyn ExecutionPlan>,
            series_scan.label_source().clone(),
            series_scan.raw_series_ordering(),
        ));
    }
    fused
        .to_execution_plan_with_parallelism(series_inputs, target_partitions)
        .map(Some)
}

/// Builds the region half of a [`RawSeriesSource`]: the region's raw-series leaf for its binding.
///
/// The node only means something over the region scan it binds, so any other child is an error
/// rather than a plan another planner could build.
fn plan_raw_series_source(
    source: &RawSeriesSource,
    physical_inputs: &[Arc<dyn ExecutionPlan>],
    target_partitions: usize,
) -> DfResult<Arc<dyn ExecutionPlan>> {
    let [input] = physical_inputs else {
        return Err(DataFusionError::Plan(
            "RawSeriesSource requires exactly one physical input".into(),
        ));
    };
    let Some(scan) = input.downcast_ref::<RegionScanExec>() else {
        return Err(DataFusionError::Plan(format!(
            "RawSeriesSource for metric {:?} must be planned over a region scan, got {}",
            source.metric_name(),
            input.name()
        )));
    };
    let series_scan = series_scan(
        scan,
        source.metric_name(),
        source.tags(),
        source.fields(),
        target_partitions,
    )?;
    source.to_per_series_execution_plan(series_scan)
}

/// The raw-series leaf of a region scan bound to one metric.
///
/// The leaf needs a prepared scan: range assignment runs after physical planning and cannot see a
/// `SeriesScanExec`. Preparation keeps the scan's parallel layout, and the series-producer contract
/// guarantees that one series stays in one scan partition.
fn series_scan(
    scan: &RegionScanExec,
    metric_name: &str,
    tags: &[String],
    fields: &[String],
    target_partitions: usize,
) -> DfResult<Arc<SeriesScanExec>> {
    let scan = if scan.is_partition_set() {
        Arc::new(scan.clone())
    } else {
        let partitions = table::table::scan::assign_partition_ranges(
            scan.get_partition_ranges(),
            target_partitions.max(1),
        );
        Arc::new(
            scan.with_new_partitions(partitions, target_partitions.max(1))
                .map_err(|error| DataFusionError::External(error.into_inner()))?,
        )
    };
    // `__name__` is the metric name rather than a column of the region, so it is not one of the
    // logical tags the scan is asked to decode; the frame names each series' own metric.
    let logical_tags = tags
        .iter()
        .filter(|tag| tag.as_str() != METRIC_NAME)
        .cloned()
        .collect::<Vec<_>>();
    scan.try_to_named_series_scan(metric_name, &logical_tags, fields)?
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "the region scan could not serve a raw series frame for metric {metric_name:?}: \
                 the scanner refused the raw series path for this scan"
            ))
        })
}

/// The ordering an input declares when it produces raw frames that carry their own labels.
/// Restate a raw union's ordering as the weakest guarantee its members make.
///
/// `UnionExec` reports the frame metadata of its last child, so a fragmented member beside one
/// whose series are complete can look contiguous, and the fused node would skip the reconciliation
/// of the fragments. The union itself is not rewritten; only the declaration on this edge is
/// lowered, and the fused node's own interleaved-input path inserts the router.
fn normalize_raw_union(input: &Arc<dyn ExecutionPlan>) -> DfResult<Arc<dyn ExecutionPlan>> {
    let Some(union) = input.downcast_ref::<UnionExec>() else {
        return Ok(Arc::clone(input));
    };
    if explicit_label_frame_ordering(input.schema().as_ref()) != Some(SeriesOrdering::Contiguous)
        || weakest_union_ordering(union) != SeriesOrdering::Interleaved
    {
        return Ok(Arc::clone(input));
    }
    let mut metadata = input.schema().metadata().clone();
    metadata.insert(RAW_SERIES_ORDERING_KEY.into(), "interleaved".into());
    let schema = Arc::new(Schema::new_with_metadata(
        input.schema().fields().clone(),
        metadata,
    ));
    let exprs = input
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            (
                Arc::new(Column::new(field.name(), index)) as Arc<dyn PhysicalExpr>,
                field.name().clone(),
            )
        })
        .collect::<Vec<_>>();
    Ok(Arc::new(ProjectionExec::try_new_with_schema_metadata(
        exprs,
        Arc::clone(input),
        schema.as_ref(),
    )?))
}

/// The schema metadata key a frame's ordering is declared under; see
/// [`common_query::series_frame`].
const RAW_SERIES_ORDERING_KEY: &str = "greptime.series.ordering";

/// The weakest ordering any member of this union declares. A member that is itself a union is
/// asked the same question; a member that already reconciles keeps its own guarantee.
fn weakest_union_ordering(union: &UnionExec) -> SeriesOrdering {
    union
        .children()
        .iter()
        .map(|child| match child.downcast_ref::<UnionExec>() {
            Some(nested) => weakest_union_ordering(nested),
            None => explicit_label_frame_ordering(child.schema().as_ref())
                .unwrap_or(SeriesOrdering::Contiguous),
        })
        .find(|ordering| *ordering == SeriesOrdering::Interleaved)
        .unwrap_or(SeriesOrdering::Contiguous)
}

fn explicit_label_frame_ordering(schema: &Schema) -> Option<SeriesOrdering> {
    let declaration = SeriesFrameDeclaration::parse(schema).ok()?;
    if declaration.labels() != FrameLabelMode::Explicit {
        return None;
    }
    series_frame_ordering(schema).ok()
}

#[cfg(test)]
mod tests {
    use std::fmt::{self, Formatter};
    use std::sync::Arc;

    use common_error::ext::BoxedError;
    use common_query::series_frame::{
        raw_series_frame_schema_with_ordering, series_frame_ordering,
    };
    use common_recordbatch::{EmptyRecordBatchStream, SendableRecordBatchStream};
    use datafusion::common::DFSchema;
    use datafusion::logical_expr::{EmptyRelation, LogicalPlan};
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use datafusion::physical_plan::union::UnionExec;
    use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, SchemaRef as DatatypesSchemaRef};
    use datatypes::series::SeriesOrdering;
    use datatypes::series::samples::{FieldSpec, SeriesSchema};
    use datatypes::series::time::{EvalGrid, GridSpec};
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder, RegionMetadataRef};
    use store_api::region_engine::{
        PrepareRequest, QueryScanContext, RegionScanner, ScannerProperties,
    };
    use store_api::storage::{RegionId, ScanRequest};
    use table::table::scan::RegionScanExec;

    use super::{METRIC_NAME, normalize_raw_union, plan_fused_series};
    use crate::extension_plan::{FusedSeries, FusedSeriesBinding};
    use crate::native::plan::{BoundInput, NativeExpr, NativeNode, NativeTree, NativeType};

    fn raw_frame(ordering: SeriesOrdering) -> Arc<datafusion::arrow::datatypes::Schema> {
        let series = SeriesSchema {
            fields: vec![FieldSpec {
                name: Arc::from("val"),
            }]
            .into(),
            eval_grid: None,
        };
        raw_series_frame_schema_with_ordering(
            &series,
            &store_api::region_engine::RawLabelSource::Explicit,
            ordering,
        )
        .expect("a raw schema declares an explicit-label frame")
    }

    fn union(children: &[SeriesOrdering]) -> Arc<dyn ExecutionPlan> {
        let children = children
            .iter()
            .map(|ordering| {
                Arc::new(EmptyExec::new(raw_frame(*ordering))) as Arc<dyn ExecutionPlan>
            })
            .collect::<Vec<_>>();
        UnionExec::try_new(children).expect("two framed children")
    }

    /// A union reports the ordering of its last child. The declaration this edge hands to the
    /// fused node must instead be the weakest of the members, in either order, and must stay the
    /// union's own when every member is complete.
    #[test]
    fn raw_union_declaration_lowers_to_the_weakest_member() {
        for members in [
            vec![SeriesOrdering::Interleaved, SeriesOrdering::Contiguous],
            vec![SeriesOrdering::Contiguous, SeriesOrdering::Interleaved],
        ] {
            let union = union(&members);
            assert_eq!(
                series_frame_ordering(union.schema().as_ref()).unwrap(),
                *members.last().unwrap(),
                "the union itself keeps its last child's declaration"
            );
            let normalized =
                normalize_raw_union(&Arc::clone(&union)).expect("a raw union normalizes");
            assert_eq!(
                series_frame_ordering(normalized.schema().as_ref()).unwrap(),
                SeriesOrdering::Interleaved,
                "the fused edge sees the weakest member"
            );
        }

        let union = union(&[SeriesOrdering::Contiguous, SeriesOrdering::Contiguous]);
        let normalized = normalize_raw_union(&(Arc::clone(&union) as Arc<dyn ExecutionPlan>))
            .expect("a raw union normalizes");
        assert_eq!(
            series_frame_ordering(normalized.schema().as_ref()).unwrap(),
            SeriesOrdering::Contiguous,
            "nothing is lowered when every member is complete"
        );
    }

    /// A region whose only column is the time index: the scan below publishes an ordinary region
    /// schema, not a series frame.
    fn region_metadata() -> RegionMetadataRef {
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
            .primary_key(vec![]);
        Arc::new(builder.build().expect("the fixture metadata is valid"))
    }

    /// The scanner of [`bare_region_scan`], which never answers a chunk here.
    #[derive(Debug)]
    struct BareScanner {
        metadata: RegionMetadataRef,
        properties: ScannerProperties,
    }

    impl DisplayAs for BareScanner {
        fn fmt_as(&self, _format: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
            write!(f, "BareScanner")
        }
    }

    impl RegionScanner for BareScanner {
        fn name(&self) -> &str {
            "BareScanner"
        }

        fn properties(&self) -> &ScannerProperties {
            &self.properties
        }

        fn schema(&self) -> DatatypesSchemaRef {
            Arc::clone(&self.metadata.schema)
        }

        fn metadata(&self) -> RegionMetadataRef {
            Arc::clone(&self.metadata)
        }

        fn prepare(&mut self, request: PrepareRequest) -> Result<(), BoxedError> {
            self.properties.prepare(request);
            Ok(())
        }

        fn scan_partition(
            &self,
            _ctx: &QueryScanContext,
            _metrics: &ExecutionPlanMetricsSet,
            _partition: usize,
        ) -> Result<SendableRecordBatchStream, BoxedError> {
            Ok(Box::pin(EmptyRecordBatchStream::new(Arc::clone(
                &self.metadata.schema,
            ))))
        }

        fn set_logical_region(&mut self, logical_region: bool) {
            self.properties.set_logical_region(logical_region);
        }

        fn set_query_load_region_id(&mut self, region_id: RegionId) {
            self.properties.set_query_load_region_id(region_id);
        }

        fn has_predicate_without_region(&self) -> bool {
            true
        }

        fn add_dyn_filter_to_predicate(
            &mut self,
            filter_exprs: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
        ) -> Vec<bool> {
            vec![false; filter_exprs.len()]
        }
    }

    /// A bare region scan: the input of a node that runs inside the region it reads.
    fn bare_region_scan() -> Arc<dyn ExecutionPlan> {
        let scanner = BareScanner {
            metadata: region_metadata(),
            properties: ScannerProperties::default(),
        };
        Arc::new(
            RegionScanExec::new(Box::new(scanner), ScanRequest::default(), None)
                .expect("the fixture scanner scans"),
        )
    }

    /// The compiled tree a region-local node runs: one source, evaluated on the statement grid.
    fn one_source_tree() -> (NativeTree, Arc<EvalGrid>) {
        let grid = Arc::new(
            EvalGrid::try_new(GridSpec::try_new(0, 60_000, 2).expect("the grid spec is legal"))
                .expect("the grid is legal"),
        );
        let samples = NativeType::Samples(Arc::new(SeriesSchema {
            fields: vec![FieldSpec {
                name: Arc::from("val"),
            }]
            .into(),
            eval_grid: Some(Arc::clone(&grid)),
        }));
        let tree = NativeTree::try_new(NativeNode {
            expr: NativeExpr::Input(BoundInput {
                output: samples.clone(),
                source_index: 0,
                ordering: SeriesOrdering::Contiguous,
            }),
            output: samples,
        })
        .expect("the fixture tree is legal");
        (tree, grid)
    }

    /// The narrowed admission guard's premise: the bare-scan arm still needs the binding's own
    /// metric name, because it asks a region scan for the series of that one metric.
    ///
    /// A discovered selector never reaches that arm - its prepared input is a `Union` of raw sources
    /// that each name the metric they read - so this refusal only has to hold for a scan.
    #[test]
    fn a_bare_region_scan_without_a_bound_metric_name_is_refused() {
        let (tree, grid) = one_source_tree();
        let node = FusedSeries::try_new(
            Arc::from(r#"absent_over_time({job="api"}[5m])"#),
            tree,
            FusedSeriesBinding {
                grid,
                metric_name: None,
                time_index: "time".to_string(),
                tags: vec![METRIC_NAME.to_string()],
            },
            vec![LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: Arc::new(
                    DFSchema::try_from(raw_frame(SeriesOrdering::Contiguous).as_ref().clone())
                        .expect("the frame is a legal schema"),
                ),
            })],
            Arc::new(
                DFSchema::try_from(datafusion::arrow::datatypes::Schema::new(vec![
                    datafusion::arrow::datatypes::Field::new(
                        "val",
                        datafusion::arrow::datatypes::DataType::Float64,
                        true,
                    ),
                ]))
                .expect("the output is a legal schema"),
            ),
        )
        .expect("one source binds one child");
        assert_eq!(1, node.children().len(), "the node has its one input");
        let error = plan_fused_series(&node, &[bare_region_scan()], 4)
            .expect_err("a region scan has no series to answer without the bound name");
        assert!(
            error.to_string().contains("binds no metric name"),
            "the refusal names what is missing: {error}"
        );
    }
}
