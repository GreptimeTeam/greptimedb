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

//! A region scan that answers raw series frames with explicit labels.
//!
//! A [`FusedSeries`](super::FusedSeries) node runs inside a region when everything it reads is that
//! region. A tree that reads two tables reads two regions, so the node has to run where both arrive,
//! and each region answers with its own series instead. [`RawSeriesSource`] is that answer: it runs
//! in the region, binds the region's scan to one metric, its tags and fields, and hands the series
//! on as one raw frame stream.
//!
//! The frame names each series by its complete labels rather than by its storage key. The node's
//! schema is fixed while the frontend plans it, where the region's key layout is unknown, and the
//! receiver compares that schema exactly; an explicit frame depends only on the field names. The
//! stream preserves the scan's partitions, so each requested partition reads only its own series
//! and keeps them contiguous.

use std::fmt::{self, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use common_error::ext::BoxedError;
use common_query::series_frame::{
    pack_raw_series, raw_series_frame_schema_with_ordering, unpack_raw_series,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, InputDistributionRequirements,
    Partitioning, PlanProperties, SendableRecordBatchStream,
};
use datatypes::series::SeriesOrdering;
use datatypes::series::raw::RawSeriesChunk;
use datatypes::series::samples::{FieldSpec, SeriesSchema};
use futures::Stream;
use store_api::region_engine::{RawLabelSource, RawSeriesStream, SendableRawSeriesStream};

use crate::source::materialize_raw_labels;

/// The ordering every frame of this node declares.
///
/// The scan's output order is not known when the frame schema is declared, so advertise only the
/// conservative contiguous-series ordering.
const ORDERING: SeriesOrdering = SeriesOrdering::Contiguous;

/// The logical node: one table scan bound to one metric's raw series.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RawSeriesSource {
    metric_name: Arc<str>,
    /// The logical tags the scan decodes, without `__name__`, which is the metric name.
    tags: Vec<String>,
    /// The field columns each frame carries, in the order the tree reading them was compiled with.
    fields: Vec<String>,
    input: LogicalPlan,
    schema: DFSchemaRef,
}

impl RawSeriesSource {
    pub const fn name() -> &'static str {
        "RawSeriesSource"
    }

    pub fn try_new(
        metric_name: Arc<str>,
        tags: Vec<String>,
        fields: Vec<String>,
        input: LogicalPlan,
    ) -> DataFusionResult<Self> {
        let schema = Arc::new(DFSchema::try_from(frame_schema(&fields)?)?);
        Ok(Self {
            metric_name,
            tags,
            fields,
            input,
            schema,
        })
    }

    pub fn metric_name(&self) -> &str {
        &self.metric_name
    }

    pub fn tags(&self) -> &[String] {
        &self.tags
    }

    pub fn fields(&self) -> &[String] {
        &self.fields
    }

    /// The physical node over `series_scan`, the region's raw-series leaf for this binding.
    pub fn to_execution_plan(
        &self,
        series_scan: Arc<dyn ExecutionPlan>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(RawSeriesSourceExec::try_new(
            series_scan,
            Arc::clone(&self.metric_name),
            self.fields.clone(),
        )?))
    }
}

fn series_schema(fields: &[String]) -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: fields
            .iter()
            .map(|name| FieldSpec {
                name: Arc::from(name.as_str()),
            })
            .collect::<Vec<_>>()
            .into(),
        eval_grid: None,
    })
}

fn frame_schema(fields: &[String]) -> DataFusionResult<SchemaRef> {
    raw_series_frame_schema_with_ordering(
        &series_schema(fields),
        &RawLabelSource::Explicit,
        ORDERING,
    )
}

impl PartialOrd for RawSeriesSource {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // The schema is derived from the fields, so it adds nothing to the order.
        (&self.metric_name, &self.tags, &self.fields)
            .partial_cmp(&(&other.metric_name, &other.tags, &other.fields))
            .and_then(|ordering| match ordering {
                std::cmp::Ordering::Equal => self.input.partial_cmp(&other.input),
                ordering => Some(ordering),
            })
    }
}

impl UserDefinedLogicalNodeCore for RawSeriesSource {
    fn name(&self) -> &str {
        Self::name()
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        Vec::new()
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "RawSeriesSource: metric={}, tags=[{}], fields=[{}]",
            self.metric_name,
            self.tags.join(", "),
            self.fields.join(", ")
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> DataFusionResult<Self> {
        if inputs.len() != 1 {
            return Err(DataFusionError::Internal(
                "RawSeriesSource should have exactly one input".to_string(),
            ));
        }
        Ok(Self {
            metric_name: Arc::clone(&self.metric_name),
            tags: self.tags.clone(),
            fields: self.fields.clone(),
            input: inputs.swap_remove(0),
            schema: Arc::clone(&self.schema),
        })
    }
}

/// The physical node: the region's raw-series leaf, relabelled with one output per scan partition.
#[derive(Debug)]
pub struct RawSeriesSourceExec {
    input: Arc<dyn ExecutionPlan>,
    metric_name: Arc<str>,
    series_schema: Arc<SeriesSchema>,
    properties: Arc<PlanProperties>,
}

impl RawSeriesSourceExec {
    fn try_new(
        input: Arc<dyn ExecutionPlan>,
        metric_name: Arc<str>,
        fields: Vec<String>,
    ) -> DataFusionResult<Self> {
        let series_schema = series_schema(&fields);
        // Rejects a child that is not a raw frame of these fields before the plan can run.
        unpack_raw_series(
            Box::pin(
                datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                    input.schema(),
                    futures::stream::empty(),
                ),
            ),
            Some(Arc::clone(&series_schema)),
        )?;
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(frame_schema(&fields)?),
            Partitioning::UnknownPartitioning(input.properties().partitioning.partition_count()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            metric_name,
            series_schema,
            properties,
        })
    }
}

impl DisplayAs for RawSeriesSourceExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "RawSeriesSourceExec: metric={}, fields={}",
            self.metric_name,
            self.series_schema.fields.len()
        )
    }
}

impl ExecutionPlan for RawSeriesSourceExec {
    fn name(&self) -> &str {
        "RawSeriesSourceExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    /// The scan's own partitions are what keep each series in one piece, so nothing may be inserted
    /// below this node to redistribute them.
    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        InputDistributionRequirements::new(vec![Distribution::UnspecifiedDistribution])
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DataFusionResult<TreeNodeRecursion>,
    ) -> DataFusionResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(format!(
                "RawSeriesSourceExec requires one child, got {}",
                children.len()
            )));
        }
        let fields = self
            .series_schema
            .fields
            .iter()
            .map(|field| field.name.to_string())
            .collect();
        Ok(Arc::new(Self::try_new(
            children.swap_remove(0),
            Arc::clone(&self.metric_name),
            fields,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let partition_count = self.properties.partitioning.partition_count();
        if partition >= partition_count {
            return Err(DataFusionError::Execution(format!(
                "RawSeriesSourceExec has {partition_count} partitions, got {partition}"
            )));
        }
        let frames = self.input.execute(partition, context)?;
        let raw = unpack_raw_series(frames, Some(Arc::clone(&self.series_schema)))?;
        let raw = materialize_raw_labels(raw)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        pack_raw_series(Box::pin(ContiguousRawSeries {
            schema: Arc::clone(&self.series_schema),
            label_source: RawLabelSource::Explicit,
            input: raw,
        }))
    }
}

/// A scan partition with its labels materialized as explicit labels.
struct ContiguousRawSeries {
    schema: Arc<SeriesSchema>,
    label_source: RawLabelSource,
    input: SendableRawSeriesStream,
}

impl Stream for ContiguousRawSeries {
    type Item = Result<RawSeriesChunk, BoxedError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().input.as_mut().poll_next(cx)
    }
}

impl RawSeriesStream for ContiguousRawSeries {
    fn schema(&self) -> &Arc<SeriesSchema> {
        &self.schema
    }

    fn label_source(&self) -> &RawLabelSource {
        &self.label_source
    }

    fn ordering(&self) -> SeriesOrdering {
        ORDERING
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use datafusion::arrow::array::Float64Array;
    use datafusion::arrow::buffer::{Buffer, ScalarBuffer};
    use datafusion::common::tree_node::TreeNodeRecursion;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_plan::execution_plan::{
        ChildrenPropertiesMode, ReplaceChildrenOptions,
    };
    use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
    use datatypes::series::frame::FrameLabelMode;
    use datatypes::series::raw::RawTimestamps;
    use datatypes::series::samples::{Samples, SeriesFields};
    use datatypes::series::{Label, SeriesLabels};
    use futures::TryStreamExt;

    use super::*;

    struct CountedExec {
        input: Arc<dyn ExecutionPlan>,
        calls: Arc<Vec<AtomicUsize>>,
    }

    impl std::fmt::Debug for CountedExec {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            f.debug_struct("CountedExec").finish()
        }
    }

    impl DisplayAs for CountedExec {
        fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
            self.input.fmt_as(t, f)
        }
    }

    impl ExecutionPlan for CountedExec {
        fn name(&self) -> &str {
            "CountedExec"
        }
        fn properties(&self) -> &Arc<PlanProperties> {
            self.input.properties()
        }
        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![&self.input]
        }
        fn apply_expressions(
            &self,
            f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DataFusionResult<TreeNodeRecursion>,
        ) -> DataFusionResult<TreeNodeRecursion> {
            self.input.apply_expressions(f)
        }
        fn with_new_children(
            self: Arc<Self>,
            mut children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            if children.len() != 1 {
                return Err(DataFusionError::Plan(
                    "CountedExec requires one child".into(),
                ));
            }
            Ok(Arc::new(Self {
                input: children.remove(0),
                calls: Arc::clone(&self.calls),
            }))
        }
        fn execute(
            &self,
            partition: usize,
            context: Arc<TaskContext>,
        ) -> DataFusionResult<SendableRecordBatchStream> {
            self.calls[partition].fetch_add(1, Ordering::SeqCst);
            self.input.execute(partition, context)
        }
    }

    fn source(partition_count: usize) -> (Arc<dyn ExecutionPlan>, Arc<Vec<AtomicUsize>>) {
        let fields = vec!["value".to_string()];
        let schema = frame_schema(&fields).unwrap();
        let series_schema = series_schema(&fields);
        let batches = ["p0", "p1", "p2"]
            .iter()
            .zip([10.0, 20.0, 30.0])
            .take(partition_count)
            .map(|(host, value)| {
                let chunk = RawSeriesChunk::try_new(
                    &series_schema,
                    Buffer::from_slice_ref(host.as_bytes()),
                    Some(
                        SeriesLabels::try_new(
                            Some(Arc::from("metric")),
                            vec![Label {
                                name: Arc::from("host"),
                                value: Arc::from(*host),
                            }],
                        )
                        .unwrap(),
                    ),
                    RawTimestamps::try_new(ScalarBuffer::from(vec![1000_i64])).unwrap(),
                    SeriesFields::try_from_vec(vec![Samples::from_float(Float64Array::from(
                        vec![Some(value)],
                    ))])
                    .unwrap(),
                    true,
                )
                .unwrap();
                datatypes::series::frame::pack_raw_series_chunk(
                    &series_schema,
                    FrameLabelMode::Explicit,
                    None,
                    &chunk,
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let input = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&batches, schema, None).unwrap(),
        )));
        let calls = Arc::new((0..partition_count).map(|_| AtomicUsize::new(0)).collect());
        (
            Arc::new(CountedExec {
                input,
                calls: Arc::clone(&calls),
            }),
            calls,
        )
    }

    async fn read_partition(
        exec: &RawSeriesSourceExec,
        partition: usize,
    ) -> Vec<(String, String, i64, f64, bool)> {
        let stream = exec
            .execute(partition, Arc::new(TaskContext::default()))
            .unwrap();
        assert_eq!(exec.schema(), stream.schema());
        let chunks = unpack_raw_series(stream, Some(Arc::clone(&exec.series_schema))).unwrap();
        chunks
            .map_ok(|chunk| {
                let metric_name = chunk.metric.as_ref().unwrap().name().unwrap().to_string();
                let host = chunk
                    .metric
                    .as_ref()
                    .unwrap()
                    .get("host")
                    .unwrap()
                    .to_string();
                let time = chunk.time.timestamp(0).unwrap();
                let value = match chunk.fields.as_slice()[0].sample(0) {
                    datatypes::series::samples::SampleRef::Float(value) => value,
                    _ => panic!("expected float sample"),
                };
                (metric_name, host, time, value, chunk.is_last)
            })
            .try_collect()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn preserves_partitioned_raw_series_and_rebuilds_partition_count() {
        let fields = vec!["value".to_string()];
        let (input, calls) = source(3);
        let counted = input.clone();
        let exec =
            RawSeriesSourceExec::try_new(counted, Arc::from("metric"), fields.clone()).unwrap();
        assert_eq!(3, exec.properties.partitioning.partition_count());
        assert_eq!(
            vec![("metric".into(), "p1".into(), 1000, 20.0, true)],
            read_partition(&exec, 1).await
        );
        assert_eq!(
            vec![0, 1, 0],
            calls
                .iter()
                .map(|c| c.load(Ordering::SeqCst))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            vec![("metric".into(), "p0".into(), 1000, 10.0, true)],
            read_partition(&exec, 0).await
        );
        assert_eq!(
            vec![("metric".into(), "p2".into(), 1000, 30.0, true)],
            read_partition(&exec, 2).await
        );
        assert_eq!(
            vec![1, 1, 1],
            calls
                .iter()
                .map(|c| c.load(Ordering::SeqCst))
                .collect::<Vec<_>>()
        );
        assert!(exec.execute(3, Arc::new(TaskContext::default())).is_err());
        assert_eq!(
            vec![1, 1, 1],
            calls
                .iter()
                .map(|c| c.load(Ordering::SeqCst))
                .collect::<Vec<_>>()
        );

        let (replacement, _) = source(2);
        let rebuilt = Arc::new(exec)
            .replace_children(
                vec![replacement],
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )
            .unwrap();
        assert_eq!(2, rebuilt.properties().partitioning.partition_count());
        assert_eq!(frame_schema(&fields).unwrap(), rebuilt.schema());
        assert_eq!(
            vec![("metric".into(), "p1".into(), 1000, 20.0, true)],
            read_partition(rebuilt.downcast_ref::<RawSeriesSourceExec>().unwrap(), 1).await
        );
    }
}
