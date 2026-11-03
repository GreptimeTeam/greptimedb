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
//! receiver compares that schema exactly; an explicit frame depends only on the field names. Each
//! scan partition is re-labelled independently, keeping its place in the partition layout and
//! each series contiguous within its scan partition.

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
use datatypes::series::frame::LabelDecodeSpec;
use datatypes::series::raw::RawSeriesChunk;
use datatypes::series::samples::{FieldSpec, SeriesSchema};
use futures::Stream;
use greptime_proto::substrait_extension::RawSeriesSource as RawSeriesSourceProto;
use prost::Message;
use store_api::region_engine::{RawLabelSource, RawSeriesStream, SendableRawSeriesStream};
use table::table::scan::SeriesScanExec;

use crate::source::materialize_raw_labels;

/// The ordering every frame of this node declares.
///
/// The frontend plans this node before the region's scan declares how it orders its keys, so the
/// schema cannot carry the scan's own promise. The node therefore states the weakest one that its
/// output always keeps: one scan partition emits one series after another, so each series stays
/// contiguous, while the finer key-then-time order of the scan is not repeated here.
const ORDERING: SeriesOrdering = SeriesOrdering::Contiguous;
const WIRE_VERSION: u32 = 1;

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

    /// Encodes only the binding; Substrait carries the input plan separately.
    pub fn serialize(&self) -> Vec<u8> {
        RawSeriesSourceProto {
            version: WIRE_VERSION,
            metric_name: self.metric_name.to_string(),
            tags: self.tags.clone(),
            fields: self.fields.clone(),
        }
        .encode_to_vec()
    }

    /// Decodes a binding with a placeholder, replaced when Substrait restores the child.
    pub fn deserialize(bytes: &[u8]) -> DataFusionResult<Self> {
        let binding = RawSeriesSourceProto::decode(bytes).map_err(|error| {
            DataFusionError::Substrait(format!("Failed to deserialize RawSeriesSource: {error}"))
        })?;
        if binding.version != WIRE_VERSION {
            return Err(DataFusionError::Substrait(format!(
                "Unsupported RawSeriesSource version: {}",
                binding.version
            )));
        }
        let input = LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        });
        Self::try_new(
            Arc::from(binding.metric_name),
            binding.tags,
            binding.fields,
            input,
        )
    }

    /// The physical node over `series_scan`, the region's raw-series leaf for this binding.
    pub fn to_per_series_execution_plan(
        &self,
        series_scan: Arc<dyn ExecutionPlan>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(RawSeriesSourceExec::try_new_with_provenance(
            series_scan,
            Arc::clone(&self.metric_name),
            self.fields.clone(),
            true,
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

/// One partition's relabelled frames, published under this node's declared ordering.
///
/// The scan states its own key order, which the node cannot repeat in a schema it fixed at plan
/// time. Every other property is forwarded unchanged.
struct DeclaredOrdering(SendableRawSeriesStream);

impl Stream for DeclaredOrdering {
    type Item = Result<RawSeriesChunk, BoxedError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.as_mut().poll_next(cx)
    }
}

impl RawSeriesStream for DeclaredOrdering {
    fn schema(&self) -> &Arc<SeriesSchema> {
        self.0.schema()
    }

    fn label_source(&self) -> &RawLabelSource {
        self.0.label_source()
    }

    fn label_decode_spec(&self) -> Option<&LabelDecodeSpec> {
        self.0.label_decode_spec()
    }

    fn ordering(&self) -> SeriesOrdering {
        ORDERING
    }
}

/// The physical node: the region's raw-series leaf, re-labelling each scan partition in place.
#[derive(Debug)]
pub struct RawSeriesSourceExec {
    input: Arc<dyn ExecutionPlan>,
    metric_name: Arc<str>,
    series_schema: Arc<SeriesSchema>,
    properties: Arc<PlanProperties>,
    per_series_scan: bool,
}

impl RawSeriesSourceExec {
    fn try_new_with_provenance(
        input: Arc<dyn ExecutionPlan>,
        metric_name: Arc<str>,
        fields: Vec<String>,
        per_series_scan: bool,
    ) -> DataFusionResult<Self> {
        let per_series_scan = per_series_scan && input.downcast_ref::<SeriesScanExec>().is_some();
        let series_schema = series_schema(&fields);
        let partition_count = input.properties().partitioning.partition_count();
        if partition_count == 0 {
            return Err(DataFusionError::Plan(
                "RawSeriesSourceExec child has zero partitions".to_string(),
            ));
        }
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
            Partitioning::UnknownPartitioning(partition_count),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            metric_name,
            series_schema,
            properties,
            per_series_scan,
        })
    }

    /// Whether this node was built by a caller that declared its child a per-series scan, and that
    /// child is one.
    ///
    /// A consumer may publish an identity as soon as its chain closes only under this property, so
    /// it is answered from what the constructor established rather than from the node's type.
    pub fn is_per_series_scan(&self) -> bool {
        self.per_series_scan
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
        Ok(Arc::new(Self::try_new_with_provenance(
            children.swap_remove(0),
            Arc::clone(&self.metric_name),
            fields,
            self.per_series_scan,
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
                "RawSeriesSourceExec partition {partition} is out of bounds for {partition_count} partitions"
            )));
        }
        let frames = self.input.execute(partition, context)?;
        let raw = unpack_raw_series(frames, Some(Arc::clone(&self.series_schema)))?;
        let relabelled = materialize_raw_labels(raw)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        // The scan may promise a finer order than this node declares. Exchange the promise here so
        // the published frame carries the schema the plan was built with, which is what the region
        // and the frontend both compare.
        pack_raw_series(Box::pin(DeclaredOrdering(relabelled)))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::logical_expr::EmptyRelation;

    use super::*;

    const FIXTURE: &str =
        include_str!("../../../../tests/compatibility/wire/raw_series_source/v1_payload.hex");

    fn fixture_bytes() -> Vec<u8> {
        let hex = FIXTURE.trim().as_bytes();
        assert_eq!(hex.len() % 2, 0);
        hex.chunks_exact(2)
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect()
    }

    #[test]
    fn raw_series_source_wire_fixture() {
        let bytes = fixture_bytes();
        let source = RawSeriesSource::deserialize(&bytes).unwrap();
        assert_eq!(source.metric_name(), "http_requests_total");
        assert_eq!(source.tags(), ["z", "a"]);
        assert_eq!(source.fields(), ["value_b", "value_a"]);
        assert_eq!(source.serialize(), bytes);
        assert!(matches!(
            source.inputs()[0],
            LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                ..
            })
        ));

        let field = source.schema().field_with_name(None, "fields").unwrap();
        let DataType::Struct(fields) = field.data_type() else {
            panic!("expected struct field in frame schema");
        };
        assert_eq!(
            fields
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            ["value_b", "value_a"]
        );

        let child = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: true,
            schema: Arc::new(DFSchema::empty()),
        });
        let rebound = source
            .with_exprs_and_inputs(vec![], vec![child.clone()])
            .unwrap();
        assert_eq!(rebound.inputs(), vec![&child]);
        assert_eq!(rebound.schema(), source.schema());
        assert_eq!(rebound.serialize(), bytes);
        assert!(source.with_exprs_and_inputs(vec![], vec![]).is_err());
        assert!(
            source
                .with_exprs_and_inputs(vec![], vec![child.clone(), child])
                .is_err()
        );
    }

    #[test]
    fn raw_series_source_wire_rejects_unsupported_and_truncated_payloads() {
        let fixture = fixture_bytes();
        for version in [0, 2] {
            let mut bytes = fixture.clone();
            bytes[1] = version;
            let error = RawSeriesSource::deserialize(&bytes).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("Unsupported RawSeriesSource version")
            );
        }
        let error = RawSeriesSource::deserialize(&fixture[..fixture.len() - 1]).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Failed to deserialize RawSeriesSource")
        );

        // Unknown protobuf fields do not invalidate a known binding version.
        let mut bytes = fixture;
        bytes.extend([0x28, 0x01]);
        assert!(RawSeriesSource::deserialize(&bytes).is_ok());
    }
}
