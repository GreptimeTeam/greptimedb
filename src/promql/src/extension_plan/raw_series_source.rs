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
    frame_schema_with, pack_raw_series, raw_series_frame_schema_with_ordering,
    series_frame_ordering, unpack_raw_series,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, InputDistributionRequirements,
    Partitioning, PlanProperties, SendableRecordBatchStream,
};
use datatypes::series::SeriesOrdering;
use datatypes::series::frame::{
    FrameContent, FrameLabelMode, LabelDecodeSpec, SeriesFrameDeclaration,
};
use datatypes::series::raw::RawSeriesChunk;
use datatypes::series::samples::{FieldSpec, SeriesSchema};
use futures::{Stream, StreamExt};
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

/// Returns the per-series scan below `input`, if `input` is one behind transparent wrappers.
///
/// A caller declares the child a per-series scan; this confirms the plan below still is one. The
/// physical optimizer may put a [`CooperativeExec`] around the scan, because a leaf that does not
/// yield cooperatively gets one, and that wrapper keeps the scan's partitions, its ordering and its
/// output unchanged. Only that wrapper is looked through: any node that redistributes or reorders
/// rows - an exchange, a coalesce, a sort - breaks the one-series-one-partition contract this
/// answer is about, so its presence must answer `None` rather than be seen through.
fn per_series_scan_child(input: &Arc<dyn ExecutionPlan>) -> Option<&SeriesScanExec> {
    let mut node = input.as_ref();
    loop {
        if let Some(scan) = node.downcast_ref::<SeriesScanExec>() {
            return Some(scan);
        }
        let cooperative = node.downcast_ref::<CooperativeExec>()?;
        node = cooperative.input().as_ref();
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
    /// The frame schema this node publishes, derived from the fields and [`ORDERING`].
    frame_schema: SchemaRef,
    /// The child's own frame schema when every batch only needs to be re-declared under
    /// [`Self::frame_schema`], as [`pass_through_frame`] decided at construction.
    pass_through: Option<SchemaRef>,
    properties: Arc<PlanProperties>,
    per_series_scan: bool,
}

/// The child's frame schema when the child already publishes this node's frame.
///
/// Such a child needs no decode and no re-encode: publishing each batch under this node's
/// declaration is the whole job. "This node's frame" is the canonical frame of these fields with
/// explicit labels and no label decode description, which is what the decode-and-repack path would
/// publish for the same chunks, so the declared ordering is the only difference that can remain -
/// and only downwards. This node declares [`ORDERING`], the weakest per-series promise it keeps, so
/// a child that promises key-then-time already keeps it, while an interleaved child does not:
/// forwarding that one would promise more than the child does.
///
/// Anything else - other content, key-encoded labels, a description, a grid, other fields, a
/// metadata entry this node would not write - keeps the batches on the decode-and-repack path.
fn pass_through_frame(child: &SchemaRef, series_schema: &Arc<SeriesSchema>) -> Option<SchemaRef> {
    let declaration = SeriesFrameDeclaration::parse(child.as_ref()).ok()?;
    if declaration.content() != FrameContent::Raw
        || declaration.labels() != FrameLabelMode::Explicit
        || declaration.label_decode().is_some()
    {
        return None;
    }
    let ordering = series_frame_ordering(child.as_ref()).ok()?;
    if ordering == SeriesOrdering::Interleaved {
        return None;
    }
    // Equality with the canonical frame proves the rest: an explicit-label raw frame of these
    // fields, in this order and with no grid, is exactly the schema a batch is published under
    // after it was decoded and re-encoded.
    let canonical =
        frame_schema_with(series_schema, FrameLabelMode::Explicit, None, ordering).ok()?;
    (*canonical == **child).then(|| Arc::clone(child))
}

impl RawSeriesSourceExec {
    fn try_new_with_provenance(
        input: Arc<dyn ExecutionPlan>,
        metric_name: Arc<str>,
        fields: Vec<String>,
        per_series_scan: bool,
    ) -> DataFusionResult<Self> {
        let per_series_scan = per_series_scan && per_series_scan_child(&input).is_some();
        let series_schema = series_schema(&fields);
        let partition_count = input.properties().partitioning.partition_count();
        if partition_count == 0 {
            return Err(DataFusionError::Plan(
                "RawSeriesSourceExec child has zero partitions".to_string(),
            ));
        }
        // Rejects a child that is not a raw frame of these fields before the plan can run, and
        // validates the declared layout, the label binding of that layout, and an empty batch.
        unpack_raw_series(
            Box::pin(RecordBatchStreamAdapter::new(
                input.schema(),
                futures::stream::empty(),
            )),
            Some(Arc::clone(&series_schema)),
        )?;
        let published_frame = frame_schema(&fields)?;
        // That preflight is all this node validates before it runs; a child that already writes
        // this node's frame then needs nothing but a re-declaration per batch.
        let pass_through = pass_through_frame(&input.schema(), &series_schema);
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&published_frame)),
            Partitioning::UnknownPartitioning(partition_count),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            metric_name,
            series_schema,
            frame_schema: published_frame,
            pass_through,
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
        if let Some(child_schema) = &self.pass_through {
            // The child already writes this node's frame, so a batch differs only in the ordering
            // it declares. Re-declaring keeps the child's column data untouched and publishes
            // exactly the schema the plan was built with; one batch in, one batch out, so the
            // child's polling and backpressure are unchanged.
            let declared = Arc::clone(&self.frame_schema);
            let child_schema = Arc::clone(child_schema);
            let batches = frames.map(move |batch| {
                let batch = batch?;
                if batch.schema() != child_schema {
                    return Err(DataFusionError::Execution(
                        "raw frame schema mismatch".to_string(),
                    ));
                }
                RecordBatch::try_new(Arc::clone(&declared), batch.columns().to_vec())
                    .map_err(DataFusionError::from)
            });
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&self.frame_schema),
                batches,
            )));
        }
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
pub(crate) mod tests {
    use api::v1::SemanticType;
    use common_recordbatch::EmptyRecordBatchStream;
    use datafusion::arrow::array::Float64Array;
    use datafusion::arrow::buffer::{Buffer, ScalarBuffer};
    use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::logical_expr::EmptyRelation;
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::execution_plan::{
        ChildrenPropertiesMode, ReplaceChildrenOptions,
    };
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use datafusion::prelude::SessionContext;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::prelude::Value;
    use datatypes::schema::{ColumnSchema, SchemaRef as DatatypesSchemaRef};
    use datatypes::series::frame::{
        LABEL_DECODE_VERSION, LabelDecodeColumn, LabelDecodeEncoding, LabelDecodeTag,
        pack_raw_series_chunk,
    };
    use datatypes::series::raw::RawTimestamps;
    use datatypes::series::samples::{Samples, SeriesFields};
    use datatypes::series::time::{EvalGrid, GridSpec};
    use datatypes::series::{Label, SeriesLabels};
    use futures::TryStreamExt;
    use mito_codec::row_converter::{DensePrimaryKeyCodec, PrimaryKeyCodec, SortField};
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder, RegionMetadataRef};
    use store_api::region_engine::{
        BoundTag, PrepareRequest, QueryScanContext, RegionScanner, ScannerProperties,
    };
    use store_api::storage::{RegionId, ScanRequest};
    use table::table::scan::RegionScanExec;

    use super::*;

    const FIXTURE: &str =
        include_str!("../../../../tests/compatibility/wire/raw_series_source/v1_payload.hex");

    /// The one field the fixtures below scan, which is also the field this node declares.
    const FIELD: &str = "value";

    fn declared_fields() -> Vec<String> {
        vec![FIELD.to_string()]
    }

    fn fixture_bytes() -> Vec<u8> {
        let hex = FIXTURE.trim().as_bytes();
        assert_eq!(hex.len() % 2, 0);
        hex.as_chunks::<2>()
            .0
            .iter()
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

    /// The region layout the fixtures below scan: the tag `host`, the time index and one field.
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
                column_schema: ColumnSchema::new(FIELD, ConcreteDataType::float64_datatype(), true),
                semantic_type: SemanticType::Field,
                column_id: 3,
            })
            .primary_key(vec![1]);
        Arc::new(builder.build().expect("the fixture metadata is valid"))
    }

    /// The label binding a per-series scan of [`region_metadata`] publishes: the one key column is
    /// bound, so one series' chunks stay in one scan partition.
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

    /// The region's scanner, answering the raw-series path the region planner selects.
    struct RawSeriesScanner {
        metadata: RegionMetadataRef,
        properties: ScannerProperties,
    }

    impl fmt::Debug for RawSeriesScanner {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            write!(f, "RawSeriesScanner")
        }
    }

    impl DisplayAs for RawSeriesScanner {
        fn fmt_as(&self, _format: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
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
        ) -> Result<common_recordbatch::SendableRecordBatchStream, BoxedError> {
            Ok(Box::pin(EmptyRecordBatchStream::new(Arc::clone(
                &self.metadata.schema,
            ))))
        }

        fn raw_series_schema(
            &self,
            _label_source: &RawLabelSource,
        ) -> Result<Option<Arc<SeriesSchema>>, BoxedError> {
            Ok(Some(series_schema(&declared_fields())))
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
    pub(crate) fn per_series_scan(partitions: usize) -> Arc<dyn ExecutionPlan> {
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

    /// A [`RawSeriesSourceExec`] over `child`, as the region planner builds one.
    pub(crate) fn raw_source(child: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let fields = declared_fields();
        RawSeriesSource::try_new(
            Arc::from("cpu"),
            vec!["host".to_string()],
            fields.clone(),
            LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: Arc::new(
                    DFSchema::try_from(frame_schema(&fields).expect("the frame is legal"))
                        .expect("the frame is a legal schema"),
                ),
            }),
        )
        .expect("the source binds one metric")
        .to_per_series_execution_plan(child)
        .expect("the child is a raw frame of these fields")
    }

    fn provenance(plan: &Arc<dyn ExecutionPlan>) -> bool {
        plan.downcast_ref::<RawSeriesSourceExec>()
            .expect("the fixture root is the region's raw source")
            .is_per_series_scan()
    }

    /// Rebuilds `plan` over `child` the way the physical optimizer does, recomputing its properties.
    fn rebuilt_over(
        plan: &Arc<dyn ExecutionPlan>,
        child: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::clone(plan)
            .replace_children(
                vec![child],
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )
            .expect("the child is still a raw frame of these fields")
    }

    #[test]
    fn a_cooperative_child_keeps_the_per_series_provenance() {
        let root = raw_source(per_series_scan(2));
        assert!(provenance(&root), "the fixture root is the per-series case");

        // The physical optimizer wraps a leaf that does not yield cooperatively, so the scan arrives
        // as a child of a wrapper instead of as the child itself.
        let cooperative: Arc<dyn ExecutionPlan> =
            Arc::new(CooperativeExec::new(per_series_scan(2)));
        let rebuilt = rebuilt_over(&root, Arc::clone(&cooperative));
        assert_eq!(
            2,
            rebuilt.properties().output_partitioning().partition_count()
        );
        assert!(
            provenance(&rebuilt),
            "a cooperative wrapper passes rows through unchanged, so the scan is still per-series"
        );

        // Repeating the wrapper changes nothing either.
        let nested: Arc<dyn ExecutionPlan> = Arc::new(CooperativeExec::new(cooperative));
        assert!(
            provenance(&rebuilt_over(&root, nested)),
            "wrappers nest without moving rows"
        );
    }

    #[test]
    fn a_child_that_moves_rows_loses_the_per_series_provenance() {
        let root = raw_source(per_series_scan(2));
        // Coalescing merges the scan's partitions, so one series is no longer confined to one of the
        // partitions this node publishes.
        let coalesced: Arc<dyn ExecutionPlan> =
            Arc::new(CoalescePartitionsExec::new(per_series_scan(2)));
        assert!(
            !provenance(&rebuilt_over(&root, coalesced)),
            "the provenance stops at the scan, not at whatever node carries its frames"
        );
    }

    /// The one series the frame fixtures below publish: `{__name__="cpu", host="a"}`.
    fn fixture_labels() -> SeriesLabels {
        SeriesLabels::try_new(
            Some(Arc::from("cpu")),
            vec![Label {
                name: Arc::from("host"),
                value: Arc::from("a"),
            }],
        )
        .expect("the fixture labels are legal")
    }

    /// The description a dense key of the one string column `host` declares.
    fn host_decode() -> LabelDecodeSpec {
        LabelDecodeSpec {
            version: LABEL_DECODE_VERSION,
            metric_name: "cpu".to_string(),
            encoding: LabelDecodeEncoding::Dense,
            primary_key: vec![LabelDecodeColumn {
                column_id: 1,
                name: "host".to_string(),
                physical_type: ConcreteDataType::string_datatype(),
            }],
            logical_tags: vec![LabelDecodeTag {
                column_id: 1,
                name: "host".to_string(),
            }],
        }
    }

    /// The dense key bytes of `{host="a"}`.
    fn host_key() -> Buffer {
        let mut bytes = Vec::new();
        DensePrimaryKeyCodec::with_fields(vec![(
            1,
            SortField::new(ConcreteDataType::string_datatype()),
        )])
        .encode_values(&[(1, Value::String("a".into()))], &mut bytes)
        .expect("a dense key of one string");
        Buffer::from(bytes)
    }

    /// One chunk of the fixture series: carrying `metric`, or naming it by [`host_key`] instead.
    fn fixture_chunk(metric: Option<SeriesLabels>) -> RawSeriesChunk {
        RawSeriesChunk::try_new(
            &series_schema(&declared_fields()),
            host_key(),
            metric,
            RawTimestamps::try_new(ScalarBuffer::from(vec![1000i64, 2000, 3000]))
                .expect("ascending timestamps"),
            SeriesFields::try_from_vec(vec![Samples::from_float(Float64Array::from(vec![
                Some(1.0),
                Some(2.5),
                None,
            ]))])
            .expect("one sample per timestamp per field"),
            true,
        )
        .expect("a legal raw chunk")
    }

    /// The frame this node publishes for `fields`.
    fn root_frame(fields: &[String]) -> SchemaRef {
        frame_schema(fields).expect("the fixture frame is legal")
    }

    /// The frame a child declares over this node's fields.
    fn child_frame(decode: Option<&LabelDecodeSpec>, ordering: SeriesOrdering) -> SchemaRef {
        let schema = series_schema(&declared_fields());
        let (labels, decode) = match decode {
            Some(decode) => (FrameLabelMode::Key, Some(decode)),
            None => (FrameLabelMode::Explicit, None),
        };
        frame_schema_with(&schema, labels, decode, ordering)
            .expect("the fixture declares one frame")
    }

    /// The one frame batch of `chunk`, declared under `frame`.
    fn frame_batch(
        chunk: &RawSeriesChunk,
        decode: Option<&LabelDecodeSpec>,
        frame: &SchemaRef,
    ) -> RecordBatch {
        let labels = match decode {
            Some(_) => FrameLabelMode::Key,
            None => FrameLabelMode::Explicit,
        };
        let packed =
            pack_raw_series_chunk(&series_schema(&declared_fields()), labels, decode, chunk)
                .expect("the fixture chunk packs");
        assert_eq!(1, packed.len(), "one chunk is one frame row");
        RecordBatch::try_new(Arc::clone(frame), packed[0].columns().to_vec())
            .expect("the packed columns match the declared frame")
    }

    /// A child leaf answering `batches` under `frame`, in one partition.
    fn framed_child(frame: SchemaRef, batches: Vec<RecordBatch>) -> Arc<dyn ExecutionPlan> {
        Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[batches], frame, None)
                .expect("the fixture batches match their frame"),
        )))
    }

    /// Every batch this node publishes for its one partition.
    async fn publish(root: &Arc<dyn ExecutionPlan>) -> Vec<RecordBatch> {
        let published = root
            .execute(0, SessionContext::default().task_ctx())
            .expect("the fixture root has one partition");
        published.try_collect().await.expect("the frame publishes")
    }

    /// A child that already writes this node's frame is published, not decoded and re-encoded.
    ///
    /// The columns are handed on as they are, which is the whole point: this node's declared frame
    /// is what a decode and a re-encode would produce anyway, so the only thing a batch needs is
    /// this node's own declaration.
    #[tokio::test]
    async fn an_explicit_child_frame_is_published_without_decoding_it() {
        let frame = child_frame(None, SeriesOrdering::Contiguous);
        let batch = frame_batch(&fixture_chunk(Some(fixture_labels())), None, &frame);
        let root = raw_source(framed_child(Arc::clone(&frame), vec![batch.clone()]));

        let published = publish(&root).await;
        assert_eq!(1, published.len(), "one batch in, one batch out");
        assert_eq!(
            root.schema().as_ref(),
            published[0].schema().as_ref(),
            "the batch is published under this node's own frame"
        );
        assert_eq!(
            SeriesOrdering::Contiguous,
            series_frame_ordering(published[0].schema().as_ref()).expect("a declared ordering")
        );
        for (child, forwarded) in batch.columns().iter().zip(published[0].columns()) {
            assert!(
                Arc::ptr_eq(child, forwarded),
                "a forwarded column is the child's own, not a re-encoded copy"
            );
        }
    }

    /// A forwarded frame carries exactly what the decode-and-repack path publishes.
    ///
    /// The same payload is offered twice: once as a key-then-time child, which this node forwards
    /// after exchanging that promise for its own `contiguous` one, and once as an interleaved
    /// child, which it cannot forward because interleaving does not imply contiguity. The two
    /// outputs are the same frame, byte for byte, so forwarding is not an approximation of the old
    /// path.
    #[tokio::test]
    async fn a_forwarded_frame_carries_what_the_decoded_path_publishes() {
        let chunk = fixture_chunk(Some(fixture_labels()));

        let forwarded_frame = child_frame(None, SeriesOrdering::KeyThenTime);
        let forwarded = publish(&raw_source(framed_child(
            Arc::clone(&forwarded_frame),
            vec![frame_batch(&chunk, None, &forwarded_frame)],
        )))
        .await;

        let decoded_frame = child_frame(None, SeriesOrdering::Interleaved);
        let decoded = publish(&raw_source(framed_child(
            Arc::clone(&decoded_frame),
            vec![frame_batch(&chunk, None, &decoded_frame)],
        )))
        .await;

        assert_eq!(1, forwarded.len(), "the forwarded batch is not split");
        assert_eq!(1, decoded.len(), "the decoded path flushes one batch");
        assert_eq!(
            SeriesOrdering::Contiguous,
            series_frame_ordering(forwarded[0].schema().as_ref()).expect("a declared ordering"),
            "a finer child promise is exchanged for this node's own"
        );
        assert!(
            !Arc::ptr_eq(decoded[0].column(0), forwarded[0].column(0)),
            "the interleaved child really did go through the decoder"
        );
        assert_eq!(
            forwarded, decoded,
            "both paths publish the same frame, so the shortcut changes no byte"
        );
    }

    /// A key-encoded child is decoded, not forwarded: its frame names its labels by its key.
    ///
    /// Decoding `{host="a"}` from the declared description has to land on the same explicit frame
    /// the labels themselves would have produced.
    #[tokio::test]
    async fn a_key_encoded_child_frame_is_decoded() {
        let decode = host_decode();
        let keyed_frame = child_frame(Some(&decode), SeriesOrdering::KeyThenTime);
        let keyed = frame_batch(&fixture_chunk(None), Some(&decode), &keyed_frame);
        let published = publish(&raw_source(framed_child(keyed_frame, vec![keyed]))).await;

        let explicit_frame = child_frame(None, SeriesOrdering::Contiguous);
        let expected = frame_batch(
            &fixture_chunk(Some(fixture_labels())),
            None,
            &explicit_frame,
        );
        assert_eq!(
            vec![expected],
            published,
            "the decoded key publishes the explicit frame's own bytes"
        );
    }

    /// A child frame this node would not have written is re-encoded even when it is explicit.
    ///
    /// The extra metadata is not this node's declaration, so forwarding the batch would publish
    /// rows under a frame they were not written against; the decoded path rewrites them instead.
    #[tokio::test]
    async fn a_foreign_explicit_child_frame_keeps_the_pack_path() {
        let canonical = child_frame(None, SeriesOrdering::Contiguous);
        let mut metadata = canonical.metadata().clone();
        metadata.insert("application.note".to_string(), "foreign".to_string());
        let foreign = Arc::new(ArrowSchema::new_with_metadata(
            canonical.fields().clone(),
            metadata,
        ));
        let payload = fixture_chunk(Some(fixture_labels()));
        let batch = frame_batch(&payload, None, &foreign);
        let published = publish(&raw_source(framed_child(foreign, vec![batch.clone()]))).await;

        assert_eq!(1, published.len());
        assert_eq!(
            root_frame(&declared_fields()).as_ref(),
            published[0].schema().as_ref(),
            "the published frame is this node's, not the child's"
        );
        assert!(
            !Arc::ptr_eq(batch.column(0), published[0].column(0)),
            "the child's columns are rewritten, not forwarded"
        );
        let expected = frame_batch(&payload, None, &root_frame(&declared_fields()));
        assert_eq!(vec![expected], published);
    }

    /// A child that is not a raw frame is refused while the plan is built, as it always was.
    #[test]
    fn a_non_raw_child_is_refused_before_the_plan_runs() {
        let evaluated = Arc::new(SeriesSchema {
            fields: series_schema(&declared_fields()).fields.clone(),
            eval_grid: Some(Arc::new(
                EvalGrid::try_new(GridSpec::try_new(0, 1_000, 2).expect("a legal grid"))
                    .expect("a legal grid"),
            )),
        });
        let frame = frame_schema_with(
            &evaluated,
            FrameLabelMode::Explicit,
            None,
            SeriesOrdering::Contiguous,
        )
        .expect("an evaluated frame declares its grid");
        let error = RawSeriesSource::try_new(
            Arc::from("cpu"),
            vec!["host".to_string()],
            declared_fields(),
            LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: Arc::new(DFSchema::empty()),
            }),
        )
        .expect("the source binds one metric")
        .to_per_series_execution_plan(framed_child(frame, vec![]))
        .expect_err("an evaluated child is not a raw frame");
        assert!(
            error.to_string().contains("expected a raw frame"),
            "the preflight decides, not the shortcut: {error}"
        );
    }
}
