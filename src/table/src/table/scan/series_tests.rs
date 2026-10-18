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

use std::fmt;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use api::v1::SemanticType;
use common_error::ext::BoxedError;
use common_query::request::RawSeriesRequest;
use common_query::series_frame::raw_series_frame_ordering;
use common_recordbatch::{
    QueryMemoryTracker, RecordBatch, RecordBatches, SendableRecordBatchStream,
};
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricValue};
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, ReplaceChildrenOptions,
};
use datafusion_physical_expr::{Partitioning, PhysicalExpr};
use datatypes::arrow::array::Float64Array;
use datatypes::arrow::buffer::Buffer;
use datatypes::data_type::ConcreteDataType;
use datatypes::error::InvalidSeriesSnafu;
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use datatypes::series::SeriesOrdering;
use datatypes::series::raw::{RawSeriesChunk, RawTimestamps};
use datatypes::series::samples::{FieldSpec, Samples, SeriesFields, SeriesSchema};
use datatypes::vectors::{Int32Vector, TimestampMillisecondVector, VectorRef};
use futures::{Stream, StreamExt, TryStreamExt};
use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
use store_api::region_engine::{
    BoundTag, PrepareRequest, QueryScanContext, RawLabelSource, RawSeriesStream, RegionScanner,
    ScannerProperties,
};
use store_api::storage::{RegionId, ScanRequest};

use super::*;

#[derive(Debug, Default)]
struct State {
    preflights: usize,
    raw_calls: usize,
    legacy_calls: usize,
    raw_schema: Option<Option<Arc<SeriesSchema>>>,
    stream_schema: Option<Arc<SeriesSchema>>,
    stream_source: Option<RawLabelSource>,
    raw_none: bool,
    raw_error: bool,
    fail_first_poll: bool,
    error_after_poll: Option<usize>,
    pending_at_poll: Option<usize>,
    polls: usize,
    drops: usize,
    resets: usize,
    planned_ordering: Option<SeriesOrdering>,
    stream_ordering: Option<SeriesOrdering>,
}

fn raw_error(reason: &str) -> BoxedError {
    BoxedError::new(
        InvalidSeriesSnafu {
            reason: reason.to_string(),
        }
        .build(),
    )
}

#[derive(Debug)]
struct RawStream {
    schema: Arc<SeriesSchema>,
    source: RawLabelSource,
    chunks: Vec<RawSeriesChunk>,
    state: Arc<Mutex<State>>,
    fail_first_poll: bool,
    error_after_poll: Option<usize>,
    pending_at_poll: Option<usize>,
    ordering: SeriesOrdering,
}

impl Drop for RawStream {
    fn drop(&mut self) {
        self.state.lock().unwrap().drops += 1;
    }
}

impl Stream for RawStream {
    type Item = Result<RawSeriesChunk, BoxedError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let (should_fail, should_pend) = {
            let mut state = self.state.lock().unwrap();
            state.polls += 1;
            let polls = state.polls;
            (
                (self.fail_first_poll && polls == 1) || self.error_after_poll == Some(polls),
                self.pending_at_poll == Some(polls),
            )
        };
        if should_fail {
            return Poll::Ready(Some(Err(raw_error("raw failure"))));
        }
        if should_pend {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        Poll::Ready((!self.chunks.is_empty()).then(|| Ok(self.chunks.remove(0))))
    }
}

impl RawSeriesStream for RawStream {
    fn schema(&self) -> &Arc<SeriesSchema> {
        &self.schema
    }
    fn label_source(&self) -> &RawLabelSource {
        &self.source
    }
    fn ordering(&self) -> SeriesOrdering {
        self.ordering
    }
}

#[derive(Debug)]
struct TestScanner {
    properties: ScannerProperties,
    metadata: store_api::metadata::RegionMetadataRef,
    batches: RecordBatches,
    raw_schema: Option<Arc<SeriesSchema>>,
    chunks: Vec<RawSeriesChunk>,
    state: Arc<Mutex<State>>,
}

impl fmt::Display for TestScanner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("TestScanner")
    }
}
impl DisplayAs for TestScanner {
    fn fmt_as(&self, _format: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}
impl RegionScanner for TestScanner {
    fn name(&self) -> &str {
        "TestScanner"
    }
    fn properties(&self) -> &ScannerProperties {
        &self.properties
    }
    fn schema(&self) -> SchemaRef {
        self.batches.schema()
    }
    fn metadata(&self) -> store_api::metadata::RegionMetadataRef {
        self.metadata.clone()
    }
    fn raw_series_ordering(&self) -> SeriesOrdering {
        self.state
            .lock()
            .unwrap()
            .planned_ordering
            .unwrap_or(SeriesOrdering::KeyThenTime)
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
        self.state.lock().unwrap().legacy_calls += 1;
        Ok(self.batches.as_stream())
    }
    fn raw_series_schema(
        &self,
        _source: &RawLabelSource,
    ) -> Result<Option<Arc<SeriesSchema>>, BoxedError> {
        let mut state = self.state.lock().unwrap();
        state.preflights += 1;
        Ok(state
            .raw_schema
            .clone()
            .unwrap_or_else(|| self.raw_schema.clone()))
    }
    fn scan_series_partition(
        &self,
        _ctx: &QueryScanContext,
        _metrics: &ExecutionPlanMetricsSet,
        _partition: usize,
        source: &RawLabelSource,
    ) -> Result<Option<store_api::region_engine::SendableRawSeriesStream>, BoxedError> {
        let mut state = self.state.lock().unwrap();
        state.raw_calls += 1;
        if state.raw_error {
            return Err(raw_error("raw entrypoint failure"));
        }
        if state.raw_none {
            return Ok(None);
        }
        let ordering = state.stream_ordering.unwrap_or(SeriesOrdering::KeyThenTime);
        Ok(Some(Box::pin(RawStream {
            ordering,
            schema: state
                .stream_schema
                .clone()
                .unwrap_or_else(|| self.raw_schema.clone().unwrap()),
            source: state
                .stream_source
                .clone()
                .unwrap_or_else(|| source.clone()),
            chunks: self.chunks.clone(),
            state: self.state.clone(),
            fail_first_poll: state.fail_first_poll,
            error_after_poll: state.error_after_poll,
            pending_at_poll: state.pending_at_poll,
        })))
    }
    fn has_predicate_without_region(&self) -> bool {
        false
    }
    fn add_dyn_filter_to_predicate(&mut self, filters: Vec<Arc<dyn PhysicalExpr>>) -> Vec<bool> {
        filters.into_iter().map(|_| false).collect()
    }
    fn reset_state(&mut self) {
        self.state.lock().unwrap().resets += 1;
    }
    fn set_logical_region(&mut self, value: bool) {
        self.properties.set_logical_region(value);
    }
    fn set_query_load_region_id(&mut self, id: RegionId) {
        self.properties.set_query_load_region_id(id);
    }
}

fn fixture(
    raw_schema: Option<Arc<SeriesSchema>>,
    chunks: Vec<RawSeriesChunk>,
) -> (Arc<RegionScanExec>, Arc<Mutex<State>>) {
    let schema = Arc::new(Schema::new(vec![
        ColumnSchema::new("value", ConcreteDataType::int32_datatype(), false),
        ColumnSchema::new(
            "ts",
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        ),
    ]));
    let batch = RecordBatch::new(
        schema.clone(),
        vec![
            Arc::new(Int32Vector::from_slice([7])) as VectorRef,
            Arc::new(TimestampMillisecondVector::from_slice([42])) as VectorRef,
        ],
    )
    .unwrap();
    let batches = RecordBatches::try_new(schema, vec![batch]).unwrap();
    let mut metadata = RegionMetadataBuilder::new(RegionId::new(1, 1));
    metadata
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new("value", ConcreteDataType::int32_datatype(), false),
            semantic_type: SemanticType::Field,
            column_id: 1,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            semantic_type: SemanticType::Timestamp,
            column_id: 2,
        })
        .primary_key(vec![]);
    let state = Arc::new(Mutex::new(State::default()));
    let scanner = TestScanner {
        properties: ScannerProperties::default(),
        metadata: Arc::new(metadata.build().unwrap()),
        batches,
        raw_schema,
        chunks,
        state: state.clone(),
    };
    (
        Arc::new(RegionScanExec::new(Box::new(scanner), ScanRequest::default(), None).unwrap()),
        state,
    )
}

fn named_fixture() -> (
    Arc<RegionScanExec>,
    Arc<Mutex<State>>,
    store_api::metadata::RegionMetadataRef,
) {
    let schema = Arc::new(Schema::new(vec![
        ColumnSchema::new("host", ConcreteDataType::string_datatype(), false),
        ColumnSchema::new("aux", ConcreteDataType::float64_datatype(), false),
        ColumnSchema::new("value", ConcreteDataType::float64_datatype(), false),
        ColumnSchema::new(
            "ts",
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        ),
    ]));
    let batch = RecordBatch::new(
        schema.clone(),
        vec![
            Arc::new(datatypes::vectors::StringVector::from_slice(&["node"])) as VectorRef,
            Arc::new(datatypes::vectors::Float64Vector::from_slice([2.0])) as VectorRef,
            Arc::new(datatypes::vectors::Float64Vector::from_slice([1.0])) as VectorRef,
            Arc::new(TimestampMillisecondVector::from_slice([42])) as VectorRef,
        ],
    )
    .unwrap();
    let batches = RecordBatches::try_new(schema, vec![batch]).unwrap();
    let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 2));
    builder
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new("host", ConcreteDataType::string_datatype(), false),
            semantic_type: SemanticType::Tag,
            column_id: 7,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new("aux", ConcreteDataType::float64_datatype(), false),
            semantic_type: SemanticType::Field,
            column_id: 11,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new("value", ConcreteDataType::float64_datatype(), false),
            semantic_type: SemanticType::Field,
            column_id: 12,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            semantic_type: SemanticType::Timestamp,
            column_id: 13,
        })
        .primary_key(vec![7]);
    let metadata = Arc::new(builder.build().unwrap());
    let raw_schema = Arc::new(SeriesSchema {
        fields: vec![
            FieldSpec {
                name: Arc::from("aux"),
            },
            FieldSpec {
                name: Arc::from("value"),
            },
        ]
        .into(),
        eval_grid: None,
    });
    let state = Arc::new(Mutex::new(State::default()));
    let scanner = TestScanner {
        properties: ScannerProperties::default(),
        metadata: metadata.clone(),
        batches,
        raw_schema: Some(raw_schema),
        chunks: vec![],
        state: state.clone(),
    };
    (
        Arc::new(RegionScanExec::new(Box::new(scanner), ScanRequest::default(), None).unwrap()),
        state,
        metadata,
    )
}

fn series_schema() -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: vec![FieldSpec {
            name: Arc::from("cpu"),
        }]
        .into(),
        eval_grid: None,
    })
}
fn chunk(schema: &SeriesSchema) -> RawSeriesChunk {
    RawSeriesChunk::try_new(
        schema,
        Buffer::from(b"complete-key".as_slice()),
        Some(datatypes::series::MetricName::try_new(Some(Arc::from("cpu_usage")), vec![]).unwrap()),
        RawTimestamps::try_new(vec![10, 20].into()).unwrap(),
        SeriesFields::one(Samples::from_float(Float64Array::from(vec![
            Some(1.5),
            None,
        ]))),
        true,
    )
    .unwrap()
}
fn context() -> Arc<TaskContext> {
    datafusion::prelude::SessionContext::new().task_ctx()
}

#[test]
fn named_series_scan_uses_actual_metadata_and_requested_field_order() {
    let (scan, state, metadata) = named_fixture();
    let scan = Arc::new(scan.with_new_partitions(vec![vec![]], 1).unwrap());
    let native = scan
        .try_to_named_series_scan(
            "cpu",
            &["host".to_string()],
            &["aux".to_string(), "value".to_string()],
        )
        .unwrap()
        .unwrap();
    let RawLabelSource::PrimaryKey {
        physical_metadata,
        logical_tags,
        ..
    } = native.label_source()
    else {
        panic!("expected primary-key label source");
    };
    assert!(Arc::ptr_eq(&metadata, physical_metadata));
    assert_eq!(logical_tags.len(), 1);
    assert_eq!(logical_tags[0].column_id, 7);
    assert_ne!(logical_tags[0].column_id, 1);
    assert_eq!(logical_tags[0].name.as_ref(), "host");
    assert_eq!(
        native
            .series_schema
            .fields
            .iter()
            .map(|field| field.name.as_ref())
            .collect::<Vec<_>>(),
        ["aux", "value"]
    );
    let state = state.lock().unwrap();
    assert_eq!(state.preflights, 1);
    assert_eq!(state.raw_calls, 0);
    assert_eq!(state.legacy_calls, 0);
}

#[test]
fn named_series_scan_declines_invalid_names_before_raw_preflight() {
    let (scan, state, _) = named_fixture();
    let scan = Arc::new(scan.with_new_partitions(vec![vec![]], 1).unwrap());
    for (tags, fields) in [
        (vec!["missing"], vec!["value"]),
        (vec!["aux"], vec!["value"]),
        (vec!["ts"], vec!["value"]),
        (vec!["host", "host"], vec!["value"]),
        (vec!["__sequence"], vec!["value"]),
        (vec!["__table_id"], vec!["value"]),
        (vec!["host"], vec!["missing"]),
        (vec!["host"], vec!["ts"]),
        (vec!["host"], vec!["value", "value"]),
    ] {
        let tags = tags.into_iter().map(str::to_string).collect::<Vec<_>>();
        let fields = fields.into_iter().map(str::to_string).collect::<Vec<_>>();
        assert!(
            scan.clone()
                .try_to_named_series_scan("cpu", &tags, &fields)
                .unwrap()
                .is_none()
        );
        assert!(
            scan.clone()
                .try_to_named_series_binding("cpu", &tags, &fields)
                .unwrap()
                .is_none()
        );
    }
    let state = state.lock().unwrap();
    assert_eq!(state.preflights, 0);
    assert_eq!(state.raw_calls, 0);
    assert_eq!(state.legacy_calls, 0);
}

#[test]
fn named_series_binding_reports_the_binding_of_the_actual_metadata() {
    let (scan, state, metadata) = named_fixture();
    state.lock().unwrap().planned_ordering = Some(SeriesOrdering::Contiguous);
    let scan = Arc::new(scan.with_new_partitions(vec![vec![]], 1).unwrap());
    let (binding, native) = scan
        .try_to_named_series_binding(
            "cpu",
            &["host".to_string()],
            &["aux".to_string(), "value".to_string()],
        )
        .unwrap()
        .expect("complete logical names must resolve against the actual metadata");
    assert_eq!(binding.version, 1);
    assert_eq!(binding.metric_name, "cpu");
    assert_eq!(binding.physical_metadata.region_id, metadata.region_id);
    assert_eq!(
        binding.physical_metadata.schema_version,
        metadata.schema_version
    );
    assert_eq!(
        binding
            .physical_metadata
            .column_metadatas
            .iter()
            .map(|column| column.column_id)
            .collect::<Vec<_>>(),
        metadata
            .column_metadatas
            .iter()
            .map(|column| column.column_id)
            .collect::<Vec<_>>()
    );
    assert_eq!(binding.logical_tags.len(), 1);
    assert_eq!(binding.logical_tags[0].name, "host");
    assert_eq!(binding.logical_tags[0].column_id, 7);
    assert_eq!(binding.field_ids, [11, 12]);
    assert_eq!(native.raw_series_ordering(), SeriesOrdering::Contiguous);
    assert_eq!(
        native
            .series_schema
            .fields
            .iter()
            .map(|field| field.name.as_ref())
            .collect::<Vec<_>>(),
        ["aux", "value"]
    );
    let RawLabelSource::PrimaryKey {
        physical_metadata, ..
    } = native.label_source()
    else {
        panic!("expected primary-key label source");
    };
    assert!(Arc::ptr_eq(&metadata, physical_metadata));
    let state = state.lock().unwrap();
    assert_eq!(state.preflights, 1);
    assert_eq!(state.raw_calls, 0);
    assert_eq!(state.legacy_calls, 0);
}

#[test]
fn named_series_binding_never_reorders_or_omits_requested_fields() {
    let (scan, state, _) = named_fixture();
    let scan = Arc::new(scan.with_new_partitions(vec![vec![]], 1).unwrap());
    let (binding, native) = scan
        .clone()
        .try_to_named_series_binding(
            "cpu",
            &["host".to_string()],
            &["aux".to_string(), "value".to_string()],
        )
        .unwrap()
        .expect("the requested fields are the projected fields");
    assert_eq!(binding.field_ids, [11, 12]);
    assert_eq!(
        native
            .series_schema
            .fields
            .iter()
            .map(|field| field.name.as_ref())
            .collect::<Vec<_>>(),
        ["aux", "value"]
    );
    for fields in [
        vec!["value", "aux"],
        vec!["aux"],
        vec!["value"],
        vec!["aux", "value", "value"],
    ] {
        let fields = fields.into_iter().map(str::to_string).collect::<Vec<_>>();
        assert!(
            scan.clone()
                .try_to_named_series_binding("cpu", &["host".to_string()], &fields)
                .unwrap()
                .is_none()
        );
    }
    let state = state.lock().unwrap();
    assert_eq!(state.preflights, 1);
    assert_eq!(state.raw_calls, 0);
    assert_eq!(state.legacy_calls, 0);
}

#[tokio::test]
async fn named_series_scan_raw_capability_declines_and_legacy_scan_is_unchanged() {
    let (scan, state, _) = named_fixture();
    state.lock().unwrap().raw_schema = Some(None);
    let scan = Arc::new(scan.with_new_partitions(vec![vec![]], 1).unwrap());
    assert!(
        scan.clone()
            .try_to_named_series_scan(
                "cpu",
                &["host".to_string()],
                &["aux".to_string(), "value".to_string()]
            )
            .unwrap()
            .is_none()
    );
    assert_eq!(state.lock().unwrap().preflights, 1);
    assert_eq!(state.lock().unwrap().raw_calls, 0);
    let rows = scan
        .execute(0, context())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(rows.iter().map(|batch| batch.num_rows()).sum::<usize>(), 1);
    assert_eq!(state.lock().unwrap().legacy_calls, 1);
}

#[tokio::test]
async fn unsupported_native_scan_preserves_ordinary_data() {
    let (scan, state) = fixture(None, vec![]);
    assert!(
        scan.clone()
            .try_to_series_scan(RawLabelSource::Explicit)
            .unwrap()
            .is_none()
    );
    assert_eq!(state.lock().unwrap().raw_calls, 0);
    let rows = scan
        .execute(0, context())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(rows.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    assert_eq!(
        rows[0]
            .column(0)
            .as_any()
            .downcast_ref::<datatypes::arrow::array::Int32Array>()
            .unwrap()
            .value(0),
        7
    );
    assert_eq!(state.lock().unwrap().legacy_calls, 1);
    assert_eq!(scan.metrics().unwrap().output_rows(), Some(1));
}

#[tokio::test]
async fn native_raw_frame_executes_and_decodes_bound_data() {
    let schema = series_schema();
    let (scan, state) = fixture(Some(schema.clone()), vec![chunk(&schema)]);
    let plan = scan
        .clone()
        .try_to_series_scan(RawLabelSource::Explicit)
        .unwrap()
        .unwrap();
    assert_eq!(state.lock().unwrap().raw_calls, 0);
    let frames = plan
        .execute(0, context())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].schema(), plan.schema());
    let decoded = datatypes::series::frame::unpack_raw_series_frame(
        &schema,
        RawLabelMode::Explicit,
        &frames[0],
    )
    .unwrap();
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].primary_key.as_slice(), b"complete-key");
    assert_eq!(
        decoded[0].metric.as_ref().unwrap().name(),
        Some("cpu_usage")
    );
    assert_eq!(decoded[0].time.timestamp(0), Some(10));
    assert_eq!(decoded[0].time.timestamp(1), Some(20));
    assert!(
        matches!(decoded[0].fields.get(0).unwrap().sample(0), datatypes::series::samples::SampleRef::Float(v) if v.to_bits() == 1.5f64.to_bits())
    );
    assert!(matches!(
        decoded[0].fields.get(0).unwrap().sample(1),
        datatypes::series::samples::SampleRef::Missing
    ));
    assert_eq!(state.lock().unwrap().raw_calls, 1);
    assert_eq!(state.lock().unwrap().legacy_calls, 0);
    assert_eq!(state.lock().unwrap().drops, 1);
    let native_metrics = plan.metrics().unwrap();
    assert_eq!(native_metrics.output_rows(), Some(frames[0].num_rows()));
    assert_eq!(
        scan.metrics().unwrap().output_rows(),
        Some(frames[0].num_rows())
    );
    let native_bytes = native_metrics
        .iter()
        .find_map(|metric| match metric.value() {
            MetricValue::OutputBytes(count) => Some(count.value()),
            _ => None,
        })
        .unwrap();
    assert!(native_bytes > 0);
    assert!(matches!(
        plan.properties().output_partitioning(),
        Partitioning::UnknownPartitioning(1)
    ));
    assert!(plan.properties().output_ordering().is_none());
}

#[tokio::test]
async fn supported_empty_stream_remains_native_with_declared_frame_schema() {
    let schema = series_schema();
    let (scan, state) = fixture(Some(schema), vec![]);
    let plan = scan
        .try_to_series_scan(RawLabelSource::Explicit)
        .unwrap()
        .unwrap();
    let mut stream = plan.execute(0, context()).unwrap();
    assert_eq!(stream.schema(), plan.schema());
    assert!(stream.next().await.is_none());
    assert_eq!(state.lock().unwrap().drops, 1);
    drop(stream);
    assert_eq!(
        plan.schema(),
        raw_series_frame_schema_with_ordering(
            plan.series_schema(),
            RawLabelMode::Explicit,
            SeriesOrdering::KeyThenTime
        )
        .unwrap()
    );
    let state = state.lock().unwrap();
    assert_eq!(state.raw_calls, 1);
    assert_eq!(state.legacy_calls, 0);
}

#[tokio::test]
async fn planned_ordering_is_preserved_for_empty_and_nonempty_frames() {
    for ordering in [SeriesOrdering::KeyThenTime, SeriesOrdering::Contiguous] {
        for nonempty in [false, true] {
            let schema = series_schema();
            let chunks = if nonempty {
                vec![chunk(&schema)]
            } else {
                vec![]
            };
            let (scan, state) = fixture(Some(schema), chunks);
            {
                let mut state = state.lock().unwrap();
                state.planned_ordering = Some(ordering);
                state.stream_ordering = Some(ordering);
            }
            let plan = scan
                .clone()
                .try_to_series_scan(RawLabelSource::Explicit)
                .unwrap()
                .unwrap();
            let planned_schema = plan.schema();
            assert_eq!(
                ordering,
                raw_series_frame_ordering(&planned_schema).unwrap()
            );
            {
                let state = state.lock().unwrap();
                assert_eq!(0, state.raw_calls);
                assert_eq!(0, state.polls);
                assert_eq!(0, state.legacy_calls);
            }
            let stream = plan.execute(0, context()).unwrap();
            assert_eq!(stream.schema(), planned_schema);
            assert_eq!(
                plan.properties().equivalence_properties().schema(),
                &planned_schema
            );
            assert_eq!(
                ordering,
                raw_series_frame_ordering(stream.schema().as_ref()).unwrap()
            );
            let frames = stream.try_collect::<Vec<_>>().await.unwrap();
            assert_eq!(usize::from(nonempty), frames.len());
            for frame in &frames {
                assert_eq!(planned_schema.as_ref(), frame.schema().as_ref());
                assert_eq!(
                    ordering,
                    raw_series_frame_ordering(frame.schema().as_ref()).unwrap()
                );
            }
            let state = state.lock().unwrap();
            assert_eq!(1, state.raw_calls);
            assert_eq!(0, state.legacy_calls);
        }
    }
}

#[test]
fn interleaved_planned_ordering_cannot_start_a_native_or_legacy_read() {
    let schema = series_schema();
    let (scan, state) = fixture(Some(schema.clone()), vec![chunk(&schema)]);
    state.lock().unwrap().planned_ordering = Some(SeriesOrdering::Interleaved);
    let error = scan
        .clone()
        .try_to_series_scan(RawLabelSource::Explicit)
        .expect_err("interleaved planning must not produce a native raw-series plan");
    assert!(error.to_string().contains("ordering"), "{error}");
    // The preflight ran, but no entrypoint was consumed and no chunk was polled.
    let state = state.lock().unwrap();
    assert_eq!(1, state.preflights);
    assert_eq!(0, state.raw_calls);
    assert_eq!(0, state.legacy_calls);
    assert_eq!(0, state.polls);
    assert_eq!(0, state.drops);
}

#[tokio::test]
async fn planned_and_stream_ordering_mismatches_fail_without_legacy_fallback() {
    for (planned, actual) in [
        (SeriesOrdering::KeyThenTime, SeriesOrdering::Contiguous),
        (SeriesOrdering::Contiguous, SeriesOrdering::KeyThenTime),
        // An interleaved stream must not be packed under a stricter planned guarantee.
        (SeriesOrdering::KeyThenTime, SeriesOrdering::Interleaved),
        (SeriesOrdering::Contiguous, SeriesOrdering::Interleaved),
    ] {
        let schema = series_schema();
        let (scan, state) = fixture(Some(schema.clone()), vec![chunk(&schema)]);
        state.lock().unwrap().planned_ordering = Some(planned);
        let plan = scan
            .clone()
            .try_to_series_scan(RawLabelSource::Explicit)
            .unwrap()
            .unwrap();
        assert_eq!(
            planned,
            raw_series_frame_ordering(plan.schema().as_ref()).unwrap()
        );
        state.lock().unwrap().stream_ordering = Some(actual);
        let error = plan
            .execute(0, context())
            .err()
            .expect("ordering mismatch rejected before returning stream");
        assert!(error.to_string().contains("ordering"), "{error}");
        let state = state.lock().unwrap();
        assert_eq!(1, state.raw_calls);
        assert_eq!(0, state.polls);
        assert_eq!(1, state.drops);
        assert_eq!(0, state.legacy_calls);
    }
}

#[tokio::test]
async fn ordering_changed_after_planning_fails_before_raw_execution() {
    let schema = series_schema();
    let (scan, state) = fixture(Some(schema), vec![]);
    let plan = scan
        .clone()
        .try_to_series_scan(RawLabelSource::Explicit)
        .unwrap()
        .unwrap();
    state.lock().unwrap().planned_ordering = Some(SeriesOrdering::Contiguous);
    assert!(plan.execute(0, context()).is_err());
    let state = state.lock().unwrap();
    assert_eq!(0, state.raw_calls);
    assert_eq!(0, state.polls);
    assert_eq!(0, state.drops);
    assert_eq!(0, state.legacy_calls);
}

#[tokio::test]
async fn default_scanner_unsupported_native_preserves_two_column_row() {
    let schema = Arc::new(Schema::new(vec![
        ColumnSchema::new("value", ConcreteDataType::int32_datatype(), false),
        ColumnSchema::new(
            "ts",
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        ),
    ]));
    let batch = RecordBatch::new(
        schema.clone(),
        vec![
            Arc::new(Int32Vector::from_slice([7])) as VectorRef,
            Arc::new(TimestampMillisecondVector::from_slice([42])) as VectorRef,
        ],
    )
    .unwrap();
    let batches = RecordBatches::try_new(schema.clone(), vec![batch]).unwrap();
    let mut metadata = RegionMetadataBuilder::new(RegionId::new(1, 3));
    metadata
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new("value", ConcreteDataType::int32_datatype(), false),
            semantic_type: SemanticType::Field,
            column_id: 1,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            semantic_type: SemanticType::Timestamp,
            column_id: 2,
        })
        .primary_key(vec![]);
    let scanner = Box::new(store_api::region_engine::SinglePartitionScanner::new(
        batches.as_stream(),
        false,
        Arc::new(metadata.build().unwrap()),
        None,
    ));
    let plan = Arc::new(RegionScanExec::new(scanner, ScanRequest::default(), None).unwrap());
    assert!(
        plan.clone()
            .try_to_series_scan(RawLabelSource::Explicit)
            .unwrap()
            .is_none()
    );
    let rows = plan
        .execute(0, context())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(rows.iter().map(|batch| batch.num_rows()).sum::<usize>(), 1);
    assert_eq!(
        rows[0]
            .column(0)
            .as_any()
            .downcast_ref::<datatypes::arrow::array::Int32Array>()
            .unwrap()
            .value(0),
        7
    );
    assert_eq!(
        rows[0]
            .column(1)
            .as_any()
            .downcast_ref::<datatypes::arrow::array::TimestampMillisecondArray>()
            .unwrap()
            .value(0),
        42
    );
}

#[test]
fn invalid_preflight_schema_declines_before_raw_execution() {
    let invalid = Arc::new(SeriesSchema {
        fields: Arc::from([]),
        eval_grid: None,
    });
    let (scan, state) = fixture(Some(invalid), vec![]);
    assert!(scan.try_to_series_scan(RawLabelSource::Explicit).is_err());
    assert_eq!(state.lock().unwrap().raw_calls, 0);
}

#[tokio::test]
async fn runtime_schema_change_fails_before_raw_execution() {
    let schema = series_schema();
    let (scan, state) = fixture(Some(schema), vec![]);
    let plan = scan
        .try_to_series_scan(RawLabelSource::Explicit)
        .unwrap()
        .unwrap();
    state.lock().unwrap().raw_schema = Some(None);
    assert!(plan.execute(0, context()).is_err());
    assert_eq!(state.lock().unwrap().raw_calls, 0);
}

#[tokio::test]
async fn runtime_schema_name_and_grid_changes_fail_before_raw_call() {
    for changed in [
        Arc::new(SeriesSchema {
            fields: vec![FieldSpec {
                name: Arc::from("other"),
            }]
            .into(),
            eval_grid: None,
        }),
        Arc::new(SeriesSchema {
            fields: vec![FieldSpec {
                name: Arc::from("cpu"),
            }]
            .into(),
            eval_grid: Some(Arc::new(
                datatypes::series::time::EvalGrid::try_new(
                    datatypes::series::time::GridSpec::try_new(0, 1, 1).unwrap(),
                )
                .unwrap(),
            )),
        }),
    ] {
        let (scan, state) = fixture(Some(series_schema()), vec![]);
        let plan = scan
            .try_to_series_scan(RawLabelSource::Explicit)
            .unwrap()
            .unwrap();
        state.lock().unwrap().raw_schema = Some(Some(changed));
        assert!(plan.execute(0, context()).is_err());
        assert_eq!(state.lock().unwrap().raw_calls, 0);
    }
}

#[tokio::test]
async fn malformed_returned_stream_binding_fails_before_poll_even_when_empty() {
    let schema = series_schema();
    let (scan, state) = fixture(Some(schema.clone()), vec![]);
    let plan = scan
        .clone()
        .try_to_series_scan(RawLabelSource::Explicit)
        .unwrap()
        .unwrap();
    state.lock().unwrap().stream_source = Some(RawLabelSource::PrimaryKey {
        metric_name: Arc::from("other"),
        physical_metadata: scan.scanner.lock().unwrap().metadata(),
        logical_tags: Arc::from([]),
    });
    assert!(plan.execute(0, context()).is_err());
    let state = state.lock().unwrap();
    assert_eq!(state.raw_calls, 1);
    assert_eq!(state.polls, 0);
    assert_eq!(state.legacy_calls, 0);
}

#[tokio::test]
async fn primary_key_binding_mismatch_is_rejected_before_poll() {
    let logical_tags: Arc<[BoundTag]> = vec![
        BoundTag {
            column_id: 1,
            name: Arc::from("zone"),
        },
        BoundTag {
            column_id: 2,
            name: Arc::from("rack"),
        },
    ]
    .into();
    for case in 0..6 {
        let (scan, state) = fixture(Some(series_schema()), vec![]);
        let metadata = scan.scanner.lock().unwrap().metadata();
        let bound = RawLabelSource::PrimaryKey {
            metric_name: Arc::from("cpu"),
            physical_metadata: metadata.clone(),
            logical_tags: logical_tags.clone(),
        };
        let plan = scan.clone().try_to_series_scan(bound).unwrap().unwrap();
        let mut tags = logical_tags.to_vec();
        let mut metric = Arc::from("cpu");
        let mut physical_metadata = metadata.clone();
        match case {
            0 => metric = Arc::from("other"),
            1 => physical_metadata = Arc::new((*metadata).clone()),
            2 => tags[0].column_id += 10,
            3 => tags[0].name = Arc::from("region"),
            4 => tags.swap(0, 1),
            5 => {
                tags.pop();
            }
            _ => unreachable!(),
        }
        state.lock().unwrap().stream_source = Some(RawLabelSource::PrimaryKey {
            metric_name: metric,
            physical_metadata,
            logical_tags: tags.into(),
        });
        assert!(plan.execute(0, context()).is_err());
        let state = state.lock().unwrap();
        assert_eq!(state.raw_calls, 1);
        assert_eq!(state.polls, 0);
        assert_eq!(state.drops, 1);
        assert_eq!(state.legacy_calls, 0);
    }
}

#[tokio::test]
async fn unchanged_primary_key_binding_accepts_empty_stream() {
    let (scan, state) = fixture(Some(series_schema()), vec![]);
    let metadata = scan.scanner.lock().unwrap().metadata();
    let source = RawLabelSource::PrimaryKey {
        metric_name: Arc::from("cpu"),
        physical_metadata: metadata,
        logical_tags: Arc::from([
            BoundTag {
                column_id: 1,
                name: Arc::from("zone"),
            },
            BoundTag {
                column_id: 2,
                name: Arc::from("rack"),
            },
        ]),
    };
    let plan = scan.try_to_series_scan(source).unwrap().unwrap();
    assert!(
        plan.execute(0, context())
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty()
    );
    let state = state.lock().unwrap();
    assert_eq!(state.raw_calls, 1);
    assert_eq!(state.legacy_calls, 0);
}

#[tokio::test]
async fn returned_wrong_field_schema_is_rejected_before_poll() {
    let (scan, state) = fixture(Some(series_schema()), vec![]);
    let plan = scan
        .try_to_series_scan(RawLabelSource::Explicit)
        .unwrap()
        .unwrap();
    state.lock().unwrap().stream_schema = Some(Arc::new(SeriesSchema {
        fields: vec![FieldSpec {
            name: Arc::from("wrong"),
        }]
        .into(),
        eval_grid: None,
    }));
    assert!(plan.execute(0, context()).is_err());
    let state = state.lock().unwrap();
    assert_eq!(state.polls, 0);
    assert_eq!(state.drops, 1);
    assert_eq!(state.legacy_calls, 0);
}

#[tokio::test]
async fn raw_none_or_error_does_not_replay_ordinary_scan() {
    let schema = series_schema();
    let (scan, state) = fixture(Some(schema.clone()), vec![chunk(&schema)]);
    let plan = scan
        .try_to_series_scan(RawLabelSource::Explicit)
        .unwrap()
        .unwrap();
    state.lock().unwrap().raw_none = true;
    assert!(plan.execute(0, context()).is_err());
    {
        let mut state = state.lock().unwrap();
        state.raw_none = false;
        state.raw_error = true;
    }
    assert!(plan.execute(0, context()).is_err());
    let state = state.lock().unwrap();
    assert_eq!(state.raw_calls, 2);
    assert_eq!(state.legacy_calls, 0);
    assert_eq!(state.drops, 0);
}

#[tokio::test]
async fn error_after_emitted_frame_is_not_replayed() {
    let schema = series_schema();
    let (scan, state) = fixture(Some(schema.clone()), vec![chunk(&schema)]);
    let plan = scan
        .try_to_series_scan(RawLabelSource::Explicit)
        .unwrap()
        .unwrap();
    {
        let mut state = state.lock().unwrap();
        state.pending_at_poll = Some(2);
        state.error_after_poll = Some(3);
    }
    let mut stream = plan.execute(0, context()).unwrap();
    let frame = stream.next().await.unwrap().unwrap();
    assert_eq!(frame.schema(), plan.schema());
    let decoded = datatypes::series::frame::unpack_raw_series_frame(
        plan.series_schema(),
        RawLabelMode::Explicit,
        &frame,
    )
    .unwrap();
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].primary_key.as_slice(), b"complete-key");
    assert_eq!(
        decoded[0].metric.as_ref().unwrap().name(),
        Some("cpu_usage")
    );
    assert_eq!(decoded[0].time.timestamp(0), Some(10));
    assert_eq!(decoded[0].time.timestamp(1), Some(20));
    assert_eq!(decoded[0].fields.get(0).unwrap().len(), 2);
    assert!(
        matches!(decoded[0].fields.get(0).unwrap().sample(0), datatypes::series::samples::SampleRef::Float(v) if v.to_bits() == 1.5f64.to_bits())
    );
    assert!(matches!(
        decoded[0].fields.get(0).unwrap().sample(1),
        datatypes::series::samples::SampleRef::Missing
    ));
    assert!(decoded[0].is_last);
    {
        let state = state.lock().unwrap();
        assert_eq!(state.polls, 2);
        assert_eq!(state.drops, 0);
    }
    let error = stream.next().await.unwrap().unwrap_err();
    assert!(error.to_string().contains("raw failure"), "{error}");
    assert_eq!(
        common_query::error::datafusion_status_code::<common_recordbatch::error::Error>(
            &error, None,
        ),
        common_error::status_code::StatusCode::InvalidArguments
    );
    {
        let state = state.lock().unwrap();
        assert_eq!(state.polls, 3);
        assert_eq!(state.drops, 1);
    }
    assert!(stream.next().await.is_none());
    assert!(stream.next().await.is_none());
    drop(stream);
    let state = state.lock().unwrap();
    assert_eq!(state.raw_calls, 1);
    assert_eq!(state.legacy_calls, 0);
    assert_eq!(state.drops, 1);
    assert_eq!(state.polls, 3);
}

#[tokio::test]
async fn dropping_unpolled_stream_releases_source() {
    let schema = series_schema();
    let (scan, state) = fixture(Some(schema), vec![]);
    let plan = scan
        .try_to_series_scan(RawLabelSource::Explicit)
        .unwrap()
        .unwrap();
    let stream = plan.execute(0, context()).unwrap();
    assert_eq!(state.lock().unwrap().drops, 0);
    drop(stream);
    let state = state.lock().unwrap();
    assert_eq!(state.drops, 1);
    assert_eq!(state.legacy_calls, 0);
}

#[tokio::test]
async fn raw_stream_error_releases_source_without_legacy_replay() {
    let schema = series_schema();
    let (scan, state) = fixture(Some(schema.clone()), vec![chunk(&schema)]);
    let plan = scan
        .try_to_series_scan(RawLabelSource::Explicit)
        .unwrap()
        .unwrap();
    state.lock().unwrap().fail_first_poll = true;
    let mut stream = plan.execute(0, context()).unwrap();
    assert!(stream.next().await.unwrap().is_err());
    assert_eq!(state.lock().unwrap().drops, 1);
    assert!(stream.next().await.is_none());
    drop(stream);
    let state = state.lock().unwrap();
    assert_eq!(state.raw_calls, 1);
    assert_eq!(state.legacy_calls, 0);
    assert_eq!(state.drops, 1);
}

#[tokio::test]
async fn reset_state_clears_outer_filter_and_allows_fresh_execution() {
    let schema = series_schema();
    let (scan, state) = fixture(Some(schema.clone()), vec![chunk(&schema)]);
    let plan = scan
        .clone()
        .try_to_series_scan(RawLabelSource::Explicit)
        .unwrap()
        .unwrap();
    scan.pushed_dyn_filters.lock().unwrap().push(Arc::new(
        datafusion::physical_expr::expressions::Column::new("value", 0),
    ));
    assert!(
        scan.try_to_series_scan(RawLabelSource::Explicit)
            .unwrap()
            .is_none()
    );
    assert!(plan.execute(0, context()).is_err());
    let reset = Arc::clone(&plan).reset_state().unwrap();
    assert_eq!(state.lock().unwrap().resets, 1);
    let frames = reset
        .execute(0, context())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(frames.len(), 1);
    assert_eq!(state.lock().unwrap().raw_calls, 1);
}

#[tokio::test]
async fn invalid_partition_and_nonempty_children_are_rejected() {
    let (scan, state) = fixture(Some(series_schema()), vec![]);
    let prepared = Arc::new(scan.with_new_partitions(vec![vec![], vec![]], 2).unwrap());
    let plan = prepared
        .clone()
        .try_to_series_scan(RawLabelSource::Explicit)
        .unwrap()
        .unwrap();
    assert!(matches!(
        plan.properties().output_partitioning(),
        Partitioning::UnknownPartitioning(2)
    ));
    assert!(
        plan.execute(1, context())
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty()
    );
    let changed = Arc::new(
        prepared
            .with_new_partitions(vec![vec![], vec![], vec![]], 3)
            .unwrap(),
    );
    let changed_plan = changed
        .clone()
        .try_to_series_scan(RawLabelSource::Explicit)
        .unwrap()
        .unwrap();
    assert!(matches!(
        changed_plan.properties().output_partitioning(),
        Partitioning::UnknownPartitioning(3)
    ));
    assert!(plan.execute(0, context()).is_err());
    assert!(plan.execute(2, context()).is_err());
    let child: Arc<dyn ExecutionPlan> = scan;
    assert!(
        plan.clone()
            .replace_children(
                vec![child],
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )
            .is_err()
    );
    assert_eq!(state.lock().unwrap().raw_calls, 1);
}

#[tokio::test]
async fn tracked_native_frame_holds_and_releases_quota_on_cancel() {
    let schema = series_schema();
    let (scan, state) = fixture(Some(schema), vec![chunk(&series_schema())]);
    let policy = serde_json::from_str("\"fail\"").unwrap();
    let tracker = QueryMemoryTracker::builder(1024 * 1024, policy).build();
    let tracker_for_assertion = tracker.clone();
    let mut scan = scan;
    Arc::get_mut(&mut scan).unwrap().query_memory_tracker = Some(tracker);
    let plan = scan
        .try_to_series_scan(RawLabelSource::Explicit)
        .unwrap()
        .unwrap();
    let mut stream = plan.execute(0, context()).unwrap();
    let frame = stream.next().await.unwrap().unwrap();
    assert_eq!(frame.schema(), plan.schema());
    assert_eq!(
        datatypes::series::frame::unpack_raw_series_frame(
            plan.series_schema(),
            RawLabelMode::Explicit,
            &frame,
        )
        .unwrap()[0]
            .fields
            .get(0)
            .unwrap()
            .len(),
        2
    );
    assert!(tracker_for_assertion.current() > 0);
    drop(stream);
    assert_eq!(tracker_for_assertion.current(), 0);
    assert_eq!(state.lock().unwrap().drops, 1);
}

#[tokio::test]
async fn memory_quota_failure_does_not_replay_raw_scan() {
    let schema = series_schema();
    let mut oversized = chunk(&schema);
    oversized.primary_key = Buffer::from(vec![0; 2048]);
    let (scan, state) = fixture(Some(schema.clone()), vec![oversized]);
    let policy = serde_json::from_str("\"fail\"").unwrap();
    let tracker = QueryMemoryTracker::builder(1, policy).build();
    let tracker_for_assertion = tracker.clone();
    let mut scan = scan;
    Arc::get_mut(&mut scan).unwrap().query_memory_tracker = Some(tracker);
    let plan = scan
        .try_to_series_scan(RawLabelSource::Explicit)
        .unwrap()
        .unwrap();
    let mut stream = plan.execute(0, context()).unwrap();
    assert!(stream.next().await.unwrap().is_err());
    drop(stream);
    assert_eq!(state.lock().unwrap().legacy_calls, 0);
    assert_eq!(state.lock().unwrap().drops, 1);
    assert_eq!(tracker_for_assertion.current(), 0);
}

#[test]
fn native_series_scan_forwards_query_load_region_id() {
    let schema = series_schema();
    let (scan, _) = fixture(Some(schema), vec![]);
    let region_id = RegionId::new(1, 99);
    scan.scanner
        .lock()
        .unwrap()
        .set_query_load_region_id(region_id);
    let plan = scan
        .try_to_series_scan(RawLabelSource::Explicit)
        .unwrap()
        .unwrap();

    assert_eq!(plan.query_load_region_id(), Some(region_id.as_u64()));
    assert_eq!(
        plan.name(),
        common_recordbatch::adapter::SERIES_SCAN_EXEC_NAME
    );
}

#[test]
fn bound_series_scan_uses_open_scanner_metadata_without_execution() {
    let schema = Arc::new(SeriesSchema {
        fields: vec![FieldSpec {
            name: Arc::from("value"),
        }]
        .into(),
        eval_grid: None,
    });
    let (scan, state) = fixture(Some(schema), vec![]);
    let actual = scan.scanner.lock().unwrap().metadata();
    let request = RawSeriesRequest {
        version: 1,
        physical_metadata: (*actual).clone(),
        metric_name: "cpu".to_string(),
        logical_tags: vec![],
        field_ids: vec![1],
    };
    let native = scan
        .clone()
        .try_to_bound_series_scan(&request)
        .unwrap()
        .unwrap();
    assert_eq!(native.series_schema.fields[0].name.as_ref(), "value");
    let RawLabelSource::PrimaryKey {
        physical_metadata, ..
    } = native.label_source()
    else {
        panic!("expected key labels")
    };
    assert!(Arc::ptr_eq(&actual, physical_metadata));
    assert_eq!(state.lock().unwrap().preflights, 1);
    let mut wrong_projection = request.clone();
    wrong_projection.field_ids = vec![2];
    assert!(
        scan.clone()
            .try_to_bound_series_scan(&wrong_projection)
            .unwrap()
            .is_none()
    );
    let mut wrong_metadata = request.clone();
    wrong_metadata.physical_metadata.schema_version += 1;
    assert!(
        scan.clone()
            .try_to_bound_series_scan(&wrong_metadata)
            .is_err()
    );
    assert_eq!(state.lock().unwrap().preflights, 1);
    assert_eq!(state.lock().unwrap().raw_calls, 0);
    assert_eq!(state.lock().unwrap().legacy_calls, 0);
}

#[tokio::test]
async fn raw_series_admission_captures_scanner_ordering_before_execution() {
    for ordering in [SeriesOrdering::KeyThenTime, SeriesOrdering::Contiguous] {
        let schema = series_schema();
        let chunk = chunk(&schema);
        let (scan, state) = fixture(Some(schema), vec![chunk]);
        {
            let mut state = state.lock().unwrap();
            state.planned_ordering = Some(ordering);
            state.stream_ordering = Some(ordering);
        }

        let plan = scan
            .try_to_series_scan(RawLabelSource::Explicit)
            .unwrap()
            .unwrap();
        assert_eq!(plan.raw_series_ordering(), ordering);
        assert_eq!(
            raw_series_frame_ordering(plan.schema().as_ref()).unwrap(),
            ordering
        );
        {
            let state = state.lock().unwrap();
            assert_eq!(state.preflights, 1);
            assert_eq!(state.raw_calls, 0);
            assert_eq!(state.legacy_calls, 0);
            assert_eq!(state.polls, 0);
            assert_eq!(state.drops, 0);
        }

        state.lock().unwrap().planned_ordering = Some(match ordering {
            SeriesOrdering::KeyThenTime => SeriesOrdering::Contiguous,
            SeriesOrdering::Contiguous => SeriesOrdering::KeyThenTime,
            // `Interleaved` is rejected before consumption, so this two-mode loop never supplies it.
            SeriesOrdering::Interleaved => {
                unreachable!("interleaved ordering is not an admitted raw series plan")
            }
        });
        assert_eq!(plan.raw_series_ordering(), ordering);
        assert!(plan.execute(0, context()).is_err());
        let state = state.lock().unwrap();
        assert_eq!(state.preflights, 2);
        assert_eq!(state.raw_calls, 0);
        assert_eq!(state.legacy_calls, 0);
        assert_eq!(state.polls, 0);
        assert_eq!(state.drops, 0);
    }
}
