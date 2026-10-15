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
use datatypes::series::raw::{RawSeriesChunk, RawSeriesOrdering, RawTimeAxis};
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
    polls: usize,
    drops: usize,
    resets: usize,
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
}

impl Drop for RawStream {
    fn drop(&mut self) {
        self.state.lock().unwrap().drops += 1;
    }
}

impl Stream for RawStream {
    type Item = Result<RawSeriesChunk, BoxedError>;
    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let should_fail = {
            let mut state = self.state.lock().unwrap();
            state.polls += 1;
            (self.fail_first_poll && state.polls == 1) || self.error_after_poll == Some(state.polls)
        };
        if should_fail {
            return Poll::Ready(Some(Err(raw_error("raw failure"))));
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
    fn ordering(&self) -> RawSeriesOrdering {
        RawSeriesOrdering::ReadKeyThenTime
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
        Ok(Some(Box::pin(RawStream {
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
        RawTimeAxis::try_new(vec![10, 20].into()).unwrap(),
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
    assert_eq!(decoded[0].read_key.as_slice(), b"complete-key");
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
    assert!(stream.next().await.is_none());
    assert_eq!(state.lock().unwrap().drops, 1);
    drop(stream);
    assert_eq!(
        plan.schema(),
        datatypes::series::frame::raw_series_frame_schema(
            plan.series_schema(),
            RawLabelMode::Explicit
        )
        .unwrap()
    );
    let state = state.lock().unwrap();
    assert_eq!(state.raw_calls, 1);
    assert_eq!(state.legacy_calls, 0);
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
    state.lock().unwrap().error_after_poll = Some(2);
    let mut stream = plan.execute(0, context()).unwrap();
    assert!(stream.next().await.unwrap().is_ok());
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
    oversized.read_key = Buffer::from(vec![0; 2048]);
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
