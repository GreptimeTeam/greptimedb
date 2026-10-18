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

//! End-to-end checks of [`FusedSeriesExec`] against the same kernels driven directly.
//!
//! The reference path pushes the same chunks through a fresh [`RangeEvaluator`], applies the same
//! `map_math` chain and converts with the same private conversion, so a test fails when the
//! node's plumbing (finalization, ordering, labels, trimming) diverges, not when a kernel's
//! numerics change. Kernel numerics have their own tests.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll};

use common_query::prometheus::PROMETHEUS_STALE_NAN_BITS;
use datafusion::arrow::array::{Array, Float64Array, StringArray, TimestampMillisecondArray};
use datafusion::arrow::buffer::{Buffer, ScalarBuffer};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::test::TestMemoryExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream, displayable,
};
use datatypes::series::frame::{
    RawLabelMode, RawSeriesChunkCursor, RawSeriesFrameBuilder, raw_series_frame_schema,
};
use datatypes::series::raw::{RawSeriesChunk, RawTimestamps};
use datatypes::series::samples::{FieldSpec, Samples, Series, SeriesFields, SeriesSchema};
use datatypes::series::time::{EvalGrid, GridSpec, TimeAxis};
use datatypes::series::{Label, MetricName, SeriesOrdering};
use futures::{Stream, StreamExt};
use store_api::region_engine::RawLabelSource;

use super::{
    FusedSeriesExec, MAX_CHUNKS_PER_POLL, apply_maps, classify_output, occupied_runs,
    series_to_record_batch,
};
use crate::map::MathOp;
use crate::range_eval::{RangeEvaluator, RangeFunction, RangeWindow};

const TIME: &str = "time";
const VALUE: &str = "value";
const VALUE2: &str = "value2";
const GRID_START: i64 = 300_000;
const GRID_STEP: i64 = 60_000;
const GRID_LEN: u64 = 5;

/// Source timestamp and `value` of one raw row; `None` is a missing value.
type Row = (i64, Option<f64>);

fn series_schema() -> Arc<SeriesSchema> {
    fields_schema(&[VALUE])
}

/// A raw series schema with one `Float64` field per name.
fn fields_schema(names: &[&str]) -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: names
            .iter()
            .map(|name| FieldSpec {
                name: Arc::from(*name),
            })
            .collect(),
        eval_grid: None,
    })
}

/// The two-field schema of the multi-field case.
fn two_field_schema() -> Arc<SeriesSchema> {
    fields_schema(&[VALUE, VALUE2])
}

/// One `Float64` value column plus one nullable `Utf8` column per tag and a non-null timestamp.
fn output_schema(tags: &[&str]) -> SchemaRef {
    output_schema_of(&[VALUE], tags)
}

/// One `Float64` column per field name, one nullable `Utf8` column per tag, then the timestamp.
fn output_schema_of(fields: &[&str], tags: &[&str]) -> SchemaRef {
    let mut columns = fields
        .iter()
        .map(|name| Field::new(*name, DataType::Float64, true))
        .collect::<Vec<_>>();
    columns.extend(
        tags.iter()
            .map(|tag| Field::new(*tag, DataType::Utf8, true)),
    );
    columns.push(Field::new(
        TIME,
        DataType::Timestamp(TimeUnit::Millisecond, None),
        false,
    ));
    Arc::new(Schema::new(columns))
}

fn metric(host: &str) -> MetricName {
    named_metric(host, "http_requests_total")
}

fn named_metric(host: &str, name: &str) -> MetricName {
    MetricName::try_new(
        Some(Arc::from(name)),
        vec![Label {
            name: Arc::from("host"),
            value: Arc::from(host),
        }],
    )
    .unwrap()
}

fn chunk(key: u8, host: &str, rows: &[Row], is_last: bool) -> RawSeriesChunk {
    chunk_with(key, metric(host), rows, is_last)
}

/// [`chunk`] with an explicit metric name: `rate` drops `__name__`, so two differently named raw
/// series can still reach the same final label set.
fn named_chunk(name: &str, key: u8, host: &str, rows: &[Row], is_last: bool) -> RawSeriesChunk {
    chunk_with(key, named_metric(host, name), rows, is_last)
}

fn chunk_with(key: u8, metric: MetricName, rows: &[Row], is_last: bool) -> RawSeriesChunk {
    let timestamps = rows
        .iter()
        .map(|(timestamp, _)| *timestamp)
        .collect::<Vec<_>>();
    let values = rows.iter().map(|(_, value)| *value).collect::<Vec<_>>();
    raw_chunk(
        &series_schema(),
        key,
        metric,
        &timestamps,
        vec![Samples::from_float(Float64Array::from(values))],
        is_last,
    )
}

/// One two-field raw chunk, from `(timestamp, value, value2)` rows.
fn two_field_chunk(
    key: u8,
    rows: &[(i64, Option<f64>, Option<f64>)],
    is_last: bool,
) -> RawSeriesChunk {
    let timestamps = rows
        .iter()
        .map(|(timestamp, _, _)| *timestamp)
        .collect::<Vec<_>>();
    let values = rows.iter().map(|(_, value, _)| *value).collect::<Vec<_>>();
    let others = rows.iter().map(|(_, _, value)| *value).collect::<Vec<_>>();
    raw_chunk(
        &two_field_schema(),
        key,
        metric("a"),
        &timestamps,
        vec![
            Samples::from_float(Float64Array::from(values)),
            Samples::from_float(Float64Array::from(others)),
        ],
        is_last,
    )
}

/// Builds one raw chunk from explicit per-field samples.
fn raw_chunk(
    schema: &SeriesSchema,
    key: u8,
    metric: MetricName,
    timestamps: &[i64],
    fields: Vec<Samples>,
    is_last: bool,
) -> RawSeriesChunk {
    RawSeriesChunk::try_new(
        schema,
        Buffer::from(vec![key]),
        Some(metric),
        RawTimestamps::try_new(ScalarBuffer::from(timestamps.to_vec())).unwrap(),
        SeriesFields::try_from_vec(fields).unwrap(),
        is_last,
    )
    .unwrap()
}

fn frame_schema() -> SchemaRef {
    frame_schema_of(&series_schema())
}

fn frame_schema_of(schema: &SeriesSchema) -> SchemaRef {
    raw_series_frame_schema(schema, RawLabelMode::Explicit).unwrap()
}

/// Packs `chunks` into raw frame batches, buffering rows across chunks like a real producer.
fn frame_batches(chunks: &[RawSeriesChunk]) -> Vec<RecordBatch> {
    frame_batches_of(&series_schema(), chunks)
}

fn frame_batches_of(schema: &SeriesSchema, chunks: &[RawSeriesChunk]) -> Vec<RecordBatch> {
    let mut builder = RawSeriesFrameBuilder::try_new(
        schema,
        RawLabelMode::Explicit,
        RawSeriesFrameBuilder::DEFAULT_MAX_ROWS,
        RawSeriesFrameBuilder::DEFAULT_MAX_BYTES,
    )
    .unwrap();
    let mut batches = Vec::new();
    for chunk in chunks {
        let mut cursor =
            RawSeriesChunkCursor::try_new(schema, RawLabelMode::Explicit, chunk.clone()).unwrap();
        while let Some(batch) = cursor.append_to(&mut builder).unwrap() {
            batches.push(batch);
        }
    }
    if let Some(batch) = builder.flush().unwrap() {
        batches.push(batch);
    }
    batches
}

fn memory_child(chunks: &[RawSeriesChunk]) -> Arc<dyn ExecutionPlan> {
    memory_child_of(&series_schema(), chunks)
}

fn memory_child_of(schema: &SeriesSchema, chunks: &[RawSeriesChunk]) -> Arc<dyn ExecutionPlan> {
    Arc::new(
        TestMemoryExec::try_new(
            &[frame_batches_of(schema, chunks)],
            frame_schema_of(schema),
            None,
        )
        .unwrap(),
    )
}

/// A child whose chunks are split across one partition per slice.
fn partitioned_child(partitions: &[&[RawSeriesChunk]]) -> Arc<dyn ExecutionPlan> {
    let batches: Vec<Vec<_>> = partitions
        .iter()
        .map(|chunks| frame_batches_of(&series_schema(), chunks))
        .collect();
    Arc::new(TestMemoryExec::try_new(&batches, frame_schema_of(&series_schema()), None).unwrap())
}

fn grid() -> Arc<EvalGrid> {
    Arc::new(
        EvalGrid::try_new(GridSpec::try_new(GRID_START, GRID_STEP, GRID_LEN).unwrap()).unwrap(),
    )
}

fn stale() -> f64 {
    f64::from_bits(PROMETHEUS_STALE_NAN_BITS)
}

/// Nine once-per-minute counter samples from 60s to 540s, rising `step` per sample, so that every
/// grid point from 300s on has a full five-sample window.
fn counter_rows(step: f64) -> Vec<Row> {
    (1..=9)
        .map(|minute| (minute * 60_000, Some(minute as f64 * step)))
        .collect()
}

/// Splits `rows` into chunks at the given row boundaries; the last chunk is the final one.
fn counter_chunks(key: u8, host: &str, rows: &[Row], bounds: &[usize]) -> Vec<RawSeriesChunk> {
    let mut chunks = Vec::new();
    let mut start = 0;
    for &end in bounds {
        chunks.push(chunk(key, host, &rows[start..end], end == rows.len()));
        start = end;
    }
    if start < rows.len() {
        chunks.push(chunk(key, host, &rows[start..], true));
    }
    chunks
}

/// One fused node configuration plus the reference path over the same configuration.
#[derive(Clone)]
struct Case {
    schema: Arc<SeriesSchema>,
    grid: Arc<EvalGrid>,
    window: RangeWindow,
    function: RangeFunction,
    maps: Vec<MathOp>,
    output_schema: SchemaRef,
    tags: Vec<String>,
}

impl Case {
    fn new(tags: &[&str], window: RangeWindow, function: RangeFunction, maps: Vec<MathOp>) -> Self {
        Self {
            schema: series_schema(),
            grid: grid(),
            window,
            function,
            maps,
            output_schema: output_schema(tags),
            tags: tags.iter().map(|tag| tag.to_string()).collect(),
        }
    }

    fn with_series_schema(&self, schema: Arc<SeriesSchema>) -> Self {
        Self {
            schema,
            ..self.clone()
        }
    }

    fn with_output_schema(&self, output_schema: SchemaRef) -> Self {
        Self {
            output_schema,
            ..self.clone()
        }
    }

    fn fused(&self, child: Arc<dyn ExecutionPlan>) -> Result<FusedSeriesExec> {
        FusedSeriesExec::try_new(
            child,
            self.schema.clone(),
            RawLabelSource::Explicit,
            SeriesOrdering::Contiguous,
            self.grid.clone(),
            self.window,
            self.function,
            self.maps.clone(),
            self.output_schema.clone(),
            self.tags.clone(),
            TIME.to_string(),
        )
    }

    /// Evaluation, then the same map chain, then the same conversion, without the node.
    fn reference(&self, chunks: &[RawSeriesChunk]) -> Vec<(Series, RecordBatch)> {
        let schema = self.schema.clone();
        let mut evaluator =
            RangeEvaluator::try_new(&schema, self.grid.clone(), self.window, self.function)
                .unwrap();
        let classification =
            classify_output(&self.output_schema, &schema, &self.tags, TIME).unwrap();
        let mut out = Vec::new();
        for chunk in chunks {
            let Some(series) = evaluator.push(chunk).unwrap() else {
                continue;
            };
            let series = apply_maps(&evaluator, &self.maps, series).unwrap();
            let batch =
                series_to_record_batch(&classification, &self.tags, &self.output_schema, &series)
                    .unwrap();
            out.push((series, batch));
        }
        out
    }
}

async fn run(exec: FusedSeriesExec) -> Vec<Result<RecordBatch>> {
    let mut stream = exec.execute(0, Arc::new(TaskContext::default())).unwrap();
    let mut items = Vec::new();
    while let Some(item) = stream.next().await {
        items.push(item);
    }
    items
}

fn collect(items: Vec<Result<RecordBatch>>) -> Vec<RecordBatch> {
    items
        .into_iter()
        .map(|item| item.expect("the fused stream must not fail"))
        .collect()
}

fn float_column(batch: &RecordBatch, name: &str) -> Vec<Option<f64>> {
    let array = batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("missing column {name}"))
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    (0..array.len())
        .map(|row| array.is_valid(row).then(|| array.value(row)))
        .collect()
}

fn timestamp_column(batch: &RecordBatch, name: &str) -> Vec<i64> {
    let array = batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("missing column {name}"))
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();
    assert!(array.null_count() == 0, "timestamp column has nulls");
    array.values().to_vec()
}

fn string_column(batch: &RecordBatch, name: &str) -> Vec<Option<String>> {
    let array = batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("missing column {name}"))
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    (0..array.len())
        .map(|row| array.is_valid(row).then(|| array.value(row).to_string()))
        .collect()
}

fn grid_range(series: &Series) -> (usize, usize) {
    let TimeAxis::Grid(range) = &series.time else {
        panic!("expected a grid series, got {:?}", series.time);
    };
    (range.start(), range.len())
}

/// A single dense, null-free float field covers every row of the range, so the fast path must
/// report exactly one run spanning it, in absolute grid indices.
#[test]
fn a_single_dense_float_field_occupies_the_whole_range() {
    let chunks = vec![chunk(
        1,
        "a",
        &[
            (300_000, Some(1.0)),
            (360_000, Some(2.0)),
            (420_000, Some(3.0)),
        ],
        true,
    )];
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 60_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::SumOverTime,
        vec![],
    );
    let reference = case.reference(&chunks);
    let (series, _) = &reference[0];
    let (start, len) = grid_range(series);
    assert_eq!(len, 3, "the fixture must cover three grid points");
    assert_eq!(
        occupied_runs(series).unwrap(),
        vec![(start, start + len)],
        "one dense float field occupies the whole range"
    );
}

/// A field with a missing sample does not cover the range, so the scan reports the runs it finds
/// rather than the whole range.
#[test]
fn a_field_with_a_missing_sample_occupies_only_its_runs() {
    let chunks = vec![chunk(
        1,
        "a",
        &[(300_000, Some(1.0)), (360_000, None), (420_000, Some(3.0))],
        true,
    )];
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 60_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::SumOverTime,
        vec![],
    );
    let reference = case.reference(&chunks);
    let (series, _) = &reference[0];
    let (start, len) = grid_range(series);
    assert_eq!(len, 3);
    assert_eq!(
        occupied_runs(series).unwrap(),
        vec![(start, start + 1), (start + 2, start + 3)],
        "the missing point splits the range into two runs"
    );
}

/// Asserts the node emitted exactly the reference batches, comparing values bit for bit.
fn assert_matches_reference(
    actual: &[RecordBatch],
    reference: &[(Series, RecordBatch)],
    context: &str,
) {
    assert_eq!(
        actual.len(),
        reference.len(),
        "{context}: batch count, actual {actual:?}"
    );
    for (index, (actual, (_, expected))) in actual.iter().zip(reference).enumerate() {
        let context = format!("{context} batch {index}");
        assert_eq!(actual.schema(), expected.schema(), "{context}: schema");
        assert_eq!(
            actual.num_rows(),
            expected.num_rows(),
            "{context}: row count"
        );
        for (actual, expected) in actual.columns().iter().zip(expected.columns()) {
            match expected.data_type() {
                DataType::Float64 => {
                    let actual = actual.as_any().downcast_ref::<Float64Array>().unwrap();
                    let expected = expected.as_any().downcast_ref::<Float64Array>().unwrap();
                    let bits = |array: &Float64Array| {
                        (0..array.len())
                            .map(|row| array.is_valid(row).then(|| array.value(row).to_bits()))
                            .collect::<Vec<_>>()
                    };
                    assert_eq!(bits(actual), bits(expected), "{context}: value column");
                }
                DataType::Utf8 => {
                    let actual = actual.as_any().downcast_ref::<StringArray>().unwrap();
                    let expected = expected.as_any().downcast_ref::<StringArray>().unwrap();
                    let values = |array: &StringArray| {
                        (0..array.len())
                            .map(|row| array.is_valid(row).then(|| array.value(row).to_string()))
                            .collect::<Vec<_>>()
                    };
                    assert_eq!(values(actual), values(expected), "{context}: string column");
                }
                DataType::Timestamp(TimeUnit::Millisecond, None) => {
                    let actual = actual
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap();
                    let expected = expected
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap();
                    assert_eq!(
                        actual.values().to_vec(),
                        expected.values().to_vec(),
                        "{context}: timestamp column"
                    );
                }
                other => panic!("{context}: unexpected column type {other:?}"),
            }
        }
    }
}

/// Concatenates every message in an error's source chain.
fn error_chain(error: &DataFusionError) -> String {
    let mut message = error.to_string();
    let mut source = std::error::Error::source(error);
    while let Some(error) = source {
        message.push_str(": ");
        message.push_str(&error.to_string());
        source = error.source();
    }
    message
}

#[tokio::test]
async fn rate_over_three_chunks_matches_reference_and_prometheus() {
    let chunks = vec![
        chunk(
            1,
            "a",
            &[
                (60_000, Some(60.0)),
                (120_000, Some(120.0)),
                (180_000, Some(180.0)),
            ],
            false,
        ),
        chunk(
            1,
            "a",
            &[
                (240_000, Some(240.0)),
                (300_000, Some(300.0)),
                (360_000, Some(360.0)),
            ],
            false,
        ),
        chunk(
            1,
            "a",
            &[
                (420_000, Some(420.0)),
                (480_000, Some(480.0)),
                (540_000, Some(540.0)),
            ],
            true,
        ),
    ];
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 300_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::Rate,
        vec![],
    );
    let reference = case.reference(&chunks);
    let batches = collect(run(case.fused(memory_child(&chunks)).unwrap()).await);
    assert_matches_reference(&batches, &reference, "rate");

    // A counter rising 60 per minute sampled once a minute extrapolates over a 5m window to
    // exactly one increment per second at every point.
    assert_eq!(
        float_column(&batches[0], VALUE),
        vec![Some(1.0); GRID_LEN as usize]
    );
    assert_eq!(
        timestamp_column(&batches[0], TIME),
        vec![300_000, 360_000, 420_000, 480_000, 540_000]
    );
    let (series, _) = &reference[0];
    assert_eq!(grid_range(series), (0, 5));
    assert_eq!(series.metric.get("host"), Some("a"));
    // `rate` drops the metric name on its own, so the reference keeps only the ordinary label.
    assert_eq!(series.metric.get("__name__"), None);
}

#[tokio::test]
async fn abs_of_rate_drops_the_metric_name_column() {
    let chunks = counter_chunks(1, "a", &counter_rows(60.0), &[5]);
    let case = Case::new(
        &["__name__", "host"],
        RangeWindow {
            range_ms: 300_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::Rate,
        vec![MathOp::Abs],
    );
    let reference = case.reference(&chunks);
    let batches = collect(run(case.fused(memory_child(&chunks)).unwrap()).await);
    assert_matches_reference(&batches, &reference, "abs(rate)");

    let batch = &batches[0];
    assert_eq!(
        string_column(batch, "__name__"),
        vec![None; batch.num_rows()],
        "the metric name is dropped, so a `__name__` column is null"
    );
    assert_eq!(
        string_column(batch, "host"),
        vec![Some("a".to_string()); batch.num_rows()]
    );
    assert_eq!(
        float_column(batch, VALUE),
        vec![Some(1.0); batch.num_rows()],
        "every five-minute window holds a full five-sample rate of one per second"
    );

    let mut expected = metric("a");
    expected.remove("__name__");
    let (series, _) = &reference[0];
    assert_eq!(series.metric, expected);
    assert!(
        series
            .metric
            .tags()
            .iter()
            .all(|tag| tag.name.as_ref() != "__name__")
    );
}

#[tokio::test]
async fn sqrt_of_abs_of_rate_matches_reference() {
    let chunks = counter_chunks(1, "a", &counter_rows(60.0), &[5]);
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 300_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::Rate,
        vec![MathOp::Abs, MathOp::Sqrt],
    );
    let reference = case.reference(&chunks);
    let batches = collect(run(case.fused(memory_child(&chunks)).unwrap()).await);
    assert_matches_reference(&batches, &reference, "sqrt(abs(rate))");
    assert_eq!(
        float_column(&batches[0], VALUE),
        vec![Some(1.0); batches[0].num_rows()]
    );
}

#[tokio::test]
async fn two_series_in_one_frame_stay_separate() {
    let mut chunks = counter_chunks(1, "a", &counter_rows(60.0), &[5]);
    chunks.extend(counter_chunks(2, "b", &counter_rows(120.0), &[5]));
    // Both series fit in one frame batch, so the node splits them only by `is_last`.
    assert_eq!(frame_batches(&chunks).len(), 1);
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 300_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::Rate,
        vec![],
    );
    let reference = case.reference(&chunks);
    let batches = collect(run(case.fused(memory_child(&chunks)).unwrap()).await);
    assert_matches_reference(&batches, &reference, "two series");

    assert_eq!(batches.len(), 2);
    assert_eq!(
        string_column(&batches[0], "host"),
        vec![Some("a".to_string()); 5]
    );
    assert_eq!(
        float_column(&batches[0], VALUE),
        vec![Some(1.0); 5],
        "series a must not see series b's counter"
    );
    assert_eq!(
        string_column(&batches[1], "host"),
        vec![Some("b".to_string()); 5]
    );
    assert_eq!(
        float_column(&batches[1], VALUE),
        vec![Some(2.0); 5],
        "series b must not see series a's counter"
    );
}

#[tokio::test]
async fn duplicate_final_label_set_at_one_grid_point_is_an_error() {
    // `rate` drops `__name__`, so both raw series reach `{host="x"}` and both cover every grid
    // point: Prometheus rejects that at a shared evaluation timestamp.
    let rows = counter_rows(60.0);
    let chunks = vec![
        named_chunk("a_total", 1, "x", &rows, true),
        named_chunk("b_total", 2, "x", &rows, true),
    ];
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 300_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::Rate,
        vec![],
    );
    let items = run(case.fused(memory_child(&chunks)).unwrap()).await;
    assert_eq!(
        items.len(),
        2,
        "the first series is emitted, then the collision surfaces"
    );
    assert!(items[0].is_ok(), "{:?}", items[0]);
    let error = items.into_iter().nth(1).unwrap().err().unwrap();
    let message = error_chain(&error);
    assert!(
        message.contains("vector cannot contain metrics with the same labelset"),
        "{message}"
    );
    assert!(
        message.contains("host=\"x\""),
        "the label set must be readable: {message}"
    );
}

#[tokio::test]
async fn duplicate_final_label_set_across_partitions_is_an_error() {
    // One series per partition, and `rate` drops `__name__` so both reach `{host="x"}` while both
    // cover the same grid points. A per-partition check would see one series each and report
    // nothing, so this is what pins the check to the whole output.
    let rows = counter_rows(60.0);
    let a = vec![named_chunk("a_total", 1, "x", &rows, true)];
    let b = vec![named_chunk("b_total", 2, "x", &rows, true)];
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 300_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::Rate,
        vec![],
    );
    let exec = case.fused(partitioned_child(&[&a, &b])).unwrap();
    assert_eq!(
        exec.properties().output_partitioning().partition_count(),
        2,
        "this test needs two partitions to place one series in each"
    );
    let plan: Arc<dyn ExecutionPlan> = Arc::new(exec);
    let error = datafusion::physical_plan::collect(plan, Arc::new(TaskContext::default()))
        .await
        .expect_err("two series with the same final labels must collide across partitions");
    let message = error_chain(&error);
    assert!(
        message.contains("vector cannot contain metrics with the same labelset"),
        "{message}"
    );
    assert!(
        message.contains("host=\"x\""),
        "the label set must be readable: {message}"
    );
}

#[tokio::test]
async fn duplicate_final_label_set_with_disjoint_grid_points_is_emitted() {
    // `a_total` only has a window result at 300s while `b_total` only has them at 480s and 540s,
    // so the two series share a final label set but no evaluation timestamp: Prometheus merges
    // them into one output series, this node emits both batches instead.
    let chunks = vec![
        named_chunk(
            "a_total",
            1,
            "x",
            &[(60_000, Some(60.0)), (120_000, Some(120.0))],
            true,
        ),
        named_chunk(
            "b_total",
            2,
            "x",
            &[(420_000, Some(420.0)), (480_000, Some(480.0))],
            true,
        ),
    ];
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 300_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::Rate,
        vec![],
    );
    let reference = case.reference(&chunks);
    let batches = collect(run(case.fused(memory_child(&chunks)).unwrap()).await);
    assert_matches_reference(&batches, &reference, "disjoint label sets");

    assert_eq!(batches.len(), 2, "{batches:?}");
    assert_eq!(
        batches
            .iter()
            .map(RecordBatch::num_rows)
            .collect::<Vec<_>>(),
        vec![1, 2],
        "each series keeps the points its own windows produced"
    );
    let timestamps = batches
        .iter()
        .flat_map(|batch| timestamp_column(batch, TIME))
        .collect::<Vec<_>>();
    let mut distinct = timestamps.clone();
    distinct.sort_unstable();
    distinct.dedup();
    assert_eq!(
        distinct.len(),
        timestamps.len(),
        "no grid point may repeat: {timestamps:?}"
    );
    assert_eq!(
        distinct,
        vec![300_000, 480_000, 540_000],
        "both series must survive with every point"
    );
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        3,
        "no point may be lost"
    );
}

#[tokio::test]
async fn empty_frame_emits_no_batch() {
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 300_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::Rate,
        vec![],
    );
    let items = run(case.fused(memory_child(&[])).unwrap()).await;
    assert!(items.is_empty(), "unexpected items: {items:?}");
}

#[tokio::test]
async fn short_lived_series_is_trimmed_to_its_absolute_interval() {
    let chunks = vec![chunk(
        1,
        "a",
        &[(420_000, Some(1.0)), (480_000, Some(2.0))],
        true,
    )];
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 60_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::SumOverTime,
        vec![],
    );
    let reference = case.reference(&chunks);
    let batches = collect(run(case.fused(memory_child(&chunks)).unwrap()).await);
    assert_matches_reference(&batches, &reference, "trimmed");

    let batch = &batches[0];
    // Only grid points 2 and 3 have a sample in their window.
    assert_eq!(timestamp_column(batch, TIME), vec![420_000, 480_000]);
    assert_eq!(
        float_column(batch, VALUE),
        vec![Some(1.0), Some(2.0)],
        "the trimmed interval keeps its absolute grid timestamps"
    );
    assert_eq!(grid_range(&reference[0].0), (2, 2));
}

#[tokio::test]
async fn nonzero_offset_is_passed_through_without_moving_the_grid() {
    let chunks = vec![chunk(
        1,
        "a",
        &[
            (420_000, Some(1.0)),
            (480_000, Some(2.0)),
            (540_000, Some(3.0)),
        ],
        true,
    )];
    let offset = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 60_000,
            offset_ms: 60_000,
            anchor_ms: None,
        },
        RangeFunction::SumOverTime,
        vec![],
    );
    let plain = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 60_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::SumOverTime,
        vec![],
    );
    let reference = offset.reference(&chunks);
    let batches = collect(run(offset.fused(memory_child(&chunks)).unwrap()).await);
    assert_matches_reference(&batches, &reference, "offset");

    let timestamps = timestamp_column(&batches[0], TIME);
    assert!(
        timestamps
            .iter()
            .all(|timestamp| (timestamp - GRID_START) % GRID_STEP == 0),
        "the node must not shift the grid timestamps: {timestamps:?}"
    );
    let plain_batches = collect(run(plain.fused(memory_child(&chunks)).unwrap()).await);
    assert_ne!(
        float_column(&plain_batches[0], VALUE),
        float_column(&batches[0], VALUE),
        "the offset must reach the evaluator instead of being ignored"
    );
}

#[tokio::test]
async fn two_fields_convert_both_columns_and_keep_a_half_missing_row() {
    let schema = two_field_schema();
    let chunks = vec![two_field_chunk(
        1,
        &[
            (420_000, Some(1.0), Some(10.0)),
            (480_000, None, Some(20.0)),
            (540_000, Some(3.0), Some(30.0)),
        ],
        true,
    )];
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 60_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::SumOverTime,
        vec![],
    )
    .with_series_schema(schema.clone())
    .with_output_schema(output_schema_of(&[VALUE, VALUE2], &["host"]));
    let reference = case.reference(&chunks);
    let batches = collect(run(case.fused(memory_child_of(&schema, &chunks)).unwrap()).await);
    assert_matches_reference(&batches, &reference, "two fields");

    let batch = &batches[0];
    assert_eq!(
        timestamp_column(batch, TIME),
        vec![420_000, 480_000, 540_000]
    );
    assert_eq!(
        float_column(batch, VALUE),
        vec![Some(1.0), None, Some(3.0)],
        "only the missing field is null"
    );
    assert_eq!(
        float_column(batch, VALUE2),
        vec![Some(10.0), Some(20.0), Some(30.0)],
        "the row stays because this field has a value"
    );
}

#[tokio::test]
async fn missing_point_inside_the_trimmed_range_becomes_a_null_row() {
    let chunks = vec![chunk(
        1,
        "a",
        &[(420_000, Some(1.0)), (540_000, Some(3.0))],
        true,
    )];
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 60_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::SumOverTime,
        vec![],
    );
    let reference = case.reference(&chunks);
    let batches = collect(run(case.fused(memory_child(&chunks)).unwrap()).await);
    assert_matches_reference(&batches, &reference, "missing point");

    let batch = &batches[0];
    assert_eq!(
        timestamp_column(batch, TIME),
        vec![420_000, 480_000, 540_000]
    );
    assert_eq!(
        float_column(batch, VALUE),
        vec![Some(1.0), None, Some(3.0)],
        "the empty middle window is a null row, not a dropped or shifted one"
    );
    assert_eq!(grid_range(&reference[0].0), (2, 3));
}

#[tokio::test]
async fn nan_stays_a_value_and_a_stale_marker_only_clears_its_window() {
    // (context, function, rows, expected timestamps, expected values)
    let cases = [
        (
            "ordinary NaN",
            RangeFunction::AvgOverTime,
            vec![(420_000, Some(f64::NAN)), (540_000, Some(f64::NAN))],
            vec![420_000, 480_000, 540_000],
            vec![Some(f64::NAN), None, Some(f64::NAN)],
        ),
        (
            "stale marker",
            RangeFunction::SumOverTime,
            vec![
                (420_000, Some(stale())),
                (480_000, Some(1.0)),
                (540_000, Some(3.0)),
            ],
            vec![480_000, 540_000],
            vec![Some(1.0), Some(3.0)],
        ),
    ];
    let describe = |values: &[Option<f64>]| -> Vec<Option<u64>> {
        values.iter().map(|value| value.map(f64::to_bits)).collect()
    };
    for (context, function, rows, expected_timestamps, expected_values) in cases {
        let chunks = vec![chunk(1, "a", &rows, true)];
        let case = Case::new(
            &["host"],
            RangeWindow {
                range_ms: 60_000,
                offset_ms: 0,
                anchor_ms: None,
            },
            function,
            vec![MathOp::Abs],
        );
        let reference = case.reference(&chunks);
        let batches = collect(run(case.fused(memory_child(&chunks)).unwrap()).await);
        assert_matches_reference(&batches, &reference, context);

        let batch = &batches[0];
        assert_eq!(
            timestamp_column(batch, TIME),
            expected_timestamps,
            "{context}: absolute grid timestamps"
        );
        let actual = float_column(batch, VALUE);
        assert_eq!(
            describe(&actual),
            describe(&expected_values),
            "{context}: expected {expected_values:?}, got {actual:?}"
        );
    }

    // An ordinary NaN is a value, never a null, and the missing middle window is a row, so the
    // row count stays that of the trimmed interval.
    let nan_chunks = vec![chunk(
        1,
        "a",
        &[(420_000, Some(f64::NAN)), (540_000, Some(f64::NAN))],
        true,
    )];
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 60_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::AvgOverTime,
        vec![MathOp::Abs],
    );
    let batch = &collect(run(case.fused(memory_child(&nan_chunks)).unwrap()).await)[0];
    let values = float_column(batch, VALUE);
    assert_eq!(batch.num_rows(), 3, "the null row is kept, not dropped");
    assert!(values[0].is_some_and(f64::is_nan), "{values:?}");
    assert_eq!(values[1], None);
    assert!(values[2].is_some_and(f64::is_nan), "{values:?}");
}

#[tokio::test]
async fn truncated_input_fails_at_eof_without_reexecuting_the_child() {
    let chunks = vec![chunk(
        1,
        "a",
        &[(60_000, Some(60.0)), (120_000, Some(120.0))],
        false,
    )];
    let executes = Arc::new(AtomicUsize::new(0));
    let plan = ProbeExec::new(
        frame_batches(&chunks),
        Arc::clone(&executes),
        Arc::new(AtomicBool::new(false)),
    );
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 300_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::Rate,
        vec![],
    );
    let items = run(case.fused(Arc::new(plan)).unwrap()).await;
    assert_eq!(items.len(), 1, "expected exactly one error item");
    let error = items.into_iter().next().unwrap().err().unwrap();
    assert!(
        error_chain(&error).contains("final"),
        "unexpected error: {error}"
    );
    assert_eq!(
        executes.load(Ordering::SeqCst),
        1,
        "the child must not be re-executed after an error"
    );
}

#[tokio::test]
async fn out_of_order_chunks_are_rejected_by_raw_frame_validation() {
    let chunks = vec![
        chunk(1, "a", &[(420_000, Some(1.0)), (540_000, Some(2.0))], false),
        chunk(1, "a", &[(300_000, Some(3.0))], true),
    ];
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 300_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::Rate,
        vec![],
    );
    let items = run(case.fused(memory_child(&chunks)).unwrap()).await;
    assert_eq!(items.len(), 1, "expected exactly one error item");
    let error = items.into_iter().next().unwrap().err().unwrap();
    assert!(
        error_chain(&error).contains("not increasing"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn construction_rejects_unsupported_inputs() {
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 300_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::Rate,
        vec![],
    );

    // The node runs one evaluator per input partition, so its partition count follows the child's
    // instead of being pinned to one.
    let two_partitions: Arc<dyn ExecutionPlan> =
        Arc::new(TestMemoryExec::try_new(&[vec![], vec![]], frame_schema(), None).unwrap());
    let partitions = two_partitions.properties().partitioning.partition_count();
    assert_eq!(
        partitions, 2,
        "this test needs a two-partition child to check the count follows it"
    );
    let fused = case.fused(two_partitions).unwrap();
    assert_eq!(
        fused.properties().output_partitioning().partition_count(),
        partitions,
        "the fused node must run the partitions its child has"
    );

    let mut fields = output_schema(&["host"])
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.push(Field::new("bogus", DataType::Utf8, true));
    let unknown = Arc::new(Schema::new(fields));
    let error = case
        .with_output_schema(unknown)
        .fused(memory_child(&[]))
        .err()
        .unwrap();
    assert!(matches!(error, DataFusionError::Plan(_)), "{error}");
    assert!(error.to_string().contains("unknown column"), "{error}");

    let wrong_type = Arc::new(Schema::new(vec![
        Field::new(VALUE, DataType::Int64, true),
        Field::new("host", DataType::Utf8, true),
        Field::new(
            TIME,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]));
    let error = case
        .with_output_schema(wrong_type)
        .fused(memory_child(&[]))
        .err()
        .unwrap();
    assert!(matches!(error, DataFusionError::Plan(_)), "{error}");
    assert!(error.to_string().contains("must be Float64"), "{error}");
}

#[tokio::test]
async fn construction_rejects_a_non_nullable_field_column() {
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 300_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::Rate,
        vec![],
    );
    let not_nullable = Arc::new(Schema::new(vec![
        Field::new(VALUE, DataType::Float64, false),
        Field::new("host", DataType::Utf8, true),
        Field::new(
            TIME,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]));
    let error = case
        .with_output_schema(not_nullable)
        .fused(memory_child(&[]))
        .err()
        .unwrap();
    assert!(matches!(error, DataFusionError::Plan(_)), "{error}");
    let message = error.to_string();
    assert!(message.contains(VALUE), "{message}");
    assert!(message.contains("must be nullable"), "{message}");
}

#[tokio::test]
async fn construction_accepts_a_non_nullable_tag_column() {
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 300_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::Rate,
        vec![],
    );
    // The PromQL plan declares a label non-nullable and expresses an absent label with the empty
    // value Prometheus reads, so a non-nullable tag is a supported shape.
    let not_nullable = Arc::new(Schema::new(vec![
        Field::new(VALUE, DataType::Float64, true),
        Field::new("host", DataType::Utf8, false),
        Field::new(
            TIME,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]));
    let chunks = vec![chunk(
        1,
        "a",
        &[(300_000, Some(1.0)), (360_000, Some(2.0))],
        true,
    )];
    let case = case.with_output_schema(not_nullable);
    let reference = case.reference(&chunks);
    let batches = collect(run(case.fused(memory_child(&chunks)).unwrap()).await);
    assert_matches_reference(&batches, &reference, "non-nullable tag");
    assert_eq!(
        string_column(&batches[0], "host"),
        vec![Some("a".to_string()); batches[0].num_rows()],
        "a non-nullable tag carries its value on every row"
    );
}

#[tokio::test]
async fn execute_rejects_a_partition_out_of_bounds() {
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 300_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::Rate,
        vec![],
    );
    let exec = case.fused(memory_child(&[])).unwrap();
    let partitions = exec.properties().output_partitioning().partition_count();
    let error = exec
        .execute(partitions, Arc::new(TaskContext::default()))
        .err()
        .unwrap();
    assert!(matches!(error, DataFusionError::Execution(_)), "{error}");
}

#[tokio::test]
async fn explain_shows_the_function_and_the_map_chain() {
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 300_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::Rate,
        vec![MathOp::Abs, MathOp::Sqrt],
    );
    let exec = case.fused(memory_child(&[])).unwrap();
    // `DisplayableExecutionPlan` has no `Display` impl in this fork; `indent` covers both
    // display formats.
    for text in [
        displayable(&exec).indent(false).to_string(),
        displayable(&exec).indent(true).to_string(),
    ] {
        assert!(text.starts_with("FusedSeriesExec:"), "{text}");
        assert!(text.contains("eval=rate"), "{text}");
        assert!(text.contains("range=300000ms"), "{text}");
        assert!(text.contains("offset=0ms"), "{text}");
        assert!(text.contains("maps=[abs, sqrt]"), "{text}");
        assert_eq!(text.matches("sqrt").count(), 1, "{text}");
    }
}

#[tokio::test]
async fn dropping_the_stream_releases_the_child() {
    let chunks = vec![
        chunk(
            1,
            "a",
            &[(60_000, Some(60.0)), (120_000, Some(120.0))],
            true,
        ),
        chunk(
            2,
            "b",
            &[(60_000, Some(120.0)), (120_000, Some(240.0))],
            true,
        ),
    ];
    let dropped = Arc::new(AtomicBool::new(false));
    let plan = ProbeExec::new(
        frame_batches(&chunks),
        Arc::new(AtomicUsize::new(0)),
        Arc::clone(&dropped),
    );
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 300_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::Rate,
        vec![],
    );
    let exec = case.fused(Arc::new(plan)).unwrap();
    let mut stream = exec.execute(0, Arc::new(TaskContext::default())).unwrap();
    assert!(stream.next().await.unwrap().is_ok());
    assert!(
        !dropped.load(Ordering::SeqCst),
        "the child stream is still owned"
    );
    drop(stream);
    assert!(
        dropped.load(Ordering::SeqCst),
        "dropping the fused stream must drop the child stream"
    );
}

#[tokio::test]
async fn a_poll_consumes_at_most_the_chunk_budget() {
    // Every series has a single sample, so `rate` has no window result and no batch comes out of
    // any of these chunks: without a budget one poll would drain the whole always-ready child.
    // The input size is fixed rather than derived from the constant, so changing the budget
    // changes one side of the comparison instead of rescaling both.
    const SERIES: u32 = 200;
    let chunks = (0..SERIES)
        .map(|index| {
            let key = u8::try_from(index).unwrap();
            let host = format!("h{index}");
            chunk(key, &host, &[(60_000, Some(1.0))], true)
        })
        .collect::<Vec<_>>();
    // One frame batch per chunk, so the child hands out exactly one chunk per poll.
    let batches = chunks
        .iter()
        .flat_map(|chunk| frame_batches(std::slice::from_ref(chunk)))
        .collect::<Vec<_>>();
    assert_eq!(batches.len(), chunks.len());
    let plan = ProbeExec::new(
        batches,
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicBool::new(false)),
    );
    let yields = Arc::clone(&plan.yields);
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 300_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::Rate,
        vec![],
    );
    let exec = case.fused(Arc::new(plan)).unwrap();
    let mut stream = exec.execute(0, Arc::new(TaskContext::default())).unwrap();

    let waker = futures::task::noop_waker();
    let mut context = Context::from_waker(&waker);
    let polled = stream.as_mut().poll_next(&mut context);
    assert!(
        polled.is_pending(),
        "one poll must not drain the input: {polled:?}"
    );
    let consumed = yields.load(Ordering::SeqCst);
    assert!(consumed > 0, "the poll must make progress");
    assert!(
        consumed <= MAX_CHUNKS_PER_POLL,
        "consumed {consumed} chunks in one poll, budget is {MAX_CHUNKS_PER_POLL}"
    );
    assert!(
        (consumed as u32) < SERIES,
        "consumed {consumed} of {SERIES} chunks, the whole input was drained"
    );
}

/// Emits prepared raw frame batches and reports how often it ran and when its stream was dropped.
struct ProbeExec {
    batches: Vec<RecordBatch>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    executes: Arc<AtomicUsize>,
    dropped: Arc<AtomicBool>,
    /// Number of batches the child stream handed out, i.e. the fused node's chunk inputs.
    yields: Arc<AtomicUsize>,
}

impl ProbeExec {
    fn new(
        batches: Vec<RecordBatch>,
        executes: Arc<AtomicUsize>,
        dropped: Arc<AtomicBool>,
    ) -> Self {
        let schema = frame_schema();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            batches,
            schema,
            properties,
            executes,
            dropped,
            yields: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl std::fmt::Debug for ProbeExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProbeExec").finish()
    }
}

impl DisplayAs for ProbeExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ProbeExec")
    }
}

impl ExecutionPlan for ProbeExec {
    fn name(&self) -> &str {
        "ProbeExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.executes.fetch_add(1, Ordering::SeqCst);
        Ok(Box::pin(ProbeStream {
            batches: self.batches.clone().into_iter(),
            schema: self.schema.clone(),
            dropped: Arc::clone(&self.dropped),
            yields: Arc::clone(&self.yields),
        }))
    }
}

struct ProbeStream {
    batches: std::vec::IntoIter<RecordBatch>,
    schema: SchemaRef,
    dropped: Arc<AtomicBool>,
    yields: Arc<AtomicUsize>,
}

impl RecordBatchStream for ProbeStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for ProbeStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let batch = self.batches.next();
        if batch.is_some() {
            self.yields.fetch_add(1, Ordering::SeqCst);
        }
        Poll::Ready(batch.map(Ok))
    }
}

impl Drop for ProbeStream {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}
