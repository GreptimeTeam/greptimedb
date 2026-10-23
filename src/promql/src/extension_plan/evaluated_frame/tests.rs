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

//! What the evaluated frame has to carry unchanged and what it has to refuse.
//!
//! The chunks here are what an evaluator hands over: absolute positions on a shared grid, with
//! holes between the runs it decided. Pack and read are checked against each other for the shapes
//! that matter (sparse runs, chunked floats, an empty close, absent and empty labels) and against
//! malformed input on their own. The sequence checks are scalar, so they need no chunks.

use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, Float64Array, ListArray, StringArray, UInt32Array, UInt64Array,
};
use datafusion::arrow::buffer::ScalarBuffer;
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datatypes::series::histogram::HistogramBatchBuilder;
use datatypes::series::samples::{
    FieldSpec, HistogramSamples, SampleRef, Samples, Series, SeriesFields, SeriesSchema,
};
use datatypes::series::time::{EvalGrid, GridSpan, GridSpec, TimeAxis};
use datatypes::series::{Label, MetricName};

use super::{
    FRAME_ORDINAL, IS_LAST, LABEL_NAMES, LABEL_VALUES, POSITIONS, PRODUCER, Sequence, VALUES, list,
    pack, read, schema,
};
use crate::evaluated::EvaluatedChunk;

const VALUE: &str = "value";
const GRID_START_MS: i64 = 300_000;
const GRID_STEP_MS: i64 = 60_000;
const GRID_LEN: u64 = 8;

fn grid(start_ms: i64, len: u64) -> Arc<EvalGrid> {
    Arc::new(EvalGrid::try_new(GridSpec::try_new(start_ms, GRID_STEP_MS, len).unwrap()).unwrap())
}

fn one_field() -> Arc<[FieldSpec]> {
    Arc::from([FieldSpec { name: VALUE.into() }])
}

fn schema_on(grid: Arc<EvalGrid>) -> SeriesSchema {
    SeriesSchema {
        fields: one_field(),
        eval_grid: Some(grid),
    }
}

fn series_schema() -> SeriesSchema {
    schema_on(grid(GRID_START_MS, GRID_LEN))
}

fn two_field_schema(grid: Arc<EvalGrid>) -> SeriesSchema {
    SeriesSchema {
        fields: Arc::from([
            FieldSpec { name: VALUE.into() },
            FieldSpec {
                name: "other".into(),
            },
        ]),
        eval_grid: Some(grid),
    }
}

fn bound(schema: &SeriesSchema) -> Arc<SeriesSchema> {
    Arc::new(schema.clone())
}

fn metric(name: Option<&str>, tags: &[(&str, &str)]) -> MetricName {
    MetricName::try_new(
        name.map(Arc::<str>::from),
        tags.iter()
            .map(|(name, value)| Label {
                name: Arc::<str>::from(*name),
                value: Arc::<str>::from(*value),
            })
            .collect(),
    )
    .unwrap()
}

fn sample(value: Option<f64>) -> Samples {
    Samples::from_float(Float64Array::from(vec![value]))
}

fn chunk_with(
    schema: &SeriesSchema,
    metric: MetricName,
    spans: Vec<GridSpan>,
    samples: Samples,
    is_last: bool,
) -> EvaluatedChunk {
    let axis = TimeAxis::from_grid_spans(schema.eval_grid.clone().unwrap(), spans).unwrap();
    let series = Series::try_new(schema, metric, axis, SeriesFields::one(samples)).unwrap();
    EvaluatedChunk::try_new(series, is_last).unwrap()
}

/// A chunk over `runs`, each `(start position, one value or null per row)`.
fn chunk(
    schema: &SeriesSchema,
    metric: MetricName,
    runs: &[(usize, Vec<Option<f64>>)],
    is_last: bool,
) -> EvaluatedChunk {
    let mut spans = Vec::new();
    let mut values = Vec::new();
    for (start, run) in runs {
        if run.is_empty() {
            continue;
        }
        spans.push(GridSpan::new(*start, run.len()));
        values.extend(run.iter().copied());
    }
    chunk_with(
        schema,
        metric,
        spans,
        Samples::from_float(Float64Array::from(values)),
        is_last,
    )
}

fn two_field_evaluated(schema: &SeriesSchema) -> EvaluatedChunk {
    let axis =
        TimeAxis::from_grid_spans(schema.eval_grid.clone().unwrap(), vec![GridSpan::new(0, 1)])
            .unwrap();
    let fields = SeriesFields::try_from_vec(vec![sample(Some(1.0)), sample(Some(2.0))]).unwrap();
    let series = Series::try_new(schema, metric(Some("m"), &[]), axis, fields).unwrap();
    EvaluatedChunk::try_new(series, true).unwrap()
}

fn positions_of(chunk: &EvaluatedChunk) -> Vec<usize> {
    chunk
        .series
        .time
        .positions()
        .map(|(position, _)| position)
        .collect()
}

/// The chunks' float values by bits, so a NaN payload is compared, not just `is_nan`.
fn float_bits(chunk: &EvaluatedChunk) -> Vec<Option<u64>> {
    chunk.series.fields.as_slice()[0]
        .iter()
        .map(|sample| match sample {
            SampleRef::Missing => None,
            SampleRef::Float(value) => Some(value.to_bits()),
            SampleRef::Histogram(_) => panic!("the test chunk holds a histogram"),
        })
        .collect()
}

/// One frame of producer `producer` with a metric and a single point.
fn packed(schema: &SeriesSchema, producer: u64, ordinal: u64, is_last: bool) -> RecordBatch {
    let chunk = chunk(
        schema,
        metric(Some("m"), &[("a", "1")]),
        &[(0, vec![Some(1.0)])],
        is_last,
    );
    pack(&chunk, schema, producer, ordinal).unwrap()
}

fn one_row(item: DataType, nullable: bool, values: ArrayRef) -> ListArray {
    list(item, nullable, values).unwrap()
}

fn replace(batch: &RecordBatch, column: &str, values: ArrayRef) -> RecordBatch {
    let index = batch.schema().index_of(column).unwrap();
    let mut columns = batch.columns().to_vec();
    columns[index] = values;
    RecordBatch::try_new(batch.schema(), columns).unwrap()
}

#[test]
fn schema_is_the_fixed_evaluated_frame_layout() {
    let column = |item: DataType, nullable: bool| {
        DataType::List(Arc::new(Field::new("item", item, nullable)))
    };
    let expected = Schema::new(vec![
        Field::new("producer", DataType::UInt64, false),
        Field::new("frame_ordinal", DataType::UInt64, false),
        Field::new("metric_name", DataType::Utf8, true),
        Field::new("label_names", column(DataType::Utf8, false), false),
        Field::new("label_values", column(DataType::Utf8, false), false),
        Field::new("positions", column(DataType::UInt64, false), false),
        Field::new("values", column(DataType::Float64, true), false),
        Field::new("is_last", DataType::Boolean, false),
    ]);
    assert_eq!(schema().as_ref(), &expected);
}

#[test]
fn round_trip_preserves_sparse_positions_nulls_and_nan_bits() {
    let series_schema = series_schema();
    let metric = metric(None, &[("host", "a"), ("empty", "")]);
    let nan = f64::from_bits(0x7ff8_0000_0000_0001);
    let chunk = chunk(
        &series_schema,
        metric.clone(),
        &[(1, vec![Some(1.5), None]), (4, vec![Some(nan), Some(-0.0)])],
        true,
    );
    let batch = pack(&chunk, &series_schema, 7, 3).unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.schema().as_ref(), schema().as_ref());

    let (producer, ordinal, read) = read(&batch, 0, &bound(&series_schema)).unwrap();
    assert_eq!((producer, ordinal), (7, 3));
    assert!(read.is_last);
    assert_eq!(read.series.metric, metric);
    assert_eq!(positions_of(&read), vec![1, 2, 4, 5]);
    assert_eq!(
        float_bits(&read),
        vec![
            Some(1.5f64.to_bits()),
            None,
            Some(nan.to_bits()),
            Some((-0.0f64).to_bits()),
        ]
    );
}

#[test]
fn round_trip_keeps_an_absent_metric_name_distinct_from_an_empty_one() {
    let series_schema = series_schema();
    for name in [None, Some("")] {
        let metric = metric(name, &[("a", "")]);
        let chunk = chunk(
            &series_schema,
            metric.clone(),
            &[(2, vec![Some(1.0)])],
            false,
        );
        let batch = pack(&chunk, &series_schema, 0, 1).unwrap();
        let (_, _, read) = read(&batch, 0, &bound(&series_schema)).unwrap();
        assert_eq!(read.series.metric, metric);
        assert_eq!(read.series.metric.name(), name);
        assert_eq!(read.series.metric.get("a"), Some(""));
        assert!(!read.is_last);
    }
}

#[test]
fn an_empty_closing_chunk_is_still_one_row() {
    let series_schema = series_schema();
    let metric = metric(Some("m"), &[("a", "1")]);
    let chunk = chunk(&series_schema, metric.clone(), &[], true);
    let batch = pack(&chunk, &series_schema, 2, 0).unwrap();
    assert_eq!(batch.num_rows(), 1);

    let (producer, ordinal, read) = read(&batch, 0, &bound(&series_schema)).unwrap();
    assert_eq!((producer, ordinal), (2, 0));
    assert!(read.is_last);
    assert_eq!(read.series.metric, metric);
    assert!(read.series.time.is_empty());
    assert!(positions_of(&read).is_empty());
    assert!(float_bits(&read).is_empty());
    assert_eq!(
        read.series.time.grid().map(|grid| grid.spec()),
        Some(series_schema.eval_grid.as_ref().unwrap().spec())
    );
}

#[test]
fn chunked_float_samples_round_trip() {
    let series_schema = series_schema();
    let nan = f64::from_bits(0x7ff8_0000_0000_0002);
    let samples = Samples::try_from_chunks(vec![
        Samples::from_float(Float64Array::from(vec![Some(1.0), None])),
        Samples::from_float(Float64Array::from(vec![Some(nan)])),
    ])
    .unwrap();
    let chunk = chunk_with(
        &series_schema,
        metric(Some("m"), &[]),
        vec![GridSpan::new(2, 3)],
        samples,
        false,
    );
    let batch = pack(&chunk, &series_schema, 1, 0).unwrap();
    let (_, _, read) = read(&batch, 0, &bound(&series_schema)).unwrap();
    assert_eq!(positions_of(&read), vec![2, 3, 4]);
    assert_eq!(
        float_bits(&read),
        vec![Some(1.0f64.to_bits()), None, Some(nan.to_bits())]
    );
    assert!(!read.is_last);
}

#[test]
fn read_accepts_a_rebatched_batch_at_any_row() {
    let series_schema = series_schema();
    let mut batches = Vec::new();
    for (ordinal, name) in ["a", "b", "c"].into_iter().enumerate() {
        let chunk = chunk(
            &series_schema,
            metric(Some(name), &[]),
            &[(ordinal, vec![Some(ordinal as f64)])],
            ordinal == 2,
        );
        batches.push(pack(&chunk, &series_schema, 0, ordinal as u64).unwrap());
    }
    let combined = concat_batches(&schema(), &batches).unwrap();
    assert_eq!(combined.num_rows(), 3);

    for (row, name, is_last) in [(2usize, "c", true), (0, "a", false), (1, "b", false)] {
        let (producer, ordinal, read) = read(&combined, row, &bound(&series_schema)).unwrap();
        assert_eq!((producer, ordinal), (0, row as u64));
        assert_eq!(read.series.metric.name(), Some(name));
        assert_eq!(read.is_last, is_last);
        assert_eq!(float_bits(&read), vec![Some((row as f64).to_bits())]);
    }

    // DataFusion may hand over a slice of a larger batch, not only whole batches.
    let sliced = combined.slice(1, 1);
    let (_, ordinal, read) = read(&sliced, 0, &bound(&series_schema)).unwrap();
    assert_eq!(ordinal, 1);
    assert_eq!(read.series.metric.name(), Some("b"));
}

#[test]
fn pack_refuses_a_chunk_the_bound_schema_does_not_describe() {
    let series_schema = series_schema();
    let two_fields = two_field_schema(series_schema.eval_grid.clone().unwrap());
    let two_field_chunk = two_field_evaluated(&two_fields);
    let no_grid = SeriesSchema {
        fields: one_field(),
        eval_grid: None,
    };
    let explicit = {
        let time = TimeAxis::try_new(ScalarBuffer::from(vec![0i64])).unwrap();
        let series = Series::try_new(
            &no_grid,
            metric(Some("m"), &[]),
            time,
            SeriesFields::one(sample(Some(1.0))),
        )
        .unwrap();
        // `try_new` refuses an explicit axis, so the invalid chunk is built literally: this is
        // the producer defect `pack` has to catch.
        EvaluatedChunk {
            series,
            is_last: true,
        }
    };
    let other_grid = schema_on(grid(GRID_START_MS - GRID_STEP_MS, GRID_LEN));
    let wrong_grid = chunk(
        &other_grid,
        metric(Some("m"), &[]),
        &[(0, vec![Some(1.0)])],
        true,
    );
    let cases = [
        ("two-field schema", &two_fields, &two_field_chunk),
        ("two-field chunk", &series_schema, &two_field_chunk),
        ("schema without a grid", &no_grid, &explicit),
        ("chunk without a grid", &series_schema, &explicit),
        ("chunk on another grid", &series_schema, &wrong_grid),
    ];
    for (case, bound_schema, chunk) in cases {
        assert!(
            pack(chunk, bound_schema, 0, 0).is_err(),
            "{case} was packed"
        );
    }
}

#[test]
fn pack_refuses_histogram_and_mixed_samples_even_when_empty() {
    let series_schema = series_schema();
    let histogram = Samples::try_from_histogram(
        UInt32Array::from(Vec::<u32>::new()),
        HistogramBatchBuilder::new().finish().unwrap(),
    )
    .unwrap();
    let mixed = Samples::try_from_mixed(
        Float64Array::from(Vec::<f64>::new()),
        HistogramSamples::try_new(
            UInt32Array::from(Vec::<u32>::new()),
            HistogramBatchBuilder::new().finish().unwrap(),
        )
        .unwrap(),
    )
    .unwrap();
    for (case, samples) in [("histogram", histogram), ("mixed", mixed)] {
        let chunk = chunk_with(
            &series_schema,
            metric(Some("h"), &[]),
            Vec::new(),
            samples,
            true,
        );
        assert!(
            matches!(
                pack(&chunk, &series_schema, 0, 0),
                Err(DataFusionError::NotImplemented(_))
            ),
            "{case} samples were packed"
        );
    }
}

#[test]
fn read_refuses_null_required_columns() {
    let series_schema = series_schema();
    let batch = packed(&series_schema, 0, 0, false);
    for column in [
        PRODUCER,
        FRAME_ORDINAL,
        LABEL_NAMES,
        LABEL_VALUES,
        POSITIONS,
        VALUES,
        IS_LAST,
    ] {
        let index = batch.schema().index_of(column).unwrap();
        let field = batch.schema().field(index).clone();
        let nulls = datafusion::arrow::array::new_null_array(field.data_type(), 1);
        let mut columns = batch.columns().to_vec();
        columns[index] = nulls;
        assert!(
            RecordBatch::try_new(batch.schema(), columns.clone()).is_err(),
            "Arrow accepted a null {column}"
        );

        // The same nulls under a schema that claims the column nullable: Arrow builds it, and
        // only the fixed-layout check stands between it and the reader.
        let mut fields = batch.schema().fields().to_vec();
        fields[index] = Arc::new(field.with_nullable(true));
        let foreign = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
        let error = read(&foreign, 0, &bound(&series_schema)).unwrap_err();
        assert!(
            error.to_string().contains("schema mismatch"),
            "a null {column} reported {error}"
        );
    }
}

#[test]
fn read_refuses_positions_that_are_not_strictly_ordered_or_in_bounds() {
    let series_schema = series_schema();
    let batch = packed(&series_schema, 0, 0, false);
    let cases = [
        ("duplicate", vec![1u64, 1]),
        ("backwards", vec![2, 1]),
        ("out of bounds", vec![GRID_LEN]),
        ("length mismatch", vec![0, 1]),
    ];
    for (case, positions) in cases {
        let malformed = replace(
            &batch,
            POSITIONS,
            Arc::new(one_row(
                DataType::UInt64,
                false,
                Arc::new(UInt64Array::from(positions)),
            )),
        );
        assert!(
            read(&malformed, 0, &bound(&series_schema)).is_err(),
            "positions {case} were accepted"
        );
    }

    let malformed = replace(
        &batch,
        VALUES,
        Arc::new(one_row(
            DataType::Float64,
            true,
            Arc::new(Float64Array::from(vec![Some(1.0), Some(2.0)])),
        )),
    );
    assert!(read(&malformed, 0, &bound(&series_schema)).is_err());
}

#[test]
fn read_refuses_label_lists_that_are_not_one_label_set() {
    let series_schema = series_schema();
    let batch = packed(&series_schema, 0, 0, false);
    let cases = [
        (
            "duplicate names",
            vec!["a", "a"],
            vec!["1", "2"],
            "duplicate label name",
        ),
        (
            "unequal lengths",
            vec!["a", "b"],
            vec!["1"],
            "label names and",
        ),
        ("name as a tag", vec!["__name__"], vec!["x"], "__name__"),
    ];
    for (case, names, values, expected) in cases {
        let pair = |values: Vec<&str>| {
            Arc::new(one_row(
                DataType::Utf8,
                false,
                Arc::new(StringArray::from(values)),
            ))
        };
        let malformed = replace(&batch, LABEL_NAMES, pair(names));
        let malformed = replace(&malformed, LABEL_VALUES, pair(values));
        let error = read(&malformed, 0, &bound(&series_schema)).unwrap_err();
        assert!(
            error.to_string().contains(expected),
            "{case} reported {error}"
        );
    }
}

#[test]
fn read_refuses_a_batch_that_is_not_the_fixed_layout() {
    let series_schema = series_schema();
    let batch = packed(&series_schema, 0, 0, false);

    assert!(read(&batch, 1, &bound(&series_schema)).is_err());

    let fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|field| {
            let nullable = field.is_nullable() || field.name() == PRODUCER;
            Field::new(field.name().as_str(), field.data_type().clone(), nullable)
        })
        .collect();
    let foreign =
        RecordBatch::try_new(Arc::new(Schema::new(fields)), batch.columns().to_vec()).unwrap();
    assert!(read(&foreign, 0, &bound(&series_schema)).is_err());

    let short = RecordBatch::try_new(
        Arc::new(Schema::new(batch.schema().fields()[..7].to_vec())),
        batch.columns()[..7].to_vec(),
    )
    .unwrap();
    assert!(read(&short, 0, &bound(&series_schema)).is_err());
}

#[test]
fn read_refuses_a_binding_it_cannot_address() {
    let series_schema = series_schema();
    let batch = packed(&series_schema, 0, 0, false);
    let two_fields = two_field_schema(series_schema.eval_grid.clone().unwrap());
    assert!(read(&batch, 0, &bound(&two_fields)).is_err());
    let no_grid = SeriesSchema {
        fields: one_field(),
        eval_grid: None,
    };
    assert!(read(&batch, 0, &bound(&no_grid)).is_err());
}

#[test]
fn sequence_accepts_frames_in_producer_order() {
    let mut sequence = Sequence::default();
    sequence.finish().unwrap();
    sequence.check(0, 0, false).unwrap();
    sequence.check(0, 1, false).unwrap();
    sequence.check(0, 2, true).unwrap();
    // A later producer number may skip values; only a decrease is backwards.
    sequence.check(2, 0, false).unwrap();
    sequence.check(2, 1, true).unwrap();
    sequence.finish().unwrap();
}

#[test]
fn sequence_rejects_duplicate_gapped_and_first_nonzero_ordinals() {
    let mut sequence = Sequence::default();
    assert!(sequence.check(0, 1, false).is_err());
    sequence.check(0, 0, false).unwrap();
    assert!(sequence.check(0, 0, false).is_err());
    assert!(sequence.check(0, 2, false).is_err());
    // The rejected frames left the sequence where it was.
    sequence.check(0, 1, true).unwrap();
    sequence.finish().unwrap();
}

#[test]
fn sequence_accepts_many_closed_series_from_one_producer() {
    // One producer hands over many series, so a closing frame ends a series and not the producer:
    // the next frame of that producer is still expected, as the pattern Main accepts shows.
    let mut sequence = Sequence::default();
    sequence.check(0, 0, true).unwrap();
    sequence.check(0, 1, true).unwrap();
    sequence.check(0, 2, false).unwrap();
    sequence.check(0, 3, true).unwrap();
    sequence.check(1, 0, true).unwrap();
    sequence.finish().unwrap();
}

#[test]
fn sequence_rejects_a_new_producer_before_the_previous_closed() {
    let mut sequence = Sequence::default();
    sequence.check(0, 0, false).unwrap();
    assert!(sequence.check(1, 0, true).is_err());
    sequence.check(0, 1, true).unwrap();
    sequence.check(1, 0, true).unwrap();
    sequence.finish().unwrap();

    // A closed frame only closes the series it carried, so it does not end its producer either.
    let mut sequence = Sequence::default();
    sequence.check(0, 0, true).unwrap();
    sequence.check(0, 1, false).unwrap();
    assert!(sequence.check(1, 0, true).is_err());
    sequence.check(0, 2, true).unwrap();
    sequence.check(1, 0, true).unwrap();
    sequence.finish().unwrap();
}

#[test]
fn sequence_rejects_a_backwards_producer() {
    let mut sequence = Sequence::default();
    sequence.check(2, 0, true).unwrap();
    assert!(sequence.check(1, 0, true).is_err());
    assert!(sequence.check(2, 0, true).is_err());
    // The rejected frames left the sequence where it was.
    sequence.check(2, 1, true).unwrap();
    sequence.check(3, 0, true).unwrap();
    sequence.finish().unwrap();
}

#[test]
fn sequence_rejects_a_new_producer_with_a_nonzero_ordinal() {
    let mut sequence = Sequence::default();
    sequence.check(0, 0, true).unwrap();
    assert!(sequence.check(1, 3, true).is_err());
    sequence.check(1, 0, true).unwrap();
}

#[test]
fn sequence_finish_requires_the_last_frame_to_close() {
    let mut sequence = Sequence::default();
    sequence.check(0, 0, false).unwrap();
    assert!(sequence.finish().is_err());
    sequence.check(0, 1, true).unwrap();
    sequence.finish().unwrap();
}

#[test]
fn sequence_rejects_an_ordinal_that_cannot_be_incremented() {
    // No real stream reaches this state, but the checked increment is what keeps the state from
    // wrapping; the fields are set directly so the guard itself is exercised.
    let mut sequence = Sequence {
        previous_producer: Some(0),
        next_ordinal: u64::MAX,
        last_closed: false,
    };
    assert!(sequence.check(0, u64::MAX, true).is_err());
    assert_eq!(sequence.next_ordinal, u64::MAX);
    assert!(!sequence.last_closed);
}
