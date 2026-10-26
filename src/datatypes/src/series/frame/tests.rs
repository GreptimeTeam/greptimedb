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

use arrow::array::{
    Array, BinaryArray, BooleanArray, Float64Array, Int64Array, ListArray, StringArray,
    StructArray, UInt32Array,
};
use arrow::buffer::{Buffer, OffsetBuffer, ScalarBuffer};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::{reader::StreamReader, writer::StreamWriter};

use super::*;
use crate::series::histogram::{BucketSideView, CounterResetHint, HistogramBatchBuilder};
use crate::series::samples::{
    DenseSamplesRef, FieldSpec, SampleRef, Samples, SeriesFields, SeriesSchema,
};
use crate::series::{Label, SeriesLabels};

fn test_schema(names: &[&str]) -> SeriesSchema {
    SeriesSchema {
        fields: names
            .iter()
            .map(|name| FieldSpec {
                name: Arc::from(*name),
            })
            .collect::<Vec<_>>()
            .into(),
        eval_grid: None,
    }
}

fn metric(name: Option<&str>, labels: &[(&str, &str)]) -> SeriesLabels {
    SeriesLabels::try_new(
        name.map(Arc::from),
        labels
            .iter()
            .map(|(name, value)| Label {
                name: Arc::from(*name),
                value: Arc::from(*value),
            })
            .collect(),
    )
    .unwrap()
}

fn float_chunk(
    schema: &SeriesSchema,
    key: &[u8],
    metric: Option<SeriesLabels>,
    times: Vec<i64>,
    values: Vec<Option<f64>>,
    is_last: bool,
) -> RawSeriesChunk {
    RawSeriesChunk::try_new(
        schema,
        Buffer::from(key.to_vec()),
        metric,
        RawTimestamps::try_new(times.into()).unwrap(),
        SeriesFields::one(Samples::from_float(Float64Array::from(values))),
        is_last,
    )
    .unwrap()
}

fn histogram_batch() -> Arc<crate::series::histogram::HistogramBatch> {
    histogram_batch_with_sum(-9.5)
}

fn histogram_batch_with_sum(sum: f64) -> Arc<crate::series::histogram::HistogramBatch> {
    let custom = [1.5, 4.5];
    let positive_offsets = [1];
    let positive_lengths = [2];
    let positive_buckets = [3.25, 7.5];
    let negative_offsets = [-2];
    let negative_lengths = [1];
    let negative_buckets = [0.75];
    let view = |sum, start_timestamp| crate::series::histogram::HistogramView {
        schema: -7,
        zero_threshold: 0.125,
        sum,
        count: 17.25,
        zero_count: 2.5,
        reset_hint: CounterResetHint::Unrecognized(37),
        start_timestamp,
        custom_values: &custom,
        positive: BucketSideView {
            span_offsets: &positive_offsets,
            span_lengths: &positive_lengths,
            buckets: &positive_buckets,
        },
        negative: BucketSideView {
            span_offsets: &negative_offsets,
            span_lengths: &negative_lengths,
            buckets: &negative_buckets,
        },
    };
    let mut builder = HistogramBatchBuilder::new();
    builder.push(view(sum, Some(-123))).unwrap();
    builder.push(view(sum + 100.0, None)).unwrap();
    builder.finish().unwrap()
}

fn hist_schema_frame_field(schema: &SeriesSchema) -> DataType {
    series_frame_schema(schema, FrameLabelMode::Key, Some(&dense_description()))
        .unwrap()
        .field(3)
        .data_type()
        .clone()
}

fn with_timestamp_values(
    batch: &arrow::record_batch::RecordBatch,
    values: Vec<i64>,
) -> arrow::record_batch::RecordBatch {
    let time = batch
        .column(2)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let timestamp_item = match batch.schema().field(2).data_type() {
        DataType::Struct(fields) => match fields[0].data_type() {
            DataType::List(item) => item.clone(),
            _ => panic!("timestamp field is not a List"),
        },
        _ => panic!("time field is not a Struct"),
    };
    let list = ListArray::try_new(
        timestamp_item,
        OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, values.len() as i32])),
        Arc::new(Int64Array::from(values)),
        None,
    )
    .unwrap();
    let time = StructArray::try_new(time.fields().clone(), vec![Arc::new(list)], None).unwrap();
    let mut columns = batch.columns().to_vec();
    columns[2] = Arc::new(time);
    arrow::record_batch::RecordBatch::try_new(batch.schema(), columns).unwrap()
}

fn frame_field_payload(batch: &arrow::record_batch::RecordBatch, field: usize) -> &StructArray {
    let fields = batch
        .column(3)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    fields
        .column(field)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap()
}

fn frame_histogram_table(batch: &arrow::record_batch::RecordBatch, field: usize) -> StructArray {
    let histogram = frame_field_payload(batch, field)
        .column(1)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let tables = histogram
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    tables
        .value(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap()
        .clone()
}

fn assert_histogram(histogram: crate::series::histogram::HistogramView<'_>, sum: f64) {
    assert_histogram_with_start(histogram, sum, Some(-123));
}

fn assert_histogram_with_start(
    histogram: crate::series::histogram::HistogramView<'_>,
    sum: f64,
    start_timestamp: Option<i64>,
) {
    assert_eq!(histogram.schema, -7);
    assert_eq!(histogram.zero_threshold.to_bits(), 0.125_f64.to_bits());
    assert_eq!(histogram.sum.to_bits(), sum.to_bits());
    assert_eq!(histogram.count.to_bits(), 17.25_f64.to_bits());
    assert_eq!(histogram.zero_count.to_bits(), 2.5_f64.to_bits());
    assert_eq!(histogram.reset_hint, CounterResetHint::Unrecognized(37));
    assert_eq!(histogram.start_timestamp, start_timestamp);
    assert_eq!(
        histogram
            .custom_values
            .iter()
            .map(|v| v.to_bits())
            .collect::<Vec<_>>(),
        [1.5_f64.to_bits(), 4.5_f64.to_bits()]
    );
    assert_eq!(histogram.positive.span_offsets, [1]);
    assert_eq!(histogram.positive.span_lengths, [2]);
    assert_eq!(
        histogram
            .positive
            .buckets
            .iter()
            .map(|v| v.to_bits())
            .collect::<Vec<_>>(),
        [3.25_f64.to_bits(), 7.5_f64.to_bits()]
    );
    assert_eq!(histogram.negative.span_offsets, [-2]);
    assert_eq!(histogram.negative.span_lengths, [1]);
    assert_eq!(
        histogram
            .negative
            .buckets
            .iter()
            .map(|v| v.to_bits())
            .collect::<Vec<_>>(),
        [0.75_f64.to_bits()]
    );
}

fn assert_unreferenced_histogram(histogram: crate::series::histogram::HistogramView<'_>) {
    assert_eq!(histogram.schema, -7);
    assert_histogram_with_start(histogram, 90.5, None);
}

fn assert_float_bits(actual: &Samples, expected: &[Option<f64>]) {
    assert_eq!(actual.len(), expected.len());
    for (index, expected) in expected.iter().enumerate() {
        match (actual.sample(index), expected) {
            (SampleRef::Missing, None) => {}
            (SampleRef::Float(actual), Some(expected)) => {
                assert_eq!(actual.to_bits(), expected.to_bits(), "position {index}");
            }
            _ => panic!("unexpected sample at position {index}"),
        }
    }
}

/// Packs one owned chunk through the public cursor/builder pair, mirroring the explicit
/// single-chunk wrapper: every batch the walk produces, then the final flush.
fn pack_chunk_with_budget(
    schema: &SeriesSchema,
    mode: FrameLabelMode,
    chunk: &RawSeriesChunk,
    max_rows: usize,
    max_bytes: usize,
) -> crate::error::Result<Vec<RecordBatch>> {
    // Only a key frame declares a label decode description; explicit frames carry their labels.
    let decode = (mode == FrameLabelMode::Key).then(dense_description);
    let mut builder =
        SeriesFrameBuilder::try_new(schema, mode, decode.as_ref(), max_rows, max_bytes)?;
    let mut cursor = SeriesFrameCursor::try_new_raw(schema, mode, chunk.clone())?;
    let mut batches = Vec::new();
    while let Some(batch) = cursor.append_to(&mut builder)? {
        batches.push(batch);
    }
    if let Some(batch) = builder.flush()? {
        batches.push(batch);
    }
    Ok(batches)
}

/// Advances one owned chunk through an existing builder. A partially filled buffer stays buffered
/// instead of becoming a batch of its own, which is what makes cross-chunk grouping observable.
fn append_chunk(
    builder: &mut SeriesFrameBuilder,
    schema: &SeriesSchema,
    mode: FrameLabelMode,
    chunk: &RawSeriesChunk,
) -> crate::error::Result<Vec<RecordBatch>> {
    let mut cursor = SeriesFrameCursor::try_new_raw(schema, mode, chunk.clone())?;
    let mut batches = Vec::new();
    while let Some(batch) = cursor.append_to(builder)? {
        batches.push(batch);
    }
    Ok(batches)
}

/// Decodes every batch with one reused decoder, so repeated calls exercise the cached schema.
fn decode_batches(
    schema: &SeriesSchema,
    mode: FrameLabelMode,
    batches: &[RecordBatch],
) -> Vec<RawSeriesChunk> {
    let decode = (mode == FrameLabelMode::Key).then(dense_description);
    let decoder = SeriesFrameDecoder::try_new(schema, mode, decode.as_ref()).unwrap();
    batches
        .iter()
        .flat_map(|batch| decoder.decode_raw(batch).unwrap())
        .collect()
}

/// Row-local histogram index and table lists of one field payload of a frame batch.
fn frame_histogram_lists(batch: &RecordBatch, field: usize) -> (ListArray, ListArray) {
    let histogram = frame_field_payload(batch, field)
        .column(1)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let indices = histogram
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .clone();
    let tables = histogram
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .clone();
    (indices, tables)
}

/// Every row is expected to resolve index 0 inside its own table only, with the whole table
/// retained and the other index entry null.
fn assert_row_local_histogram_rows(rows: &[RawSeriesChunk]) {
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].time.as_buffer().as_ref(), &[1_i64, 2]);
    assert_eq!(rows[1].time.as_buffer().as_ref(), &[3_i64, 4]);

    let DenseSamplesRef::Histogram(first) = rows[0].fields.get(0).unwrap().dense().unwrap() else {
        panic!("expected histogram samples");
    };
    assert_eq!(first.batch().len(), 2);
    assert_eq!(first.indices().len(), 2);
    assert_eq!(first.indices().value(0), 0);
    assert!(!first.indices().is_valid(1));
    assert_histogram(first.view_at(0).unwrap(), -9.5);
    assert_unreferenced_histogram(first.batch().view(1).unwrap());

    let DenseSamplesRef::Histogram(second) = rows[1].fields.get(0).unwrap().dense().unwrap() else {
        panic!("expected histogram samples");
    };
    assert_eq!(second.batch().len(), 2);
    assert_eq!(second.indices().len(), 2);
    assert!(!second.indices().is_valid(0));
    assert_eq!(second.indices().value(1), 0);
    // A global index remap would resolve index 0 into the first row's table instead.
    assert_histogram(second.view_at(1).unwrap(), 42.25);
    assert_histogram_with_start(second.batch().view(1).unwrap(), 142.25, None);
}

#[test]
fn sliced_histogram_frames_densify_without_duplicating_shared_table() {
    let schema = test_schema(&["histogram"]);
    let source_table = histogram_batch();
    let source_samples =
        Samples::try_from_histogram(UInt32Array::from(vec![Some(0); 64]), source_table).unwrap();
    let samples = Samples::try_from_chunks(
        (0..64)
            .map(|position| source_samples.slice(position, 1).unwrap())
            .collect(),
    )
    .unwrap();
    let chunk = RawSeriesChunk::try_new(
        &schema,
        Buffer::from(vec![0x42_u8]),
        None,
        RawTimestamps::try_new((0_i64..64).collect::<Vec<_>>().into()).unwrap(),
        SeriesFields::one(samples),
        true,
    )
    .unwrap();

    // One row per batch keeps the original segment-boundary granularity, so the shared-table fast
    // paths below stay observable; the default budget coalesces the same rows later on.
    let frames = pack_chunk_with_budget(
        &schema,
        FrameLabelMode::Key,
        &chunk,
        1,
        SeriesFrameBuilder::DEFAULT_MAX_BYTES,
    )
    .unwrap();
    assert_eq!(frames.len(), 64);
    let mut decoded_chunks = Vec::with_capacity(frames.len());
    for frame in &frames {
        let decoded = unpack_raw_series_frame(
            &schema,
            FrameLabelMode::Key,
            Some(&dense_description()),
            frame,
        )
        .unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].time.len(), 1);
        decoded_chunks.push(decoded.into_iter().next().unwrap());
    }

    let first_table = match decoded_chunks[0].fields.get(0).unwrap().dense().unwrap() {
        DenseSamplesRef::Histogram(histograms) => histograms.batch().clone(),
        _ => panic!("expected histogram samples"),
    };
    let second_table = match decoded_chunks[1].fields.get(0).unwrap().dense().unwrap() {
        DenseSamplesRef::Histogram(histograms) => histograms.batch(),
        _ => panic!("expected histogram samples"),
    };
    assert!(!Arc::ptr_eq(&first_table, second_table));
    let first_values = first_table
        .as_arrow()
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let second_values = second_table
        .as_arrow()
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(
        first_values.values().as_ptr(),
        second_values.values().as_ptr()
    );

    // The default budget groups the same 64 rows into one multirow batch; every row still decodes
    // to its own slice of the shared table with bit-exact values.
    let coalesced = pack_raw_series_chunk(
        &schema,
        FrameLabelMode::Key,
        Some(&dense_description()),
        &chunk,
    )
    .unwrap();
    assert_eq!(coalesced.len(), 1);
    assert_eq!(coalesced[0].num_rows(), 64);
    let coalesced_rows = decode_batches(&schema, FrameLabelMode::Key, &coalesced);
    assert_eq!(coalesced_rows.len(), 64);
    for (position, row) in coalesced_rows.iter().enumerate() {
        assert_eq!(row.primary_key.as_slice(), &[0x42]);
        assert_eq!(row.time.timestamp(0), Some(position as i64));
        let DenseSamplesRef::Histogram(histograms) = row.fields.get(0).unwrap().dense().unwrap()
        else {
            panic!("expected histogram samples");
        };
        assert_eq!(histograms.batch().len(), first_table.len());
        assert_histogram(histograms.view_at(0).unwrap(), -9.5);
    }

    let samples = Samples::try_from_chunks(
        decoded_chunks
            .iter()
            .map(|decoded| decoded.fields.get(0).unwrap().clone())
            .collect(),
    )
    .unwrap();
    drop(chunk);
    drop(frames);
    drop(decoded_chunks);
    drop(source_samples);

    let dense = samples.into_dense().unwrap();
    let DenseSamplesRef::Histogram(histograms) = dense.dense().unwrap() else {
        panic!("expected dense histogram samples");
    };
    assert_eq!(histograms.len(), 64);
    assert_eq!(histograms.batch().len(), first_table.len());
    for position in 0..64 {
        assert_histogram(histograms.view_at(position).unwrap(), -9.5);
    }
    let dense_values = histograms
        .batch()
        .as_arrow()
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(
        dense_values.values().as_ptr(),
        first_values.values().as_ptr()
    );
}

#[test]
fn raw_frame_field_names_bind_payloads_to_their_schema() {
    let schema = test_schema(&["cpu_usage", "cpu_idle"]);
    let fields = SeriesFields::try_from_vec(vec![
        Samples::from_float(Float64Array::from(vec![Some(1.0), Some(2.0)])),
        Samples::from_float(Float64Array::from(vec![Some(10.0), Some(20.0)])),
    ])
    .unwrap();

    for mode in [FrameLabelMode::Key, FrameLabelMode::Explicit] {
        let decode = (mode == FrameLabelMode::Key).then(dense_description);
        let metric = (mode == FrameLabelMode::Explicit).then(|| metric(Some("host"), &[]));
        let chunk = RawSeriesChunk::try_new(
            &schema,
            Buffer::from(vec![0xaa_u8]),
            metric,
            RawTimestamps::try_new(vec![100, 200].into()).unwrap(),
            fields.clone(),
            true,
        )
        .unwrap();
        let batch = &pack_raw_series_chunk(&schema, mode, decode.as_ref(), &chunk).unwrap()[0];
        let payloads = batch
            .column(3)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(
            payloads
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            ["cpu_usage", "cpu_idle"]
        );

        let decoded = unpack_raw_series_frame(&schema, mode, decode.as_ref(), batch).unwrap();
        assert_eq!(decoded[0].primary_key.as_slice(), &[0xaa]);
        assert!(decoded[0].is_last);
        let times = decoded[0].time.as_buffer();
        assert_eq!(times.as_ref(), &[100, 200]);
        assert_float_bits(decoded[0].fields.get(0).unwrap(), &[Some(1.0), Some(2.0)]);
        assert_float_bits(decoded[0].fields.get(1).unwrap(), &[Some(10.0), Some(20.0)]);

        assert!(
            unpack_raw_series_frame(
                &test_schema(&["cpu_idle", "cpu_usage"]),
                mode,
                decode.as_ref(),
                batch
            )
            .is_err()
        );
        assert!(
            unpack_raw_series_frame(
                &test_schema(&["cpu_usage", "cpu_busy"]),
                mode,
                decode.as_ref(),
                batch
            )
            .is_err()
        );
    }
}

/// A key frame declares the label decode description that makes its `read_key` readable, so its
/// metadata carries that text; an explicit frame declares none, and its `read_key` column is
/// nullable because an evaluated explicit row carries no storage key at all.
#[test]
fn schema_and_key_explicit_round_trips_preserve_identity_and_bits() {
    let schema = test_schema(&["value"]);
    let key_schema =
        series_frame_schema(&schema, FrameLabelMode::Key, Some(&dense_description())).unwrap();
    let explicit_schema = raw_series_frame_schema(&schema, FrameLabelMode::Explicit).unwrap();
    let payload = DataType::Struct(
        vec![
            Field::new(
                "float_values",
                DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                true,
            ),
            Field::new(
                "histogram",
                DataType::Struct(
                    vec![
                        Field::new(
                            "indices",
                            DataType::List(Arc::new(Field::new("item", DataType::UInt32, true))),
                            false,
                        ),
                        Field::new(
                            "table",
                            DataType::List(Arc::new(Field::new(
                                "item",
                                DataType::Struct(crate::series::histogram::fields()),
                                false,
                            ))),
                            false,
                        ),
                    ]
                    .into(),
                ),
                true,
            ),
        ]
        .into(),
    );
    let mut expected_key_metadata = std::collections::HashMap::new();
    expected_key_metadata.insert("greptime.series.layout".to_string(), "v2".to_string());
    expected_key_metadata.insert("greptime.series.content".to_string(), "raw".to_string());
    expected_key_metadata.insert("greptime.series.labels".to_string(), "key".to_string());
    expected_key_metadata.insert("greptime.series.time".to_string(), "timestamps".to_string());
    expected_key_metadata.insert(ORDERING.to_string(), "contiguous".to_string());
    // A key frame also declares the description that decodes its labels. Its exact text is pinned by
    // the metadata fixture, so it is spelled here by the same serializer the writer uses.
    expected_key_metadata.insert(
        LABEL_DECODE.to_string(),
        serde_json::to_string(&dense_description()).unwrap(),
    );
    let expected_key = Schema::new_with_metadata(
        vec![
            Field::new("read_key", DataType::Binary, false),
            Field::new("is_last", DataType::Boolean, false),
            Field::new(
                "time",
                DataType::Struct(
                    vec![Field::new(
                        "timestamps",
                        DataType::List(Arc::new(Field::new("item", DataType::Int64, false))),
                        false,
                    )]
                    .into(),
                ),
                false,
            ),
            Field::new(
                "fields",
                DataType::Struct(vec![Field::new("value", payload, false)].into()),
                false,
            ),
        ],
        expected_key_metadata,
    );
    assert_eq!(key_schema.as_ref(), &expected_key);
    // An explicit frame still has the key column, but only a raw explicit row carries a key: an
    // evaluated one has none, so the column is nullable here while a key frame's is required.
    let mut explicit_fields = expected_key.fields().iter().cloned().collect::<Vec<_>>();
    explicit_fields[0] = Field::new("read_key", DataType::Binary, true).into();
    explicit_fields.push(Field::new("name", DataType::Utf8, true).into());
    explicit_fields.push(
        Field::new(
            "labels",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(
                    vec![
                        Field::new("name", DataType::Utf8, false),
                        Field::new("value", DataType::Utf8, false),
                    ]
                    .into(),
                ),
                false,
            ))),
            false,
        )
        .into(),
    );
    let mut explicit_metadata = expected_key.metadata().clone();
    explicit_metadata.insert("greptime.series.labels".to_string(), "explicit".to_string());
    // An explicit frame carries complete labels, so it declares no decode description.
    explicit_metadata.remove(LABEL_DECODE);
    let expected_explicit = Schema::new_with_metadata(explicit_fields, explicit_metadata);
    assert_eq!(explicit_schema.as_ref(), &expected_explicit);

    let nan = f64::from_bits(0x7ff8_0000_0000_0042);
    let stale_nan = f64::from_bits(0x7ff0_0000_0000_0002);
    let values = [Some(-0.0), None, Some(nan), Some(stale_nan)];
    let native = float_chunk(
        &schema,
        &[0, 0xff, 0, 0x80],
        None,
        vec![10, 20, 30, 40],
        values.to_vec(),
        true,
    );
    let packed = pack_raw_series_chunk(
        &schema,
        FrameLabelMode::Key,
        Some(&dense_description()),
        &native,
    )
    .unwrap();
    assert_eq!(packed.len(), 1);
    let decoded = unpack_raw_series_frame(
        &schema,
        FrameLabelMode::Key,
        Some(&dense_description()),
        &packed[0],
    )
    .unwrap();
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].primary_key.as_slice(), &[0, 0xff, 0, 0x80]);
    assert!(decoded[0].metric.is_none());
    assert!(decoded[0].is_last);
    assert_eq!(
        (0..4)
            .map(|i| decoded[0].time.timestamp(i).unwrap())
            .collect::<Vec<_>>(),
        [10, 20, 30, 40]
    );
    assert_float_bits(decoded[0].fields.get(0).unwrap(), &values);

    let explicit = float_chunk(
        &schema,
        &[0x80, 0, 0xff],
        Some(metric(None, &[("rack", ""), ("zone", "west")])),
        vec![7],
        vec![Some(1.25)],
        false,
    );
    let packed = pack_raw_series_chunk(&schema, FrameLabelMode::Explicit, None, &explicit).unwrap();
    let decoded =
        unpack_raw_series_frame(&schema, FrameLabelMode::Explicit, None, &packed[0]).unwrap();
    assert_eq!(decoded[0].primary_key.as_slice(), &[0x80, 0, 0xff]);
    assert!(!decoded[0].is_last);
    let labels = decoded[0].metric.as_ref().unwrap();
    assert_eq!(labels.name(), None);
    assert_eq!(labels.get("rack"), Some(""));
    assert_eq!(labels.get("zone"), Some("west"));
    assert!(
        pack_raw_series_chunk(
            &schema,
            FrameLabelMode::Key,
            Some(&dense_description()),
            &explicit
        )
        .is_err()
    );
    assert!(pack_raw_series_chunk(&schema, FrameLabelMode::Explicit, None, &native).is_err());
    assert!(
        unpack_raw_series_frame(
            &schema,
            FrameLabelMode::Key,
            Some(&dense_description()),
            &packed[0]
        )
        .is_err()
    );

    let key_packed = pack_raw_series_chunk(
        &schema,
        FrameLabelMode::Key,
        Some(&dense_description()),
        &native,
    )
    .unwrap();
    assert!(
        unpack_raw_series_frame(&schema, FrameLabelMode::Explicit, None, &key_packed[0]).is_err()
    );
    for name in [None, Some(""), Some("cpu")] {
        let named = float_chunk(
            &schema,
            &[0xff, 0, 0xfe],
            Some(metric(name, &[("empty", "")])),
            vec![9],
            vec![Some(2.0)],
            false,
        );
        let encoded =
            pack_raw_series_chunk(&schema, FrameLabelMode::Explicit, None, &named).unwrap();
        let restored =
            unpack_raw_series_frame(&schema, FrameLabelMode::Explicit, None, &encoded[0]).unwrap();
        assert_eq!(restored[0].metric.as_ref().unwrap().name(), name);
        assert_eq!(restored[0].metric.as_ref().unwrap().get("empty"), Some(""));
    }
}

#[test]
fn segmented_mixed_fields_emit_every_aligned_position_and_marker() {
    let schema = test_schema(&["first", "second"]);
    let first_table = histogram_batch();
    let second_table = histogram_batch_with_sum(42.25);
    let first = Samples::try_from_chunks(vec![
        Samples::from_float(Float64Array::from(vec![Some(1.0), None])),
        Samples::try_from_mixed(
            Float64Array::from(vec![Some(3.5), None]),
            crate::series::samples::HistogramSamples::try_new(
                UInt32Array::from(vec![None, Some(0)]),
                first_table.clone(),
            )
            .unwrap(),
        )
        .unwrap(),
    ])
    .unwrap();
    let second = Samples::try_from_chunks(vec![
        Samples::from_float(Float64Array::from(vec![Some(5.0)])),
        Samples::try_from_histogram(UInt32Array::from(vec![Some(0), None]), second_table).unwrap(),
        Samples::from_float(Float64Array::from(vec![Some(8.0)])),
    ])
    .unwrap();
    let chunk = RawSeriesChunk::try_new(
        &schema,
        Buffer::from(vec![0_u8, 0x91, 0xfe]),
        None,
        RawTimestamps::try_new(vec![10_i64, 20, 30, 40].into()).unwrap(),
        SeriesFields::try_from_vec(vec![first, second]).unwrap(),
        true,
    )
    .unwrap();
    // Variable field boundaries now group every aligned interval into one multirow batch instead
    // of one batch per boundary, so decode the actual rows that batch carries.
    let frames = pack_raw_series_chunk(
        &schema,
        FrameLabelMode::Key,
        Some(&dense_description()),
        &chunk,
    )
    .unwrap();
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].num_rows(), 4);
    let mut observed_times = Vec::new();
    let mut observed_first = Vec::new();
    let mut observed_second = Vec::new();
    let mut observed_keys = Vec::new();
    let mut observed_markers = Vec::new();
    let rows = unpack_raw_series_frame(
        &schema,
        FrameLabelMode::Key,
        Some(&dense_description()),
        &frames[0],
    )
    .unwrap();
    assert_eq!(rows.len(), 4);
    for (row_index, row) in rows.iter().enumerate() {
        assert_eq!(row.is_last, row_index == rows.len() - 1);
        observed_markers.push(row.is_last);
        assert_eq!(row.primary_key.as_slice(), &[0, 0x91, 0xfe]);
        observed_keys.push(row.primary_key.as_slice().to_vec());
        for position in 0..row.time.len() {
            observed_times.push(row.time.timestamp(position).unwrap());
            match row.fields.get(0).unwrap().sample(position) {
                SampleRef::Missing => observed_first.push(("m", 0.0_f64.to_bits())),
                SampleRef::Float(value) => observed_first.push(("f", value.to_bits())),
                SampleRef::Histogram(histogram) => {
                    assert_histogram(histogram, -9.5);
                    observed_first.push(("h", histogram.sum.to_bits()));
                }
            }
            match row.fields.get(1).unwrap().sample(position) {
                SampleRef::Missing => observed_second.push(("m", 0.0_f64.to_bits())),
                SampleRef::Float(value) => observed_second.push(("f", value.to_bits())),
                SampleRef::Histogram(histogram) => {
                    assert_histogram(histogram, 42.25);
                    observed_second.push(("h", histogram.sum.to_bits()));
                }
            }
        }
    }
    assert_eq!(observed_times, [10, 20, 30, 40]);
    assert_eq!(observed_keys, [[0, 0x91, 0xfe]; 4]);
    assert_eq!(observed_markers, [false, false, false, true]);
    assert_eq!(
        observed_first,
        [
            ("f", 1.0_f64.to_bits()),
            ("m", 0.0_f64.to_bits()),
            ("f", 3.5_f64.to_bits()),
            ("h", (-9.5_f64).to_bits())
        ]
    );
    assert_eq!(
        observed_second,
        [
            ("f", 5.0_f64.to_bits()),
            ("h", 42.25_f64.to_bits()),
            ("m", 0.0_f64.to_bits()),
            ("f", 8.0_f64.to_bits())
        ]
    );

    // Different chunk boundaries (A: 2+3, B: 1+4) yield aligned
    // emitted intervals of 1, 1, and 3 positions.
    let aligned_schema = test_schema(&["a", "b"]);
    let a_owner = Float64Array::from(vec![Some(1.0), Some(2.0), Some(3.0), None, Some(5.0)]);
    let b_owner = Float64Array::from(vec![Some(10.0), None, Some(30.0), Some(40.0), Some(50.0)]);
    let a = Samples::try_from_chunks(vec![
        Samples::from_float(a_owner.slice(0, 2)),
        Samples::from_float(a_owner.slice(2, 3)),
    ])
    .unwrap();
    let b = Samples::try_from_chunks(vec![
        Samples::from_float(b_owner.slice(0, 1)),
        Samples::from_float(b_owner.slice(1, 4)),
    ])
    .unwrap();
    let aligned_chunk = RawSeriesChunk::try_new(
        &aligned_schema,
        Buffer::from(vec![0xa1_u8]),
        None,
        RawTimestamps::try_new(vec![101_i64, 102, 103, 104, 105].into()).unwrap(),
        SeriesFields::try_from_vec(vec![a, b]).unwrap(),
        true,
    )
    .unwrap();
    let aligned_frames = pack_raw_series_chunk(
        &aligned_schema,
        FrameLabelMode::Key,
        Some(&dense_description()),
        &aligned_chunk,
    )
    .unwrap();
    assert_eq!(aligned_frames.len(), 1);
    assert_eq!(aligned_frames[0].num_rows(), 3);
    let aligned_rows = unpack_raw_series_frame(
        &aligned_schema,
        FrameLabelMode::Key,
        Some(&dense_description()),
        &aligned_frames[0],
    )
    .unwrap();
    assert_eq!(aligned_rows.len(), 3);
    let mut interval_lengths = Vec::new();
    let mut aligned_times = Vec::new();
    let mut aligned_a = Vec::new();
    let mut aligned_b = Vec::new();
    for (i, row) in aligned_rows.iter().enumerate() {
        assert_eq!(row.is_last, i == aligned_rows.len() - 1);
        interval_lengths.push(row.time.len());
        let start = aligned_times.len();
        let DenseSamplesRef::Float(decoded_a) = row.fields.get(0).unwrap().dense().unwrap() else {
            panic!("expected float field");
        };
        let DenseSamplesRef::Float(decoded_b) = row.fields.get(1).unwrap().dense().unwrap() else {
            panic!("expected float field");
        };
        // Grouping several constituents into one row may copy their payloads, so compare values
        // and validity bit-exactly instead of requiring the source buffers by pointer.
        for (decoded_values, owner) in [(&decoded_a, &a_owner), (&decoded_b, &b_owner)] {
            assert_eq!(decoded_values.len(), row.time.len());
            for position in 0..decoded_values.len() {
                match (
                    decoded_values.is_valid(position),
                    owner.is_valid(start + position),
                ) {
                    (true, true) => assert_eq!(
                        decoded_values.value(position).to_bits(),
                        owner.value(start + position).to_bits(),
                        "grouping changed a value"
                    ),
                    (false, false) => {}
                    _ => panic!("grouping changed validity representation"),
                }
            }
        }
        for position in 0..row.time.len() {
            aligned_times.push(row.time.timestamp(position).unwrap());
            aligned_a.push(match row.fields.get(0).unwrap().sample(position) {
                SampleRef::Missing => None,
                SampleRef::Float(value) => Some(value.to_bits()),
                SampleRef::Histogram(_) => panic!("expected float field"),
            });
            aligned_b.push(match row.fields.get(1).unwrap().sample(position) {
                SampleRef::Missing => None,
                SampleRef::Float(value) => Some(value.to_bits()),
                SampleRef::Histogram(_) => panic!("expected float field"),
            });
        }
    }
    assert_eq!(interval_lengths, [1, 1, 3]);
    assert_eq!(aligned_times, [101, 102, 103, 104, 105]);
    assert_eq!(
        aligned_a,
        [
            Some(1.0_f64.to_bits()),
            Some(2.0_f64.to_bits()),
            Some(3.0_f64.to_bits()),
            None,
            Some(5.0_f64.to_bits())
        ]
    );
    assert_eq!(
        aligned_b,
        [
            Some(10.0_f64.to_bits()),
            None,
            Some(30.0_f64.to_bits()),
            Some(40.0_f64.to_bits()),
            Some(50.0_f64.to_bits())
        ]
    );
    drop(aligned_chunk);
    drop(aligned_frames);

    let mut distinct_rows = frames.clone();
    let alternate_schema = test_schema(&["first", "second"]);
    let first_distinct = Samples::try_from_histogram(
        UInt32Array::from(vec![Some(0)]),
        histogram_batch_with_sum(123.0),
    )
    .unwrap();
    let second_distinct = Samples::try_from_histogram(
        UInt32Array::from(vec![Some(0)]),
        histogram_batch_with_sum(456.0),
    )
    .unwrap();
    let alternate = RawSeriesChunk::try_new(
        &alternate_schema,
        Buffer::from(vec![0xde_u8, 0xad]),
        None,
        RawTimestamps::try_new(vec![100_i64].into()).unwrap(),
        SeriesFields::try_from_vec(vec![first_distinct, second_distinct]).unwrap(),
        false,
    )
    .unwrap();
    distinct_rows.push(
        pack_raw_series_chunk(
            &alternate_schema,
            FrameLabelMode::Key,
            Some(&dense_description()),
            &alternate,
        )
        .unwrap()
        .remove(0),
    );
    let multirow = concat_batches(&frames[0].schema(), &distinct_rows).unwrap();
    let decoded = unpack_raw_series_frame(
        &schema,
        FrameLabelMode::Key,
        Some(&dense_description()),
        &multirow,
    )
    .unwrap();
    assert_eq!(decoded.len(), 5);
    assert_eq!(
        decoded.iter().map(|row| row.is_last).collect::<Vec<_>>(),
        [false, false, false, true, false]
    );
    assert_eq!(
        decoded
            .iter()
            .map(|row| row.time.timestamp(0).unwrap())
            .collect::<Vec<_>>(),
        [10, 20, 30, 40, 100]
    );
    assert_eq!(decoded[4].primary_key.as_slice(), &[0xde, 0xad]);
    assert!(
        matches!(decoded[4].fields.get(0).unwrap().sample(0), SampleRef::Histogram(h) if h.sum.to_bits() == 123.0_f64.to_bits())
    );
    assert!(
        matches!(decoded[4].fields.get(1).unwrap().sample(0), SampleRef::Histogram(h) if h.sum.to_bits() == 456.0_f64.to_bits())
    );
    let sliced = multirow.slice(1, 3);
    let mut altered = sliced.columns().to_vec();
    altered[0] = Arc::new(arrow::array::BinaryArray::from(vec![
        Some(&[0xee][..]),
        Some(&[0xef][..]),
        Some(&[0xed][..]),
    ]));
    let sliced = arrow::record_batch::RecordBatch::try_new(sliced.schema(), altered).unwrap();
    let sliced_rows = unpack_raw_series_frame(
        &schema,
        FrameLabelMode::Key,
        Some(&dense_description()),
        &sliced,
    )
    .unwrap();
    assert_eq!(
        sliced_rows
            .iter()
            .map(|row| row.time.timestamp(0).unwrap())
            .collect::<Vec<_>>(),
        [20, 30, 40]
    );
    assert_eq!(
        sliced_rows
            .iter()
            .map(|row| row.primary_key.as_slice())
            .collect::<Vec<_>>(),
        [&[0xee][..], &[0xef][..], &[0xed][..]]
    );
    assert!(matches!(
        sliced_rows[0].fields.get(0).unwrap().sample(0),
        SampleRef::Missing
    ));
    assert!(matches!(
        sliced_rows[0].fields.get(1).unwrap().sample(0),
        SampleRef::Histogram(h) if h.sum.to_bits() == 42.25_f64.to_bits()
    ));
    assert!(
        matches!(sliced_rows[1].fields.get(0).unwrap().sample(0), SampleRef::Float(v) if v.to_bits() == 3.5_f64.to_bits())
    );
    assert!(matches!(
        sliced_rows[1].fields.get(1).unwrap().sample(0),
        SampleRef::Missing
    ));
    assert!(
        matches!(sliced_rows[2].fields.get(0).unwrap().sample(0), SampleRef::Histogram(h) if h.sum.to_bits() == (-9.5_f64).to_bits())
    );
    assert!(
        matches!(sliced_rows[2].fields.get(1).unwrap().sample(0), SampleRef::Float(v) if v.to_bits() == 8.0_f64.to_bits())
    );

    let not_last = RawSeriesChunk::try_new(
        &schema,
        Buffer::from(vec![0_u8, 0x91, 0xfe]),
        None,
        RawTimestamps::try_new(vec![50_i64, 60, 70, 80].into()).unwrap(),
        chunk.fields.clone(),
        false,
    )
    .unwrap();
    let not_last_frames = pack_raw_series_chunk(
        &schema,
        FrameLabelMode::Key,
        Some(&dense_description()),
        &not_last,
    )
    .unwrap();
    let markers = not_last_frames
        .iter()
        .flat_map(|frame| {
            unpack_raw_series_frame(
                &schema,
                FrameLabelMode::Key,
                Some(&dense_description()),
                frame,
            )
            .unwrap()
        })
        .map(|row| row.is_last)
        .collect::<Vec<_>>();
    assert_eq!(markers, [false; 4]);
}

#[test]
fn histogram_payload_and_present_all_null_representation_survive() {
    let schema = test_schema(&["histogram", "missing_histogram", "missing_float"]);
    let mut builder = HistogramBatchBuilder::new();
    let custom = [1.5, 4.5];
    let po = [1];
    let pl = [2];
    let pb = [3.25, 7.5];
    let no = [-2];
    let nl = [1];
    let nb = [0.75];
    for (sum, start_timestamp) in [(-9.5, Some(-123)), (90.5, None)] {
        builder
            .push(crate::series::histogram::HistogramView {
                schema: -7,
                zero_threshold: 0.125,
                sum,
                count: 17.25,
                zero_count: 2.5,
                reset_hint: CounterResetHint::Unrecognized(37),
                start_timestamp,
                custom_values: &custom,
                positive: BucketSideView {
                    span_offsets: &po,
                    span_lengths: &pl,
                    buckets: &pb,
                },
                negative: BucketSideView {
                    span_offsets: &no,
                    span_lengths: &nl,
                    buckets: &nb,
                },
            })
            .unwrap();
    }
    let table = builder.finish().unwrap();
    let fields = SeriesFields::try_from_vec(vec![
        Samples::try_from_histogram(UInt32Array::from(vec![Some(0), Some(0)]), table.clone())
            .unwrap(),
        Samples::try_from_histogram(UInt32Array::from(vec![None, None]), table.clone()).unwrap(),
        Samples::from_float(Float64Array::new_null(2)),
    ])
    .unwrap();
    let chunk = RawSeriesChunk::try_new(
        &schema,
        Buffer::from(vec![0x12_u8, 0x00]),
        None,
        RawTimestamps::try_new(vec![1_i64, 2].into()).unwrap(),
        fields,
        false,
    )
    .unwrap();
    let frame = pack_raw_series_chunk(
        &schema,
        FrameLabelMode::Key,
        Some(&dense_description()),
        &chunk,
    )
    .unwrap();
    let decoded = unpack_raw_series_frame(
        &schema,
        FrameLabelMode::Key,
        Some(&dense_description()),
        &frame[0],
    )
    .unwrap();
    let DenseSamplesRef::Histogram(histograms) = decoded[0].fields.get(0).unwrap().dense().unwrap()
    else {
        panic!("histogram representation was normalized");
    };
    assert_eq!(histograms.indices().len(), 2);
    assert!(histograms.indices().is_valid(0));
    assert!(histograms.indices().is_valid(1));
    assert_histogram(histograms.view_at(0).unwrap(), -9.5);
    assert_histogram(histograms.view_at(1).unwrap(), -9.5);

    let DenseSamplesRef::Histogram(all_null_histograms) =
        decoded[0].fields.get(1).unwrap().dense().unwrap()
    else {
        panic!("all-null histogram representation was normalized");
    };
    assert_eq!(all_null_histograms.indices().len(), 2);
    assert!((0..2).all(|i| !all_null_histograms.indices().is_valid(i)));
    let DenseSamplesRef::Float(all_null_float) = decoded[0].fields.get(2).unwrap().dense().unwrap()
    else {
        panic!("all-null float representation was normalized");
    };
    assert_eq!(all_null_float.len(), 2);
    assert!((0..2).all(|i| all_null_float.is_null(i)));

    let histogram = match decoded[0].fields.get(0).unwrap().sample(0) {
        SampleRef::Histogram(histogram) => histogram,
        _ => panic!("histogram was lost"),
    };
    assert_histogram(histogram, -9.5);

    let mixed_schema = test_schema(&["mixed"]);
    let table = histogram_batch();
    let mixed = Samples::try_from_mixed(
        Float64Array::new_null(2),
        crate::series::samples::HistogramSamples::try_new(
            UInt32Array::from(vec![None, None]),
            table.clone(),
        )
        .unwrap(),
    )
    .unwrap();
    let mixed_chunk = RawSeriesChunk::try_new(
        &mixed_schema,
        Buffer::from(vec![0x42_u8]),
        None,
        RawTimestamps::try_new(vec![5_i64, 6].into()).unwrap(),
        SeriesFields::one(mixed),
        false,
    )
    .unwrap();
    let mixed_frame = pack_raw_series_chunk(
        &mixed_schema,
        FrameLabelMode::Key,
        Some(&dense_description()),
        &mixed_chunk,
    )
    .unwrap();
    let mixed_decoded = unpack_raw_series_frame(
        &mixed_schema,
        FrameLabelMode::Key,
        Some(&dense_description()),
        &mixed_frame[0],
    )
    .unwrap();
    let DenseSamplesRef::Mixed(mixed) = mixed_decoded[0].fields.get(0).unwrap().dense().unwrap()
    else {
        panic!("all-null mixed representation was normalized");
    };
    assert_eq!(mixed.floats().len(), 2);
    assert_eq!(mixed.histograms().indices().len(), 2);
    assert!(
        (0..2).all(|i| !mixed.floats().is_valid(i) && !mixed.histograms().indices().is_valid(i))
    );
}

#[test]
fn direct_decoder_validates_ordering_for_raw_and_evaluated_batches() {
    let raw_schema = test_schema(&["value"]);
    let raw_frame_schema =
        series_frame_schema(&raw_schema, FrameLabelMode::Key, Some(&dense_description())).unwrap();
    let raw_chunk = float_chunk(&raw_schema, &[1], None, vec![1], vec![Some(2.0)], true);
    let raw_batch = pack_raw_series_chunk(
        &raw_schema,
        FrameLabelMode::Key,
        Some(&dense_description()),
        &raw_chunk,
    )
    .unwrap()
    .remove(0);
    let raw_decoder =
        SeriesFrameDecoder::try_new(&raw_schema, FrameLabelMode::Key, Some(&dense_description()))
            .unwrap();

    let evaluated_schema = SeriesSchema {
        fields: raw_schema.fields.clone(),
        eval_grid: Some(Arc::new(
            crate::series::time::EvalGrid::try_new(
                crate::series::time::GridSpec::try_new(0, 10, 3).unwrap(),
            )
            .unwrap(),
        )),
    };
    let evaluated_frame_schema =
        series_frame_schema(&evaluated_schema, FrameLabelMode::Explicit, None).unwrap();
    let series = Series::try_new(
        &evaluated_schema,
        metric(Some("cpu"), &[]),
        TimeAxis::from_grid_spans(
            evaluated_schema.eval_grid.clone().unwrap(),
            vec![crate::series::time::GridSpan::new(1, 1)],
        )
        .unwrap(),
        SeriesFields::one(Samples::from_float(Float64Array::from(vec![Some(3.0)]))),
    )
    .unwrap();
    let mut builder =
        SeriesFrameBuilder::try_new(&evaluated_schema, FrameLabelMode::Explicit, None, 8, 1024)
            .unwrap();
    let mut cursor = SeriesFrameCursor::try_new_evaluated(&evaluated_schema, series, true).unwrap();
    while cursor.append_to(&mut builder).unwrap().is_some() {}
    let evaluated_batch = builder.flush().unwrap().unwrap();
    let evaluated_decoder =
        SeriesFrameDecoder::try_new(&evaluated_schema, FrameLabelMode::Explicit, None).unwrap();

    let raw_empty = arrow::record_batch::RecordBatch::new_empty(raw_frame_schema);
    let evaluated_empty = arrow::record_batch::RecordBatch::new_empty(evaluated_frame_schema);
    for (content, empty, nonempty) in [
        ("raw", &raw_empty, &raw_batch),
        ("evaluated", &evaluated_empty, &evaluated_batch),
    ] {
        for (ordering, accepted) in [
            (None, false),
            (Some("future_value"), false),
            (Some("interleaved"), false),
            (Some("contiguous"), true),
            (Some("read_key_then_time"), content == "raw"),
        ] {
            for batch in [empty, nonempty] {
                let mut metadata = batch.schema().metadata().clone();
                match ordering {
                    Some(value) => {
                        metadata.insert(ORDERING.to_string(), value.to_string());
                    }
                    None => {
                        metadata.remove(ORDERING);
                    }
                }
                let schema = Arc::new(Schema::new_with_metadata(
                    batch.schema().fields().clone(),
                    metadata,
                ));
                let batch = arrow::record_batch::RecordBatch::try_new(
                    schema.clone(),
                    batch.columns().to_vec(),
                )
                .unwrap();
                let declaration = SeriesFrameDeclaration::parse(schema.as_ref());
                if accepted {
                    assert!(declaration.is_ok(), "{content} ordering {ordering:?}");
                    if content == "raw" {
                        let rows = raw_decoder.decode_raw(&batch).unwrap();
                        assert_eq!(rows.len(), batch.num_rows());
                        if !rows.is_empty() {
                            assert!(rows[0].is_last);
                            assert!(matches!(
                                rows[0].fields.get(0).unwrap().sample(0),
                                SampleRef::Float(value) if value == 2.0
                            ));
                        }
                    } else {
                        let rows = evaluated_decoder.decode_evaluated(&batch).unwrap();
                        assert_eq!(rows.len(), batch.num_rows());
                        if !rows.is_empty() {
                            assert!(rows[0].1);
                            assert!(matches!(
                                rows[0].0.fields.get(0).unwrap().sample(0),
                                SampleRef::Float(value) if value == 3.0
                            ));
                        }
                    }
                } else {
                    assert!(
                        declaration.unwrap_err().to_string().contains("ordering"),
                        "{content} ordering {ordering:?}"
                    );
                    let result = if content == "raw" {
                        raw_decoder.decode_raw(&batch).map(|_| ())
                    } else {
                        evaluated_decoder.decode_evaluated(&batch).map(|_| ())
                    };
                    assert!(
                        result.unwrap_err().to_string().contains("ordering"),
                        "{content} ordering {ordering:?}"
                    );
                }
            }
        }
    }
}

/// The encoder's own declarations are refused when they contradict the frame, and the schema with
/// no fields is refused; an evaluation grid, by contrast, now describes a legal evaluated frame
/// instead of making the schema unusable here.
#[test]
fn rejects_invalid_schema_and_metadata_and_accepts_valid_empty() {
    let schema = test_schema(&["value"]);
    let grid_schema = SeriesSchema {
        fields: schema.fields.clone(),
        eval_grid: Some(Arc::new(
            crate::series::time::EvalGrid::try_new(
                crate::series::time::GridSpec::try_new(1, 2, 3).unwrap(),
            )
            .unwrap(),
        )),
    };
    // A schema with an evaluation grid describes evaluated samples, which is a legal frame in this
    // family: the raw-only constructor still refuses it, while the one constructor accepts it as an
    // evaluated frame that declares its grid and addresses time by grid spans.
    assert!(raw_series_frame_schema(&grid_schema, FrameLabelMode::Explicit).is_err());
    let evaluated = series_frame_schema(&grid_schema, FrameLabelMode::Explicit, None).unwrap();
    assert_eq!(
        evaluated.metadata().get(CONTENT).map(String::as_str),
        Some("evaluated")
    );
    assert_eq!(
        evaluated.metadata().get(TIME).map(String::as_str),
        Some("grid_spans")
    );
    assert!(evaluated.metadata().get(GRID).is_some());
    // An evaluated frame names its series by label, so a key identity stays refused.
    assert!(
        series_frame_schema(
            &grid_schema,
            FrameLabelMode::Key,
            Some(&dense_description())
        )
        .is_err()
    );
    let empty_schema =
        series_frame_schema(&schema, FrameLabelMode::Key, Some(&dense_description())).unwrap();
    let decoder =
        SeriesFrameDecoder::try_new(&schema, FrameLabelMode::Key, Some(&dense_description()))
            .unwrap();
    let empty = arrow::record_batch::RecordBatch::new_empty(empty_schema.clone());
    assert!(
        unpack_raw_series_frame(
            &schema,
            FrameLabelMode::Key,
            Some(&dense_description()),
            &empty
        )
        .unwrap()
        .is_empty()
    );
    let wrong =
        arrow::record_batch::RecordBatch::new_empty(Arc::new(Schema::new(vec![Field::new(
            "read_key",
            DataType::Utf8,
            false,
        )])));
    assert!(
        unpack_raw_series_frame(
            &schema,
            FrameLabelMode::Key,
            Some(&dense_description()),
            &wrong
        )
        .is_err()
    );
    let no_fields = SeriesSchema {
        fields: Arc::from([]),
        eval_grid: None,
    };
    assert!(
        series_frame_schema(&no_fields, FrameLabelMode::Key, Some(&dense_description())).is_err()
    );

    let mut metadata = empty_schema.metadata().clone();
    metadata.insert(
        "greptime.series.content".to_string(),
        "evaluated".to_string(),
    );
    let contaminated = Arc::new(Schema::new_with_metadata(
        empty_schema.fields().clone(),
        metadata,
    ));
    let empty_bad = arrow::record_batch::RecordBatch::new_empty(contaminated);
    assert!(
        unpack_raw_series_frame(
            &schema,
            FrameLabelMode::Key,
            Some(&dense_description()),
            &empty_bad
        )
        .is_err()
    );

    let chunk = float_chunk(&schema, &[1], None, vec![1], vec![Some(2.0)], false);
    assert!(
        pack_raw_series_chunk(
            &grid_schema,
            FrameLabelMode::Key,
            Some(&dense_description()),
            &chunk
        )
        .is_err()
    );

    let valid = pack_raw_series_chunk(
        &schema,
        FrameLabelMode::Key,
        Some(&dense_description()),
        &chunk,
    )
    .unwrap()
    .remove(0);
    let mut extra_metadata = valid.schema().metadata().clone();
    extra_metadata.insert("application.note".to_string(), "ignored".to_string());
    let unrelated = arrow::record_batch::RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(
            valid.schema().fields().clone(),
            extra_metadata,
        )),
        valid.columns().to_vec(),
    )
    .unwrap();
    assert_eq!(
        unpack_raw_series_frame(
            &schema,
            FrameLabelMode::Key,
            Some(&dense_description()),
            &unrelated
        )
        .unwrap()
        .len(),
        1
    );

    let mut wrong_metadata = valid.schema().metadata().clone();
    wrong_metadata.remove("greptime.series.time");
    let wrong = arrow::record_batch::RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(
            valid.schema().fields().clone(),
            wrong_metadata,
        )),
        valid.columns().to_vec(),
    )
    .unwrap();
    assert!(
        unpack_raw_series_frame(
            &schema,
            FrameLabelMode::Key,
            Some(&dense_description()),
            &wrong
        )
        .is_err()
    );

    // A raw frame must not declare an evaluation grid: `greptime.series.grid` belongs to an
    // evaluated frame only.
    let mut grid_metadata = valid.schema().metadata().clone();
    grid_metadata.insert(
        "greptime.series.grid".to_string(),
        "{\"start_ms\":0,\"step_ms\":1,\"len\":3}".to_string(),
    );
    let contaminated = arrow::record_batch::RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(
            valid.schema().fields().clone(),
            grid_metadata,
        )),
        valid.columns().to_vec(),
    )
    .unwrap();
    assert!(
        unpack_raw_series_frame(
            &schema,
            FrameLabelMode::Key,
            Some(&dense_description()),
            &contaminated
        )
        .is_err()
    );

    // A reserved key this revision does not know is refused when the declarations are parsed,
    // rather than travelling along as dead metadata or being silently ignored.
    let mut unknown_key_metadata = valid.schema().metadata().clone();
    unknown_key_metadata.insert("greptime.series.grid.start".to_string(), "0".to_string());
    let unregistered =
        Schema::new_with_metadata(valid.schema().fields().clone(), unknown_key_metadata);
    assert!(series_frame_label_decode(&unregistered).is_err());
    assert!(SeriesFrameDeclaration::parse(&unregistered).is_err());

    for metadata_case in ["wrong_layout", "wrong_labels", "wrong_time"] {
        let mut metadata = valid.schema().metadata().clone();
        match metadata_case {
            "wrong_layout" => {
                // `v2` is the only layout this revision reads, so the previously unreadable `v1`
                // is what a wrong layout is spelled as now.
                metadata.insert("greptime.series.layout".to_string(), "v1".to_string());
            }
            "wrong_labels" => {
                metadata.insert("greptime.series.labels".to_string(), "explicit".to_string());
            }
            _ => {
                metadata.insert("greptime.series.time".to_string(), "grid".to_string());
            }
        }
        let malformed = arrow::record_batch::RecordBatch::try_new(
            Arc::new(Schema::new_with_metadata(
                valid.schema().fields().clone(),
                metadata,
            )),
            valid.columns().to_vec(),
        )
        .unwrap();
        assert!(
            unpack_raw_series_frame(
                &schema,
                FrameLabelMode::Key,
                Some(&dense_description()),
                &malformed
            )
            .is_err()
        );
    }
}

#[test]
fn rejects_invalid_timestamps_and_empty_raw_payloads() {
    assert!(RawTimestamps::try_new(vec![1_i64, 1].into()).is_err());
    assert!(RawTimestamps::try_new(vec![2_i64, 1].into()).is_err());

    let schema = test_schema(&["value"]);
    let chunk = float_chunk(&schema, &[1], None, vec![1], vec![Some(2.0)], false);
    let valid = pack_raw_series_chunk(
        &schema,
        FrameLabelMode::Key,
        Some(&dense_description()),
        &chunk,
    )
    .unwrap()
    .remove(0);
    for bad_timestamps in [vec![], vec![1, 1], vec![2, 1]] {
        let malformed = with_timestamp_values(&valid, bad_timestamps);
        assert!(
            unpack_raw_series_frame(
                &schema,
                FrameLabelMode::Key,
                Some(&dense_description()),
                &malformed
            )
            .is_err()
        );
    }
    let mut empty_columns = valid.columns().to_vec();
    let empty_time = with_timestamp_values(&valid, vec![]);
    empty_columns[2] = empty_time.column(2).clone();
    let fields = valid.columns()[3]
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let payload = fields
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let empty_float_list = ListArray::try_new(
        match payload.fields()[0].data_type() {
            DataType::List(item) => item.clone(),
            _ => unreachable!(),
        },
        OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 0])),
        Arc::new(Float64Array::new_null(0)),
        None,
    )
    .unwrap();
    let empty_payload = StructArray::try_new(
        payload.fields().clone(),
        vec![Arc::new(empty_float_list), payload.column(1).clone()],
        None,
    )
    .unwrap();
    let empty_fields =
        StructArray::try_new(fields.fields().clone(), vec![Arc::new(empty_payload)], None).unwrap();
    empty_columns[3] = Arc::new(empty_fields);
    let empty_time_and_payload =
        arrow::record_batch::RecordBatch::try_new(valid.schema(), empty_columns).unwrap();
    assert!(
        unpack_raw_series_frame(
            &schema,
            FrameLabelMode::Key,
            Some(&dense_description()),
            &empty_time_and_payload
        )
        .is_err()
    );

    let two_samples = float_chunk(
        &schema,
        &[1],
        None,
        vec![1, 2],
        vec![Some(2.0), Some(3.0)],
        false,
    );
    let valid_two_samples = pack_raw_series_chunk(
        &schema,
        FrameLabelMode::Key,
        Some(&dense_description()),
        &two_samples,
    )
    .unwrap()
    .remove(0);
    for bad_timestamps in [vec![1, 1], vec![2, 1]] {
        let malformed = with_timestamp_values(&valid_two_samples, bad_timestamps);
        assert!(
            unpack_raw_series_frame(
                &schema,
                FrameLabelMode::Key,
                Some(&dense_description()),
                &malformed
            )
            .is_err()
        );
    }

    let mut empty_payload_columns = valid.columns().to_vec();
    let empty_time = with_timestamp_values(&valid, vec![]);
    empty_payload_columns[2] = empty_time.column(2).clone();
    let float_fields = valid.columns()[3]
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let float_payload = float_fields
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let empty_float_list = ListArray::try_new(
        match float_payload.fields()[0].data_type() {
            DataType::List(item) => item.clone(),
            _ => unreachable!(),
        },
        OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 0])),
        Arc::new(Float64Array::from(Vec::<Option<f64>>::new())),
        None,
    )
    .unwrap();
    let empty_payload = StructArray::try_new(
        float_payload.fields().clone(),
        vec![Arc::new(empty_float_list), float_payload.column(1).clone()],
        None,
    )
    .unwrap();
    let empty_fields = StructArray::try_new(
        float_fields.fields().clone(),
        vec![Arc::new(empty_payload)],
        None,
    )
    .unwrap();
    empty_payload_columns[3] = Arc::new(empty_fields);
    let empty_payload =
        arrow::record_batch::RecordBatch::try_new(valid.schema(), empty_payload_columns).unwrap();
    assert!(
        unpack_raw_series_frame(
            &schema,
            FrameLabelMode::Key,
            Some(&dense_description()),
            &empty_payload
        )
        .is_err()
    );

    let mut columns = valid.columns().to_vec();
    columns[3] = Arc::new(StructArray::new_null(
        match valid.schema().field(3).data_type() {
            DataType::Struct(fields) => fields.clone(),
            _ => unreachable!(),
        },
        1,
    ));
    // Arrow rejects nulls in this canonical non-nullable top-level field before
    // the decoder can inspect it; keep this a constructor-invariant assertion.
    assert!(arrow::record_batch::RecordBatch::try_new(valid.schema(), columns).is_err());

    let mut bad = valid.columns().to_vec();
    bad[2] = Arc::new(
        with_timestamp_values(&valid, vec![1, 1])
            .column(2)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .clone(),
    );
    let invalid_later_row = arrow::record_batch::RecordBatch::try_new(valid.schema(), bad).unwrap();
    let later_invalid =
        concat_batches(&valid.schema(), &[valid.clone(), invalid_later_row]).unwrap();
    assert!(
        unpack_raw_series_frame(
            &schema,
            FrameLabelMode::Key,
            Some(&dense_description()),
            &later_invalid
        )
        .is_err()
    );
}

#[test]
fn rejects_invalid_explicit_labels() {
    let explicit_schema = test_schema(&["value"]);
    let explicit_chunk = float_chunk(
        &explicit_schema,
        &[1],
        Some(metric(None, &[])),
        vec![1],
        vec![Some(1.0)],
        false,
    );
    let explicit = pack_raw_series_chunk(
        &explicit_schema,
        FrameLabelMode::Explicit,
        None,
        &explicit_chunk,
    )
    .unwrap()
    .remove(0);
    let name = explicit
        .column(4)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .clone();
    let null_label_struct = StructArray::new_null(
        match explicit.schema().field(5).data_type() {
            DataType::List(item) => match item.data_type() {
                DataType::Struct(fields) => fields.clone(),
                _ => unreachable!(),
            },
            _ => unreachable!(),
        },
        1,
    );
    // Arrow rejects null list items because the canonical label item is non-nullable;
    // this malformed value cannot reach the decoder through a safe constructor.
    assert!(
        ListArray::try_new(
            match explicit.schema().field(5).data_type() {
                DataType::List(item) => item.clone(),
                _ => unreachable!(),
            },
            OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 1])),
            Arc::new(null_label_struct),
            None,
        )
        .is_err()
    );
    let mut columns = explicit.columns().to_vec();
    columns[4] = Arc::new(name);

    let mut duplicate_labels = explicit.columns().to_vec();
    let duplicate_items = StructArray::try_new(
        match explicit.schema().field(5).data_type() {
            DataType::List(item) => match item.data_type() {
                DataType::Struct(fields) => fields.clone(),
                _ => unreachable!(),
            },
            _ => unreachable!(),
        },
        vec![
            Arc::new(StringArray::from(vec!["x", "x"])),
            Arc::new(StringArray::from(vec!["1", "2"])),
        ],
        None,
    )
    .unwrap();
    let duplicate_list = ListArray::try_new(
        match explicit.schema().field(5).data_type() {
            DataType::List(item) => item.clone(),
            _ => unreachable!(),
        },
        OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 2])),
        Arc::new(duplicate_items),
        None,
    )
    .unwrap();
    duplicate_labels[5] = Arc::new(duplicate_list);
    let duplicate =
        arrow::record_batch::RecordBatch::try_new(explicit.schema(), duplicate_labels).unwrap();
    assert!(
        unpack_raw_series_frame(&explicit_schema, FrameLabelMode::Explicit, None, &duplicate)
            .is_err()
    );

    let mut reserved_labels = explicit.columns().to_vec();
    let reserved_items = StructArray::try_new(
        match explicit.schema().field(5).data_type() {
            DataType::List(item) => match item.data_type() {
                DataType::Struct(fields) => fields.clone(),
                _ => unreachable!(),
            },
            _ => unreachable!(),
        },
        vec![
            Arc::new(StringArray::from(vec!["__name__"])),
            Arc::new(StringArray::from(vec!["x"])),
        ],
        None,
    )
    .unwrap();
    let reserved_list = ListArray::try_new(
        match explicit.schema().field(5).data_type() {
            DataType::List(item) => item.clone(),
            _ => unreachable!(),
        },
        OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 1])),
        Arc::new(reserved_items),
        None,
    )
    .unwrap();
    reserved_labels[5] = Arc::new(reserved_list);
    let reserved =
        arrow::record_batch::RecordBatch::try_new(explicit.schema(), reserved_labels).unwrap();
    assert!(
        unpack_raw_series_frame(&explicit_schema, FrameLabelMode::Explicit, None, &reserved)
            .is_err()
    );
}

#[test]
fn rejects_malformed_histogram_and_payload_encodings() {
    let hist_schema = test_schema(&["hist"]);
    let valid_hist = pack_raw_series_chunk(
        &hist_schema,
        FrameLabelMode::Key,
        Some(&dense_description()),
        &RawSeriesChunk::try_new(
            &hist_schema,
            Buffer::from(vec![1_u8]),
            None,
            RawTimestamps::try_new(vec![1_i64].into()).unwrap(),
            SeriesFields::one(
                Samples::try_from_histogram(UInt32Array::from(vec![Some(0)]), histogram_batch())
                    .unwrap(),
            ),
            false,
        )
        .unwrap(),
    )
    .unwrap()
    .remove(0);
    let columns = valid_hist.columns().to_vec();
    let fields = columns[3].as_any().downcast_ref::<StructArray>().unwrap();
    let payload = fields
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let null_hist = payload
        .column(1)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap()
        .clone();
    let hidden_indices = ListArray::try_new(
        match hist_schema_frame_field(&hist_schema) {
            DataType::Struct(f) => match f[0].data_type() {
                DataType::Struct(p) => match p[1].data_type() {
                    DataType::Struct(h) => match h[0].data_type() {
                        DataType::List(item) => item.clone(),
                        _ => unreachable!(),
                    },
                    _ => unreachable!(),
                },
                _ => unreachable!(),
            },
            _ => unreachable!(),
        },
        OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 2])),
        Arc::new(UInt32Array::from(vec![Some(99), None])),
        None,
    )
    .unwrap();
    let null_hist = StructArray::try_new(
        null_hist.fields().clone(),
        vec![Arc::new(hidden_indices), null_hist.column(1).clone()],
        Some(arrow::buffer::NullBuffer::new_null(1)),
    )
    .unwrap();
    let visible_float = ListArray::try_new(
        match hist_schema_frame_field(&hist_schema) {
            DataType::Struct(f) => match f[0].data_type() {
                DataType::Struct(p) => match p[0].data_type() {
                    DataType::List(item) => item.clone(),
                    _ => unreachable!(),
                },
                _ => unreachable!(),
            },
            _ => unreachable!(),
        },
        OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 1])),
        Arc::new(Float64Array::from(vec![Some(1.25)])),
        None,
    )
    .unwrap();
    let payload = StructArray::try_new(
        payload.fields().clone(),
        vec![Arc::new(visible_float), Arc::new(null_hist.clone())],
        None,
    )
    .unwrap();
    let fields = StructArray::try_new(
        fields.fields().clone(),
        vec![Arc::new(payload.clone())],
        None,
    )
    .unwrap();
    let mut absent_hist_columns = valid_hist.columns().to_vec();
    absent_hist_columns[3] = Arc::new(fields.clone());
    let absent_hist =
        arrow::record_batch::RecordBatch::try_new(valid_hist.schema(), absent_hist_columns)
            .unwrap();
    let decoded_absent_hist = unpack_raw_series_frame(
        &hist_schema,
        FrameLabelMode::Key,
        Some(&dense_description()),
        &absent_hist,
    )
    .unwrap();
    assert!(
        matches!(decoded_absent_hist[0].fields.get(0).unwrap().sample(0), SampleRef::Float(v) if v == 1.25)
    );

    let mut absent_both_columns = valid_hist.columns().to_vec();
    let absent_both_fields = absent_both_columns[3]
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let absent_both_payload = absent_both_fields
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let absent_both_float = ListArray::try_new(
        match absent_both_payload.fields()[0].data_type() {
            DataType::List(item) => item.clone(),
            _ => unreachable!(),
        },
        OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 1])),
        Arc::new(Float64Array::from(vec![Some(1.25)])),
        Some(arrow::buffer::NullBuffer::new_null(1)),
    )
    .unwrap();
    let absent_both_hist = absent_both_payload
        .column(1)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let absent_both_hist = StructArray::try_new(
        absent_both_hist.fields().clone(),
        absent_both_hist.columns().to_vec(),
        Some(arrow::buffer::NullBuffer::new_null(1)),
    )
    .unwrap();
    let absent_both_payload = StructArray::try_new(
        absent_both_payload.fields().clone(),
        vec![Arc::new(absent_both_float), Arc::new(absent_both_hist)],
        None,
    )
    .unwrap();
    let absent_both_fields = StructArray::try_new(
        absent_both_fields.fields().clone(),
        vec![Arc::new(absent_both_payload)],
        None,
    )
    .unwrap();
    absent_both_columns[3] = Arc::new(absent_both_fields);
    let absent_both =
        arrow::record_batch::RecordBatch::try_new(valid_hist.schema(), absent_both_columns)
            .unwrap();
    assert!(
        unpack_raw_series_frame(
            &hist_schema,
            FrameLabelMode::Key,
            Some(&dense_description()),
            &absent_both
        )
        .is_err()
    );

    let DataType::List(index_item) = null_hist.fields()[0].data_type() else {
        panic!("expected histogram indices list");
    };
    let bad_indices = ListArray::try_new(
        index_item.clone(),
        OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 1])),
        Arc::new(UInt32Array::from(vec![Some(99)])),
        None,
    )
    .unwrap();
    let bad_hist = StructArray::try_new(
        null_hist.fields().clone(),
        vec![Arc::new(bad_indices), null_hist.column(1).clone()],
        None,
    )
    .unwrap();
    let bad_payload = StructArray::try_new(
        payload.fields().clone(),
        vec![payload.column(0).clone(), Arc::new(bad_hist)],
        None,
    )
    .unwrap();
    let bad_fields =
        StructArray::try_new(fields.fields().clone(), vec![Arc::new(bad_payload)], None).unwrap();
    let mut bad_parent_columns = valid_hist.columns().to_vec();
    bad_parent_columns[3] = Arc::new(bad_fields);
    let present_bad_parent =
        arrow::record_batch::RecordBatch::try_new(valid_hist.schema(), bad_parent_columns).unwrap();
    assert!(
        unpack_raw_series_frame(
            &hist_schema,
            FrameLabelMode::Key,
            Some(&dense_description()),
            &present_bad_parent
        )
        .is_err()
    );

    let valid_float = ListArray::try_new(
        match payload.fields()[0].data_type() {
            DataType::List(item) => item.clone(),
            _ => unreachable!(),
        },
        OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 1])),
        Arc::new(Float64Array::from(vec![Some(1.25)])),
        None,
    )
    .unwrap();
    let overlap_indices = ListArray::try_new(
        index_item.clone(),
        OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 1])),
        Arc::new(UInt32Array::from(vec![Some(0)])),
        None,
    )
    .unwrap();
    let overlap_hist = StructArray::try_new(
        null_hist.fields().clone(),
        vec![Arc::new(overlap_indices), null_hist.column(1).clone()],
        None,
    )
    .unwrap();
    let overlap_payload = StructArray::try_new(
        payload.fields().clone(),
        vec![Arc::new(valid_float), Arc::new(overlap_hist)],
        None,
    )
    .unwrap();
    let overlap_fields = StructArray::try_new(
        fields.fields().clone(),
        vec![Arc::new(overlap_payload)],
        None,
    )
    .unwrap();
    let mut overlap_columns = valid_hist.columns().to_vec();
    overlap_columns[3] = Arc::new(overlap_fields);
    let overlap =
        arrow::record_batch::RecordBatch::try_new(valid_hist.schema(), overlap_columns).unwrap();
    assert!(
        unpack_raw_series_frame(
            &hist_schema,
            FrameLabelMode::Key,
            Some(&dense_description()),
            &overlap
        )
        .is_err()
    );

    let short_indices = ListArray::try_new(
        index_item.clone(),
        OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 0])),
        Arc::new(UInt32Array::from(Vec::<Option<u32>>::new())),
        None,
    )
    .unwrap();
    let short_hist = StructArray::try_new(
        null_hist.fields().clone(),
        vec![Arc::new(short_indices), null_hist.column(1).clone()],
        None,
    )
    .unwrap();
    let short_payload = StructArray::try_new(
        payload.fields().clone(),
        vec![payload.column(0).clone(), Arc::new(short_hist)],
        None,
    )
    .unwrap();
    let short_fields =
        StructArray::try_new(fields.fields().clone(), vec![Arc::new(short_payload)], None).unwrap();
    let mut short_columns = valid_hist.columns().to_vec();
    short_columns[3] = Arc::new(short_fields);
    let short_indices =
        arrow::record_batch::RecordBatch::try_new(valid_hist.schema(), short_columns).unwrap();
    assert!(
        unpack_raw_series_frame(
            &hist_schema,
            FrameLabelMode::Key,
            Some(&dense_description()),
            &short_indices
        )
        .is_err()
    );

    let malformed_histogram = || {
        let mut columns = valid_hist.columns().to_vec();
        let fields = columns[3].as_any().downcast_ref::<StructArray>().unwrap();
        let payload = fields
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let hist = payload
            .column(1)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let table_list = hist.column(1).as_any().downcast_ref::<ListArray>().unwrap();
        let table_value = table_list.value(0);
        let table = table_value.as_any().downcast_ref::<StructArray>().unwrap();
        let lengths = ListArray::try_new(
            match table.fields()[9].data_type() {
                DataType::List(item) => item.clone(),
                _ => unreachable!(),
            },
            OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 1, 1])),
            Arc::new(arrow::array::Int32Array::from(vec![1])),
            None,
        )
        .unwrap();
        let buckets = ListArray::try_new(
            match table.fields()[10].data_type() {
                DataType::List(item) => item.clone(),
                _ => unreachable!(),
            },
            OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 2, 5])),
            Arc::new(Float64Array::from(vec![3.25, 7.5, 3.0, 7.0, 8.0])),
            None,
        )
        .unwrap();
        let mut table_columns = table.columns().to_vec();
        table_columns[9] = Arc::new(lengths);
        table_columns[10] = Arc::new(buckets);
        let malformed_table =
            StructArray::try_new(table.fields().clone(), table_columns, None).unwrap();
        let malformed_tables = ListArray::try_new(
            match hist.fields()[1].data_type() {
                DataType::List(item) => item.clone(),
                _ => unreachable!(),
            },
            OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 1])),
            Arc::new(malformed_table),
            None,
        )
        .unwrap();
        let malformed_hist = StructArray::try_new(
            hist.fields().clone(),
            vec![hist.column(0).clone(), Arc::new(malformed_tables)],
            None,
        )
        .unwrap();
        let malformed_payload = StructArray::try_new(
            payload.fields().clone(),
            vec![payload.column(0).clone(), Arc::new(malformed_hist)],
            None,
        )
        .unwrap();
        let malformed_fields = StructArray::try_new(
            fields.fields().clone(),
            vec![Arc::new(malformed_payload)],
            None,
        )
        .unwrap();
        columns[3] = Arc::new(malformed_fields);
        arrow::record_batch::RecordBatch::try_new(valid_hist.schema(), columns).unwrap()
    };
    assert!(
        unpack_raw_series_frame(
            &hist_schema,
            FrameLabelMode::Key,
            Some(&dense_description()),
            &malformed_histogram()
        )
        .is_err()
    );

    let mut hidden_float = valid_hist.columns().to_vec();
    let fields = hidden_float[3]
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let payload = fields
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let float_field = match hist_schema_frame_field(&hist_schema) {
        DataType::Struct(fields) => match fields[0].data_type() {
            DataType::Struct(payload_fields) => payload_fields[0].clone(),
            _ => unreachable!(),
        },
        _ => unreachable!(),
    };
    let floats = ListArray::try_new(
        match float_field.data_type() {
            DataType::List(field) => field.clone(),
            _ => unreachable!(),
        },
        OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 1])),
        Arc::new(Float64Array::from(vec![Some(999.0)])),
        Some(arrow::buffer::NullBuffer::new_null(1)),
    )
    .unwrap();
    let payload = StructArray::try_new(
        payload.fields().clone(),
        vec![Arc::new(floats), payload.column(1).clone()],
        None,
    )
    .unwrap();
    let fields =
        StructArray::try_new(fields.fields().clone(), vec![Arc::new(payload)], None).unwrap();
    hidden_float[3] = Arc::new(fields);
    let absent_float =
        arrow::record_batch::RecordBatch::try_new(valid_hist.schema(), hidden_float).unwrap();
    let decoded_absent_float = unpack_raw_series_frame(
        &hist_schema,
        FrameLabelMode::Key,
        Some(&dense_description()),
        &absent_float,
    )
    .unwrap();
    assert_histogram(
        match decoded_absent_float[0].fields.get(0).unwrap().sample(0) {
            SampleRef::Histogram(histogram) => histogram,
            _ => panic!("valid histogram was masked"),
        },
        -9.5,
    );
}

#[test]
fn decoded_key_and_payload_slices_keep_buffers_alive() {
    let schema = test_schema(&["value"]);
    let (owner, chunk, frames, decoded) = {
        let owner = Float64Array::from(vec![Some(99.0), Some(2.5), None, Some(8.0)]);
        let values = Samples::from_float(owner.slice(1, 2));
        let chunk = RawSeriesChunk::try_new(
            &schema,
            Buffer::from(vec![0xaa_u8, 0x10, 0x20, 0xbb]),
            None,
            RawTimestamps::try_new(vec![0_i64, 11, 22, 33].into())
                .unwrap()
                .slice(1, 2)
                .unwrap(),
            SeriesFields::one(values),
            true,
        )
        .unwrap();
        let frames = pack_raw_series_chunk(
            &schema,
            FrameLabelMode::Key,
            Some(&dense_description()),
            &chunk,
        )
        .unwrap();
        let decoded = unpack_raw_series_frame(
            &schema,
            FrameLabelMode::Key,
            Some(&dense_description()),
            &frames[0],
        )
        .unwrap();
        (owner, chunk, frames, decoded)
    };
    let batch = frames[0].clone();
    let fields = batch
        .column(3)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let field_payload = fields
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let floats = field_payload
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::ListArray>()
        .unwrap()
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let decoded_floats = decoded[0].fields.get(0).unwrap();
    let DenseSamplesRef::Float(source_floats) = chunk.fields.get(0).unwrap().dense().unwrap()
    else {
        panic!()
    };
    let DenseSamplesRef::Float(decoded_floats) = decoded_floats.dense().unwrap() else {
        panic!()
    };
    assert_eq!(decoded_floats.values().as_ptr(), floats.values().as_ptr());
    assert_eq!(
        decoded_floats.values().as_ptr(),
        owner.values().as_ptr().wrapping_add(1)
    );
    assert_eq!(
        decoded_floats.values().as_ptr(),
        source_floats.values().as_ptr()
    );
    let decoded_nulls = decoded_floats.nulls().unwrap();
    let source_nulls = source_floats.nulls().unwrap();
    assert_eq!(
        decoded_nulls.buffer().as_ptr(),
        source_nulls.buffer().as_ptr()
    );
    assert_eq!(decoded_nulls.offset(), source_nulls.offset());
    let time_struct = batch
        .column(2)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let timestamps = time_struct
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::ListArray>()
        .unwrap();
    let time_values = timestamps
        .values()
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    let decoded_time = decoded[0].time.as_buffer();
    assert_eq!(decoded_time.as_ptr(), time_values.values().as_ptr());
    let source_time = chunk.time.as_buffer();
    assert_eq!(decoded_time.as_ptr(), source_time.as_ptr());
    drop(owner);
    drop(chunk);
    drop(frames);
    drop(batch);
    assert_eq!(&decoded_time[..], &[11, 22]);
    assert!(matches!(decoded[0].fields.get(0).unwrap().sample(0), SampleRef::Float(v) if v == 2.5));
    assert!(matches!(
        decoded[0].fields.get(0).unwrap().sample(1),
        SampleRef::Missing
    ));
    assert_eq!(decoded[0].primary_key.as_slice(), &[0xaa, 0x10, 0x20, 0xbb]);
}

#[test]
fn sliced_canonical_mixed_histogram_round_trip_keeps_buffers_alive() {
    let schema = test_schema(&["mixed", "histogram"]);
    let mut builder = HistogramBatchBuilder::new();
    let custom = [1.5, 4.5];
    let po = [1];
    let pl = [2];
    let pb = [3.25, 7.5];
    let no = [-2];
    let nl = [1];
    let nb = [0.75];
    for (sum, start_timestamp) in [(99.5, Some(-123)), (-9.5, Some(-123)), (90.5, None)] {
        builder
            .push(crate::series::histogram::HistogramView {
                schema: -7,
                zero_threshold: 0.125,
                sum,
                count: 17.25,
                zero_count: 2.5,
                reset_hint: CounterResetHint::Unrecognized(37),
                start_timestamp,
                custom_values: &custom,
                positive: BucketSideView {
                    span_offsets: &po,
                    span_lengths: &pl,
                    buckets: &pb,
                },
                negative: BucketSideView {
                    span_offsets: &no,
                    span_lengths: &nl,
                    buckets: &nb,
                },
            })
            .unwrap();
    }
    let original_table = builder.finish().unwrap();
    let source_table = original_table.as_arrow();
    let sliced_table = Arc::new(
        crate::series::histogram::HistogramBatch::try_from_arrow(source_table.slice(1, 2)).unwrap(),
    );
    let indices_owner = UInt32Array::from(vec![Some(1), Some(0), None, Some(1)]);
    let indices = indices_owner.slice(1, 2);
    let histogram_indices_owner = UInt32Array::from(vec![Some(0), Some(1), Some(1), None]);
    let histogram_indices = histogram_indices_owner.slice(1, 2);
    let float_owner = Float64Array::from(vec![
        Some(99.0),
        None,
        Some(f64::from_bits(0x400c_0000_0000_0001)),
        Some(77.0),
    ]);
    let mixed = Samples::try_from_mixed(
        float_owner.slice(1, 2),
        crate::series::samples::HistogramSamples::try_new(indices.clone(), sliced_table.clone())
            .unwrap(),
    )
    .unwrap();
    let histogram =
        Samples::try_from_histogram(histogram_indices.clone(), sliced_table.clone()).unwrap();
    let chunk = RawSeriesChunk::try_new(
        &schema,
        Buffer::from(vec![0x42_u8]),
        None,
        RawTimestamps::try_new(vec![0_i64, 11, 22, 33].into())
            .unwrap()
            .slice(1, 2)
            .unwrap(),
        SeriesFields::try_from_vec(vec![mixed, histogram]).unwrap(),
        true,
    )
    .unwrap();

    let source_indices = match chunk.fields.get(0).unwrap().dense().unwrap() {
        DenseSamplesRef::Mixed(mixed) => mixed.histograms().indices(),
        _ => panic!("expected mixed source"),
    };
    assert_eq!(source_indices.offset(), 0);
    assert_eq!(source_indices.value(0), 0);
    assert!(!source_indices.is_valid(1));
    assert_eq!(
        source_indices.values().as_ptr(),
        indices_owner.values().as_ptr().wrapping_add(1)
    );
    assert_eq!(
        source_indices.nulls().unwrap().buffer().as_ptr(),
        indices_owner.nulls().unwrap().buffer().as_ptr()
    );
    assert_eq!(source_indices.nulls().unwrap().offset(), 1);
    assert_eq!(histogram_indices.value(0), 1);
    assert_eq!(histogram_indices.value(1), 1);
    assert_eq!(
        histogram_indices.values().as_ptr(),
        histogram_indices_owner.values().as_ptr().wrapping_add(1)
    );
    assert_eq!(
        histogram_indices.nulls().unwrap().buffer().as_ptr(),
        histogram_indices_owner.nulls().unwrap().buffer().as_ptr()
    );
    assert_eq!(histogram_indices.nulls().unwrap().offset(), 1);
    assert_eq!(sliced_table.len(), 2);
    let source_sum = source_table
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let sliced_sum = sliced_table
        .as_arrow()
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(
        sliced_sum.values().as_ptr(),
        source_sum.values().as_ptr().wrapping_add(1)
    );
    let source_positive = source_table
        .column(10)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let sliced_positive = sliced_table
        .as_arrow()
        .column(10)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let sliced_table_offset = sliced_table.as_arrow().offset();
    assert_eq!(sliced_positive.offset(), source_positive.offset());
    assert_eq!(
        sliced_positive.value_offsets().as_ptr(),
        source_positive.value_offsets().as_ptr().wrapping_add(1)
    );
    assert_eq!(
        sliced_positive.value_offsets()[sliced_table_offset],
        source_positive.value_offsets()[1]
    );
    assert_eq!(
        sliced_positive.value_offsets()[sliced_table_offset + 1],
        source_positive.value_offsets()[2]
    );
    assert_eq!(
        sliced_positive
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values()
            .as_ptr(),
        source_positive
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values()
            .as_ptr()
    );
    let source_times = chunk.time.as_buffer();

    // One row per batch keeps this fixture's single constituents and their nonzero offsets
    // observable: the default budget could coalesce several rows and copy their payloads.
    let frames = pack_chunk_with_budget(
        &schema,
        FrameLabelMode::Key,
        &chunk,
        1,
        SeriesFrameBuilder::DEFAULT_MAX_BYTES,
    )
    .unwrap();
    assert_eq!(frames.len(), 1);
    let packed = &frames[0];
    let packed_times = packed
        .column(2)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap()
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .values()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(packed_times.values().as_ptr(), source_times.as_ptr());
    assert_eq!(packed_times.values(), source_times);
    let packed_mixed_indices = frame_field_payload(packed, 0)
        .column(1)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap()
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .values()
        .as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap();
    let packed_hist_indices = frame_field_payload(packed, 1)
        .column(1)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap()
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .values()
        .as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap();
    for (packed_indices, source_indices) in [
        (packed_mixed_indices, source_indices),
        (packed_hist_indices, &histogram_indices),
    ] {
        assert_eq!(
            packed_indices.values().as_ptr(),
            source_indices.values().as_ptr()
        );
        assert_eq!(
            packed_indices.nulls().unwrap().buffer().as_ptr(),
            source_indices.nulls().unwrap().buffer().as_ptr()
        );
        assert_eq!(
            packed_indices.nulls().unwrap().offset(),
            source_indices.nulls().unwrap().offset()
        );
    }
    let packed_mixed_table = frame_histogram_table(packed, 0);
    let packed_hist_table = frame_histogram_table(packed, 1);
    for packed_table in [&packed_mixed_table, &packed_hist_table] {
        let packed_sum = packed_table
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(packed_sum.values().as_ptr(), sliced_sum.values().as_ptr());
        let packed_positive = packed_table
            .column(10)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(packed_positive.offset(), sliced_positive.offset());
        assert_eq!(
            packed_positive.value_offsets().as_ptr(),
            sliced_positive.value_offsets().as_ptr()
        );
        assert_eq!(
            packed_positive.value_offsets()[sliced_table_offset],
            sliced_positive.value_offsets()[sliced_table_offset]
        );
        assert_eq!(
            packed_positive.value_offsets()[sliced_table_offset + 1],
            sliced_positive.value_offsets()[sliced_table_offset + 1]
        );
        assert_eq!(
            packed_positive
                .values()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .values()
                .as_ptr(),
            sliced_positive
                .values()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .values()
                .as_ptr()
        );
    }
    let decoded = unpack_raw_series_frame(
        &schema,
        FrameLabelMode::Key,
        Some(&dense_description()),
        packed,
    )
    .unwrap();
    let DenseSamplesRef::Mixed(decoded_mixed) = decoded[0].fields.get(0).unwrap().dense().unwrap()
    else {
        panic!("mixed representation was normalized");
    };
    let DenseSamplesRef::Histogram(decoded_histogram) =
        decoded[0].fields.get(1).unwrap().dense().unwrap()
    else {
        panic!("histogram representation was normalized");
    };
    let decoded_time_values = decoded[0].time.as_buffer();
    assert_eq!(decoded_time_values.as_ptr(), packed_times.values().as_ptr());
    assert_eq!(decoded_time_values, source_times);
    for (decoded_indices, source_indices) in [
        (decoded_mixed.histograms().indices(), source_indices),
        (decoded_histogram.indices(), &histogram_indices),
    ] {
        assert_eq!(
            decoded_indices.values().as_ptr(),
            source_indices.values().as_ptr()
        );
        assert_eq!(
            decoded_indices.nulls().unwrap().buffer().as_ptr(),
            source_indices.nulls().unwrap().buffer().as_ptr()
        );
        assert_eq!(
            decoded_indices.nulls().unwrap().offset(),
            source_indices.nulls().unwrap().offset()
        );
    }
    assert_eq!(
        decoded_mixed.histograms().indices().values().as_ptr(),
        indices_owner.values().as_ptr().wrapping_add(1)
    );
    assert_eq!(
        decoded_histogram.indices().values().as_ptr(),
        histogram_indices_owner.values().as_ptr().wrapping_add(1)
    );
    assert_eq!(decoded_mixed.histograms().indices().value(0), 0);
    assert!(!decoded_mixed.histograms().indices().is_valid(1));
    assert_eq!(
        decoded_mixed.floats().values().as_ptr(),
        float_owner.values().as_ptr().wrapping_add(1)
    );
    assert_eq!(
        decoded_mixed.floats().nulls().unwrap().buffer().as_ptr(),
        float_owner.nulls().unwrap().buffer().as_ptr()
    );
    assert_eq!(decoded_mixed.floats().nulls().unwrap().offset(), 1);
    assert_eq!(decoded_histogram.indices().value(0), 1);
    assert_eq!(decoded_histogram.indices().value(1), 1);
    assert_eq!(decoded_histogram.batch().len(), 2);
    let decoded_table = decoded_histogram.batch().as_arrow();
    let decoded_sum = decoded_table
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(decoded_sum.values().as_ptr(), sliced_sum.values().as_ptr());
    let decoded_mixed_table = decoded_mixed.histograms().batch().as_arrow();
    for decoded_table_view in [decoded_mixed_table, decoded_table] {
        let primitive = decoded_table_view
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(
            primitive.values().as_ptr(),
            source_sum.values().as_ptr().wrapping_add(1)
        );
        let positive = decoded_table_view
            .column(10)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(positive.offset(), source_positive.offset());
        assert_eq!(
            positive.value_offsets()[sliced_table_offset],
            source_positive.value_offsets()[1]
        );
        assert_eq!(
            positive.value_offsets().as_ptr(),
            sliced_positive.value_offsets().as_ptr()
        );
        assert_eq!(
            positive.value_offsets()[sliced_table_offset + 1],
            source_positive.value_offsets()[2]
        );
        assert_eq!(
            positive
                .values()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .values()
                .as_ptr(),
            source_positive
                .values()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .values()
                .as_ptr()
        );
    }
    let decoded_positive_buckets = decoded_table
        .column(10)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let source_positive_buckets = source_table
        .column(10)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(
        decoded_positive_buckets.values().as_ptr(),
        source_positive_buckets.values().as_ptr()
    );

    drop(packed_mixed_table);
    drop(packed_hist_table);
    drop(original_table);
    drop(indices);
    drop(histogram_indices);
    drop(indices_owner);
    drop(histogram_indices_owner);
    drop(float_owner);
    drop(sliced_table);
    drop(chunk);
    drop(frames);

    assert_eq!(decoded_table.len(), 2);
    assert_histogram(decoded_mixed.histograms().batch().view(0).unwrap(), -9.5);
    assert_unreferenced_histogram(decoded_mixed.histograms().batch().view(1).unwrap());
    assert_histogram(decoded_histogram.batch().view(0).unwrap(), -9.5);
    assert_unreferenced_histogram(decoded_histogram.batch().view(1).unwrap());
    assert!(matches!(
        decoded[0].fields.get(0).unwrap().sample(0),
        SampleRef::Histogram(_)
    ));
    assert!(
        matches!(decoded[0].fields.get(0).unwrap().sample(1), SampleRef::Float(v) if v.to_bits() == 0x400c_0000_0000_0001)
    );
}

#[test]
fn malformed_pack_input_is_revalidated_before_encoding() {
    let schema = test_schema(&["value"]);
    let chunk = float_chunk(
        &schema,
        &[1],
        None,
        vec![1, 2],
        vec![Some(3.0), Some(4.0)],
        true,
    );
    let invalid = RawSeriesChunk {
        fields: SeriesFields::one(Samples::from_float(Float64Array::from(vec![Some(3.0)]))),
        ..chunk
    };
    assert!(
        pack_raw_series_chunk(
            &schema,
            FrameLabelMode::Key,
            Some(&dense_description()),
            &invalid
        )
        .is_err()
    );
}

#[test]
fn public_segments_are_flat_ordered_and_retain_empty_dense_kinds() {
    let histogram =
        Samples::try_from_histogram(UInt32Array::from(vec![Some(0)]), histogram_batch()).unwrap();
    let mixed = Samples::try_from_mixed(
        Float64Array::from(vec![Some(2.5)]),
        crate::series::samples::HistogramSamples::try_new(
            UInt32Array::from(vec![None]),
            histogram_batch(),
        )
        .unwrap(),
    )
    .unwrap();
    let empty_float = Samples::from_float(Float64Array::new_null(0));
    let empty_histogram = Samples::try_from_histogram(
        UInt32Array::from(Vec::<Option<u32>>::new()),
        histogram_batch(),
    )
    .unwrap();
    let empty_mixed = Samples::try_from_mixed(
        Float64Array::new_null(0),
        crate::series::samples::HistogramSamples::try_new(
            UInt32Array::from(Vec::<Option<u32>>::new()),
            histogram_batch(),
        )
        .unwrap(),
    )
    .unwrap();

    for (samples, expected_len) in [(&empty_float, 0), (&empty_histogram, 0), (&empty_mixed, 0)] {
        assert_eq!(samples.segments().len(), 1);
        assert_eq!(samples.segments()[0].len(), expected_len);
        assert!(samples.segments()[0].dense().is_some());
    }

    let first_segment_owner = Float64Array::from(vec![1.0, 2.0]);
    let nested = Samples::try_from_chunks(vec![
        Samples::try_from_chunks(vec![
            empty_float.clone(),
            Samples::from_float(first_segment_owner.clone()),
        ])
        .unwrap(),
        empty_histogram.clone(),
        Samples::try_from_chunks(vec![histogram, empty_mixed.clone(), mixed]).unwrap(),
    ])
    .unwrap();
    assert_eq!(nested.len(), 4);
    assert_eq!(nested.segments().len(), 3);
    let DenseSamplesRef::Float(first_segment) = nested.segments()[0].dense().unwrap() else {
        panic!("expected float segment");
    };
    assert_eq!(
        first_segment.values().as_ptr(),
        first_segment_owner.values().as_ptr()
    );
    let mut start = 0;
    let mut starts = Vec::new();
    for segment in nested.segments() {
        starts.push(start);
        assert!(segment.dense().is_some());
        start += segment.len();
    }
    assert_eq!(starts, [0, 2, 3]);
    assert_eq!(start, nested.len());
    assert!(nested.dense().is_none());

    let discarded_empty = Samples::try_from_chunks(vec![
        empty_float.clone(),
        Samples::from_float(Float64Array::from(vec![1.0])),
        empty_histogram.clone(),
        Samples::from_float(Float64Array::from(vec![2.0])),
    ])
    .unwrap();
    assert_eq!(discarded_empty.segments().len(), 2);
    assert_eq!(discarded_empty.segments()[0].len(), 1);
    assert_eq!(discarded_empty.segments()[1].len(), 1);
    let all_empty = Samples::try_from_chunks(vec![empty_histogram, empty_mixed]).unwrap();
    assert_eq!(all_empty.segments().len(), 1);
    assert_eq!(all_empty.segments()[0].len(), 0);
    assert!(matches!(
        all_empty.segments()[0].dense(),
        Some(DenseSamplesRef::Float(_))
    ));
}

#[test]
fn cross_chunk_rows_coalesce_into_one_multirow_batch() {
    let schema = test_schema(&["value"]);
    let first = float_chunk(
        &schema,
        &[0xaa],
        None,
        vec![10, 20],
        vec![Some(1.0), None],
        false,
    );
    let second = float_chunk(
        &schema,
        &[0xbb],
        None,
        vec![30, 40, 50],
        vec![Some(3.0), Some(4.0), None],
        true,
    );
    assert_eq!(SeriesFrameBuilder::DEFAULT_MAX_ROWS, 256);
    assert_eq!(SeriesFrameBuilder::DEFAULT_MAX_BYTES, 1 << 20);
    let expected =
        series_frame_schema(&schema, FrameLabelMode::Key, Some(&dense_description())).unwrap();

    let mut builder = SeriesFrameBuilder::try_new(
        &schema,
        FrameLabelMode::Key,
        Some(&dense_description()),
        SeriesFrameBuilder::DEFAULT_MAX_ROWS,
        SeriesFrameBuilder::DEFAULT_MAX_BYTES,
    )
    .unwrap();
    assert_eq!(builder.schema(), &expected);

    let mut batches = append_chunk(&mut builder, &schema, FrameLabelMode::Key, &first).unwrap();
    batches.extend(append_chunk(&mut builder, &schema, FrameLabelMode::Key, &second).unwrap());
    if let Some(batch) = builder.flush().unwrap() {
        batches.push(batch);
    }
    // Rows of both chunks coalesce into one batch: a partially filled buffer must survive the
    // cursor boundary instead of becoming a batch of its own.
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.schema(), expected);
    assert_eq!(
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["read_key", "is_last", "time", "fields"]
    );
    for (key, value) in [
        ("greptime.series.layout", "v2"),
        ("greptime.series.content", "raw"),
        ("greptime.series.labels", "key"),
        ("greptime.series.time", "timestamps"),
    ] {
        assert_eq!(
            batch.schema().metadata().get(key).map(String::as_str),
            Some(value)
        );
    }

    let keys = batch
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(keys.value(0), &[0xaa]);
    assert_eq!(keys.value(1), &[0xbb]);
    let lasts = batch
        .column(1)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert_eq!(
        (0..2).map(|row| lasts.value(row)).collect::<Vec<_>>(),
        [false, true]
    );
    let times = batch
        .column(2)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap()
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(times.value_offsets(), [0, 2, 5]);
    assert_eq!(
        times
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[10_i64, 20, 30, 40, 50]
    );
    let floats = frame_field_payload(batch, 0)
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(floats.value_offsets(), [0, 2, 5]);
    let float_values = floats
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(
        (0..float_values.len())
            .map(|position| float_values.is_valid(position))
            .collect::<Vec<_>>(),
        [true, false, true, true, false]
    );
    assert_eq!(float_values.value(0).to_bits(), 1.0_f64.to_bits());
    assert_eq!(float_values.value(2).to_bits(), 3.0_f64.to_bits());
    assert_eq!(float_values.value(3).to_bits(), 4.0_f64.to_bits());

    let decoded = decode_batches(&schema, FrameLabelMode::Key, &batches);
    assert_eq!(decoded.len(), 2);
    assert_eq!(decoded[0].primary_key.as_slice(), &[0xaa]);
    assert!(!decoded[0].is_last);
    assert_eq!(
        (0..decoded[0].time.len())
            .map(|position| decoded[0].time.timestamp(position).unwrap())
            .collect::<Vec<_>>(),
        [10, 20]
    );
    assert_float_bits(decoded[0].fields.get(0).unwrap(), &[Some(1.0), None]);
    assert_eq!(decoded[1].primary_key.as_slice(), &[0xbb]);
    assert!(decoded[1].is_last);
    assert_eq!(
        (0..decoded[1].time.len())
            .map(|position| decoded[1].time.timestamp(position).unwrap())
            .collect::<Vec<_>>(),
        [30, 40, 50]
    );
    assert_float_bits(
        decoded[1].fields.get(0).unwrap(),
        &[Some(3.0), Some(4.0), None],
    );
    assert_eq!(decoded.iter().map(|row| row.time.len()).sum::<usize>(), 5);
}

#[test]
fn raw_frame_builder_emits_at_the_row_budget_without_another_cursor() {
    let schema = test_schema(&["value"]);
    let chunks = [
        float_chunk(&schema, &[1], None, vec![1], vec![Some(1.0)], false),
        float_chunk(&schema, &[2], None, vec![2], vec![Some(2.0)], false),
        float_chunk(&schema, &[3], None, vec![3], vec![Some(3.0)], true),
    ];

    for max_rows in [2usize, 3] {
        let mut builder = SeriesFrameBuilder::try_new(
            &schema,
            FrameLabelMode::Key,
            Some(&dense_description()),
            max_rows,
            SeriesFrameBuilder::DEFAULT_MAX_BYTES,
        )
        .unwrap();
        let mut emitted_at = None;
        let mut batches = Vec::new();
        for (index, chunk) in chunks.iter().enumerate() {
            let produced = append_chunk(&mut builder, &schema, FrameLabelMode::Key, chunk).unwrap();
            assert!(produced.len() <= 1, "one cursor emits at most one batch");
            if let Some(batch) = produced.first() {
                assert!(
                    emitted_at.replace(index).is_none(),
                    "the row bound is reached once, without another cursor"
                );
                batches.push(batch.clone());
            }
        }
        if let Some(batch) = builder.flush().unwrap() {
            batches.push(batch);
        }
        // Three one-row chunks emit at the max_rows-th row: a budget of two is reached during the
        // second chunk and a budget of three during the third, so no further cursor is needed.
        assert_eq!(emitted_at, Some(max_rows - 1));
        assert_eq!(batches[0].num_rows(), max_rows);
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            3
        );
        assert!(batches.iter().all(|batch| batch.num_rows() <= max_rows));
        let keys = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(
            (0..max_rows)
                .map(|row| keys.value(row).to_vec())
                .collect::<Vec<_>>(),
            (1..=max_rows)
                .map(|key| vec![key as u8])
                .collect::<Vec<_>>()
        );

        let decoded = decode_batches(&schema, FrameLabelMode::Key, &batches);
        assert_eq!(decoded.len(), 3);
        assert_eq!(
            decoded
                .iter()
                .map(|row| row.time.timestamp(0).unwrap())
                .collect::<Vec<_>>(),
            [1, 2, 3]
        );
        assert_eq!(
            decoded.iter().map(|row| row.is_last).collect::<Vec<_>>(),
            [false, false, true]
        );
    }
}

#[test]
fn raw_frame_builder_rejects_zero_budgets_and_bounds_offsets() {
    let schema = test_schema(&["value"]);
    // Zero in either budget is not a legal configuration; the literals take the parameter's type.
    assert!(
        SeriesFrameBuilder::try_new(
            &schema,
            FrameLabelMode::Key,
            Some(&dense_description()),
            0,
            SeriesFrameBuilder::DEFAULT_MAX_BYTES,
        )
        .is_err()
    );
    assert!(
        SeriesFrameBuilder::try_new(
            &schema,
            FrameLabelMode::Key,
            Some(&dense_description()),
            SeriesFrameBuilder::DEFAULT_MAX_ROWS,
            0,
        )
        .is_err()
    );
    assert!(
        SeriesFrameBuilder::try_new(
            &schema,
            FrameLabelMode::Key,
            Some(&dense_description()),
            0,
            0
        )
        .is_err()
    );
    assert!(
        SeriesFrameBuilder::try_new(
            &schema,
            FrameLabelMode::Key,
            Some(&dense_description()),
            1,
            SeriesFrameBuilder::DEFAULT_MAX_BYTES,
        )
        .is_ok()
    );

    // Cumulative Arrow list offsets are bounded through the shared helper, so the limit is proven
    // without allocating a two-gigabyte fixture.
    assert_eq!(checked_offset(i32::MAX as usize).unwrap(), i32::MAX);
    assert!(checked_offset(i32::MAX as usize + 1).is_err());
    assert!(checked_offset(usize::MAX).is_err());
}

#[test]
fn byte_budget_emits_legal_oversized_rows_without_dropping_them() {
    let schema = test_schema(&["value"]);
    let owner = Float64Array::from(vec![Some(1.0), Some(2.0), Some(3.0)]);
    let samples = Samples::try_from_chunks(
        (0..3)
            .map(|position| Samples::from_float(owner.slice(position, 1)))
            .collect(),
    )
    .unwrap();
    let chunk = RawSeriesChunk::try_new(
        &schema,
        Buffer::from(vec![0x9a_u8]),
        None,
        RawTimestamps::try_new(vec![7_i64, 8, 9].into()).unwrap(),
        SeriesFields::one(samples),
        true,
    )
    .unwrap();

    // A one-byte budget cannot hold any row, yet every legal row is still emitted as its own
    // batch instead of being dropped or merged.
    let batches = pack_chunk_with_budget(
        &schema,
        FrameLabelMode::Key,
        &chunk,
        SeriesFrameBuilder::DEFAULT_MAX_ROWS,
        1,
    )
    .unwrap();
    assert_eq!(batches.len(), 3);
    assert!(
        batches
            .iter()
            .all(|batch| batch.num_rows() == 1 && batch.schema() == batches[0].schema()),
        "an oversized row is emitted alone, never dropped"
    );

    let decoded = decode_batches(&schema, FrameLabelMode::Key, &batches);
    assert_eq!(decoded.len(), 3);
    for (position, row) in decoded.iter().enumerate() {
        assert_eq!(row.primary_key.as_slice(), &[0x9a]);
        assert_eq!(row.time.timestamp(0), Some(7 + position as i64));
        assert_eq!(row.is_last, position == 2);
        assert_float_bits(row.fields.get(0).unwrap(), &[Some(position as f64 + 1.0)]);
    }
}

#[test]
fn byte_budget_flushes_before_exceeding_and_retries_the_pending_row() {
    let schema = test_schema(&["value"]);
    let chunks = [
        float_chunk(&schema, &[1], None, vec![10], vec![Some(1.0)], false),
        float_chunk(&schema, &[2], None, vec![20], vec![Some(2.0)], true),
    ];
    // The bound comes from the real rows instead of a fabricated counter: one byte less than
    // both rows together fits the first row alone, so the second one must not join it.
    let mut measured = Vec::with_capacity(chunks.len());
    for chunk in &chunks {
        let mut cursor =
            SeriesFrameCursor::try_new_raw(&schema, FrameLabelMode::Key, chunk.clone()).unwrap();
        measured.push(cursor.next_row().unwrap().unwrap().bytes);
    }
    let budget = measured[0] + measured[1] - 1;

    let mut builder = SeriesFrameBuilder::try_new(
        &schema,
        FrameLabelMode::Key,
        Some(&dense_description()),
        SeriesFrameBuilder::DEFAULT_MAX_ROWS,
        budget,
    )
    .unwrap();
    let mut first =
        SeriesFrameCursor::try_new_raw(&schema, FrameLabelMode::Key, chunks[0].clone()).unwrap();
    assert!(first.append_to(&mut builder).unwrap().is_none());

    let mut second =
        SeriesFrameCursor::try_new_raw(&schema, FrameLabelMode::Key, chunks[1].clone()).unwrap();
    let emitted = second.append_to(&mut builder).unwrap().unwrap();
    assert_eq!(emitted.num_rows(), 1, "the first row is flushed alone");
    // The second row stays pending across the flush and is retried, never dropped.
    assert!(second.append_to(&mut builder).unwrap().is_none());
    let remainder = builder.flush().unwrap().unwrap();
    assert_eq!(remainder.num_rows(), 1);

    let decoded = decode_batches(&schema, FrameLabelMode::Key, &[emitted, remainder]);
    assert_eq!(decoded.len(), 2);
    assert_eq!(decoded[0].primary_key.as_slice(), &[1]);
    assert_eq!(decoded[1].primary_key.as_slice(), &[2]);
    assert_eq!(decoded[0].time.timestamp(0), Some(10));
    assert_eq!(decoded[1].time.timestamp(0), Some(20));
    assert_eq!(
        decoded.iter().map(|row| row.is_last).collect::<Vec<_>>(),
        [false, true]
    );
    assert_float_bits(decoded[0].fields.get(0).unwrap(), &[Some(1.0)]);
    assert_float_bits(decoded[1].fields.get(0).unwrap(), &[Some(2.0)]);

    // A requested byte bound is clamped to the representable cumulative offset range, and that
    // range is rejected before any child array is allocated.
    let clamped = SeriesFrameBuilder::try_new(
        &schema,
        FrameLabelMode::Key,
        Some(&dense_description()),
        SeriesFrameBuilder::DEFAULT_MAX_ROWS,
        usize::MAX,
    )
    .unwrap();
    assert_eq!(clamped.max_bytes, i32::MAX as usize);
    assert!(offset_buffer(&[i32::MAX as usize, 1]).is_err());
}

#[test]
fn multirow_histogram_rows_keep_row_local_indices_and_tables() {
    let schema = test_schema(&["histogram"]);
    let first_table = histogram_batch();
    let second_table = histogram_batch_with_sum(42.25);
    let first_row =
        Samples::try_from_histogram(UInt32Array::from(vec![Some(0), None]), first_table).unwrap();
    let second_row =
        Samples::try_from_histogram(UInt32Array::from(vec![None, Some(0)]), second_table).unwrap();
    let fields = SeriesFields::one(Samples::try_from_chunks(vec![first_row, second_row]).unwrap());
    let chunk = RawSeriesChunk::try_new(
        &schema,
        Buffer::from(vec![0x5a_u8]),
        None,
        RawTimestamps::try_new(vec![1_i64, 2, 3, 4].into()).unwrap(),
        fields,
        true,
    )
    .unwrap();

    // The public wrapper groups two independent rows into one multirow batch.
    let packed = pack_raw_series_chunk(
        &schema,
        FrameLabelMode::Key,
        Some(&dense_description()),
        &chunk,
    )
    .unwrap();
    assert_eq!(packed.len(), 1);
    assert_eq!(packed[0].num_rows(), 2);

    let (indices, tables) = frame_histogram_lists(&packed[0], 0);
    let encoded_first = indices.value(0);
    let encoded_first = encoded_first
        .as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap();
    assert_eq!(encoded_first.value(0), 0);
    assert!(!encoded_first.is_valid(1));
    let encoded_second = indices.value(1);
    let encoded_second = encoded_second
        .as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap();
    assert!(!encoded_second.is_valid(0));
    assert_eq!(
        encoded_second.value(1),
        0,
        "row-local indices must not be remapped"
    );
    // The second row references its own table through a nonzero slice of the concatenated child.
    assert_eq!(tables.value_offsets(), [0, 2, 4]);
    let concatenated = tables
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(concatenated.len(), 4);

    let decoded = decode_batches(
        &schema,
        FrameLabelMode::Key,
        std::slice::from_ref(&packed[0]),
    );
    assert_row_local_histogram_rows(&decoded);

    // A real Arrow IPC round trip keeps the schema, the whole tables, the null index entries and
    // the bit-exact payloads.
    let mut ipc = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut ipc, &packed[0].schema()).unwrap();
        writer.write(&packed[0]).unwrap();
        writer.finish().unwrap();
    }
    let reader = StreamReader::try_new(std::io::Cursor::new(ipc), None).unwrap();
    let restored = reader.map(|batch| batch.unwrap()).collect::<Vec<_>>();
    assert_eq!(restored.len(), 1);
    assert_eq!(restored[0].schema(), packed[0].schema());
    assert_eq!(restored[0].num_rows(), 2);
    let (restored_indices, restored_tables) = frame_histogram_lists(&restored[0], 0);
    assert_eq!(restored_tables.value_offsets(), tables.value_offsets());
    let restored_second = restored_indices.value(1);
    assert_eq!(
        restored_second
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap()
            .value(1),
        0
    );
    let restored_rows = decode_batches(&schema, FrameLabelMode::Key, &restored);
    assert_row_local_histogram_rows(&restored_rows);
}

#[test]
fn decoder_reuses_cached_schema_and_rejects_schema_evolution() {
    let schema = test_schema(&["value"]);
    let expected =
        series_frame_schema(&schema, FrameLabelMode::Key, Some(&dense_description())).unwrap();
    let first = pack_raw_series_chunk(
        &schema,
        FrameLabelMode::Key,
        Some(&dense_description()),
        &float_chunk(
            &schema,
            &[1],
            None,
            vec![1, 2],
            vec![Some(1.0), Some(2.0)],
            false,
        ),
    )
    .unwrap()
    .remove(0);
    let second = pack_raw_series_chunk(
        &schema,
        FrameLabelMode::Key,
        Some(&dense_description()),
        &float_chunk(&schema, &[2], None, vec![3], vec![Some(3.0)], true),
    )
    .unwrap()
    .remove(0);

    let key_decoder =
        SeriesFrameDecoder::try_new(&schema, FrameLabelMode::Key, Some(&dense_description()))
            .unwrap();
    let decoded = key_decoder.decode_raw(&first).unwrap();
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].primary_key.as_slice(), &[1]);
    assert_eq!(
        match decoded[0].fields.get(0).unwrap().dense().unwrap() {
            crate::series::samples::DenseSamplesRef::Float(values) => values.value(0),
            _ => panic!("expected float samples"),
        },
        1.0
    );
    assert_eq!(
        (0..decoded[0].time.len())
            .map(|position| decoded[0].time.timestamp(position).unwrap())
            .collect::<Vec<_>>(),
        [1, 2]
    );
    // The same decoder accepts a later valid batch before the malformed ones below.
    let decoded = key_decoder.decode_raw(&second).unwrap();
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].primary_key.as_slice(), &[2]);
    assert!(decoded[0].is_last);

    // Unrelated metadata is not part of the raw frame contract.
    let mut unrelated_metadata = second.schema().metadata().clone();
    unrelated_metadata.insert("application.note".to_string(), "ignored".to_string());
    let unrelated = arrow::record_batch::RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(
            expected.fields().clone(),
            unrelated_metadata,
        )),
        second.columns().to_vec(),
    )
    .unwrap();
    assert_eq!(key_decoder.decode_raw(&unrelated).unwrap().len(), 1);

    // Reserved metadata and evolved fields are rejected, including by a zero-row batch.
    for (key, value) in [
        // `v2` is the layout this decoder expects, so the unreadable `v1` is the wrong one.
        ("greptime.series.layout", "v1"),
        ("greptime.series.content", "evaluated"),
        ("greptime.series.labels", "explicit"),
        ("greptime.series.time", "grid"),
    ] {
        let mut metadata = second.schema().metadata().clone();
        metadata.insert(key.to_string(), value.to_string());
        let contaminated = arrow::record_batch::RecordBatch::try_new(
            Arc::new(Schema::new_with_metadata(
                expected.fields().clone(),
                metadata,
            )),
            second.columns().to_vec(),
        )
        .unwrap();
        assert!(
            key_decoder.decode_raw(&contaminated).is_err(),
            "{key} must be rejected"
        );
    }
    let evolved_schema = test_schema(&["other"]);
    let evolved = pack_raw_series_chunk(
        &evolved_schema,
        FrameLabelMode::Key,
        Some(&dense_description()),
        &float_chunk(&evolved_schema, &[3], None, vec![4], vec![Some(4.0)], false),
    )
    .unwrap()
    .remove(0);
    assert!(key_decoder.decode_raw(&evolved).is_err());
    let empty_evolved = arrow::record_batch::RecordBatch::new_empty(
        series_frame_schema(
            &evolved_schema,
            FrameLabelMode::Key,
            Some(&dense_description()),
        )
        .unwrap(),
    );
    assert!(key_decoder.decode_raw(&empty_evolved).is_err());
    // Rejections leave the cached schema usable, and a valid zero-row batch stays valid.
    assert_eq!(key_decoder.decode_raw(&first).unwrap().len(), 1);
    assert!(
        key_decoder
            .decode_raw(&arrow::record_batch::RecordBatch::new_empty(
                expected.clone()
            ))
            .unwrap()
            .is_empty()
    );

    // Explicit mode reuses one decoder for a present but nameless metric and a named one.
    let explicit_decoder =
        SeriesFrameDecoder::try_new(&schema, FrameLabelMode::Explicit, None).unwrap();
    let nameless = pack_raw_series_chunk(
        &schema,
        FrameLabelMode::Explicit,
        None,
        &float_chunk(
            &schema,
            &[7],
            Some(metric(None, &[("zone", "west")])),
            vec![5],
            vec![Some(5.0)],
            true,
        ),
    )
    .unwrap()
    .remove(0);
    let named = pack_raw_series_chunk(
        &schema,
        FrameLabelMode::Explicit,
        None,
        &float_chunk(
            &schema,
            &[8],
            Some(metric(Some("cpu"), &[])),
            vec![6],
            vec![Some(6.0)],
            false,
        ),
    )
    .unwrap()
    .remove(0);
    let decoded = explicit_decoder.decode_raw(&nameless).unwrap();
    let labels = decoded[0].metric.as_ref().unwrap();
    assert_eq!(labels.name(), None);
    assert_eq!(labels.get("zone"), Some("west"));
    assert_eq!(
        explicit_decoder.decode_raw(&named).unwrap()[0]
            .metric
            .as_ref()
            .unwrap()
            .name(),
        Some("cpu")
    );
    // Label modes are not interchangeable.
    assert!(explicit_decoder.decode_raw(&first).is_err());
    assert!(key_decoder.decode_raw(&nameless).is_err());
}

/// A dense description carrying the ordered key columns and the logical tags the decoder reads.
fn dense_description() -> LabelDecodeSpec {
    LabelDecodeSpec {
        version: LABEL_DECODE_VERSION,
        metric_name: "cpu_usage".to_string(),
        encoding: LabelDecodeEncoding::Dense,
        primary_key: vec![
            LabelDecodeColumn {
                column_id: 1,
                name: "host".to_string(),
                physical_type: crate::data_type::ConcreteDataType::string_datatype(),
            },
            LabelDecodeColumn {
                column_id: 2,
                name: "region".to_string(),
                physical_type: crate::data_type::ConcreteDataType::string_datatype(),
            },
        ],
        logical_tags: vec![LabelDecodeTag {
            column_id: 1,
            name: "host".to_string(),
        }],
    }
}

#[test]
fn label_decode_description_roundtrips_through_schema_metadata() {
    let description = dense_description();
    let key = series_frame_schema(
        &test_schema(&["value"]),
        FrameLabelMode::Key,
        Some(&description),
    )
    .unwrap();
    assert_eq!(
        series_frame_label_decode(&key).unwrap(),
        Some(description.clone())
    );
    // The description is metadata-only: the column contract is untouched, so only the
    // `greptime.series.label_decode` text differs from a frame declaring another description.
    let mut other = dense_description();
    other.encoding = LabelDecodeEncoding::Sparse;
    let other =
        series_frame_schema(&test_schema(&["value"]), FrameLabelMode::Key, Some(&other)).unwrap();
    assert_eq!(key.fields(), other.fields());
    assert_ne!(
        key.metadata().get(LABEL_DECODE),
        other.metadata().get(LABEL_DECODE)
    );
    assert_eq!(key.metadata().get(LABELS).map(String::as_str), Some("key"));
}

#[test]
fn explicit_frames_declare_no_label_decode_description() {
    let schema = test_schema(&["value"]);
    let explicit = series_frame_schema(&schema, FrameLabelMode::Explicit, None).unwrap();
    assert!(explicit.metadata().get(LABEL_DECODE).is_none());
    assert_eq!(series_frame_label_decode(&explicit).unwrap(), None);
    // A key frame without a description, or an explicit frame with one, is rejected instead of
    // being written as a frame the consumer cannot decode.
    assert!(
        series_frame_schema(&schema, FrameLabelMode::Key, None).is_err(),
        "key-encoded frames must describe their labels"
    );
    assert!(
        series_frame_schema(
            &schema,
            FrameLabelMode::Explicit,
            Some(&dense_description())
        )
        .is_err(),
        "explicit frames carry complete labels"
    );
}

/// The raw frame's own columns with exactly the supplied metadata map.
fn schema_with_metadata(metadata: Vec<(String, String)>) -> Schema {
    let layout = series_frame_schema(
        &test_schema(&["value"]),
        FrameLabelMode::Key,
        Some(&dense_description()),
    )
    .unwrap();
    Schema::new_with_metadata(layout.fields().clone(), metadata.into_iter().collect())
}

/// The five metadata entries every raw v2 frame declares, with the given label mode.
fn base_metadata(labels: &str) -> Vec<(String, String)> {
    [
        ("greptime.series.layout", "v2"),
        ("greptime.series.content", "raw"),
        ("greptime.series.labels", labels),
        ("greptime.series.time", "timestamps"),
        ("greptime.series.ordering", "contiguous"),
    ]
    .into_iter()
    .map(|(key, value)| (key.to_string(), value.to_string()))
    .collect()
}

/// A description the reader would refuse cannot be written into a frame.
///
/// The three shapes below are each a self-contradictory binding: a duplicate key column, a logical
/// tag naming a column the primary key does not list, and a tag listed twice. Writing one would
/// produce a frame no consumer could decode, so the producer is where it is refused. A frame that
/// arrives already carrying one is a different case, and the decoder rejects those on read.
#[test]
fn label_decode_description_rejects_inconsistent_bindings_on_write() {
    let schema = test_schema(&["value"]);
    let mut cases = Vec::new();

    let mut duplicate_column = dense_description();
    duplicate_column.primary_key[1].column_id = 1;
    cases.push(("duplicate primary-key column", duplicate_column));

    let mut unbound_tag = dense_description();
    unbound_tag.logical_tags[0].column_id = 3;
    cases.push(("logical tag outside the primary key", unbound_tag));

    let mut duplicate_tag = dense_description();
    let tag = duplicate_tag.logical_tags[0].clone();
    duplicate_tag.logical_tags.push(tag);
    cases.push(("duplicate logical tag", duplicate_tag));

    for (label, description) in cases {
        // Both directions reject: a frame is worth writing only if it can be read back, so an
        // inconsistent description is refused at the producer instead of being written faithfully
        // and refused only once it reaches a consumer who cannot fix it. The case that must still
        // reach the wire is a hand-built frame, which the fixture below covers.
        assert!(
            series_frame_schema(&schema, FrameLabelMode::Key, Some(&description)).is_err(),
            "{label}: an inconsistent description is rejected when written"
        );
    }
}

/// A frame declaring no label mode has no label layout to describe. The rest of the parse and
/// validation rejections (unknown version, missing/unknown/wrongly typed fields, malformed JSON,
/// and mode and description disagreeing in either direction) are pinned case for case by
/// [`raw_series_label_decode_metadata_fixture`], so they are not re-asserted here.
#[test]
fn label_decode_rejects_a_frame_without_a_label_mode() {
    let without_mode = base_metadata("key")
        .into_iter()
        .filter(|(key, _)| key != LABELS)
        .collect();
    assert!(
        series_frame_label_decode(&schema_with_metadata(without_mode)).is_err(),
        "a frame without a label mode has no label layout to describe"
    );
}

#[test]
fn raw_series_label_decode_metadata_fixture() {
    use std::collections::HashMap;

    #[derive(serde::Deserialize)]
    struct Case {
        name: String,
        metadata: HashMap<String, String>,
        /// The encoding a decoding consumer expects, or `null` when the fixture must be rejected.
        encoding: Option<String>,
    }

    let cases: Vec<Case> = serde_json::from_str(include_str!(
        "../../../../../tests/compatibility/wire/raw_series_label_decode/v2_metadata.json"
    ))
    .unwrap();
    for case in cases {
        let layout = series_frame_schema(
            &test_schema(&["value"]),
            FrameLabelMode::Key,
            Some(&dense_description()),
        )
        .unwrap();
        let frame_schema =
            Schema::new_with_metadata(layout.fields().clone(), case.metadata.clone());
        match case.encoding.as_deref() {
            None => assert!(
                series_frame_label_decode(&frame_schema).is_err(),
                "{}: must be rejected",
                case.name
            ),
            Some(expected) => {
                let decode = series_frame_label_decode(&frame_schema)
                    .unwrap_or_else(|error| panic!("{}: {error}", case.name))
                    .unwrap_or_else(|| panic!("{}: no description", case.name));
                let encoding = match decode.encoding {
                    LabelDecodeEncoding::Dense => "dense",
                    LabelDecodeEncoding::Sparse => "sparse",
                };
                assert_eq!(encoding, expected, "{}", case.name);
                assert_eq!(decode.version, LABEL_DECODE_VERSION, "{}", case.name);
                assert_eq!(decode.metric_name, "cpu_usage", "{}", case.name);
                assert_eq!(
                    decode.primary_key,
                    vec![
                        LabelDecodeColumn {
                            column_id: 1,
                            name: "host".to_string(),
                            physical_type: crate::data_type::ConcreteDataType::string_datatype(),
                        },
                        LabelDecodeColumn {
                            column_id: 2,
                            name: "shard".to_string(),
                            physical_type: crate::data_type::ConcreteDataType::uint32_datatype(),
                        },
                    ],
                    "{}: the ordered key columns are decoded in place",
                    case.name
                );
                assert_eq!(
                    decode.logical_tags,
                    vec![
                        LabelDecodeTag {
                            column_id: 1,
                            name: "host".to_string(),
                        },
                        LabelDecodeTag {
                            column_id: 2,
                            name: "shard".to_string(),
                        },
                    ],
                    "{}",
                    case.name
                );
                if expected == "dense" {
                    // The fixture text is the encoder's own output, byte for byte: re-encoding the
                    // description it decodes to must reproduce it exactly.
                    let roundtrip = series_frame_schema(
                        &test_schema(&["value"]),
                        FrameLabelMode::Key,
                        Some(&decode),
                    )
                    .unwrap();
                    assert_eq!(
                        roundtrip.metadata().get(LABEL_DECODE),
                        case.metadata.get(LABEL_DECODE),
                        "{}: re-encoding reproduces the fixture text",
                        case.name
                    );
                }
            }
        }
    }
}

/// A tag column typed as a timestamp is described like any other key column: the description
/// carries its physical type, and the text is exactly what the encoder wrote.
#[test]
fn label_decode_description_carries_a_timestamp_typed_tag_column() {
    let mut description = dense_description();
    description.primary_key[1].name = "observed".to_string();
    description.primary_key[1].physical_type =
        crate::data_type::ConcreteDataType::timestamp_millisecond_datatype();
    description.logical_tags.push(LabelDecodeTag {
        column_id: 2,
        name: "observed".to_string(),
    });
    let frame = series_frame_schema(
        &test_schema(&["value"]),
        FrameLabelMode::Key,
        Some(&description),
    )
    .unwrap();
    let text = frame.metadata().get(LABEL_DECODE).unwrap();
    // The expected text is the encoder's real output for a timestamp-typed tag column.
    assert!(
        text.contains("\"physical_type\":{\"Timestamp\":{\"Millisecond\":null}}"),
        "the encoder wrote a timestamp type: {text}"
    );
    let recovered = series_frame_label_decode(&frame).unwrap().unwrap();
    assert_eq!(recovered, description);
    assert_eq!(
        recovered.primary_key[1].physical_type,
        crate::data_type::ConcreteDataType::timestamp_millisecond_datatype()
    );
}

/// Two producers that list the same logical tags in a different order write the same bytes.
///
/// The tag order carries no meaning to a decoder, so the writer sorts its own copy before
/// serializing and leaves the caller's value alone. The encoding order of `primary_key` is a
/// different thing entirely: it is the order of the key columns, so it is written as given.
#[test]
fn label_decode_text_is_independent_of_the_callers_tag_order() {
    let mut ascending = dense_description();
    ascending.logical_tags = vec![
        LabelDecodeTag {
            column_id: 1,
            name: "host".to_string(),
        },
        LabelDecodeTag {
            column_id: 2,
            name: "region".to_string(),
        },
    ];
    let mut descending = ascending.clone();
    descending.logical_tags.reverse();
    // The encoding order of the key columns is not the tag order: reverse it in the second
    // description so a writer that sorted `primary_key` too would be caught below.
    descending.primary_key.reverse();

    let write = |description: &LabelDecodeSpec| {
        let frame = series_frame_schema(
            &test_schema(&["value"]),
            FrameLabelMode::Key,
            Some(description),
        )
        .unwrap();
        frame.metadata().get(LABEL_DECODE).unwrap().clone()
    };

    let same_tags = {
        let mut reordered = ascending.clone();
        reordered.logical_tags.reverse();
        write(&reordered)
    };
    assert_eq!(
        write(&ascending),
        same_tags,
        "two tag orders wrote different bytes"
    );
    assert_eq!(
        ascending.logical_tags,
        vec![
            LabelDecodeTag {
                column_id: 1,
                name: "host".to_string(),
            },
            LabelDecodeTag {
                column_id: 2,
                name: "region".to_string(),
            },
        ],
        "the writer reordered the caller's description"
    );
    // The key columns keep the order they were given, so the two descriptions are different
    // frames rather than the same bytes: that is the part canonicalization must not touch.
    assert_ne!(
        write(&ascending),
        write(&descending),
        "the writer reordered the encoding columns"
    );
    let recovered = series_frame_label_decode(
        &series_frame_schema(
            &test_schema(&["value"]),
            FrameLabelMode::Key,
            Some(&descending),
        )
        .unwrap(),
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        recovered.primary_key, descending.primary_key,
        "the recovered key order is not the encoding order"
    );
}
/// One entry of `v1_physical_types.json`: a [`ConcreteDataType`] variant and the exact text this
/// revision's serde writes for a representative value of it.
#[derive(serde::Deserialize)]
struct FrozenPhysicalType {
    /// The [`ConcreteDataType`] variant, as [`concrete_datatype_variants`] spells it.
    variant: String,
    /// The `serde_json` text this revision writes for that variant's representative value.
    text: String,
}

/// Spells every [`ConcreteDataType`] variant exactly once, together with one representative value of
/// it.
///
/// Everything that has to notice a new variant is derived from this one list:
///
/// - [`variant_name`] matches on it with **no wildcard arm**, so adding a variant to
///   [`ConcreteDataType`] is a compile error until the variant is added here.
/// - [`datatype_representatives`] yields one value per listed variant, so the same edit is what
///   supplies the value the tests read the serde text from.
/// - [`VARIANT_NAMES`] is generated from it too, which is what
///   [`the_frozen_text_covers_every_datatype_variant`] requires a fixture entry for.
macro_rules! concrete_datatype_variants {
    ($($variant:ident => $value:expr),+ $(,)?) => {
        /// The [`ConcreteDataType`] variant a value belongs to, spelled as the fixture names it.
        ///
        /// Exhaustive on purpose: a new variant of [`ConcreteDataType`] makes this match fail to
        /// compile, which is what makes a missing fixture entry impossible to land.
        fn variant_name(data_type: &ConcreteDataType) -> &'static str {
            match data_type {
                $(ConcreteDataType::$variant(_) => stringify!($variant),)+
            }
        }

        /// One representative value per variant, in the order listed above.
        fn datatype_representatives() -> Vec<ConcreteDataType> {
            vec![$($value),+]
        }

        /// Every [`ConcreteDataType`] variant name, in the order listed above.
        const VARIANT_NAMES: &[&str] = &[$(stringify!($variant)),+];
    };
}

concrete_datatype_variants!(
    Null => ConcreteDataType::null_datatype(),
    Boolean => ConcreteDataType::boolean_datatype(),
    Int8 => ConcreteDataType::int8_datatype(),
    Int16 => ConcreteDataType::int16_datatype(),
    Int32 => ConcreteDataType::int32_datatype(),
    Int64 => ConcreteDataType::int64_datatype(),
    UInt8 => ConcreteDataType::uint8_datatype(),
    UInt16 => ConcreteDataType::uint16_datatype(),
    UInt32 => ConcreteDataType::uint32_datatype(),
    UInt64 => ConcreteDataType::uint64_datatype(),
    Float32 => ConcreteDataType::float32_datatype(),
    Float64 => ConcreteDataType::float64_datatype(),
    Decimal128 => ConcreteDataType::decimal128_datatype(10, 2),
    Binary => ConcreteDataType::binary_datatype(),
    String => ConcreteDataType::string_datatype(),
    Date => ConcreteDataType::date_datatype(),
    Timestamp => ConcreteDataType::timestamp_millisecond_datatype(),
    Time => ConcreteDataType::time_microsecond_datatype(),
    Duration => ConcreteDataType::duration_nanosecond_datatype(),
    Interval => ConcreteDataType::interval_month_day_nano_datatype(),
    List => ConcreteDataType::list_datatype(Arc::new(ConcreteDataType::string_datatype())),
    Dictionary => ConcreteDataType::dictionary_datatype(
        ConcreteDataType::int32_datatype(),
        ConcreteDataType::string_datatype(),
    ),
    Struct => ConcreteDataType::struct_datatype(crate::types::StructType::new(Arc::new(vec![
        crate::types::StructField::new("host", ConcreteDataType::string_datatype(), false),
        crate::types::StructField::new("port", ConcreteDataType::uint16_datatype(), true),
    ]))),
    Json => ConcreteDataType::json_datatype(),
    Vector => ConcreteDataType::vector_datatype(3),
);

/// The frozen text of every [`ConcreteDataType`] variant, as a label decode description embeds it.
const FROZEN_PHYSICAL_TYPES: &str = include_str!(
    "../../../../../tests/compatibility/wire/raw_series_label_decode/v1_physical_types.json"
);

fn frozen_physical_types() -> Vec<FrozenPhysicalType> {
    serde_json::from_str(FROZEN_PHYSICAL_TYPES).expect("the frozen physical-type fixture parses")
}

/// The representative value of one variant, named the way the fixture names it.
fn representative(variant: &str) -> ConcreteDataType {
    VARIANT_NAMES
        .iter()
        .zip(datatype_representatives())
        .find(|(name, _)| **name == variant)
        .unwrap_or_else(|| panic!("{variant}: no representative value is listed for this variant"))
        .1
}

/// The text a description embeds for a key column's type is not this module's to spell: it is
/// [`ConcreteDataType`]'s own serde, so it changes whenever that type's serde does.
///
/// `v1_physical_types.json` freezes that text for every variant. This test fails when a variant no
/// longer parses back from its frozen text, when it no longer re-serializes to it, or when the
/// parsed value is no longer the type that variant names — which is what a silent serde change
/// looks like. Since the frame text is compared byte for byte across nodes, such a change is a
/// [`LABEL_DECODE_VERSION`] bump, and this test is the alarm that raises it.
#[test]
fn every_datatype_round_trips_through_its_frozen_text() {
    use crate::data_type::DataType as _;

    for entry in frozen_physical_types() {
        let parsed: ConcreteDataType = serde_json::from_str(&entry.text).unwrap_or_else(|error| {
            panic!("{}: {} cannot be read: {error}", entry.variant, entry.text)
        });
        assert_eq!(
            serde_json::to_string(&parsed).unwrap(),
            entry.text,
            "{}: re-serializes to the frozen text",
            entry.variant
        );
        assert_eq!(
            parsed.name(),
            representative(&entry.variant).name(),
            "{}: the frozen text still names the same type",
            entry.variant
        );
        assert_eq!(
            parsed,
            representative(&entry.variant),
            "{}: the frozen text still describes the same value",
            entry.variant
        );
    }
}

/// The fixture above only freezes a variant if it has an entry, so every variant must have one.
///
/// This test is what makes that hold: [`variant_name`] matches every [`ConcreteDataType`] variant
/// with no wildcard arm, so a new variant does not compile until it is listed in
/// [`concrete_datatype_variants`]; that listing also extends [`VARIANT_NAMES`] and
/// [`datatype_representatives`], and this test then requires a fixture entry for it and rejects an
/// entry naming a variant that does not exist.
#[test]
fn the_frozen_text_covers_every_datatype_variant() {
    let frozen = frozen_physical_types();

    for data_type in datatype_representatives() {
        let variant = variant_name(&data_type);
        let entry = frozen
            .iter()
            .find(|entry| entry.variant == variant)
            .unwrap_or_else(|| {
                panic!("{variant}: v1_physical_types.json has no entry for this datatype variant")
            });
        // The entry's text is this variant's text: read it back and name the variant it describes.
        let parsed: ConcreteDataType = serde_json::from_str(&entry.text)
            .unwrap_or_else(|error| panic!("{variant}: {} is unreadable: {error}", entry.text));
        assert_eq!(
            variant_name(&parsed),
            variant,
            "{variant}: the entry's text describes this variant"
        );
    }

    // Nothing in the fixture names a variant that does not exist, and no variant is listed twice.
    for entry in &frozen {
        assert!(
            VARIANT_NAMES.contains(&entry.variant.as_str()),
            "{}: the fixture names a variant ConcreteDataType does not have",
            entry.variant
        );
    }
    assert_eq!(
        frozen.len(),
        VARIANT_NAMES.len(),
        "one fixture entry per datatype variant, no duplicates"
    );
}
