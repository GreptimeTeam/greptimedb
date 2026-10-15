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
    Array, Float64Array, Int64Array, ListArray, StringArray, StructArray, UInt32Array,
};
use arrow::buffer::{Buffer, OffsetBuffer, ScalarBuffer};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema};

use super::*;
use crate::series::histogram::{BucketSideView, CounterResetHint, HistogramBatchBuilder};
use crate::series::samples::{
    DenseSamplesRef, FieldSpec, SampleRef, Samples, SeriesFields, SeriesSchema,
};
use crate::series::{Label, MetricName};

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

fn metric(name: Option<&str>, labels: &[(&str, &str)]) -> MetricName {
    MetricName::try_new(
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
    metric: Option<MetricName>,
    times: Vec<i64>,
    values: Vec<Option<f64>>,
    is_last: bool,
) -> RawSeriesChunk {
    RawSeriesChunk::try_new(
        schema,
        Buffer::from(key.to_vec()),
        metric,
        RawTimeAxis::try_new(times.into()).unwrap(),
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
    raw_series_frame_schema(schema, RawLabelMode::Key)
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
        RawTimeAxis::try_new((0_i64..64).collect::<Vec<_>>().into()).unwrap(),
        SeriesFields::one(samples),
        true,
    )
    .unwrap();

    let frames = pack_raw_series_chunk(&schema, RawLabelMode::Key, &chunk).unwrap();
    assert_eq!(frames.len(), 64);
    let mut decoded_chunks = Vec::with_capacity(frames.len());
    for frame in &frames {
        let decoded = unpack_raw_series_frame(&schema, RawLabelMode::Key, frame).unwrap();
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

    for mode in [RawLabelMode::Key, RawLabelMode::Explicit] {
        let metric = (mode == RawLabelMode::Explicit).then(|| metric(Some("host"), &[]));
        let chunk = RawSeriesChunk::try_new(
            &schema,
            Buffer::from(vec![0xaa_u8]),
            metric,
            RawTimeAxis::try_new(vec![100, 200].into()).unwrap(),
            fields.clone(),
            true,
        )
        .unwrap();
        let batch = &pack_raw_series_chunk(&schema, mode, &chunk).unwrap()[0];
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

        let decoded = unpack_raw_series_frame(&schema, mode, batch).unwrap();
        assert_eq!(decoded[0].read_key.as_slice(), &[0xaa]);
        assert!(decoded[0].is_last);
        let RawTimeAxis::Explicit(times) = &decoded[0].time;
        assert_eq!(times.as_ref(), &[100, 200]);
        assert_float_bits(decoded[0].fields.get(0).unwrap(), &[Some(1.0), Some(2.0)]);
        assert_float_bits(decoded[0].fields.get(1).unwrap(), &[Some(10.0), Some(20.0)]);

        assert!(
            unpack_raw_series_frame(&test_schema(&["cpu_idle", "cpu_usage"]), mode, batch).is_err()
        );
        assert!(
            unpack_raw_series_frame(&test_schema(&["cpu_usage", "cpu_busy"]), mode, batch).is_err()
        );
    }
}

#[test]
fn schema_and_key_explicit_round_trips_preserve_identity_and_bits() {
    let schema = test_schema(&["value"]);
    let key_schema = raw_series_frame_schema(&schema, RawLabelMode::Key).unwrap();
    let explicit_schema = raw_series_frame_schema(&schema, RawLabelMode::Explicit).unwrap();
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
    expected_key_metadata.insert("greptime.series.layout".to_string(), "v1".to_string());
    expected_key_metadata.insert("greptime.series.content".to_string(), "raw".to_string());
    expected_key_metadata.insert("greptime.series.labels".to_string(), "key".to_string());
    expected_key_metadata.insert("greptime.series.time".to_string(), "timestamps".to_string());
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
    let mut explicit_fields = expected_key.fields().iter().cloned().collect::<Vec<_>>();
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
    let packed = pack_raw_series_chunk(&schema, RawLabelMode::Key, &native).unwrap();
    assert_eq!(packed.len(), 1);
    let decoded = unpack_raw_series_frame(&schema, RawLabelMode::Key, &packed[0]).unwrap();
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].read_key.as_slice(), &[0, 0xff, 0, 0x80]);
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
    let packed = pack_raw_series_chunk(&schema, RawLabelMode::Explicit, &explicit).unwrap();
    let decoded = unpack_raw_series_frame(&schema, RawLabelMode::Explicit, &packed[0]).unwrap();
    assert_eq!(decoded[0].read_key.as_slice(), &[0x80, 0, 0xff]);
    assert!(!decoded[0].is_last);
    let labels = decoded[0].metric.as_ref().unwrap();
    assert_eq!(labels.name(), None);
    assert_eq!(labels.get("rack"), Some(""));
    assert_eq!(labels.get("zone"), Some("west"));
    assert!(pack_raw_series_chunk(&schema, RawLabelMode::Key, &explicit).is_err());
    assert!(pack_raw_series_chunk(&schema, RawLabelMode::Explicit, &native).is_err());
    assert!(unpack_raw_series_frame(&schema, RawLabelMode::Key, &packed[0]).is_err());

    let key_packed = pack_raw_series_chunk(&schema, RawLabelMode::Key, &native).unwrap();
    assert!(unpack_raw_series_frame(&schema, RawLabelMode::Explicit, &key_packed[0]).is_err());
    for name in [None, Some(""), Some("cpu")] {
        let named = float_chunk(
            &schema,
            &[0xff, 0, 0xfe],
            Some(metric(name, &[("empty", "")])),
            vec![9],
            vec![Some(2.0)],
            false,
        );
        let encoded = pack_raw_series_chunk(&schema, RawLabelMode::Explicit, &named).unwrap();
        let restored =
            unpack_raw_series_frame(&schema, RawLabelMode::Explicit, &encoded[0]).unwrap();
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
        RawTimeAxis::try_new(vec![10_i64, 20, 30, 40].into()).unwrap(),
        SeriesFields::try_from_vec(vec![first, second]).unwrap(),
        true,
    )
    .unwrap();
    let frames = pack_raw_series_chunk(&schema, RawLabelMode::Key, &chunk).unwrap();
    assert_eq!(frames.len(), 4);
    let mut observed_times = Vec::new();
    let mut observed_first = Vec::new();
    let mut observed_second = Vec::new();
    let mut observed_keys = Vec::new();
    let mut observed_markers = Vec::new();
    for (frame_index, frame) in frames.iter().enumerate() {
        let decoded = unpack_raw_series_frame(&schema, RawLabelMode::Key, frame).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].is_last, frame_index == frames.len() - 1);
        observed_markers.push(decoded[0].is_last);
        assert_eq!(decoded[0].read_key.as_slice(), &[0, 0x91, 0xfe]);
        observed_keys.push(decoded[0].read_key.as_slice().to_vec());
        for position in 0..decoded[0].time.len() {
            observed_times.push(decoded[0].time.timestamp(position).unwrap());
            match decoded[0].fields.get(0).unwrap().sample(position) {
                SampleRef::Missing => observed_first.push(("m", 0.0_f64.to_bits())),
                SampleRef::Float(value) => observed_first.push(("f", value.to_bits())),
                SampleRef::Histogram(histogram) => {
                    assert_histogram(histogram, -9.5);
                    observed_first.push(("h", histogram.sum.to_bits()));
                }
            }
            match decoded[0].fields.get(1).unwrap().sample(position) {
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
        RawTimeAxis::try_new(vec![101_i64, 102, 103, 104, 105].into()).unwrap(),
        SeriesFields::try_from_vec(vec![a, b]).unwrap(),
        true,
    )
    .unwrap();
    let aligned_frames =
        pack_raw_series_chunk(&aligned_schema, RawLabelMode::Key, &aligned_chunk).unwrap();
    let mut interval_lengths = Vec::new();
    let mut aligned_times = Vec::new();
    let mut aligned_a = Vec::new();
    let mut aligned_b = Vec::new();
    for (i, batch) in aligned_frames.iter().enumerate() {
        let decoded = unpack_raw_series_frame(&aligned_schema, RawLabelMode::Key, batch).unwrap();
        assert_eq!(decoded[0].is_last, i == 2);
        interval_lengths.push(decoded[0].time.len());
        let start = aligned_times.len();
        let DenseSamplesRef::Float(decoded_a) = decoded[0].fields.get(0).unwrap().dense().unwrap()
        else {
            panic!("expected float field");
        };
        let DenseSamplesRef::Float(decoded_b) = decoded[0].fields.get(1).unwrap().dense().unwrap()
        else {
            panic!("expected float field");
        };
        assert_eq!(
            decoded_a.values().as_ptr(),
            a_owner.values().as_ptr().wrapping_add(start)
        );
        assert_eq!(
            decoded_b.values().as_ptr(),
            b_owner.values().as_ptr().wrapping_add(start)
        );
        for (decoded_values, owner) in [(&decoded_a, &a_owner), (&decoded_b, &b_owner)] {
            match (decoded_values.nulls(), owner.nulls()) {
                (Some(decoded_nulls), Some(owner_nulls)) => {
                    assert_eq!(
                        decoded_nulls.buffer().as_ptr(),
                        owner_nulls.buffer().as_ptr()
                    );
                    assert_eq!(decoded_nulls.offset(), owner_nulls.offset() + start);
                }
                (None, None) => {}
                _ => panic!("fragment changed validity representation"),
            }
        }
        for position in 0..decoded[0].time.len() {
            aligned_times.push(decoded[0].time.timestamp(position).unwrap());
            aligned_a.push(match decoded[0].fields.get(0).unwrap().sample(position) {
                SampleRef::Missing => None,
                SampleRef::Float(value) => Some(value.to_bits()),
                SampleRef::Histogram(_) => panic!("expected float field"),
            });
            aligned_b.push(match decoded[0].fields.get(1).unwrap().sample(position) {
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
        RawTimeAxis::try_new(vec![100_i64].into()).unwrap(),
        SeriesFields::try_from_vec(vec![first_distinct, second_distinct]).unwrap(),
        false,
    )
    .unwrap();
    distinct_rows.push(
        pack_raw_series_chunk(&alternate_schema, RawLabelMode::Key, &alternate)
            .unwrap()
            .remove(0),
    );
    let multirow = concat_batches(&frames[0].schema(), &distinct_rows).unwrap();
    let decoded = unpack_raw_series_frame(&schema, RawLabelMode::Key, &multirow).unwrap();
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
    assert_eq!(decoded[4].read_key.as_slice(), &[0xde, 0xad]);
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
    let sliced_rows = unpack_raw_series_frame(&schema, RawLabelMode::Key, &sliced).unwrap();
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
            .map(|row| row.read_key.as_slice())
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
        RawTimeAxis::try_new(vec![50_i64, 60, 70, 80].into()).unwrap(),
        chunk.fields.clone(),
        false,
    )
    .unwrap();
    let not_last_frames = pack_raw_series_chunk(&schema, RawLabelMode::Key, &not_last).unwrap();
    let markers = not_last_frames
        .iter()
        .flat_map(|frame| unpack_raw_series_frame(&schema, RawLabelMode::Key, frame).unwrap())
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
        RawTimeAxis::try_new(vec![1_i64, 2].into()).unwrap(),
        fields,
        false,
    )
    .unwrap();
    let frame = pack_raw_series_chunk(&schema, RawLabelMode::Key, &chunk).unwrap();
    let decoded = unpack_raw_series_frame(&schema, RawLabelMode::Key, &frame[0]).unwrap();
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
        RawTimeAxis::try_new(vec![5_i64, 6].into()).unwrap(),
        SeriesFields::one(mixed),
        false,
    )
    .unwrap();
    let mixed_frame =
        pack_raw_series_chunk(&mixed_schema, RawLabelMode::Key, &mixed_chunk).unwrap();
    let mixed_decoded =
        unpack_raw_series_frame(&mixed_schema, RawLabelMode::Key, &mixed_frame[0]).unwrap();
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
    assert!(raw_series_frame_schema(&grid_schema, RawLabelMode::Key).is_err());
    let empty_schema = raw_series_frame_schema(&schema, RawLabelMode::Key).unwrap();
    let empty = arrow::record_batch::RecordBatch::new_empty(empty_schema.clone());
    assert!(
        unpack_raw_series_frame(&schema, RawLabelMode::Key, &empty)
            .unwrap()
            .is_empty()
    );
    let wrong =
        arrow::record_batch::RecordBatch::new_empty(Arc::new(Schema::new(vec![Field::new(
            "read_key",
            DataType::Utf8,
            false,
        )])));
    assert!(unpack_raw_series_frame(&schema, RawLabelMode::Key, &wrong).is_err());
    let no_fields = SeriesSchema {
        fields: Arc::from([]),
        eval_grid: None,
    };
    assert!(raw_series_frame_schema(&no_fields, RawLabelMode::Key).is_err());

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
    assert!(unpack_raw_series_frame(&schema, RawLabelMode::Key, &empty_bad).is_err());

    let chunk = float_chunk(&schema, &[1], None, vec![1], vec![Some(2.0)], false);
    assert!(pack_raw_series_chunk(&grid_schema, RawLabelMode::Key, &chunk).is_err());

    let valid = pack_raw_series_chunk(&schema, RawLabelMode::Key, &chunk)
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
        unpack_raw_series_frame(&schema, RawLabelMode::Key, &unrelated)
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
    assert!(unpack_raw_series_frame(&schema, RawLabelMode::Key, &wrong).is_err());

    let mut grid_metadata = valid.schema().metadata().clone();
    grid_metadata.insert("greptime.series.grid.start".to_string(), "0".to_string());
    let contaminated = arrow::record_batch::RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(
            valid.schema().fields().clone(),
            grid_metadata,
        )),
        valid.columns().to_vec(),
    )
    .unwrap();
    assert!(unpack_raw_series_frame(&schema, RawLabelMode::Key, &contaminated).is_err());

    for metadata_case in ["wrong_layout", "wrong_labels", "wrong_time"] {
        let mut metadata = valid.schema().metadata().clone();
        match metadata_case {
            "wrong_layout" => {
                metadata.insert("greptime.series.layout".to_string(), "v2".to_string());
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
        assert!(unpack_raw_series_frame(&schema, RawLabelMode::Key, &malformed).is_err());
    }
}

#[test]
fn rejects_invalid_timestamps_and_empty_raw_payloads() {
    let schema = test_schema(&["value"]);
    let chunk = float_chunk(&schema, &[1], None, vec![1], vec![Some(2.0)], false);
    let valid = pack_raw_series_chunk(&schema, RawLabelMode::Key, &chunk)
        .unwrap()
        .remove(0);
    for bad_timestamps in [vec![], vec![1, 1], vec![2, 1]] {
        let malformed = with_timestamp_values(&valid, bad_timestamps);
        assert!(unpack_raw_series_frame(&schema, RawLabelMode::Key, &malformed).is_err());
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
    assert!(unpack_raw_series_frame(&schema, RawLabelMode::Key, &empty_time_and_payload).is_err());

    let two_samples = float_chunk(
        &schema,
        &[1],
        None,
        vec![1, 2],
        vec![Some(2.0), Some(3.0)],
        false,
    );
    let valid_two_samples = pack_raw_series_chunk(&schema, RawLabelMode::Key, &two_samples)
        .unwrap()
        .remove(0);
    for bad_timestamps in [vec![1, 1], vec![2, 1]] {
        let malformed = with_timestamp_values(&valid_two_samples, bad_timestamps);
        assert!(unpack_raw_series_frame(&schema, RawLabelMode::Key, &malformed).is_err());
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
    assert!(unpack_raw_series_frame(&schema, RawLabelMode::Key, &empty_payload).is_err());

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
    assert!(unpack_raw_series_frame(&schema, RawLabelMode::Key, &later_invalid).is_err());
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
    let explicit = pack_raw_series_chunk(&explicit_schema, RawLabelMode::Explicit, &explicit_chunk)
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
    assert!(unpack_raw_series_frame(&explicit_schema, RawLabelMode::Explicit, &duplicate).is_err());

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
    assert!(unpack_raw_series_frame(&explicit_schema, RawLabelMode::Explicit, &reserved).is_err());
}

#[test]
fn rejects_malformed_histogram_and_payload_encodings() {
    let hist_schema = test_schema(&["hist"]);
    let valid_hist = pack_raw_series_chunk(
        &hist_schema,
        RawLabelMode::Key,
        &RawSeriesChunk::try_new(
            &hist_schema,
            Buffer::from(vec![1_u8]),
            None,
            RawTimeAxis::try_new(vec![1_i64].into()).unwrap(),
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
    let decoded_absent_hist =
        unpack_raw_series_frame(&hist_schema, RawLabelMode::Key, &absent_hist).unwrap();
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
    assert!(unpack_raw_series_frame(&hist_schema, RawLabelMode::Key, &absent_both).is_err());

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
    assert!(unpack_raw_series_frame(&hist_schema, RawLabelMode::Key, &present_bad_parent).is_err());

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
    assert!(unpack_raw_series_frame(&hist_schema, RawLabelMode::Key, &overlap).is_err());

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
    assert!(unpack_raw_series_frame(&hist_schema, RawLabelMode::Key, &short_indices).is_err());

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
        unpack_raw_series_frame(&hist_schema, RawLabelMode::Key, &malformed_histogram()).is_err()
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
    let decoded_absent_float =
        unpack_raw_series_frame(&hist_schema, RawLabelMode::Key, &absent_float).unwrap();
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
            RawTimeAxis::try_new(vec![0_i64, 11, 22, 33].into())
                .unwrap()
                .slice(1, 2)
                .unwrap(),
            SeriesFields::one(values),
            true,
        )
        .unwrap();
        let frames = pack_raw_series_chunk(&schema, RawLabelMode::Key, &chunk).unwrap();
        let decoded = unpack_raw_series_frame(&schema, RawLabelMode::Key, &frames[0]).unwrap();
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
    let crate::series::raw::RawTimeAxis::Explicit(decoded_time) = &decoded[0].time;
    assert_eq!(decoded_time.as_ptr(), time_values.values().as_ptr());
    let crate::series::raw::RawTimeAxis::Explicit(source_time) = &chunk.time;
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
    assert_eq!(decoded[0].read_key.as_slice(), &[0xaa, 0x10, 0x20, 0xbb]);
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
        RawTimeAxis::try_new(vec![0_i64, 11, 22, 33].into())
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
    let crate::series::raw::RawTimeAxis::Explicit(source_times) = &chunk.time;

    let frames = pack_raw_series_chunk(&schema, RawLabelMode::Key, &chunk).unwrap();
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
    let decoded = unpack_raw_series_frame(&schema, RawLabelMode::Key, packed).unwrap();
    let DenseSamplesRef::Mixed(decoded_mixed) = decoded[0].fields.get(0).unwrap().dense().unwrap()
    else {
        panic!("mixed representation was normalized");
    };
    let DenseSamplesRef::Histogram(decoded_histogram) =
        decoded[0].fields.get(1).unwrap().dense().unwrap()
    else {
        panic!("histogram representation was normalized");
    };
    let crate::series::raw::RawTimeAxis::Explicit(decoded_time_values) = &decoded[0].time;
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
    assert!(pack_raw_series_chunk(&schema, RawLabelMode::Key, &invalid).is_err());
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
