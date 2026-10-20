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

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::task::{Context, Poll};

use common_error::ext::BoxedError;
use common_query::series_frame::{
    ensure_frame_matches_label_source, pack_raw_series, raw_series_frame_ordering,
    unpack_raw_series,
};
use datatypes::arrow::array::{Array, Float64Array, ListArray, UInt32Array};
use datatypes::arrow::buffer::{Buffer, ScalarBuffer};
use datatypes::series::SeriesOrdering;
use datatypes::series::histogram::{
    BucketSideView, CounterResetHint, HistogramBatch, HistogramBatchBuilder, HistogramView,
};
use datatypes::series::raw::{RawSeriesChunk, RawTimestamps};
use datatypes::series::samples::{
    DenseSamplesRef, FieldSpec, HistogramSamples, SampleRef, Samples, SeriesFields, SeriesSchema,
};
use futures::{Stream, StreamExt, TryStreamExt};
use store_api::region_engine::{RawLabelSource, RawSeriesStream, SendableRawSeriesStream};

use super::{label_source, read_key};
use crate::source::{materialize_raw_labels, merge_ordered_raw_series};

fn payload_schema() -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: vec!["temperature", "latency", "observations"]
            .into_iter()
            .map(|name| FieldSpec {
                name: Arc::from(name),
            })
            .collect::<Vec<_>>()
            .into(),
        eval_grid: None,
    })
}

fn histograms(sums: &[f64]) -> Arc<HistogramBatch> {
    let mut builder = HistogramBatchBuilder::new();
    for &sum in sums {
        builder
            .push(HistogramView {
                schema: -19,
                zero_threshold: 0.125,
                sum,
                count: 23.5,
                zero_count: 2.25,
                reset_hint: CounterResetHint::Unrecognized(37),
                start_timestamp: Some(-456),
                custom_values: &[1.5, 4.5],
                positive: BucketSideView {
                    span_offsets: &[1],
                    span_lengths: &[2],
                    buckets: &[3.25, 7.5],
                },
                negative: BucketSideView {
                    span_offsets: &[-2],
                    span_lengths: &[1],
                    buckets: &[0.75],
                },
            })
            .unwrap();
    }
    builder.finish().unwrap()
}

fn payload_chunk(
    key: Buffer,
    times: &[i64],
    floats: &[Option<f64>],
    latency: &[Option<f64>],
    mixed_sums: &[Option<f64>],
    histogram_sums: &[Option<f64>],
    last: bool,
) -> RawSeriesChunk {
    assert_eq!(times.len(), floats.len());
    assert_eq!(times.len(), latency.len());
    assert_eq!(times.len(), mixed_sums.len());
    assert_eq!(times.len(), histogram_sums.len());
    let mixed_floats = Float64Array::from(latency.to_vec());
    let mut mixed_index = 0;
    let mixed_indices = UInt32Array::from(
        mixed_sums
            .iter()
            .map(|sum| {
                sum.map(|_| {
                    let index = mixed_index;
                    mixed_index += 1;
                    index
                })
            })
            .collect::<Vec<_>>(),
    );
    let mixed = Samples::try_from_mixed(
        mixed_floats,
        HistogramSamples::try_new(
            mixed_indices,
            histograms(&mixed_sums.iter().flatten().copied().collect::<Vec<_>>()),
        )
        .unwrap(),
    )
    .unwrap();
    let mixed = if times.len() > 1 {
        Samples::try_from_chunks(vec![
            mixed.slice(0, 1).unwrap(),
            mixed.slice(1, times.len() - 1).unwrap(),
        ])
        .unwrap()
    } else {
        mixed
    };
    let mut histogram_index = 0;
    let histogram_indices = UInt32Array::from(
        histogram_sums
            .iter()
            .map(|sum| {
                sum.map(|_| {
                    let index = histogram_index;
                    histogram_index += 1;
                    index
                })
            })
            .collect::<Vec<_>>(),
    );
    let fields = SeriesFields::try_from_vec(vec![
        Samples::from_float(Float64Array::from(floats.to_vec())),
        mixed,
        {
            let table = histograms(&histogram_sums.iter().flatten().copied().collect::<Vec<_>>());
            let samples = Samples::try_from_histogram(histogram_indices, table).unwrap();
            if times.len() > 1 {
                Samples::try_from_chunks(vec![
                    samples.slice(0, 1).unwrap(),
                    samples.slice(1, times.len() - 1).unwrap(),
                ])
                .unwrap()
            } else {
                samples
            }
        },
    ])
    .unwrap();
    RawSeriesChunk::try_new(
        &payload_schema(),
        key,
        None,
        RawTimestamps::try_new(ScalarBuffer::from(times.to_vec())).unwrap(),
        fields,
        last,
    )
    .unwrap()
}

struct PayloadStream {
    schema: Arc<SeriesSchema>,
    labels: RawLabelSource,
    items: VecDeque<Result<RawSeriesChunk, BoxedError>>,
}

impl Stream for PayloadStream {
    type Item = Result<RawSeriesChunk, BoxedError>;
    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.items.pop_front())
    }
}

impl RawSeriesStream for PayloadStream {
    fn schema(&self) -> &Arc<SeriesSchema> {
        &self.schema
    }
    fn label_source(&self) -> &RawLabelSource {
        &self.labels
    }
    fn ordering(&self) -> SeriesOrdering {
        SeriesOrdering::KeyThenTime
    }
}

fn payload_stream(labels: RawLabelSource, chunks: Vec<RawSeriesChunk>) -> SendableRawSeriesStream {
    Box::pin(PayloadStream {
        schema: payload_schema(),
        labels,
        items: chunks.into_iter().map(Ok).collect(),
    })
}

fn assert_histogram(value: HistogramView<'_>, sum: f64) {
    assert_eq!(value.schema, -19);
    assert_eq!(value.zero_threshold, 0.125);
    assert_eq!(value.sum.to_bits(), sum.to_bits());
    assert_eq!(value.count, 23.5);
    assert_eq!(value.zero_count, 2.25);
    assert_eq!(value.reset_hint, CounterResetHint::Unrecognized(37));
    assert_eq!(value.start_timestamp, Some(-456));
    assert_eq!(value.custom_values, &[1.5, 4.5]);
    assert_eq!(value.positive.span_offsets, &[1]);
    assert_eq!(value.positive.span_lengths, &[2]);
    assert_eq!(value.positive.buckets, &[3.25, 7.5]);
    assert_eq!(value.negative.span_offsets, &[-2]);
    assert_eq!(value.negative.span_lengths, &[1]);
    assert_eq!(value.negative.buckets, &[0.75]);
}

#[tokio::test]
async fn merges_typed_multifield_payload_through_frame_and_label_roundtrip() {
    let key = read_key("rack-payload", "sensor-7");
    let a = payload_stream(
        label_source(1),
        vec![payload_chunk(
            key.clone(),
            &[10, 30],
            &[Some(f64::from_bits(0x7ff8_0000_0000_0042)), Some(-0.0)],
            &[Some(1.25), None],
            &[None, Some(11.0)],
            &[Some(101.0), None],
            true,
        )],
    );
    let b = payload_stream(
        label_source(2),
        vec![payload_chunk(
            key.clone(),
            &[20, 40],
            &[None, Some(f64::from_bits(0x7ff0_0000_0000_0002))],
            &[None, None],
            &[None, Some(21.0)],
            &[Some(202.0), Some(303.0)],
            true,
        )],
    );
    let merged = merge_ordered_raw_series(vec![a, b]).unwrap();
    let frame_schema = merged.schema().clone();
    let frame_binding = merged.label_source().clone();
    let frame_ordering = merged.ordering();
    let framed = pack_raw_series(merged).unwrap();
    // The producer's identity is an expectation a caller compares against the declaration, not a
    // decode input; the frame states the layout the decoder uses.
    ensure_frame_matches_label_source(framed.schema().as_ref(), &frame_binding).unwrap();
    assert_eq!(
        raw_series_frame_ordering(framed.schema().as_ref()).unwrap(),
        frame_ordering,
        "the packed frame declares the merge's ordering"
    );
    let raw = unpack_raw_series(framed, Some(frame_schema)).unwrap();
    assert_eq!(
        raw.ordering(),
        frame_ordering,
        "unpacking derives the ordering from the frame"
    );
    let rows = materialize_raw_labels(raw)
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        rows.iter()
            .map(|r| r.time.timestamp(0).unwrap())
            .collect::<Vec<_>>(),
        [10, 20, 30, 40]
    );
    assert!(rows.iter().all(|r| r.primary_key == key));
    for row in &rows {
        let metric = row.metric.as_ref().unwrap();
        assert_eq!(metric.name(), Some("room_temperature"));
        assert_eq!(metric.get("rack"), Some("rack-payload"));
        assert_eq!(metric.get("sensor"), Some("sensor-7"));
    }
    let floats: Vec<_> = rows
        .iter()
        .map(|r| match r.fields.get(0).unwrap().sample(0) {
            SampleRef::Float(v) => Some(v.to_bits()),
            SampleRef::Missing => None,
            _ => panic!("unexpected temperature sample variant"),
        })
        .collect();
    assert_eq!(
        floats,
        [
            Some(0x7ff8_0000_0000_0042),
            None,
            Some((-0.0_f64).to_bits()),
            Some(0x7ff0_0000_0000_0002)
        ]
    );
    let mixed: Vec<_> = rows
        .iter()
        .map(|r| match r.fields.get(1).unwrap().sample(0) {
            SampleRef::Float(v) => Some(("float", v.to_bits())),
            SampleRef::Missing => None,
            SampleRef::Histogram(h) => {
                assert_histogram(
                    h,
                    if r.time.timestamp(0) == Some(30) {
                        11.0
                    } else {
                        21.0
                    },
                );
                Some(("hist", 0))
            }
        })
        .collect();
    assert_eq!(
        mixed,
        [
            Some(("float", 1.25_f64.to_bits())),
            None,
            Some(("hist", 0)),
            Some(("hist", 0))
        ]
    );
    let histograms: Vec<_> = rows
        .iter()
        .map(|r| match r.fields.get(2).unwrap().sample(0) {
            SampleRef::Histogram(h) => {
                let expected = match r.time.timestamp(0).unwrap() {
                    10 => 101.0,
                    20 => 202.0,
                    40 => 303.0,
                    _ => panic!("unexpected histogram position"),
                };
                assert_histogram(h, expected);
                Some(h.sum.to_bits())
            }
            SampleRef::Missing => None,
            _ => panic!("unexpected observation sample variant"),
        })
        .collect();
    assert_eq!(
        histograms,
        [
            Some(101.0_f64.to_bits()),
            Some(202.0_f64.to_bits()),
            None,
            Some(303.0_f64.to_bits())
        ]
    );
    assert!(!rows[0].is_last && !rows[1].is_last && !rows[2].is_last && rows[3].is_last);
}

#[tokio::test]
async fn merge_retains_sliced_float_time_and_histogram_owners_until_output_drop() {
    let key = read_key("rack-slice", "sensor-9");
    let times_owner = ScalarBuffer::from(vec![1, 11, 22, 99]);
    let times = times_owner.slice(1, 2);
    let floats_owner = Float64Array::from(vec![
        Some(90.0),
        Some(f64::from_bits(0x7ff8_0000_0000_0042)),
        Some(-0.0),
        Some(80.0),
    ]);
    let floats = floats_owner.slice(1, 2);
    let hist_owner = histograms(&[99.0, 12.5, 88.0]);
    let hist_weak = Arc::downgrade(&hist_owner);
    let sliced_hist =
        Arc::new(HistogramBatch::try_from_arrow(hist_owner.as_arrow().slice(1, 1)).unwrap());
    let hist_indices_owner = UInt32Array::from(vec![Some(9), Some(0), Some(0), Some(9)]);
    let hist_indices = hist_indices_owner.slice(1, 2);
    let mixed_floats_owner = Float64Array::from(vec![Some(90.0), Some(7.0), None, Some(80.0)]);
    let mixed_floats = mixed_floats_owner.slice(1, 2);
    let mixed_indices_owner = UInt32Array::from(vec![Some(9), None, Some(0), Some(9)]);
    let mixed_indices = mixed_indices_owner.slice(1, 2);
    let fields = SeriesFields::try_from_vec(vec![
        Samples::from_float(floats.clone()),
        Samples::try_from_mixed(
            mixed_floats.clone(),
            HistogramSamples::try_new(mixed_indices.clone(), sliced_hist.clone()).unwrap(),
        )
        .unwrap(),
        Samples::try_from_histogram(hist_indices.clone(), sliced_hist.clone()).unwrap(),
    ])
    .unwrap();
    let chunk = RawSeriesChunk::try_new(
        &payload_schema(),
        key.clone(),
        None,
        RawTimestamps::try_new(times.clone()).unwrap(),
        fields,
        true,
    )
    .unwrap();
    let time_ptr = times.as_ptr();
    let key_ptr = key.as_ptr();
    let float_ptr = floats.values().as_ptr();
    let indices_ptr = hist_indices.values().as_ptr();
    let mixed_float_ptr = mixed_floats.values().as_ptr();
    let mixed_indices_ptr = mixed_indices.values().as_ptr();
    let scalar_ptr = sliced_hist
        .as_arrow()
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .values()
        .as_ptr();
    let custom_ptr = sliced_hist
        .as_arrow()
        .column(7)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .values()
        .as_ptr();
    let bucket_ptr = sliced_hist
        .as_arrow()
        .column(10)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .values()
        .as_ptr();
    let batch_weak: Weak<HistogramBatch> = Arc::downgrade(&sliced_hist);
    let interleaved = payload_chunk(
        key.clone(),
        &[15],
        &[Some(6.0)],
        &[None],
        &[None],
        &[Some(7.5)],
        true,
    );
    let mut merged = merge_ordered_raw_series(vec![
        payload_stream(label_source(1), vec![chunk]),
        payload_stream(label_source(2), vec![interleaved]),
    ])
    .unwrap();
    assert_eq!(
        merged
            .schema()
            .fields
            .iter()
            .map(|field| field.name.as_ref())
            .collect::<Vec<_>>(),
        ["temperature", "latency", "observations"]
    );
    assert!(
        matches!(merged.label_source(), RawLabelSource::PrimaryKey { metric_name, .. } if metric_name.as_ref() == "room_temperature")
    );
    assert_eq!(merged.ordering(), SeriesOrdering::KeyThenTime);
    let prefix = merged.next().await.unwrap().unwrap();
    assert_eq!(prefix.time.as_buffer().as_ref(), &[11]);
    assert_eq!(prefix.time.as_buffer().as_ptr(), time_ptr);
    assert_eq!(prefix.primary_key.as_ptr(), key_ptr);
    let interleaved = merged.next().await.unwrap().unwrap();
    assert_eq!(interleaved.time.as_buffer().as_ref(), &[15]);
    let remainder = merged.next().await.unwrap().unwrap();
    assert_eq!(remainder.primary_key.as_ptr(), key_ptr);
    assert_eq!(remainder.time.as_buffer().as_ref(), &[22]);
    assert_eq!(
        remainder.time.as_buffer().as_ptr(),
        time_ptr.wrapping_add(1)
    );
    assert!(
        matches!(prefix.fields.get(0).unwrap().dense().unwrap(), DenseSamplesRef::Float(a) if a.values().as_ptr() == float_ptr)
    );
    assert!(
        matches!(remainder.fields.get(0).unwrap().dense().unwrap(), DenseSamplesRef::Float(a) if a.values().as_ptr() == float_ptr.wrapping_add(1))
    );
    let DenseSamplesRef::Mixed(prefix_mixed) = prefix.fields.get(1).unwrap().dense().unwrap()
    else {
        panic!("expected mixed payload")
    };
    assert_eq!(prefix_mixed.floats().values().as_ptr(), mixed_float_ptr);
    assert_eq!(
        prefix_mixed.histograms().indices().values().as_ptr(),
        mixed_indices_ptr
    );
    assert!(
        matches!(prefix.fields.get(1).unwrap().sample(0), SampleRef::Float(v) if v.to_bits() == 7.0_f64.to_bits())
    );
    let DenseSamplesRef::Mixed(remainder_mixed) = remainder.fields.get(1).unwrap().dense().unwrap()
    else {
        panic!("expected mixed payload")
    };
    assert_eq!(
        remainder_mixed.floats().values().as_ptr(),
        mixed_float_ptr.wrapping_add(1)
    );
    assert_eq!(
        remainder_mixed.histograms().indices().values().as_ptr(),
        mixed_indices_ptr.wrapping_add(1)
    );
    assert!(
        matches!(remainder.fields.get(1).unwrap().sample(0), SampleRef::Histogram(h) if h.sum.to_bits() == 12.5_f64.to_bits())
    );
    let DenseSamplesRef::Histogram(prefix_hist) = prefix.fields.get(2).unwrap().dense().unwrap()
    else {
        panic!("expected histogram payload")
    };
    let DenseSamplesRef::Histogram(remainder_hist) =
        remainder.fields.get(2).unwrap().dense().unwrap()
    else {
        panic!("expected histogram payload")
    };
    assert_eq!(prefix_hist.indices().values().as_ptr(), indices_ptr);
    assert_eq!(prefix_hist.indices().values(), &[0]);
    assert_histogram(prefix_hist.view_at(0).unwrap(), 12.5);
    assert!(Arc::ptr_eq(prefix_hist.batch(), remainder_hist.batch()));
    let output_arrow = prefix_hist.batch().as_arrow();
    assert_eq!(
        output_arrow
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values()
            .as_ptr(),
        scalar_ptr
    );
    assert_eq!(
        output_arrow
            .column(7)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values()
            .as_ptr(),
        custom_ptr
    );
    assert_eq!(
        output_arrow
            .column(10)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values()
            .as_ptr(),
        bucket_ptr
    );
    let negative_bucket_ptr = sliced_hist
        .as_arrow()
        .column(13)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .values()
        .as_ptr();
    assert_eq!(
        output_arrow
            .column(13)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values()
            .as_ptr(),
        negative_bucket_ptr
    );
    assert_eq!(
        remainder_hist.indices().values().as_ptr(),
        indices_ptr.wrapping_add(1)
    );
    assert_eq!(remainder_hist.indices().values(), &[0]);
    assert_histogram(remainder_hist.view_at(0).unwrap(), 12.5);
    drop(merged);
    drop(times_owner);
    drop(times);
    drop(floats_owner);
    drop(floats);
    drop(mixed_floats_owner);
    drop(mixed_floats);
    drop(mixed_indices_owner);
    drop(mixed_indices);
    drop(hist_owner);
    drop(sliced_hist);
    drop(hist_indices_owner);
    drop(hist_indices);
    assert!(hist_weak.upgrade().is_none());
    assert!(batch_weak.upgrade().is_some());
    assert_eq!(prefix.time.as_buffer().as_ref(), &[11]);
    assert_eq!(remainder.time.as_buffer().as_ref(), &[22]);
    let SampleRef::Histogram(prefix_histogram) = prefix.fields.get(2).unwrap().sample(0) else {
        panic!("expected histogram");
    };
    assert_histogram(prefix_histogram, 12.5);
    let SampleRef::Histogram(remainder_histogram) = remainder.fields.get(2).unwrap().sample(0)
    else {
        panic!("expected histogram");
    };
    assert_histogram(remainder_histogram, 12.5);
    let SampleRef::Histogram(mixed_histogram) = remainder.fields.get(1).unwrap().sample(0) else {
        panic!("expected histogram");
    };
    assert_histogram(mixed_histogram, 12.5);
    assert!(
        matches!(prefix.fields.get(0).unwrap().sample(0), SampleRef::Float(v) if v.to_bits() == 0x7ff8_0000_0000_0042)
    );
    assert!(
        matches!(remainder.fields.get(0).unwrap().sample(0), SampleRef::Float(v) if v.to_bits() == (-0.0_f64).to_bits())
    );
    drop(prefix);
    assert!(batch_weak.upgrade().is_some());
    let SampleRef::Histogram(remainder_histogram) = remainder.fields.get(2).unwrap().sample(0)
    else {
        panic!("expected histogram");
    };
    assert_histogram(remainder_histogram, 12.5);
    assert!(
        matches!(remainder.fields.get(1).unwrap().sample(0), SampleRef::Histogram(h) if h.sum.to_bits() == 12.5_f64.to_bits() && h.custom_values == [1.5, 4.5] && h.positive.span_offsets == [1] && h.positive.span_lengths == [2] && h.positive.buckets == [3.25, 7.5] && h.negative.span_offsets == [-2] && h.negative.span_lengths == [1] && h.negative.buckets == [0.75])
    );
    drop(remainder);
    drop(interleaved);
    assert!(batch_weak.upgrade().is_none());
}
