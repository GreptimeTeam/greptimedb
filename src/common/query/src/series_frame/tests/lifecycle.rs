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

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use common_error::ext::BoxedError;
use datatypes::arrow::array::{Array, Float64Array, UInt32Array};
use datatypes::arrow::buffer::{Buffer, ScalarBuffer};
use datatypes::series::histogram::{BucketSideView, CounterResetHint, HistogramBatchBuilder};
use datatypes::series::raw::{RawSeriesChunk, RawSeriesOrdering, RawTimeAxis};
use datatypes::series::samples::{
    DenseSamplesRef, FieldSpec, HistogramSamples, SampleRef, Samples, SeriesFields, SeriesSchema,
};
use datatypes::series::{Label, MetricName};
use futures_util::{Stream, StreamExt};
use store_api::region_engine::{RawLabelSource, RawSeriesStream, SendableRawSeriesStream};

use super::super::{pack_raw_series, unpack_raw_series};

struct RawFixture {
    schema: Arc<SeriesSchema>,
    binding: RawLabelSource,
    chunk: Option<RawSeriesChunk>,
}

impl Stream for RawFixture {
    type Item = Result<RawSeriesChunk, BoxedError>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.chunk.take().map(Ok))
    }
}

impl RawSeriesStream for RawFixture {
    fn schema(&self) -> &Arc<SeriesSchema> {
        &self.schema
    }

    fn label_source(&self) -> &RawLabelSource {
        &self.binding
    }

    fn ordering(&self) -> RawSeriesOrdering {
        RawSeriesOrdering::Contiguous
    }
}

fn histogram_batch() -> Arc<datatypes::series::histogram::HistogramBatch> {
    let custom = [1.5, 4.5];
    let positive_offsets = [1];
    let positive_lengths = [2];
    let positive_buckets = [3.25, 7.5];
    let negative_offsets = [-2];
    let negative_lengths = [1];
    let negative_buckets = [0.75];
    let mut builder = HistogramBatchBuilder::new();
    for (sum, start_timestamp) in [(10.5, Some(-123)), (-9.5, Some(456)), (7.25, None)] {
        builder
            .push(datatypes::series::histogram::HistogramView {
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
            })
            .unwrap();
    }
    builder.finish().unwrap()
}

fn assert_histogram(
    view: datatypes::series::histogram::HistogramView<'_>,
    sum: f64,
    start: Option<i64>,
) {
    assert_eq!(view.schema, -7);
    assert_eq!(view.zero_threshold, 0.125);
    assert_eq!(view.sum.to_bits(), sum.to_bits());
    assert_eq!(view.count, 17.25);
    assert_eq!(view.zero_count, 2.5);
    assert_eq!(view.reset_hint, CounterResetHint::Unrecognized(37));
    assert_eq!(view.start_timestamp, start);
    assert_eq!(view.custom_values, [1.5, 4.5]);
    assert_eq!(view.positive.span_offsets, [1]);
    assert_eq!(view.positive.span_lengths, [2]);
    assert_eq!(view.positive.buckets, [3.25, 7.5]);
    assert_eq!(view.negative.span_offsets, [-2]);
    assert_eq!(view.negative.span_lengths, [1]);
    assert_eq!(view.negative.buckets, [0.75]);
}

#[tokio::test]
async fn streaming_round_trip_preserves_sliced_multifield_payload_and_owners() {
    let schema = Arc::new(SeriesSchema {
        fields: vec!["float", "histogram", "mixed"]
            .into_iter()
            .map(|name| FieldSpec {
                name: Arc::from(name),
            })
            .collect::<Vec<_>>()
            .into(),
        eval_grid: None,
    });
    let time_owner = ScalarBuffer::from(vec![0, 11, 22, 33, 44]);
    let times = RawTimeAxis::try_new(time_owner.slice(1, 3)).unwrap();
    let float_owner = Float64Array::from(vec![
        Some(90.0),
        None,
        Some(f64::from_bits(0x7ff8_0000_0000_0042)),
        Some(-0.0),
        Some(80.0),
    ]);
    let float_a = float_owner.slice(1, 2);
    let float_b = float_owner.slice(3, 1);
    let floats = Samples::try_from_chunks(vec![
        Samples::from_float(float_a.clone()),
        Samples::from_float(float_b.clone()),
    ])
    .unwrap();

    let table_owner = histogram_batch();
    let table_weak = Arc::downgrade(&table_owner);
    let source_table = table_owner.as_arrow().clone();
    let sliced_table = Arc::new(
        datatypes::series::histogram::HistogramBatch::try_from_arrow(source_table.slice(1, 2))
            .unwrap(),
    );
    let sliced_weak = Arc::downgrade(&sliced_table);
    let index_owner = UInt32Array::from(vec![Some(2), Some(0), Some(1), Some(0), Some(1)]);
    let indices = index_owner.slice(1, 3);
    let hist = Samples::try_from_histogram(indices.clone(), sliced_table.clone()).unwrap();
    let mixed_float_owner = Float64Array::from(vec![Some(1.25), None, None]);
    let mixed_index_owner = UInt32Array::from(vec![None, Some(0), None]);
    let mixed = Samples::try_from_mixed(
        mixed_float_owner.clone(),
        HistogramSamples::try_new(mixed_index_owner.clone(), sliced_table.clone()).unwrap(),
    )
    .unwrap();
    let chunk = RawSeriesChunk::try_new(
        &schema,
        Buffer::from(vec![0xaa_u8, 0x10, 0xbb]),
        Some(
            MetricName::try_new(
                Some(Arc::from("cpu")),
                vec![Label {
                    name: Arc::from("host"),
                    value: Arc::from("a"),
                }],
            )
            .unwrap(),
        ),
        times,
        SeriesFields::try_from_vec(vec![floats, hist, mixed]).unwrap(),
        true,
    )
    .unwrap();
    let input_times_ptr = chunk.time.as_explicit().unwrap().as_ptr();
    let input_float_ptr = float_a.values().as_ptr();
    let input_hist_indices_ptr = indices.values().as_ptr();
    let input_mixed_float_ptr = mixed_float_owner.values().as_ptr();
    let input_mixed_indices_ptr = mixed_index_owner.values().as_ptr();

    let input: SendableRawSeriesStream = Box::pin(RawFixture {
        schema: schema.clone(),
        binding: RawLabelSource::Explicit,
        chunk: Some(chunk),
    });
    let frames = pack_raw_series(input).unwrap();
    let mut decoded = unpack_raw_series(
        frames,
        schema,
        RawLabelSource::Explicit,
        RawSeriesOrdering::Contiguous,
    )
    .unwrap();
    let mut output = Vec::new();
    while let Some(item) = decoded.next().await {
        output.push(item.unwrap());
    }
    assert_eq!(
        output.len(),
        2,
        "the float field's segment boundary yields two frames"
    );

    // Validate buffer sharing while the source owners are still alive.
    let DenseSamplesRef::Float(decoded_float) = output[0].fields.get(0).unwrap().segments()[0]
        .dense()
        .unwrap()
    else {
        panic!("expected float")
    };
    assert_eq!(decoded_float.values().as_ptr(), input_float_ptr);
    let DenseSamplesRef::Histogram(decoded_hist) =
        output[0].fields.get(1).unwrap().dense().unwrap()
    else {
        panic!("expected histogram")
    };
    assert_eq!(
        decoded_hist.indices().values().as_ptr(),
        input_hist_indices_ptr
    );
    assert!(
        decoded_hist
            .batch()
            .as_arrow()
            .to_data()
            .ptr_eq(&sliced_table.as_arrow().to_data())
    );
    let DenseSamplesRef::Mixed(decoded_mixed) = output[0].fields.get(2).unwrap().dense().unwrap()
    else {
        panic!("expected mixed")
    };
    assert_eq!(
        decoded_mixed.floats().values().as_ptr(),
        input_mixed_float_ptr
    );
    assert_eq!(
        decoded_mixed.histograms().indices().values().as_ptr(),
        input_mixed_indices_ptr
    );
    assert!(
        decoded_mixed
            .histograms()
            .batch()
            .as_arrow()
            .to_data()
            .ptr_eq(&sliced_table.as_arrow().to_data())
    );
    let first_times = output[0].time.as_explicit().unwrap();
    assert_eq!(first_times.as_ptr(), input_times_ptr);

    drop(decoded);
    drop(time_owner);
    drop(float_owner);
    drop(float_a);
    drop(float_b);
    drop(index_owner);
    drop(indices);
    drop(mixed_float_owner);
    drop(mixed_index_owner);
    drop(source_table);
    drop(table_owner);
    drop(sliced_table);
    assert!(table_weak.upgrade().is_none());
    assert!(sliced_weak.upgrade().is_none());

    let expected_histograms = [(-9.5, Some(456)), (7.25, None), (-9.5, Some(456))];
    let mut times_seen = Vec::new();
    let mut float_seen = Vec::new();
    let mut histogram_seen = Vec::new();
    let mut mixed_seen = Vec::new();
    for chunk in &output {
        assert_eq!(chunk.read_key.as_slice(), [0xaa, 0x10, 0xbb]);
        assert_eq!(chunk.metric.as_ref().unwrap().name(), Some("cpu"));
        assert_eq!(chunk.metric.as_ref().unwrap().get("host"), Some("a"));
        assert_eq!(chunk.is_last, times_seen.len() + chunk.time.len() == 3);
        for i in 0..chunk.time.len() {
            let position = times_seen.len();
            times_seen.push(chunk.time.timestamp(i).unwrap());
            match chunk.fields.get(0).unwrap().sample(i) {
                SampleRef::Float(value) => float_seen.push(Some(value.to_bits())),
                SampleRef::Missing => float_seen.push(None),
                SampleRef::Histogram(_) => panic!("expected float field"),
            }
            let SampleRef::Histogram(value) = chunk.fields.get(1).unwrap().sample(i) else {
                panic!("expected histogram")
            };
            let (sum, start) = expected_histograms[position];
            assert_histogram(value, sum, start);
            histogram_seen.push(value.sum);
            match (position, chunk.fields.get(2).unwrap().sample(i)) {
                (0, SampleRef::Float(value)) => mixed_seen.push(Some(value.to_bits())),
                (1, SampleRef::Histogram(value)) => {
                    assert_histogram(value, -9.5, Some(456));
                    mixed_seen.push(None);
                }
                (2, SampleRef::Missing) => mixed_seen.push(None),
                _ => panic!("mixed payload variant did not match expected position {position}"),
            }
        }
    }
    assert_eq!(times_seen, [11, 22, 33]);
    assert_eq!(
        float_seen,
        [
            None,
            Some(f64::from_bits(0x7ff8_0000_0000_0042).to_bits()),
            Some((-0.0_f64).to_bits())
        ]
    );
    assert_eq!(histogram_seen, [-9.5, 7.25, -9.5]);
    assert_eq!(mixed_seen, [Some(1.25_f64.to_bits()), None, None]);
}
