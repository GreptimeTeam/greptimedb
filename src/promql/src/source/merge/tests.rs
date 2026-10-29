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

use datafusion::arrow::array::{Float64Array, UInt32Array};
use datafusion::arrow::buffer::Buffer;
use datatypes::series::histogram::{BucketSideView, CounterResetHint, HistogramBatchBuilder};
use datatypes::series::raw::{RawSeriesChunk, RawTimestamps};
use datatypes::series::samples::{Samples, SeriesFields, SeriesSchema};
use datatypes::series::{Label, SeriesLabels};

use super::merge_raw_identity;

fn schema(count: usize) -> SeriesSchema {
    SeriesSchema {
        fields: (0..count)
            .map(|i| datatypes::series::samples::FieldSpec {
                name: Arc::from(format!("f{i}")),
            })
            .collect::<Vec<_>>()
            .into(),
        eval_grid: None,
    }
}

fn histogram(sum: f64, threshold: f64) -> Samples {
    let mut builder = HistogramBatchBuilder::new();
    let side = BucketSideView {
        span_offsets: &[],
        span_lengths: &[],
        buckets: &[],
    };
    let index = builder
        .push(datatypes::series::histogram::HistogramView {
            schema: 1,
            zero_threshold: threshold,
            sum,
            count: 1.0,
            zero_count: 0.0,
            reset_hint: CounterResetHint::Unknown,
            start_timestamp: None,
            custom_values: &[],
            positive: side,
            negative: side,
        })
        .unwrap();
    Samples::try_from_histogram(
        UInt32Array::from(vec![Some(index)]),
        builder.finish().unwrap(),
    )
    .unwrap()
}

fn labels(value: &str) -> SeriesLabels {
    SeriesLabels::try_new(
        Some(Arc::from("metric")),
        vec![Label {
            name: Arc::from("tag"),
            value: Arc::from(value),
        }],
    )
    .unwrap()
}

fn chunk(
    key: &[u8],
    label: &str,
    times: Vec<i64>,
    fields: Vec<Vec<Option<f64>>>,
) -> RawSeriesChunk {
    RawSeriesChunk::try_new(
        &schema(fields.len()),
        Buffer::from(key.to_vec()),
        Some(labels(label)),
        RawTimestamps::try_new(times.into()).unwrap(),
        SeriesFields::try_from_vec(
            fields
                .into_iter()
                .map(|values| Samples::from_float(Float64Array::from(values)))
                .collect(),
        )
        .unwrap(),
        false,
    )
    .unwrap()
}

#[test]
fn interleaved_pieces_merge_in_time_order() {
    let schema = schema(2);
    let result = merge_raw_identity(
        &schema,
        &[
            chunk(
                b"k",
                "v",
                vec![1, 3],
                vec![vec![Some(10.), Some(30.)], vec![Some(1.), Some(3.)]],
            ),
            chunk(
                b"k",
                "v",
                vec![2, 4],
                vec![vec![Some(20.), Some(40.)], vec![Some(2.), Some(4.)]],
            ),
        ],
    )
    .unwrap();
    assert_eq!(result.time.as_buffer().as_ref(), &[1, 2, 3, 4]);
    assert_eq!(
        result
            .fields
            .get(0)
            .unwrap()
            .iter()
            .map(|s| match s {
                datatypes::series::samples::SampleRef::Float(v) => v,
                _ => 0.,
            })
            .collect::<Vec<_>>(),
        vec![10., 20., 30., 40.]
    );
    assert!(result.is_last);
}

#[test]
fn equal_duplicates_collapse_but_bit_differences_conflict() {
    assert!(
        merge_raw_identity(
            &schema(1),
            &[
                chunk(b"k", "v", vec![1], vec![vec![Some(1.)]]),
                chunk(b"k", "v", vec![1], vec![vec![Some(1.)]])
            ]
        )
        .is_ok()
    );
    for (left, right) in [
        (-0.0, 0.0),
        (
            f64::from_bits(0x7ff8_0000_0000_0001),
            f64::from_bits(0x7ff8_0000_0000_0002),
        ),
    ] {
        assert!(
            merge_raw_identity(
                &schema(1),
                &[
                    chunk(b"k", "v", vec![1], vec![vec![Some(left)]]),
                    chunk(b"k", "v", vec![1], vec![vec![Some(right)]])
                ]
            )
            .is_err()
        );
    }
    assert!(
        merge_raw_identity(
            &schema(1),
            &[
                chunk(b"k", "v", vec![1], vec![vec![None]]),
                chunk(b"k", "v", vec![1], vec![vec![Some(1.)]])
            ]
        )
        .is_err()
    );
}

#[test]
fn rejects_different_keys_labels_and_internal_duplicates() {
    assert!(
        merge_raw_identity(
            &schema(1),
            &[
                chunk(b"a", "v", vec![1], vec![vec![Some(1.)]]),
                chunk(b"b", "v", vec![2], vec![vec![Some(2.)]])
            ]
        )
        .is_err()
    );
    assert!(
        merge_raw_identity(
            &schema(1),
            &[
                chunk(b"a", "v", vec![1], vec![vec![Some(1.)]]),
                chunk(b"a", "other", vec![2], vec![vec![Some(2.)]])
            ]
        )
        .is_err()
    );
    assert!(RawTimestamps::try_new(vec![1, 1].into()).is_err());
    let mut no_labels = chunk(b"a", "v", vec![1], vec![vec![Some(1.)]]);
    no_labels.metric = None;
    assert!(merge_raw_identity(&schema(1), &[no_labels]).is_err());
}

#[test]
fn histogram_duplicates_compare_every_float_property() {
    let schema = schema(1);
    let make = |sample| RawSeriesChunk {
        primary_key: Buffer::from(b"k".to_vec()),
        metric: Some(labels("v")),
        time: RawTimestamps::try_new(vec![1].into()).unwrap(),
        fields: SeriesFields::one(sample),
        is_last: false,
    };
    assert!(
        merge_raw_identity(
            &schema,
            &[make(histogram(2.0, 0.5)), make(histogram(2.0, 0.5))]
        )
        .is_ok()
    );
    assert!(
        merge_raw_identity(
            &schema,
            &[make(histogram(2.0, 0.5)), make(histogram(3.0, 0.5))]
        )
        .is_err()
    );
    assert!(
        merge_raw_identity(
            &schema,
            &[make(histogram(2.0, 0.5)), make(histogram(2.0, -0.0))]
        )
        .is_err()
    );
}

#[test]
fn rejects_empty_and_shape_mismatch() {
    assert!(merge_raw_identity(&schema(1), &[]).is_err());
    let valid = chunk(b"k", "v", vec![1], vec![vec![Some(1.)]]);
    assert!(merge_raw_identity(&schema(2), &[valid]).is_err());
    let malformed = RawSeriesChunk {
        primary_key: Buffer::from(b"k".to_vec()),
        metric: Some(labels("v")),
        time: RawTimestamps::try_new(vec![1].into()).unwrap(),
        fields: SeriesFields::one(Samples::from_float(Float64Array::from(vec![
            Some(1.),
            Some(2.),
        ]))),
        is_last: false,
    };
    assert!(merge_raw_identity(&schema(1), &[malformed]).is_err());
}
