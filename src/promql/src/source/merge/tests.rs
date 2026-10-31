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

use super::merge_raw_identity_charging;

/// The same merge, with accounting the tests do not measure.
fn merge_raw_identity(
    schema: &SeriesSchema,
    chunks: &[RawSeriesChunk],
) -> datafusion::common::Result<RawSeriesChunk> {
    merge_raw_identity_charging(schema, chunks, &mut |_| Ok(()), &mut |_| {})
}

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

/// Builds a histogram with configurable comparison properties for duplicate-conflict tests.
fn histogram_with(view: datatypes::series::histogram::HistogramView<'_>) -> Samples {
    let mut builder = HistogramBatchBuilder::new();
    let index = builder.push(view).unwrap();
    Samples::try_from_histogram(
        UInt32Array::from(vec![Some(index)]),
        builder.finish().unwrap(),
    )
    .unwrap()
}

/// Two histograms are duplicates only when every property matches.
///
/// Bucket values are independently free within a valid layout and are compared directly. The
/// span/cursor layout cannot be varied independently of bucket count: each layout must have
/// nonnegative lengths summing to the bucket count and a valid cursor. The cases below vary each
/// offset and length while preserving valid layouts and equal bucket counts.
#[test]
fn histogram_duplicates_compare_every_property() {
    let schema = schema(1);
    let make = |sample| RawSeriesChunk {
        primary_key: Buffer::from(b"k".to_vec()),
        metric: Some(labels("v")),
        time: RawTimestamps::try_new(vec![1].into()).unwrap(),
        fields: SeriesFields::one(sample),
        is_last: false,
    };
    let positive = BucketSideView {
        span_offsets: &[0],
        span_lengths: &[2],
        buckets: &[1.0, 2.0],
    };
    let negative = BucketSideView {
        span_offsets: &[0],
        span_lengths: &[2],
        buckets: &[3.0, 4.0],
    };
    let build = |schema,
                 count,
                 zero_count,
                 sum,
                 zero_threshold,
                 positive,
                 negative,
                 reset_hint,
                 start_timestamp,
                 custom_values| {
        histogram_with(datatypes::series::histogram::HistogramView {
            schema,
            count,
            zero_count,
            sum,
            zero_threshold,
            positive,
            negative,
            reset_hint,
            start_timestamp,
            custom_values,
        })
    };
    let baseline = || {
        build(
            1,
            1.0,
            0.0,
            2.0,
            0.5,
            positive,
            negative,
            CounterResetHint::Unknown,
            None,
            &[],
        )
    };
    assert!(
        merge_raw_identity(&schema, &[make(baseline()), make(baseline())]).is_ok(),
        "identical histograms are duplicates"
    );

    let positive_buckets_changed = BucketSideView {
        buckets: &[1.0, 2.5],
        ..positive
    };
    let negative_buckets_changed = BucketSideView {
        buckets: &[3.0, 4.5],
        ..negative
    };
    let positive_offsets_changed = BucketSideView {
        span_offsets: &[1],
        ..positive
    };
    let negative_offsets_changed = BucketSideView {
        span_offsets: &[1],
        ..negative
    };
    // Same total bucket count (2), different split into spans, and valid nonnegative offsets.
    let positive_lengths_changed = BucketSideView {
        span_offsets: &[0, 0],
        span_lengths: &[1, 1],
        buckets: &[1.0, 2.0],
    };
    let negative_lengths_changed = BucketSideView {
        span_offsets: &[0, 0],
        span_lengths: &[1, 1],
        buckets: &[3.0, 4.0],
    };
    let cases: Vec<(&str, Samples)> = vec![
        (
            "schema (1 vs 2)",
            build(
                2,
                1.0,
                0.0,
                2.0,
                0.5,
                positive,
                negative,
                CounterResetHint::Unknown,
                None,
                &[],
            ),
        ),
        (
            "count (1.0 vs 2.0)",
            build(
                1,
                2.0,
                0.0,
                2.0,
                0.5,
                positive,
                negative,
                CounterResetHint::Unknown,
                None,
                &[],
            ),
        ),
        (
            "zero_count (0.0 vs 1.0)",
            build(
                1,
                1.0,
                1.0,
                2.0,
                0.5,
                positive,
                negative,
                CounterResetHint::Unknown,
                None,
                &[],
            ),
        ),
        (
            "sum (2.0 vs 3.0)",
            build(
                1,
                1.0,
                0.0,
                3.0,
                0.5,
                positive,
                negative,
                CounterResetHint::Unknown,
                None,
                &[],
            ),
        ),
        (
            "zero_threshold (0.5 vs -0.0)",
            build(
                1,
                1.0,
                0.0,
                2.0,
                -0.0,
                positive,
                negative,
                CounterResetHint::Unknown,
                None,
                &[],
            ),
        ),
        (
            "positive bucket value (2.0 vs 2.5)",
            build(
                1,
                1.0,
                0.0,
                2.0,
                0.5,
                positive_buckets_changed,
                negative,
                CounterResetHint::Unknown,
                None,
                &[],
            ),
        ),
        (
            "negative bucket value (4.0 vs 4.5)",
            build(
                1,
                1.0,
                0.0,
                2.0,
                0.5,
                positive,
                negative_buckets_changed,
                CounterResetHint::Unknown,
                None,
                &[],
            ),
        ),
        (
            "positive span offset ([0] vs [1])",
            build(
                1,
                1.0,
                0.0,
                2.0,
                0.5,
                positive_offsets_changed,
                negative,
                CounterResetHint::Unknown,
                None,
                &[],
            ),
        ),
        (
            "negative span offset ([0] vs [1])",
            build(
                1,
                1.0,
                0.0,
                2.0,
                0.5,
                positive,
                negative_offsets_changed,
                CounterResetHint::Unknown,
                None,
                &[],
            ),
        ),
        (
            "positive span lengths ([2] vs [1, 1])",
            build(
                1,
                1.0,
                0.0,
                2.0,
                0.5,
                positive_lengths_changed,
                negative,
                CounterResetHint::Unknown,
                None,
                &[],
            ),
        ),
        (
            "negative span lengths ([2] vs [1, 1])",
            build(
                1,
                1.0,
                0.0,
                2.0,
                0.5,
                positive,
                negative_lengths_changed,
                CounterResetHint::Unknown,
                None,
                &[],
            ),
        ),
        (
            "reset hint (Unknown vs CounterReset)",
            build(
                1,
                1.0,
                0.0,
                2.0,
                0.5,
                positive,
                negative,
                CounterResetHint::CounterReset,
                None,
                &[],
            ),
        ),
        (
            "start timestamp (None vs Some(7))",
            build(
                1,
                1.0,
                0.0,
                2.0,
                0.5,
                positive,
                negative,
                CounterResetHint::Unknown,
                Some(7),
                &[],
            ),
        ),
        (
            "custom values ([] vs [1.5])",
            build(
                1,
                1.0,
                0.0,
                2.0,
                0.5,
                positive,
                negative,
                CounterResetHint::Unknown,
                None,
                &[1.5],
            ),
        ),
    ];
    for (what, changed) in cases {
        assert!(
            merge_raw_identity(&schema, &[make(baseline()), make(changed)]).is_err(),
            "two histograms differing only in {what} are a conflict, not a duplicate"
        );
    }
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

#[test]
fn reservation_error_is_preserved() {
    let error = merge_raw_identity_charging(
        &schema(1),
        &[chunk(b"k", "v", vec![1], vec![vec![Some(1.)]])],
        &mut |_| {
            Err(datafusion::common::DataFusionError::Execution(
                "reservation refused".into(),
            ))
        },
        &mut |_| {},
    )
    .unwrap_err();
    assert!(matches!(
        error,
        datafusion::common::DataFusionError::Execution(_)
    ));
}

#[test]
fn merge_releases_transient_row_workspace() {
    let mut reserved = 0usize;
    let mut shrunk = 0usize;
    let result = merge_raw_identity_charging(
        &schema(1),
        &[
            chunk(b"k", "v", vec![1, 3], vec![vec![Some(1.), Some(3.)]]),
            chunk(b"k", "v", vec![1, 2], vec![vec![Some(1.), Some(2.)]]),
        ],
        &mut |bytes| {
            reserved = reserved.saturating_add(bytes);
            Ok(())
        },
        &mut |bytes| shrunk = shrunk.saturating_add(bytes),
    )
    .unwrap();
    assert_eq!(result.time.len(), 3);
    assert_eq!(shrunk, 2 * 4 * std::mem::size_of::<(i64, usize, usize)>());
    let retained = reserved - shrunk;
    assert!(retained < reserved);
    assert!(retained >= result.time.len() * std::mem::size_of::<i64>());
}
