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

use datatypes::arrow::array::{Float64Array, UInt32Array};
use datatypes::arrow::buffer::ScalarBuffer;
use datatypes::series::histogram::{
    BucketSideView, CounterResetHint, HistogramBatch, HistogramBatchBuilder, HistogramView,
};
use datatypes::series::raw::RawTimeAxis;
use datatypes::series::samples::{DenseSamplesRef, SampleRef, Samples, SeriesFields};

use super::select_instants;

fn axis(times: &[i64]) -> RawTimeAxis {
    RawTimeAxis::try_new(ScalarBuffer::from(times.to_vec())).unwrap()
}

fn select(
    times: &[i64],
    fields: &SeriesFields,
    range: std::ops::RangeInclusive<i64>,
    step: i64,
    lookback: i64,
    offset: i64,
) -> datafusion_common::Result<(Vec<u64>, Vec<i64>)> {
    select_instants(&axis(times), fields, range, step, lookback, offset)
}

fn histogram_batch(sums: &[f64]) -> Arc<HistogramBatch> {
    let mut builder = HistogramBatchBuilder::new();
    for &sum in sums {
        builder
            .push(HistogramView {
                schema: 1,
                zero_threshold: 0.0,
                sum,
                count: 2.0,
                zero_count: 0.0,
                reset_hint: CounterResetHint::Unknown,
                start_timestamp: None,
                custom_values: &[],
                positive: BucketSideView {
                    span_offsets: &[],
                    span_lengths: &[],
                    buckets: &[],
                },
                negative: BucketSideView {
                    span_offsets: &[],
                    span_lengths: &[],
                    buckets: &[],
                },
            })
            .unwrap();
    }
    builder.finish().unwrap()
}

#[test]
fn float_lookback_boundaries_missing_and_nan() {
    let fields = SeriesFields::one(Samples::from_float(Float64Array::from(vec![
        Some(1.0),
        Some(f64::from_bits(0x7ff8_0000_0000_0042)),
        None,
        Some(4.0),
    ])));
    let (rows, times) = select(&[0, 50, 100, 200], &fields, 50..=200, 50, 50, 0).unwrap();
    assert_eq!(rows, [1, 2, 3]);
    assert_eq!(times, [50, 100, 200]);
    assert!(matches!(
        fields.get(0).unwrap().sample(rows[0] as usize),
        SampleRef::Float(v) if v.to_bits() == 0x7ff8_0000_0000_0042
    ));
    assert!(matches!(
        fields.get(0).unwrap().sample(rows[1] as usize),
        SampleRef::Missing
    ));
    assert!(matches!(
        fields.get(0).unwrap().sample(rows[2] as usize),
        SampleRef::Float(4.0)
    ));

    let fields = SeriesFields::one(Samples::from_float(Float64Array::from(vec![Some(1.0)])));
    assert!(
        select(&[50], &fields, 100..=100, 100, 50, 0)
            .unwrap()
            .0
            .is_empty()
    );
    assert_eq!(
        select(&[52], &fields, 100..=100, 100, 49, 0).unwrap(),
        (vec![0], vec![100])
    );
    assert!(
        select(&[99], &fields, 100..=100, 100, 0, 0)
            .unwrap()
            .0
            .is_empty()
    );
    assert_eq!(
        select(&[100], &fields, 100..=100, 100, 0, 0).unwrap(),
        (vec![0], vec![100])
    );
}

#[test]
fn typed_histogram_mixed_fields_and_independent_chunks_keep_selection_aligned() {
    let histogram = Samples::try_from_histogram(
        UInt32Array::from(vec![Some(0), Some(1), Some(2)]),
        histogram_batch(&[10.0, 20.0, 30.0]),
    )
    .unwrap();
    let mixed_hist = Samples::try_from_histogram(
        UInt32Array::from(vec![Some(0), None, Some(1)]),
        histogram_batch(&[100.0, 300.0]),
    )
    .unwrap();
    let mixed = Samples::try_from_mixed(
        Float64Array::from(vec![None, Some(2.5), None]),
        match mixed_hist.dense().unwrap() {
            DenseSamplesRef::Histogram(h) => h.clone(),
            _ => unreachable!(),
        },
    )
    .unwrap();
    let fields = SeriesFields::try_from_vec(vec![
        Samples::try_from_chunks(vec![
            histogram.slice(0, 1).unwrap(),
            histogram.slice(1, 2).unwrap(),
        ])
        .unwrap(),
        Samples::try_from_chunks(vec![mixed.slice(0, 2).unwrap(), mixed.slice(2, 1).unwrap()])
            .unwrap(),
        Samples::from_float(Float64Array::from(vec![Some(1.0), Some(2.0), Some(3.0)])),
    ])
    .unwrap();
    drop(histogram);
    drop(mixed);
    drop(mixed_hist);

    let (rows, times) = select(&[10, 20, 30], &fields, 20..=30, 10, 0, 0).unwrap();
    assert_eq!(rows, [1, 2]);
    assert_eq!(times, [20, 30]);
    for (row, expected) in [(1, 20.0), (2, 30.0)] {
        let SampleRef::Histogram(value) = fields.get(0).unwrap().sample(row) else {
            panic!("expected native histogram at row {row}");
        };
        assert_eq!(value.sum, expected);
    }
    assert!(matches!(
        fields.get(1).unwrap().sample(1),
        SampleRef::Float(2.5)
    ));
    assert!(
        matches!(fields.get(1).unwrap().sample(2), SampleRef::Histogram(value) if value.sum == 300.0)
    );
}

#[test]
fn stale_sample_in_any_field_suppresses_without_falling_back() {
    let stale = f64::from_bits(common_query::prometheus::PROMETHEUS_STALE_NAN_BITS);
    let float = Samples::from_float(Float64Array::from(vec![
        Some(42.0),
        Some(stale),
        Some(43.0),
        Some(44.0),
    ]));
    let hist = Samples::try_from_histogram(
        UInt32Array::from(vec![Some(0), Some(1), Some(2), Some(3)]),
        histogram_batch(&[10.0, 20.0, stale, 40.0]),
    )
    .unwrap();
    let fields = SeriesFields::try_from_vec(vec![float, hist]).unwrap();
    let (rows, times) = select(&[0, 100, 200, 300], &fields, 0..=300, 100, 400, 0).unwrap();
    assert_eq!(rows, [0, 3]);
    assert_eq!(times, [0, 300]);
}

#[test]
fn offset_extreme_timestamps_and_point_limit_are_checked_without_large_fixture() {
    let fields = SeriesFields::one(Samples::from_float(Float64Array::from(vec![Some(7.0)])));
    assert_eq!(
        select(&[i64::MAX], &fields, i64::MAX..=i64::MAX, 1, 0, 0).unwrap(),
        (vec![0], vec![i64::MAX])
    );
    assert_eq!(
        select(&[i64::MIN], &fields, i64::MIN..=i64::MIN, 1, 0, 0).unwrap(),
        (vec![0], vec![i64::MIN])
    );
    assert_eq!(
        select(&[i64::MAX], &fields, -1..=-1, 1, 0, i64::MIN).unwrap(),
        (vec![0], vec![-1])
    );
    assert_eq!(
        select(&[100], &fields, 110..=110, 10, 20, 10).unwrap(),
        (vec![0], vec![110])
    );

    let fields = SeriesFields::one(Samples::from_float(Float64Array::from(vec![
        Some(f64::from_bits(
            common_query::prometheus::PROMETHEUS_STALE_NAN_BITS,
        )),
        Some(1.0),
    ])));
    let result = select(&[0, 1_000_000], &fields, 0..=1_000_000, 1, 0, 0);
    let error = result.unwrap_err().to_string();
    assert!(
        error.contains("output points exceed limit: 1000001 > 1000000"),
        "{error}"
    );
}

#[test]
fn invalid_manual_inputs_are_rejected() {
    let fields = SeriesFields::one(Samples::from_float(Float64Array::from(vec![
        Some(1.0),
        Some(2.0),
    ])));
    let invalid_axis = RawTimeAxis::Explicit(ScalarBuffer::from(vec![10, 10]));
    assert!(select_instants(&invalid_axis, &fields, 0..=10, 1, 0, 0).is_err());
    let descending_axis = RawTimeAxis::Explicit(ScalarBuffer::from(vec![20, 10]));
    assert!(select_instants(&descending_axis, &fields, 0..=20, 1, 0, 0).is_err());

    let mismatched = SeriesFields::one(Samples::from_float(Float64Array::from(vec![Some(1.0)])));
    assert!(select(&[10, 20], &mismatched, 0..=10, 1, 0, 0).is_err());
    assert!(select(&[10, 20], &fields, 0..=10, 0, 0, 0).is_err());
    assert!(select(&[10, 20], &fields, 10..=0, 1, 0, 0).is_err());
    assert!(select(&[10, 20], &fields, 0..=10, 1, -1, 0).is_err());
}

#[test]
fn empty_axis_returns_no_selections() {
    let fields = SeriesFields::one(Samples::from_float(Float64Array::new_null(0)));
    assert_eq!(
        select(&[], &fields, -10..=10, 1, 10, 0).unwrap(),
        (vec![], vec![])
    );
}
