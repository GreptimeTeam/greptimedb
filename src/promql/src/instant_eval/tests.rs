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

use std::collections::BTreeMap;
use std::ops::RangeInclusive;
use std::sync::Arc;

use datatypes::arrow::array::{Float64Array, UInt32Array};
use datatypes::arrow::buffer::ScalarBuffer;
use datatypes::series::histogram::{
    BucketSideView, CounterResetHint, HistogramBatch, HistogramBatchBuilder, HistogramView,
};
use datatypes::series::raw::RawTimestamps;
use datatypes::series::samples::{
    DenseSamplesRef, FieldSpec, SampleRef, Samples, Series, SeriesFields, SeriesSchema,
};
use datatypes::series::time::{EvalGrid, GridSpec};

use super::{info_sample_times, select_instants, timestamp_of_evaluated, timestamp_of_selection};
use crate::grid_series::GridSeriesBuilder;
use crate::grid_series::reference::{Labels, labels_of, metric_of};

fn axis(times: &[i64]) -> RawTimestamps {
    RawTimestamps::try_new(ScalarBuffer::from(times.to_vec())).unwrap()
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
fn offset_extreme_timestamps_are_checked_without_large_fixture() {
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

    // A grid of a million and one points used to be refused by a point limit of the kernel's own.
    // The kernel decides it now, and the reservation its caller grows before the call is what
    // stands in for that limit; the stale marker still suppresses the points row zero would answer,
    // so only the one sample after it is selected.
    let fields = SeriesFields::one(Samples::from_float(Float64Array::from(vec![
        Some(f64::from_bits(
            common_query::prometheus::PROMETHEUS_STALE_NAN_BITS,
        )),
        Some(1.0),
    ])));
    let (rows, times) = select(&[0, 1_000_000], &fields, 0..=1_000_000, 1, 0, 0).unwrap();
    assert_eq!(rows, [1]);
    assert_eq!(times, [1_000_000]);
}

#[test]
fn invalid_manual_inputs_are_rejected() {
    let fields = SeriesFields::one(Samples::from_float(Float64Array::from(vec![
        Some(1.0),
        Some(2.0),
    ])));
    // `RawTimestamps` validates at construction, so duplicate or descending axes are
    // rejected before `select_instants` can receive them.
    assert!(RawTimestamps::try_new(ScalarBuffer::from(vec![10, 10])).is_err());
    assert!(RawTimestamps::try_new(ScalarBuffer::from(vec![20, 10])).is_err());

    let mismatched = SeriesFields::one(Samples::from_float(Float64Array::from(vec![Some(1.0)])));
    assert!(select(&[10, 20], &mismatched, 0..=10, 1, 0, 0).is_err());
    assert!(select(&[10, 20], &fields, 0..=10, 0, 0, 0).is_err());
    assert!(select(&[10, 20], &fields, RangeInclusive::new(10, 0), 1, 0, 0).is_err());
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

fn grid_schema(fields: usize, start: i64, step: i64, points: u64) -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: (0..fields)
            .map(|field| FieldSpec {
                name: Arc::from(format!("f{field}")),
            })
            .collect::<Vec<_>>()
            .into(),
        eval_grid: Some(Arc::new(
            EvalGrid::try_new(GridSpec::try_new(start, step, points).unwrap()).unwrap(),
        )),
    })
}

/// Each field's value per position, `None` where it has none.
fn values_by_position(series: &Series) -> Vec<(usize, Vec<Option<u64>>)> {
    series
        .time
        .positions()
        .map(|(position, row)| {
            let values = series
                .fields
                .iter()
                .map(|field| match field.sample(row) {
                    SampleRef::Float(value) => Some(value.to_bits()),
                    _ => None,
                })
                .collect();
            (position, values)
        })
        .collect()
}

/// `timestamp()` evaluated by the Prometheus v3.15.0 engine, over a selector and over an
/// evaluated expression; see the header of `timestamp_cases.txt`.
#[test]
fn timestamp_matches_prometheus() {
    const LOOKBACK_MS: i64 = 25_000;
    let mut checked = 0;
    for case in include_str!("timestamp_cases.txt")
        .split("end\n")
        .filter(|case| case.contains("case "))
    {
        let lines: Vec<&str> = case.lines().filter(|line| !line.starts_with('#')).collect();
        let spec: Vec<&str> = lines[0].split_whitespace().skip(1).collect();
        let offset: i64 = spec[1].parse().unwrap();
        let grid: Vec<i64> = lines[2]
            .split_whitespace()
            .skip(1)
            .map(|value| value.parse().unwrap())
            .collect();
        let (start, step, points) = (grid[0], grid[1], grid[2] as u64);
        let schema = grid_schema(1, start, step, points);
        let end = start + (points as i64 - 1) * step;
        let mut actual = BTreeMap::new();
        let mut expected = BTreeMap::new();
        for line in &lines[3..] {
            let (kind, rest) = line.split_once(' ').unwrap();
            let (labels, values) = rest.split_once(" ;").unwrap();
            let labels: Labels = labels
                .split(',')
                .filter(|pair| *pair != "-")
                .map(|pair| {
                    let (name, value) = pair.split_once('=').unwrap();
                    (name.to_string(), value.to_string())
                })
                .collect();
            let points: Vec<(i64, u64)> = values
                .split_whitespace()
                .map(|point| {
                    let (at, bits) = point.split_once(':').unwrap();
                    (at.parse().unwrap(), u64::from_str_radix(bits, 16).unwrap())
                })
                .collect();
            match kind {
                "raw" => {
                    let times: Vec<i64> = points.iter().map(|(at, _)| *at).collect();
                    let fields = SeriesFields::one(Samples::from_float(Float64Array::from(
                        points
                            .iter()
                            .map(|(_, bits)| f64::from_bits(*bits))
                            .collect::<Vec<_>>(),
                    )));
                    let time = axis(&times);
                    let (indices, timestamps) =
                        select_instants(&time, &fields, start..=end, step, LOOKBACK_MS, offset)
                            .unwrap();
                    let metric = metric_of(&labels);
                    let result = if spec[0] == "selector" {
                        timestamp_of_selection(
                            &schema,
                            &metric,
                            &time,
                            &fields,
                            &indices,
                            &timestamps,
                        )
                    } else {
                        // Any evaluated series holding the selected points: only where it holds
                        // values matters to `timestamp`.
                        let mut builder = GridSeriesBuilder::try_new(schema.clone()).unwrap();
                        for at in &timestamps {
                            builder
                                .push(((at - start) / step) as usize, &[Some(1.0)])
                                .unwrap();
                        }
                        match builder.finish(metric).unwrap() {
                            Some(evaluated) => timestamp_of_evaluated(&schema, &evaluated),
                            None => Ok(None),
                        }
                    }
                    .unwrap();
                    if let Some(result) = result {
                        actual.insert(labels_of(&result.metric), values_by_position(&result));
                    }
                }
                "result" => {
                    let values = points
                        .into_iter()
                        .map(|(position, bits)| (position as usize, vec![Some(bits)]))
                        .collect::<Vec<_>>();
                    expected.insert(labels, values);
                }
                other => panic!("unexpected line {other}"),
            }
        }
        assert_eq!(actual, expected, "{case}");
        checked += 1;
    }
    assert!(checked > 40, "{checked}");
}

/// The series `info()` reads holds the original time in milliseconds, keeps its labels, and
/// leaves a field without a sample in the selected row empty; a native histogram is not an info
/// sample.
#[test]
fn info_sample_times_keep_milliseconds_labels_and_missing_fields() {
    let schema = grid_schema(2, 100, 10, 3);
    let time = axis(&[95, 107]);
    let fields = SeriesFields::try_from_vec(vec![
        Samples::from_float(Float64Array::from(vec![Some(1.0), Some(1.0)])),
        Samples::from_float(Float64Array::from(vec![Some(1.0), None])),
    ])
    .unwrap();
    let metric = metric_of(&Labels::from([
        ("__name__".to_string(), "target_info".to_string()),
        ("job".to_string(), "x".to_string()),
    ]));
    let (indices, timestamps) = select_instants(&time, &fields, 100..=120, 10, 20, 0).unwrap();
    let result = info_sample_times(&schema, &metric, &time, &fields, &indices, &timestamps)
        .unwrap()
        .unwrap();
    assert_eq!(result.metric, metric);
    assert_eq!(
        values_by_position(&result),
        [
            (0, vec![Some(95f64.to_bits()), Some(95f64.to_bits())]),
            (1, vec![Some(107f64.to_bits()), None]),
            (2, vec![Some(107f64.to_bits()), None]),
        ]
    );

    let histograms = SeriesFields::one(
        Samples::try_from_histogram(UInt32Array::from(vec![Some(0)]), histogram_batch(&[1.0]))
            .unwrap(),
    );
    let schema = grid_schema(1, 100, 10, 1);
    let time = axis(&[95]);
    let (indices, timestamps) = select_instants(&time, &histograms, 100..=100, 10, 20, 0).unwrap();
    assert!(
        info_sample_times(&schema, &metric, &time, &histograms, &indices, &timestamps).is_err()
    );
    assert!(
        timestamp_of_selection(&schema, &metric, &time, &histograms, &indices, &timestamps)
            .unwrap()
            .is_some(),
        "timestamp() reads the time of a histogram sample too"
    );
}

/// A selection must lie on the grid in increasing points and rows.
#[test]
fn selections_off_the_grid_or_going_back_are_rejected() {
    let schema = grid_schema(1, 100, 10, 3);
    let time = axis(&[95, 107]);
    let fields = SeriesFields::one(Samples::from_float(Float64Array::from(vec![1.0, 2.0])));
    let metric = metric_of(&Labels::new());
    for (indices, timestamps) in [
        (vec![0], vec![105]),
        (vec![0], vec![130]),
        (vec![2], vec![100]),
        (vec![1, 0], vec![100, 110]),
        (vec![0, 0], vec![110, 100]),
    ] {
        assert!(
            timestamp_of_selection(&schema, &metric, &time, &fields, &indices, &timestamps)
                .is_err(),
            "{indices:?} {timestamps:?}"
        );
    }
}
