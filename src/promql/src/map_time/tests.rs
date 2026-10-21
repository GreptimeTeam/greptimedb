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
use std::sync::Arc;

use datafusion::arrow::array::{Float64Array, UInt32Array};
use datatypes::series::histogram::{
    BucketSideView, CounterResetHint, HistogramBatchBuilder, HistogramView,
};
use datatypes::series::samples::{
    FieldSpec, SampleRef, Samples, Series, SeriesFields, SeriesSchema,
};
use datatypes::series::time::{EvalGrid, GridRange, GridSpan, GridSpec, TimeAxis};

use super::{DateFunction, QueryContextFunction, date_of_eval_time, map_date, time};
use crate::grid_series::merge_same_labelset;
use crate::grid_series::reference::{
    Labels, Points, canonical_bits, golden_cases, labels_of, metric_of, points_by_labels,
    points_of, run_ranges, series_of, sparse_series,
};

fn schema(grid: Arc<EvalGrid>) -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: vec![FieldSpec {
            name: Arc::from("value"),
        }]
        .into(),
        eval_grid: Some(grid),
    })
}

fn function(name: &str) -> DateFunction {
    match name {
        "day_of_month" => DateFunction::DayOfMonth,
        "day_of_week" => DateFunction::DayOfWeek,
        "day_of_year" => DateFunction::DayOfYear,
        "days_in_month" => DateFunction::DaysInMonth,
        "hour" => DateFunction::Hour,
        "minute" => DateFunction::Minute,
        "month" => DateFunction::Month,
        "year" => DateFunction::Year,
        other => panic!("unknown date function {other}"),
    }
}

#[test]
fn time_functions_match_prometheus() {
    let cases = golden_cases(include_str!("prometheus_cases.txt"));
    assert_eq!(cases.len(), 200);
    for case in cases {
        let schema = schema(case.grid.clone());
        let actual: Vec<Series> = match (case.spec[0].as_str(), case.spec[1].as_str()) {
            ("eval_time", "time") => vec![time(&schema).unwrap()],
            ("eval_time", name) => vec![date_of_eval_time(function(name), &schema).unwrap()],
            ("date", name) => {
                let mut actual = Vec::new();
                for series in case
                    .inputs
                    .iter()
                    .filter_map(|(labels, points)| series_of(&schema, labels, points))
                {
                    actual.extend(map_date(function(name), &schema, &series).unwrap());
                }
                actual
            }
            (kind, _) => panic!("unknown case {kind}"),
        };
        // Dropping `__name__` can give two series one label set.
        let actual = match (merge_same_labelset(&schema, actual), case.outcome) {
            (Ok(actual), Ok(expected)) => (actual, expected),
            (Err(actual), Err(expected)) => {
                assert!(actual.to_string().contains(&expected), "{actual}");
                continue;
            }
            (actual, expected) => panic!("case {:?}: {actual:?} is not {expected:?}", case.spec),
        };
        let (actual, expected) = actual;
        // A date function keeps the values of its input but drops histograms and names, so its
        // result is one piece per run of valued positions; the fixture pins dense points.
        let actual: BTreeMap<_, _> = points_by_labels(actual);
        let expected: BTreeMap<_, _> = expected
            .into_iter()
            .map(|(labels, points)| {
                let points = points
                    .into_iter()
                    .map(|(index, bits)| (index, canonical_bits(f64::from_bits(bits))))
                    .collect();
                (labels, points)
            })
            .collect();
        assert_eq!(actual, expected, "case {:?}", case.spec);
    }
}

/// Histogram samples are dropped and the series trimmed to the remaining floats; chunked floats
/// are mapped chunk by chunk.
#[test]
fn map_date_drops_histograms_and_maps_chunks() {
    let grid = Arc::new(EvalGrid::try_new(GridSpec::try_new(0, 1000, 4).unwrap()).unwrap());
    let schema = schema(grid.clone());
    let range = GridRange::try_new(grid, 1, 3).unwrap();
    let series = |fields: Samples| {
        Series::try_new(
            &schema,
            metric_of(
                &[
                    ("__name__", "m"),
                    ("__type__", "gauge"),
                    ("__unit__", "seconds"),
                    ("host", "a"),
                ]
                .map(|(n, v)| (n.into(), v.into()))
                .into(),
            ),
            TimeAxis::Grid(range.clone()),
            SeriesFields::one(fields),
        )
        .unwrap()
    };
    let floats = |values: &[f64]| Samples::from_float(Float64Array::from(values.to_vec()));

    let empty = BucketSideView {
        span_offsets: &[],
        span_lengths: &[],
        buckets: &[],
    };
    let mut histograms = HistogramBatchBuilder::new();
    histograms
        .push(HistogramView {
            schema: 0,
            zero_threshold: 0.0,
            sum: 1.0,
            count: 1.0,
            zero_count: 1.0,
            reset_hint: CounterResetHint::Unknown,
            start_timestamp: None,
            custom_values: &[],
            positive: empty,
            negative: empty,
        })
        .unwrap();
    let histogram =
        Samples::try_from_histogram(UInt32Array::from(vec![0]), histograms.finish().unwrap())
            .unwrap();
    let mixed = Samples::try_from_chunks(vec![histogram, floats(&[3600.0, 7200.0])]).unwrap();
    let hours = map_date(DateFunction::Hour, &schema, &series(mixed))
        .unwrap()
        .expect("a mixed series with one float");
    assert_eq!(
        points_of(&hours),
        BTreeMap::from([(2, 1f64.to_bits()), (3, 2f64.to_bits())])
    );
    assert_eq!(
        labels_of(&hours.metric),
        Labels::from([("host".into(), "a".into())])
    );

    let chunked = Samples::try_from_chunks(vec![floats(&[60.0]), floats(&[120.0, 180.0])]).unwrap();
    let minutes = map_date(DateFunction::Minute, &schema, &series(chunked))
        .unwrap()
        .expect("a chunked float series");
    assert_eq!(
        points_of(&minutes),
        BTreeMap::from([
            (1, 1f64.to_bits()),
            (2, 2f64.to_bits()),
            (3, 3f64.to_bits())
        ])
    );
}

/// `map_date` reads a series of several runs by the positions its axis carries.
///
/// A sparse input cannot be mapped through the in-place fast path when a field holds a hole, so
/// the piece-cutting path runs: the result must report the right positions, not an offset from
/// the first one.
#[test]
fn map_date_reads_a_sparse_axis_by_its_positions() {
    let grid = Arc::new(EvalGrid::try_new(GridSpec::try_new(0, 1000, 8).unwrap()).unwrap());
    let schema = schema(grid.clone());
    // Positions 1, 2 and 6 hold seconds; the axis skips 3, 4 and 5.
    let mut points = Points::new();
    for (position, seconds) in [(1, 1000), (2, 2000), (6, 6000)] {
        points.insert(position, f64::to_bits(seconds as f64));
    }
    let input = sparse_series(
        &schema,
        &Labels::from([("__name__".to_string(), "m".to_string())]),
        &points,
    )
    .unwrap();
    assert_eq!(input.time.len(), 3);
    assert_eq!(input.time.first_position(), Some(1));
    assert_eq!(input.time.last_position(), Some(6));

    let output = map_date(DateFunction::Minute, &schema, &input)
        .unwrap()
        .expect("three rows");
    // No field holds a hole, so the fast path keeps the axis as it is: one series, three rows.
    assert_eq!(output.time.len(), 3);
    assert_eq!(output.metric.name(), None);
    let mut cursor = output.fields.as_slice()[0].cursor();
    let read = output
        .time
        .positions()
        .map(|(position, row)| {
            let sample = cursor.at(row).expect("the axis and the fields are aligned");
            match sample {
                SampleRef::Float(value) => (position, value),
                SampleRef::Missing => panic!("position {position} holds no value"),
                SampleRef::Histogram(_) => panic!("position {position} holds a histogram"),
            }
        })
        .collect::<Vec<_>>();
    // `minute()` of Unix seconds 1000, 2000 and 6000.
    assert_eq!(read, vec![(1, 16.0), (2, 33.0), (6, 40.0)]);

    // A hole in the field keeps the fast path from applying, so the axis is rebuilt from the rows
    // that survive: still one series, now over the runs of consecutive valued positions.
    let mut holed = Points::new();
    for (position, seconds) in [(2, 120), (3, 180), (4, 240), (6, 360), (7, 420)] {
        holed.insert(position, f64::to_bits(seconds as f64));
    }
    let input = series_of(
        &schema,
        &Labels::from([("__name__".to_string(), "m".to_string())]),
        &holed,
    )
    .unwrap();
    let output = map_date(DateFunction::Minute, &schema, &input)
        .unwrap()
        .expect("five rows");
    assert_eq!(run_ranges(&output), vec![(2, 3), (6, 2)]);
    assert_eq!(
        points_of(&output),
        BTreeMap::from([
            (2, 2f64.to_bits()),
            (3, 3f64.to_bits()),
            (4, 4f64.to_bits()),
            (6, 6f64.to_bits()),
            (7, 7f64.to_bits()),
        ])
    );

    // A sparse axis *and* a hole in the field, with the samples split over two chunks: the fast
    // path cannot apply, so the piece-cutting path must walk the axis' positions and rebuild the
    // axis from the rows that survive. Positions 0, 1, 3, 4 and 5 hold rows and position 3 is the
    // hole, so a walk that counted rows from the first position would map the wrong seconds.
    let time =
        TimeAxis::from_grid_spans(grid, vec![GridSpan::new(0, 2), GridSpan::new(3, 3)]).unwrap();
    let chunked = Samples::try_from_chunks(vec![
        Samples::from_float(Float64Array::from(vec![60.0, 120.0])),
        Samples::from_float(Float64Array::from(vec![None, Some(240.0), Some(300.0)])),
    ])
    .unwrap();
    let input = Series::try_new(
        &schema,
        metric_of(&Labels::from([("__name__".to_string(), "m".to_string())])),
        time,
        SeriesFields::one(chunked),
    )
    .unwrap();
    assert_eq!(input.time.len(), 5);
    let output = map_date(DateFunction::Minute, &schema, &input)
        .unwrap()
        .expect("four rows");
    assert_eq!(output.time.len(), 4);
    assert_eq!(run_ranges(&output), vec![(0, 2), (4, 2)]);
    // `minute()` of Unix seconds 60, 120, 240 and 300.
    assert_eq!(
        points_of(&output),
        BTreeMap::from([
            (0, 1f64.to_bits()),
            (1, 2f64.to_bits()),
            (4, 4f64.to_bits()),
            (5, 5f64.to_bits()),
        ])
    );
}

/// `range()` and `step()` are Go `Duration.Seconds` of a millisecond count, which rounds twice
/// and so differs from `ms / 1000` in the last bit for most values.
#[test]
fn query_context_durations_match_go() {
    let mut checked = 0;
    for line in include_str!("duration_seconds_cases.txt")
        .lines()
        .filter(|line| !line.starts_with('#'))
    {
        let (ms, bits) = line.split_once(' ').unwrap();
        let ms: i64 = ms.parse().unwrap();
        let expected = u64::from_str_radix(bits, 16).unwrap();
        assert_eq!(
            QueryContextFunction::Range.value(0, ms, 1).to_bits(),
            expected,
            "range() over {ms} ms"
        );
        assert_eq!(
            QueryContextFunction::Step.value(0, 1, ms).to_bits(),
            expected,
            "step() of {ms} ms"
        );
        checked += 1;
    }
    assert!(checked > 200, "{checked} durations checked");
    assert_eq!(QueryContextFunction::Step.value(5_000, 5_000, 1_000), 0.0);
    assert_eq!(QueryContextFunction::Start.value(-1_500, 0, 1), -1.5);
    assert_eq!(QueryContextFunction::End.value(0, 2_250, 1), 2.25);
}
