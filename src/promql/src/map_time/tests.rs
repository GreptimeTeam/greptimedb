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
use datatypes::series::samples::{FieldSpec, Samples, Series, SeriesFields, SeriesSchema};
use datatypes::series::time::{EvalGrid, GridRange, GridSpec, TimeAxis};

use super::{DateFunction, date_of_eval_time, map_date, time};
use crate::grid_series::merge_same_labelset;
use crate::grid_series::reference::{
    Labels, canonical_bits, golden_cases, labels_of, metric_of, points_by_labels, points_of,
    series_of,
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
        .pop()
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
        .pop()
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
