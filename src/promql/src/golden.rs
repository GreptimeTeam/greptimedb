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

//! Fixtures evaluated by the Prometheus engine; each fixture's header names its generator.

use std::sync::Arc;

use datatypes::arrow::array::Float64Array;
use datatypes::series::samples::{
    FieldSpec, SampleRef, Samples, Series, SeriesFields, SeriesSchema,
};
use datatypes::series::time::{EvalGrid, GridRange, GridSpec, TimeAxis};
use datatypes::series::{Label, MetricName};

type Points = Vec<(usize, f64)>;

pub(crate) struct Case {
    pub(crate) header: Vec<String>,
    pub(crate) expr: String,
    pub(crate) schema: Arc<SeriesSchema>,
    pub(crate) series: Vec<Series>,
    pub(crate) scalar: Option<Series>,
    pub(crate) result: Vec<(MetricName, Points)>,
}

/// Parses a fixture of `case`/`expr`/`grid`/`series`/`scalar`/`result`/`end` blocks.
pub(crate) fn cases(text: &str) -> Vec<Case> {
    let mut cases = Vec::new();
    let mut lines = text.lines().filter(|line| !line.starts_with('#'));
    while let Some(line) = lines.next() {
        let header = line.strip_prefix("case ").expect("case line");
        let expr = lines
            .next()
            .unwrap()
            .strip_prefix("expr ")
            .unwrap()
            .to_string();
        let grid: Vec<i64> = lines.next().unwrap()["grid ".len()..]
            .split(' ')
            .map(|v| v.parse().unwrap())
            .collect();
        let schema = Arc::new(SeriesSchema {
            fields: vec![FieldSpec {
                name: "value".into(),
            }]
            .into(),
            eval_grid: Some(Arc::new(
                EvalGrid::try_new(GridSpec::try_new(grid[0], grid[1], grid[2] as u64).unwrap())
                    .unwrap(),
            )),
        });
        let mut case = Case {
            header: header.split(' ').map(String::from).collect(),
            expr,
            series: Vec::new(),
            scalar: None,
            result: Vec::new(),
            schema,
        };
        for line in lines.by_ref() {
            if line == "end" {
                break;
            }
            let (head, points) = line.split_once(';').unwrap();
            let (kind, labels) = head
                .trim_end()
                .split_once(' ')
                .unwrap_or((head.trim_end(), "-"));
            let (metric, points) = (parse_metric(labels), parse_points(points));
            match kind {
                "series" => {
                    if let Some(series) = grid_series(&case.schema, metric, &points) {
                        case.series.push(series);
                    }
                }
                "scalar" => case.scalar = grid_series(&case.schema, metric, &points),
                "result" => case.result.push((metric, points)),
                _ => panic!("unknown line {line}"),
            }
        }
        cases.push(case);
    }
    cases
}

fn parse_metric(labels: &str) -> MetricName {
    let mut name = None;
    let mut tags = Vec::new();
    for label in labels
        .split(',')
        .filter(|label| *label != "-" && !label.is_empty())
    {
        let (key, value) = label.split_once('=').unwrap();
        if key == "__name__" {
            name = Some(Arc::from(value));
        } else {
            tags.push(Label {
                name: key.into(),
                value: value.into(),
            });
        }
    }
    MetricName::try_new(name, tags).unwrap()
}

fn parse_points(points: &str) -> Points {
    points
        .split_whitespace()
        .map(|point| {
            let (index, bits) = point.split_once(':').unwrap();
            (
                index.parse().unwrap(),
                f64::from_bits(u64::from_str_radix(bits, 16).unwrap()),
            )
        })
        .collect()
}

pub(crate) fn hex(bits: &str) -> f64 {
    f64::from_bits(u64::from_str_radix(bits, 16).unwrap())
}

/// Builds the trimmed series Eval produces: from the first to the last point, nulls between.
fn grid_series(schema: &SeriesSchema, metric: MetricName, points: &Points) -> Option<Series> {
    let (&(start, _), &(last, _)) = points.first().zip(points.last())?;
    let mut values = vec![None; last - start + 1];
    for &(index, value) in points {
        values[index - start] = Some(value);
    }
    let range = GridRange::try_new(schema.eval_grid.clone().unwrap(), start, values.len()).unwrap();
    Some(
        Series::try_new(
            schema,
            metric,
            TimeAxis::Grid(range),
            SeriesFields::one(Samples::from_float(Float64Array::from(values))),
        )
        .unwrap(),
    )
}

/// Compares labels and the trimmed absolute range exactly, and values with [`close`].
pub(crate) fn assert_result(case: &Case, mut actual: Vec<Series>) {
    actual.sort_by_key(|series| label_key(&series.metric));
    let mut expected = case.result.iter().collect::<Vec<_>>();
    expected.sort_by_key(|(metric, _)| label_key(metric));
    let context = || format!("{} ({})", case.expr, case.header.join(" "));
    assert_eq!(actual.len(), expected.len(), "{}", context());
    for (actual, (metric, points)) in actual.iter().zip(expected) {
        assert_eq!(&actual.metric, metric, "{}", context());
        let TimeAxis::Grid(range) = &actual.time else {
            panic!("{}: lost grid axis", context());
        };
        let mut present = Vec::new();
        for field in actual.fields.iter() {
            for (offset, sample) in field.iter().enumerate() {
                match sample {
                    SampleRef::Float(value) => present.push((range.start() + offset, value)),
                    SampleRef::Missing => {}
                    _ => panic!("{}: non-float output", context()),
                }
            }
        }
        assert_eq!(
            (range.start(), range.start() + range.len()),
            (points[0].0, points.last().unwrap().0 + 1),
            "{}: untrimmed range",
            context()
        );
        assert_eq!(present.len(), points.len(), "{}", context());
        for (&(index, actual), &(expected_index, expected)) in present.iter().zip(points) {
            assert_eq!(index, expected_index, "{}", context());
            assert!(
                close(actual, expected),
                "{} at {index}: {actual:e} ({:016x}) != {expected:e} ({:016x})",
                context(),
                actual.to_bits(),
                expected.to_bits(),
            );
        }
    }
}

/// Prometheus `almost.Equal` with relative tolerance 1e-10, treating results below 1e-10 as
/// equal and any NaN as equal to NaN, as promql-compat compares engines.
pub(crate) fn close(a: f64, b: f64) -> bool {
    const EPSILON: f64 = 1e-10;
    if a.is_nan() || b.is_nan() {
        return a.is_nan() && b.is_nan();
    }
    if a == b {
        return true;
    }
    let (diff, sum) = ((a - b).abs(), a.abs() + b.abs());
    let equal = if a == 0.0 || b == 0.0 || sum < f64::MIN_POSITIVE {
        diff < EPSILON * f64::MIN_POSITIVE
    } else {
        diff / sum.min(f64::MAX) < EPSILON
    };
    equal || (a.abs() < EPSILON && b.abs() < EPSILON)
}

fn label_key(metric: &MetricName) -> Vec<(String, String)> {
    let mut key = metric
        .tags()
        .iter()
        .map(|label| (label.name.to_string(), label.value.to_string()))
        .collect::<Vec<_>>();
    if let Some(name) = metric.name() {
        key.insert(0, ("__name__".into(), name.into()));
    }
    key
}
