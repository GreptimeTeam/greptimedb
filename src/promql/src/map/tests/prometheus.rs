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

//! Golden cases evaluated by the Prometheus engine; see the header of `prometheus_cases.txt`.

use std::sync::Arc;

use datatypes::arrow::array::Float64Array;
use datatypes::series::samples::{
    FieldSpec, SampleRef, Samples, Series, SeriesFields, SeriesSchema,
};
use datatypes::series::time::{EvalGrid, GridRange, GridSpec, TimeAxis};
use datatypes::series::{Label, MetricName};

use crate::map::{MathOp, map_math};

const CASES: &str = include_str!("../prometheus_cases.txt");

type Points = Vec<(usize, f64)>;

pub(super) struct Case {
    pub(super) header: Vec<String>,
    pub(super) expr: String,
    pub(super) schema: SeriesSchema,
    pub(super) series: Vec<Series>,
    pub(super) scalar: Option<Series>,
    pub(super) result: Vec<(MetricName, Points)>,
}

pub(super) fn cases() -> Vec<Case> {
    let mut cases = Vec::new();
    let mut lines = CASES.lines().filter(|line| !line.starts_with('#'));
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
        let schema = SeriesSchema {
            fields: vec![FieldSpec {
                name: "value".into(),
            }]
            .into(),
            eval_grid: Some(Arc::new(
                EvalGrid::try_new(GridSpec::try_new(grid[0], grid[1], grid[2] as u64).unwrap())
                    .unwrap(),
            )),
        };
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
            let (kind, rest) = line.split_once(' ').unwrap();
            let (labels, points) = rest.split_once(" ;").unwrap();
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

pub(super) fn hex(bits: &str) -> f64 {
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

/// Compares labels, the trimmed absolute range and value bits; any NaN matches any NaN.
pub(super) fn assert_result(case: &Case, mut actual: Vec<Series>) {
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
                actual.to_bits() == expected.to_bits() || (actual.is_nan() && expected.is_nan()),
                "{} at {index}: {actual:e} ({:016x}) != {expected:e} ({:016x})",
                context(),
                actual.to_bits(),
                expected.to_bits(),
            );
        }
    }
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

fn math_op(header: &[String]) -> Option<MathOp> {
    Some(match header[1].as_str() {
        "abs" => MathOp::Abs,
        "ceil" => MathOp::Ceil,
        "floor" => MathOp::Floor,
        "sqrt" => MathOp::Sqrt,
        "round" => MathOp::Round {
            nearest: hex(&header[2]),
        },
        _ => return None,
    })
}

#[test]
fn math_matches_prometheus() {
    let mut matched = 0;
    for case in cases().iter().filter(|case| case.header[0] == "math") {
        let op = math_op(&case.header).unwrap_or_else(|| panic!("unmapped {}", case.expr));
        let output = case
            .series
            .iter()
            .map(|series| map_math(&case.schema, series, op).unwrap())
            .collect();
        assert_result(case, output);
        matched += 1;
    }
    assert!(matched >= 48, "only {matched} math cases");
}
