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

/// Compares labels and the absolute range a result covers, and its values with [`close`].
///
/// An operator hands out one series per logical output, and a stream may hand one of them over as
/// several chunks, so one label set can arrive as several series. They are folded back together
/// here, in label order, before the comparison: the values are what a fixture pins down, not how
/// the result was cut.
///
/// `actual` is whatever the operator produced: one series for a kernel with one output identity
/// (an `Option`, so `None` is "no row survived"), or several for one with several (an aggregation
/// over groups, or the chunk sequence of a stream).
pub(crate) fn assert_result(case: &Case, actual: impl IntoIterator<Item = Series>) {
    let context = || format!("{} ({})", case.expr, case.header.join(" "));
    let mut actual = fold_pieces(actual.into_iter().collect());
    actual.sort_by_key(|series| label_key(&series.metric));
    let mut expected = case.result.iter().collect::<Vec<_>>();
    expected.sort_by_key(|(metric, _)| label_key(metric));
    assert_eq!(actual.len(), expected.len(), "{}", context());
    for (actual, (metric, points)) in actual.iter().zip(expected) {
        assert_eq!(&actual.metric, metric, "{}", context());
        if actual.time.grid().is_none() {
            panic!("{}: lost grid axis", context());
        }
        // The axis names the absolute position of every row, so a result of several runs is read
        // correctly instead of being addressed from its first position.
        // One forward cursor per field: the rows only increase, so this is one pass per field.
        let mut present = Vec::new();
        let mut cursors = actual
            .fields
            .iter()
            .map(|field| field.cursor())
            .collect::<Vec<_>>();
        for (point, row) in actual.time.positions() {
            for cursor in &mut cursors {
                match cursor.at(row).expect("the axis and the fields are aligned") {
                    SampleRef::Float(value) => present.push((point, value)),
                    SampleRef::Missing => {}
                    _ => panic!("{}: non-float output", context()),
                }
            }
        }
        assert_eq!(
            (actual.time.first_position(), actual.time.last_position()),
            (Some(points[0].0), Some(points.last().unwrap().0),),
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

/// Joins the pieces of each label set into one dense series, with a null where no piece held a
/// value. A label set handed over once stays as it is, several runs and all.
pub(crate) fn fold_pieces(pieces: Vec<Series>) -> Vec<Series> {
    let mut groups: Vec<(LabelKey, Vec<Series>)> = Vec::new();
    for piece in pieces {
        let key = label_key(&piece.metric);
        match groups.iter_mut().find(|(group, _)| *group == key) {
            Some((_, members)) => members.push(piece),
            None => groups.push((key, vec![piece])),
        }
    }
    groups
        .into_iter()
        .map(|(_, members)| {
            if members.len() == 1 {
                return members.into_iter().next().unwrap();
            }
            fold_one(members)
        })
        .collect()
}

/// Folds one label set's pieces into one series over the span they cover.
fn fold_one(pieces: Vec<Series>) -> Series {
    let metric = pieces[0].metric.clone();
    let fields = pieces[0].fields.len();
    let grid = match pieces[0].time.grid() {
        Some(grid) => grid.clone(),
        None => panic!("expected a grid series, got {:?}", pieces[0].time),
    };
    let mut rows: Vec<Option<Vec<Option<f64>>>> =
        (0..grid.timestamps().len()).map(|_| None).collect();
    for piece in &pieces {
        let mut cursors = piece
            .fields
            .iter()
            .map(|field| field.cursor())
            .collect::<Vec<_>>();
        for (position, row) in piece.time.positions() {
            let values: Vec<Option<f64>> = cursors
                .iter_mut()
                .map(
                    |field| match field.at(row).expect("the axis and the fields are aligned") {
                        SampleRef::Float(value) => Some(value),
                        SampleRef::Missing => None,
                        _ => panic!("a fixture only pins float outputs"),
                    },
                )
                .collect();
            if values.iter().any(Option::is_some) {
                assert!(
                    rows[position].is_none(),
                    "two pieces hold a value at position {position}"
                );
                rows[position] = Some(values);
            }
        }
    }
    let first = rows.iter().position(Option::is_some).unwrap_or(0);
    let last = rows.iter().rposition(Option::is_some).map_or(0, |i| i + 1);
    let rows = &rows[first..last.max(first)];
    let fields = (0..fields)
        .map(|field| {
            let values: Float64Array = rows
                .iter()
                .map(|row| row.as_ref().and_then(|row| row[field]))
                .collect();
            Samples::from_float(values)
        })
        .collect::<Vec<_>>();
    let fields = SeriesFields::try_from_vec(fields).unwrap();
    let time = GridRange::try_new(grid.clone(), first, rows.len()).unwrap();
    let schema = SeriesSchema {
        fields: (0..pieces[0].fields.len())
            .map(|index| FieldSpec {
                name: Arc::from(format!("f{index}")),
            })
            .collect::<Vec<_>>()
            .into(),
        eval_grid: Some(grid),
    };
    Series::try_new(&schema, metric, TimeAxis::Grid(time), fields).unwrap()
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

/// A label set as name/value pairs, the way the fixtures name one.
type LabelKey = Vec<(String, String)>;

fn label_key(metric: &MetricName) -> LabelKey {
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
