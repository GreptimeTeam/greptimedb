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

//! Brute-force reference for operators producing grid series: results are compared as dense
//! `[grid index][field]` matrices, so a kernel cannot pass by aligning local row numbers.

use std::collections::BTreeMap;
use std::sync::Arc;

use datatypes::series::samples::{SampleRef, Series, SeriesSchema};
use datatypes::series::time::{EvalGrid, GridSpec, TimeAxis};
use datatypes::series::{Label, MetricName};
use rand::Rng;

use crate::grid_series::GridSeriesBuilder;

/// Values by absolute grid index, then by field; `None` is missing.
pub(crate) type Dense = Vec<Vec<Option<f64>>>;

/// Expands a grid series to a dense matrix over its whole grid.
fn expand(series: &Series) -> Dense {
    let TimeAxis::Grid(range) = &series.time else {
        panic!("expected a grid series, got {:?}", series.time);
    };
    let fields = series.fields.as_slice();
    let mut dense = vec![vec![None; fields.len()]; range.grid().timestamps().len()];
    for (field_index, field) in fields.iter().enumerate() {
        for (row, sample) in field.iter().enumerate() {
            dense[range.start() + row][field_index] = match sample {
                SampleRef::Missing => None,
                SampleRef::Float(value) => Some(value),
                SampleRef::Histogram(_) => panic!("unexpected histogram sample"),
            };
        }
    }
    dense
}

/// Asserts `actual` is the trimmed form of `expected`, with identical value bits and metric.
pub(crate) fn assert_series_eq(actual: Option<&Series>, expected: &Dense, metric: &MetricName) {
    assert_series_matches(actual, expected, metric, f64::to_bits, "");
}

/// Like [`assert_series_eq`], but any NaN equals any NaN: Prometheus' NaN payloads depend on how
/// the Go compiler orders commutative operands, and every NaN is serialized as `NaN`.
/// `context` names the case in failure messages.
pub(crate) fn assert_series_eq_any_nan(
    actual: Option<&Series>,
    expected: &Dense,
    metric: &MetricName,
    context: &str,
) {
    let any_nan = |value: f64| {
        if value.is_nan() {
            f64::NAN.to_bits()
        } else {
            value.to_bits()
        }
    };
    assert_series_matches(actual, expected, metric, any_nan, context);
}

fn assert_series_matches(
    actual: Option<&Series>,
    expected: &Dense,
    metric: &MetricName,
    bits_of: fn(f64) -> u64,
    context: &str,
) {
    let valid_rows = || {
        expected
            .iter()
            .enumerate()
            .filter(|(_, row)| row.iter().any(Option::is_some))
            .map(|(index, _)| index)
    };
    let Some((first, last)) = valid_rows().next().zip(valid_rows().next_back()) else {
        assert!(
            actual.is_none(),
            "{context}: expected no series, got {actual:?}"
        );
        return;
    };
    let actual =
        actual.unwrap_or_else(|| panic!("{context}: expected a series over [{first}, {last}]"));
    let TimeAxis::Grid(range) = &actual.time else {
        panic!("expected a grid series, got {:?}", actual.time);
    };
    assert_eq!(
        (range.start(), range.len()),
        (first, last + 1 - first),
        "{context}: grid range is not trimmed to the valid rows"
    );
    assert_eq!(range.grid().timestamps().len(), expected.len());
    assert_eq!(&actual.metric, metric);
    let bits = |dense: &Dense| -> Vec<Vec<Option<u64>>> {
        dense
            .iter()
            .map(|row| row.iter().map(|value| value.map(bits_of)).collect())
            .collect()
    };
    assert_eq!(bits(&expand(actual)), bits(expected), "{context}");
}

/// Values covering the bit patterns kernels most often mishandle.
pub(crate) fn special_value(rng: &mut impl Rng) -> f64 {
    const SPECIAL: [f64; 7] = [
        0.0,
        -0.0,
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::MAX,
        f64::MIN_POSITIVE,
    ];
    if rng.random_bool(0.2) {
        SPECIAL[rng.random_range(0..SPECIAL.len())]
    } else {
        rng.random_range(-1e6..1e6)
    }
}

/// A dense matrix with a random start, length and holes, all other rows missing.
pub(crate) fn random_dense(rng: &mut impl Rng, grid_len: usize, fields: usize) -> Dense {
    let mut dense = vec![vec![None; fields]; grid_len];
    let start = rng.random_range(0..grid_len);
    let end = rng.random_range(start..=grid_len);
    let missing = rng.random_range(0.0..0.8);
    for row in &mut dense[start..end] {
        for value in row.iter_mut() {
            if !rng.random_bool(missing) {
                *value = Some(special_value(rng));
            }
        }
    }
    dense
}

/// Labels with `__name__` as an ordinary entry, the way Prometheus holds them.
pub(crate) type Labels = BTreeMap<String, String>;

/// Value bits by grid index.
pub(crate) type Points = BTreeMap<usize, u64>;

/// A case of a fixture written by a promql-compat `narthex-golden-*` command.
pub(crate) struct GoldenCase {
    /// The operation and its tab-separated arguments.
    pub(crate) spec: Vec<String>,
    pub(crate) grid: Arc<EvalGrid>,
    pub(crate) inputs: Vec<(Labels, Points)>,
    /// Native histogram points of each input, as written by the generator.
    pub(crate) histograms: Vec<BTreeMap<usize, String>>,
    /// Result series by label set, or the error message.
    pub(crate) outcome: Result<BTreeMap<Labels, Points>, String>,
    /// Native histogram points of the result series, as written by the generator.
    pub(crate) result_histograms: BTreeMap<Labels, BTreeMap<usize, String>>,
}

/// Parses a fixture whose label pairs are separated by `|`.
pub(crate) fn golden_cases(text: &str) -> Vec<GoldenCase> {
    let labels = |text: &str| -> Labels {
        text.split('|')
            .filter(|pair| *pair != "-")
            .map(|pair| {
                let (name, value) = pair.split_once('=').unwrap();
                (name.to_string(), value.to_string())
            })
            .collect()
    };
    let points = |text: &str| -> (Points, BTreeMap<usize, String>) {
        let mut floats = Points::new();
        let mut histograms = BTreeMap::new();
        for point in text.split_whitespace() {
            let (index, value) = point.split_once(':').unwrap();
            let index = index.parse().unwrap();
            if let Some(histogram) = value.strip_prefix('H') {
                histograms.insert(index, histogram.to_string());
            } else {
                floats.insert(index, u64::from_str_radix(value, 16).unwrap());
            }
        }
        (floats, histograms)
    };
    let mut cases = Vec::new();
    for case in text.split("end\n").filter(|case| case.contains("case ")) {
        let lines: Vec<&str> = case.lines().filter(|line| !line.starts_with('#')).collect();
        let spec = lines[0].strip_prefix("case ").unwrap();
        let grid: Vec<i64> = lines[1]
            .split_whitespace()
            .skip(1)
            .map(|value| value.parse().unwrap())
            .collect();
        let grid = GridSpec::try_new(grid[0], grid[1], grid[2] as u64).unwrap();
        let mut inputs = Vec::new();
        let mut histograms = Vec::new();
        let mut result_histograms = BTreeMap::new();
        let mut outcome = Ok(BTreeMap::new());
        for line in &lines[2..] {
            if let Some(message) = line.strip_prefix("error ") {
                outcome = Err(message.to_string());
                continue;
            }
            let (kind, rest) = line.split_once(' ').unwrap();
            let (series_labels, series_points) = rest.split_once(" ;").unwrap();
            let (floats, histogram_points) = points(series_points);
            let entry = (labels(series_labels), floats);
            match kind {
                "series" => {
                    inputs.push(entry);
                    histograms.push(histogram_points);
                }
                "result" => {
                    if !histogram_points.is_empty() {
                        result_histograms.insert(entry.0.clone(), histogram_points);
                    }
                    outcome.as_mut().unwrap().insert(entry.0, entry.1);
                }
                other => panic!("unexpected line {other}"),
            }
        }
        cases.push(GoldenCase {
            spec: spec.split('\t').map(str::to_string).collect(),
            grid: Arc::new(EvalGrid::try_new(grid).unwrap()),
            inputs,
            histograms,
            outcome,
            result_histograms,
        });
    }
    cases
}

pub(crate) fn metric_of(labels: &Labels) -> MetricName {
    let mut labels = labels.clone();
    let name = labels.remove("__name__").map(Arc::from);
    MetricName::try_new(
        name,
        labels
            .into_iter()
            .map(|(name, value)| Label {
                name: Arc::from(name),
                value: Arc::from(value),
            })
            .collect(),
    )
    .unwrap()
}

pub(crate) fn labels_of(metric: &MetricName) -> Labels {
    let mut labels: Labels = metric
        .tags()
        .iter()
        .map(|tag| (tag.name.to_string(), tag.value.to_string()))
        .collect();
    if let Some(name) = metric.name() {
        labels.insert("__name__".to_string(), name.to_string());
    }
    labels
}

/// A single-field series of `schema` holding `points`; `None` without points.
pub(crate) fn series_of(
    schema: &Arc<SeriesSchema>,
    labels: &Labels,
    points: &Points,
) -> Option<Series> {
    let mut builder = GridSeriesBuilder::try_new(schema.clone()).unwrap();
    for (&index, &bits) in points {
        builder.push(index, &[Some(f64::from_bits(bits))]).unwrap();
    }
    builder.finish(metric_of(labels)).unwrap()
}

/// The points of a single-field series, any NaN as one NaN: Prometheus' NaN payloads depend on
/// how the Go compiler orders operands.
pub(crate) fn points_of(series: &Series) -> Points {
    let TimeAxis::Grid(range) = &series.time else {
        panic!("expected a grid series");
    };
    series.fields.as_slice()[0]
        .iter()
        .enumerate()
        .filter_map(|(row, sample)| match sample {
            SampleRef::Float(value) => Some((range.start() + row, canonical_bits(value))),
            _ => None,
        })
        .collect()
}

pub(crate) fn canonical_bits(value: f64) -> u64 {
    if value.is_nan() {
        f64::NAN.to_bits()
    } else {
        value.to_bits()
    }
}
