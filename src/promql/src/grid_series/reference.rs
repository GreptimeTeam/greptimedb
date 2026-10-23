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

use datafusion::arrow::array::{Float64Array, UInt32Array};
use datatypes::series::histogram::{
    BucketSideView, CounterResetHint, HistogramBatchBuilder, HistogramView,
};
use datatypes::series::samples::{
    HistogramSamples, SampleRef, Samples, Series, SeriesFields, SeriesSchema,
};
use datatypes::series::time::{EvalGrid, GridRange, GridSpan, GridSpec, TimeAxis};
use datatypes::series::{Label, SeriesLabels};
use rand::Rng;

use crate::histogram_eval::float_histogram::FloatHistogram;

/// Values by absolute grid index, then by field; `None` is missing.
pub(crate) type Dense = Vec<Vec<Option<f64>>>;

/// Expands a grid series to a dense matrix over its whole grid.
///
/// The axis names the absolute position of every row, so a series of several runs expands to the
/// same matrix a dense one would.
pub(crate) fn expand(series: &Series) -> Dense {
    let Some(grid) = series.time.grid() else {
        panic!("expected a grid series, got {:?}", series.time);
    };
    let fields = series.fields.as_slice();
    let mut dense = vec![vec![None; fields.len()]; grid.spec().len as usize];
    let mut cursors = fields
        .iter()
        .map(|field| field.cursor())
        .collect::<Vec<_>>();
    for (position, row) in series.time.positions() {
        for (field_index, field) in cursors.iter_mut().enumerate() {
            dense[position][field_index] =
                match field.at(row).expect("the axis and the fields are aligned") {
                    SampleRef::Missing => None,
                    SampleRef::Float(value) => Some(value),
                    SampleRef::Histogram(_) => panic!("unexpected histogram sample"),
                };
        }
    }
    dense
}

/// Asserts `actual` is the trimmed form of `expected`, with identical value bits and metric.
pub(crate) fn assert_series_eq(actual: Option<&Series>, expected: &Dense, metric: &SeriesLabels) {
    assert_series_matches(actual, expected, metric, f64::to_bits, "");
}

/// Like [`assert_series_eq`], but any NaN equals any NaN: Prometheus' NaN payloads depend on how
/// the Go compiler orders commutative operands, and every NaN is serialized as `NaN`.
/// `context` names the case in failure messages.
pub(crate) fn assert_series_eq_any_nan(
    actual: Option<&Series>,
    expected: &Dense,
    metric: &SeriesLabels,
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
    metric: &SeriesLabels,
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
    let Some(grid) = actual.time.grid() else {
        panic!("expected a grid series, got {:?}", actual.time);
    };
    // Trimmed means: the carried rows start at the first valid position and end at the last, and
    // there is one row per valid position. A series of several runs satisfies both only when it
    // carries the same positions.
    assert_eq!(
        (actual.time.first_position(), actual.time.last_position()),
        (Some(first), Some(last)),
        "{context}: grid range is not trimmed to the valid rows"
    );
    assert_eq!(
        actual.time.len(),
        last + 1 - first,
        "{context}: the result carries a row for a position that holds no value"
    );
    assert_eq!(grid.spec().len as usize, expected.len());
    assert_eq!(&actual.metric, metric);
    let bits = |dense: &Dense| -> Vec<Vec<Option<u64>>> {
        dense
            .iter()
            .map(|row| row.iter().map(|value| value.map(bits_of)).collect())
            .collect()
    };
    assert_eq!(bits(&expand(actual)), bits(expected), "{context}");
}

/// Stitches the pieces of one series back into a dense matrix over the whole grid.
///
/// A result only carries the positions holding a value, and a stream may hand it out as several
/// pieces, so comparing against a dense expectation means putting them back together. Two pieces
/// covering one position would be a protocol violation, so it fails here rather than silently
/// overwriting.
pub(crate) fn stitch(pieces: &[Series]) -> Dense {
    let mut dense: Option<Dense> = None;
    for piece in pieces {
        let Some(grid) = piece.time.grid() else {
            panic!("expected a grid series, got {:?}", piece.time);
        };
        let dense = dense
            .get_or_insert_with(|| vec![vec![None; piece.fields.len()]; grid.spec().len as usize]);
        let mut cursors = piece
            .fields
            .iter()
            .map(|field| field.cursor())
            .collect::<Vec<_>>();
        for (position, row) in piece.time.positions() {
            for (field, cursor) in cursors.iter_mut().enumerate() {
                let sample = cursor.at(row).expect("the axis and the fields are aligned");
                let target = &mut dense[position][field];
                assert!(
                    target.is_none() || matches!(sample, SampleRef::Missing),
                    "two pieces hold a value at position {position}"
                );
                *target = match sample {
                    SampleRef::Missing => None,
                    SampleRef::Float(value) => Some(value),
                    SampleRef::Histogram(_) => panic!("unexpected histogram sample"),
                };
            }
        }
    }
    dense.unwrap_or_default()
}

/// The `(start, len)` of every run of one series, in grid order.
///
/// A series names the rows it carries through its axis, so a result of several runs reports one
/// entry per run and never the envelope: the positions between two runs hold no row.
pub(crate) fn run_ranges(series: &Series) -> Vec<(usize, usize)> {
    if series.time.grid().is_none() {
        panic!("expected a grid series, got {:?}", series.time);
    }
    series
        .time
        .runs()
        .map(|run| (run.position, run.len))
        .collect()
}

/// The `(start, len)` of every run of every piece, in grid order: one entry per run, so a piece
/// made of several runs reports each of them.
pub(crate) fn piece_ranges(pieces: &[Series]) -> Vec<(usize, usize)> {
    pieces.iter().flat_map(run_ranges).collect()
}

/// Asserts `actual` -- one series, or none -- reproduces `expected` over the grid.
///
/// A result carries only the positions that hold a value, so its rows may form several runs: the
/// comparison expands it over the grid and checks it covers exactly the valued positions. `None`
/// means no row survived, which is only right when `expected` holds no value either.
pub(crate) fn assert_result_eq(actual: Option<&Series>, expected: &Dense, metric: &SeriesLabels) {
    match actual {
        Some(actual) => assert_pieces_eq(std::slice::from_ref(actual), expected, metric),
        None => assert_pieces_eq(&[], expected, metric),
    }
}

/// Asserts `pieces` are the pieces of `expected`: they reproduce it, and together they cover
/// exactly the positions that hold a value, none of them twice.
pub(crate) fn assert_pieces_eq(pieces: &[Series], expected: &Dense, metric: &SeriesLabels) {
    for piece in pieces {
        assert_eq!(&piece.metric, metric, "a piece changed the metric");
    }
    let valued: Vec<usize> = expected
        .iter()
        .enumerate()
        .filter(|(_, row)| row.iter().any(Option::is_some))
        .map(|(index, _)| index)
        .collect();
    if valued.is_empty() {
        // Nothing holds a value anywhere, so there is no piece at all -- not one piece of nulls.
        assert!(
            pieces.is_empty(),
            "expected no pieces, got {}",
            pieces.len()
        );
        return;
    }
    // Bit comparison, since a NaN never equals itself.
    let bits = |dense: &Dense| -> Vec<Vec<Option<u64>>> {
        dense
            .iter()
            .map(|row| row.iter().map(|value| value.map(f64::to_bits)).collect())
            .collect()
    };
    assert_eq!(bits(&stitch(pieces)), bits(expected));
    let mut covered: Vec<usize> = piece_ranges(pieces)
        .into_iter()
        .flat_map(|(start, len)| start..start + len)
        .collect();
    covered.sort_unstable();
    assert_eq!(
        covered, valued,
        "the pieces do not cover exactly the positions holding a value"
    );
}

/// The points of each label set of `pieces`, folded back into one dense point map first.
///
/// An operator hands out one series per logical output, and a stream may hand one of them over as
/// several chunks, while a fixture pins dense points, so the pieces of a label set are joined
/// before comparing.
pub(crate) fn points_by_labels(pieces: Vec<Series>) -> std::collections::BTreeMap<Labels, Points> {
    crate::golden::fold_pieces(pieces)
        .iter()
        .map(|series| (labels_of(&series.metric), points_of(series)))
        .collect()
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

pub(crate) fn metric_of(labels: &Labels) -> SeriesLabels {
    let mut labels = labels.clone();
    let name = labels.remove("__name__").map(Arc::from);
    SeriesLabels::try_new(
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

pub(crate) fn labels_of(metric: &SeriesLabels) -> Labels {
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
///
/// Built directly rather than through the builder: this is an *input* to an operator, so a hole
/// stays a null inside one series instead of cutting it into pieces.
pub(crate) fn series_of(
    schema: &Arc<SeriesSchema>,
    labels: &Labels,
    points: &Points,
) -> Option<Series> {
    let start = *points.keys().min()?;
    let end = *points.keys().max()? + 1;
    let values: Float64Array = (start..end)
        .map(|index| points.get(&index).map(|bits| f64::from_bits(*bits)))
        .collect();
    let time = GridRange::try_new(schema.eval_grid.clone().unwrap(), start, end - start).unwrap();
    Some(
        Series::try_new(
            schema,
            metric_of(labels),
            TimeAxis::Grid(time),
            SeriesFields::one(Samples::from_float(values)),
        )
        .unwrap(),
    )
}

/// A single-field series of `schema` holding `points` on an axis that skips the positions between
/// them: one run per maximal stretch of consecutive keys.
///
/// This is a series of several grid runs, the shape an input can already have; it is built by hand
/// here because the builder is the thing under test. The field holds one row per key, in key
/// order, so a consumer that forgets to read the axis will read the wrong values rather than
/// merely a shorter series.
pub(crate) fn sparse_series(
    schema: &Arc<SeriesSchema>,
    labels: &Labels,
    points: &Points,
) -> Option<Series> {
    let values: Float64Array = points.values().map(|bits| f64::from_bits(*bits)).collect();
    let mut spans: Vec<GridSpan> = Vec::new();
    for &position in points.keys() {
        match spans.last_mut() {
            Some(span) if span.start() + span.len() == position => {
                *span = GridSpan::new(span.start(), span.len() + 1);
            }
            _ => spans.push(GridSpan::new(position, 1)),
        }
    }
    let grid = schema
        .eval_grid
        .clone()
        .expect("a sparse series needs a grid");
    let time = TimeAxis::from_grid_spans(grid, spans).expect("the spans fit the grid");
    Some(
        Series::try_new(
            schema,
            metric_of(labels),
            // A one-run axis is the dense fast path, so a sparse fixture of one run is the same
            // series either shape would build.
            time,
            SeriesFields::one(Samples::from_float(values)),
        )
        .unwrap(),
    )
}

/// The points of a single-field series, any NaN as one NaN: Prometheus' NaN payloads depend on
/// how the Go compiler orders operands.
pub(crate) fn points_of(series: &Series) -> Points {
    if series.time.grid().is_none() {
        panic!("expected a grid series, got {:?}", series.time);
    }
    let mut cursor = series.fields.as_slice()[0].cursor();
    series
        .time
        .positions()
        .filter_map(|(position, row)| {
            match cursor.at(row).expect("the axis and the fields are aligned") {
                SampleRef::Float(value) => Some((position, canonical_bits(value))),
                _ => None,
            }
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

/// Prometheus `almost.Equal`, treating results below 1e-10 as equal and any NaN as equal to NaN,
/// as promql-compat compares engines.
pub(crate) fn close(a: f64, b: f64, epsilon: f64) -> bool {
    if a.is_nan() || b.is_nan() {
        return a.is_nan() && b.is_nan();
    }
    if a == b {
        return true;
    }
    let (diff, sum) = ((a - b).abs(), a.abs() + b.abs());
    let equal = if a == 0.0 || b == 0.0 || sum < f64::MIN_POSITIVE {
        diff < epsilon * f64::MIN_POSITIVE
    } else {
        diff / sum.min(f64::MAX) < epsilon
    };
    equal || (a.abs() < 1e-10 && b.abs() < 1e-10)
}

/// A native histogram written by the generator as `/`-separated fields.
pub(crate) struct OwnedHistogram {
    schema: i32,
    zero_threshold: f64,
    zero_count: f64,
    count: f64,
    sum: f64,
    reset_hint: i32,
    positive: (Vec<i32>, Vec<i32>, Vec<f64>),
    negative: (Vec<i32>, Vec<i32>, Vec<f64>),
    custom_values: Vec<f64>,
}

impl OwnedHistogram {
    pub(crate) fn parse(text: &str) -> Self {
        let fields: Vec<&str> = text.split('/').collect();
        let float = |text: &str| f64::from_bits(u64::from_str_radix(text, 16).unwrap());
        let floats = |text: &str| -> Vec<f64> {
            text.split(',')
                .filter(|v| !v.is_empty())
                .map(float)
                .collect()
        };
        let spans = |text: &str| -> (Vec<i32>, Vec<i32>) {
            text.split(',')
                .filter(|v| !v.is_empty())
                .map(|span| {
                    let (offset, length) = span.split_once('_').unwrap();
                    (
                        offset.parse::<i32>().unwrap(),
                        length.parse::<i32>().unwrap(),
                    )
                })
                .unzip()
        };
        let (positive_offsets, positive_lengths) = spans(fields[6]);
        let (negative_offsets, negative_lengths) = spans(fields[8]);
        Self {
            schema: fields[0].parse().unwrap(),
            zero_threshold: float(fields[1]),
            zero_count: float(fields[2]),
            count: float(fields[3]),
            sum: float(fields[4]),
            reset_hint: fields[5].parse().unwrap(),
            positive: (positive_offsets, positive_lengths, floats(fields[7])),
            negative: (negative_offsets, negative_lengths, floats(fields[9])),
            custom_values: floats(fields[10]),
        }
    }

    pub(crate) fn view(&self) -> HistogramView<'_> {
        fn side(
            (offsets, lengths, buckets): &(Vec<i32>, Vec<i32>, Vec<f64>),
        ) -> BucketSideView<'_> {
            BucketSideView {
                span_offsets: offsets,
                span_lengths: lengths,
                buckets,
            }
        }
        HistogramView {
            schema: self.schema,
            zero_threshold: self.zero_threshold,
            sum: self.sum,
            count: self.count,
            zero_count: self.zero_count,
            reset_hint: CounterResetHint::from(self.reset_hint),
            start_timestamp: None,
            custom_values: &self.custom_values,
            positive: side(&self.positive),
            negative: side(&self.negative),
        }
    }
}

/// A single-field series holding float and native histogram points.
pub(crate) fn mixed_series(
    schema: &Arc<SeriesSchema>,
    labels: &Labels,
    floats: &Points,
    histograms: &BTreeMap<usize, String>,
) -> Option<Series> {
    let points = floats.keys().chain(histograms.keys());
    let start = *points.clone().min()?;
    let end = *points.max()? + 1;
    let float_values: Float64Array = (start..end)
        .map(|point| floats.get(&point).map(|bits| f64::from_bits(*bits)))
        .collect();
    let samples = if histograms.is_empty() {
        Samples::from_float(float_values)
    } else {
        let mut builder = HistogramBatchBuilder::new();
        let indices: Vec<_> = (start..end)
            .map(|point| {
                let text = histograms.get(&point)?;
                Some(builder.push(OwnedHistogram::parse(text).view()).unwrap())
            })
            .collect();
        let histograms =
            HistogramSamples::try_new(UInt32Array::from(indices), builder.finish().unwrap())
                .unwrap();
        Samples::try_from_mixed(float_values, histograms).unwrap()
    };
    let range = GridRange::try_new(schema.eval_grid.clone().unwrap(), start, end - start).unwrap();
    Some(
        Series::try_new(
            schema,
            metric_of(labels),
            TimeAxis::Grid(range),
            SeriesFields::one(samples),
        )
        .unwrap(),
    )
}

/// Asserts `got` equals `want` with values within `epsilon`, bucket by bucket.
pub(crate) fn assert_histogram_close(
    got: &FloatHistogram,
    want: &FloatHistogram,
    epsilon: f64,
    context: &str,
) {
    let scalars = |h: &FloatHistogram| [h.zero_count, h.count, h.sum];
    let ok = got.schema == want.schema
        && got.reset_hint == want.reset_hint
        && got.zero_threshold.to_bits() == want.zero_threshold.to_bits()
        && got.custom_values == want.custom_values
        && scalars(got)
            .iter()
            .zip(scalars(want))
            .all(|(a, b)| close(*a, b, epsilon))
        && [
            (&got.positive, &want.positive),
            (&got.negative, &want.negative),
        ]
        .iter()
        .all(|(a, b)| {
            a.len() == b.len()
                && a.iter()
                    .zip(b.iter())
                    .all(|((i, x), (j, y))| i == j && close(*x, *y, epsilon))
        });
    assert!(ok, "{context}: {got:?} vs {want:?}");
}
