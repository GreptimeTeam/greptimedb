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
use datafusion::error::DataFusionError;
use datatypes::series::histogram::{
    BucketSideView, CounterResetHint, HistogramBatchBuilder, HistogramView,
};
use datatypes::series::samples::{
    FieldSpec, HistogramSamples, SampleRef, Samples, Series, SeriesFields, SeriesSchema,
};
use datatypes::series::time::{GridRange, TimeAxis};

use super::float_histogram::{AddOutcome, FloatHistogram};
use super::{
    HistogramAggregate, HistogramFunction, NativeHistogramFunction, aggregate_histograms,
    evaluate_histograms, map_native_histogram,
};
use crate::grid_series::merge_same_labelset;
use crate::grid_series::reference::{
    GoldenCase, Labels, Points, canonical_bits, golden_cases, labels_of, metric_of, points_of,
};

fn schema(case: &GoldenCase) -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: vec![FieldSpec {
            name: Arc::from("value"),
        }]
        .into(),
        eval_grid: Some(case.grid.clone()),
    })
}

fn bits_argument(case: &GoldenCase, index: usize) -> f64 {
    f64::from_bits(u64::from_str_radix(&case.spec[index], 16).unwrap())
}

/// A native histogram written by the generator as `/`-separated fields.
struct OwnedHistogram {
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
    fn parse(text: &str) -> Self {
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

    fn view(&self) -> HistogramView<'_> {
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
fn mixed_series(
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

/// Compares a kernel outcome with a fixture outcome; returns whether it was an error.
fn assert_outcome(
    case: &GoldenCase,
    actual: Result<Vec<Series>, DataFusionError>,
    expected: &Result<BTreeMap<Labels, Points>, String>,
) -> bool {
    match (actual, expected) {
        (Ok(actual), Ok(expected)) => {
            let actual: BTreeMap<_, _> = actual
                .iter()
                .map(|series| (labels_of(&series.metric), points_of(series)))
                .collect();
            let expected: BTreeMap<_, _> = expected
                .iter()
                .map(|(labels, points)| {
                    let points = points
                        .iter()
                        .map(|(&index, &bits)| (index, canonical_bits(f64::from_bits(bits))))
                        .collect::<Points>();
                    (labels.clone(), points)
                })
                .collect();
            assert_eq!(actual, expected, "case {:?}", case.spec);
            false
        }
        (Err(actual), Err(expected)) => {
            assert!(actual.to_string().contains(expected), "{actual}");
            true
        }
        (actual, expected) => panic!("case {:?}: {actual:?} is not {expected:?}", case.spec),
    }
}

#[test]
fn classic_histograms_match_prometheus() {
    let cases = golden_cases(include_str!("classic_cases.txt"));
    assert_eq!(cases.len(), 300);
    let mut failed = 0;
    for case in &cases {
        let schema = schema(case);
        // The fixture lists buckets by bound; the kernel must not rely on input order.
        let input: Vec<_> = case
            .inputs
            .iter()
            .rev()
            .filter_map(|(labels, points)| mixed_series(&schema, labels, points, &BTreeMap::new()))
            .collect();
        let function = match case.spec[0].as_str() {
            "quantile" => HistogramFunction::Quantile(bits_argument(case, 1)),
            "fraction" => HistogramFunction::Fraction {
                lower: bits_argument(case, 1),
                upper: bits_argument(case, 2),
            },
            other => panic!("unknown case {other}"),
        };
        let actual = evaluate_histograms(function, &schema, &input);
        failed += usize::from(assert_outcome(case, actual, &case.outcome));
    }
    assert!(failed > 20, "{failed} failed");
}

#[test]
fn native_histograms_match_prometheus() {
    let cases = golden_cases(include_str!("native_cases.txt"));
    assert_eq!(cases.len(), 661);
    for case in &cases {
        let schema = schema(case);
        let input: Vec<_> = case
            .inputs
            .iter()
            .zip(&case.histograms)
            .filter_map(|((labels, floats), histograms)| {
                mixed_series(&schema, labels, floats, histograms)
            })
            .collect();
        let map = |function| -> Result<Vec<Series>, DataFusionError> {
            let mapped = input
                .iter()
                .filter_map(|series| map_native_histogram(function, &schema, series).transpose())
                .collect::<Result<Vec<_>, _>>()?;
            merge_same_labelset(&schema, mapped)
        };
        let actual = match case.spec[0].as_str() {
            "histogram_count" => map(NativeHistogramFunction::Count),
            "histogram_sum" => map(NativeHistogramFunction::Sum),
            "histogram_avg" => map(NativeHistogramFunction::Avg),
            "histogram_stddev" => map(NativeHistogramFunction::StdDev),
            "histogram_stdvar" => map(NativeHistogramFunction::StdVar),
            "histogram_quantile" => evaluate_histograms(
                HistogramFunction::Quantile(bits_argument(case, 1)),
                &schema,
                &input,
            ),
            "histogram_fraction" => evaluate_histograms(
                HistogramFunction::Fraction {
                    lower: bits_argument(case, 1),
                    upper: bits_argument(case, 2),
                },
                &schema,
                &input,
            ),
            aggregate => {
                let (op, by_job) = match aggregate {
                    "sum" => (HistogramAggregate::Sum, false),
                    "avg" => (HistogramAggregate::Avg, false),
                    "sum_by_job" => (HistogramAggregate::Sum, true),
                    "avg_by_job" => (HistogramAggregate::Avg, true),
                    other => panic!("unknown case {other}"),
                };
                assert_aggregates(case, &schema, &input, op, by_job);
                continue;
            }
        };
        assert_outcome(case, actual, &case.outcome);
    }
}

fn canonical(value: f64) -> u64 {
    canonical_bits(value)
}

/// The fields of `h` as bits, NaN as one NaN, with its bucket indices.
fn fingerprint(h: &FloatHistogram) -> String {
    let side = |side: &[(i32, f64)]| -> String {
        side.iter()
            .map(|&(idx, count)| format!("{idx}:{:x}", canonical(count)))
            .collect::<Vec<_>>()
            .join(",")
    };
    format!(
        "{} {:?} zt={:x} zc={:x} count={:x} sum={:x} +[{}] -[{}] custom={:?}",
        h.schema,
        h.reset_hint,
        canonical(h.zero_threshold),
        canonical(h.zero_count),
        canonical(h.count),
        canonical(h.sum),
        side(&h.positive),
        side(&h.negative),
        h.custom_values
            .iter()
            .map(|v| canonical(*v))
            .collect::<Vec<_>>(),
    )
}

fn histogram(text: &str) -> FloatHistogram {
    FloatHistogram::try_from_view(&OwnedHistogram::parse(text).view()).unwrap()
}

#[test]
fn histogram_arithmetic_matches_prometheus() {
    let mut checked = 0;
    for line in include_str!("arithmetic_cases.txt").lines() {
        if line.starts_with('#') {
            continue;
        }
        let (inputs, outputs) = line.split_once(" => ").unwrap();
        let mut inputs = inputs.split(' ');
        let (op, argument) = (inputs.next().unwrap(), inputs.next().unwrap());
        let inputs: Vec<FloatHistogram> = inputs.map(histogram).collect();
        let outputs: Vec<&str> = outputs.split(' ').collect();
        let flags = |outcome: AddOutcome| {
            vec![
                outcome.counter_reset_collision.to_string(),
                outcome.custom_bounds_reconciled.to_string(),
            ]
        };
        let actual: Result<Vec<String>, _> = match op {
            "add" | "sub" => {
                let mut result = inputs[0].clone();
                let outcome = if op == "add" {
                    result.add(&inputs[1])
                } else {
                    result.sub(&inputs[1])
                };
                outcome.map(|outcome| [vec![fingerprint(&result)], flags(outcome)].concat())
            }
            "kahan" => {
                let mut sum = inputs[0].clone();
                let mut compensation = None;
                let mut any = AddOutcome::default();
                let mut result = Ok(());
                for h in &inputs[1..] {
                    match sum.kahan_add(h, compensation.take()) {
                        Ok((c, outcome)) => {
                            compensation = Some(c);
                            any.counter_reset_collision |= outcome.counter_reset_collision;
                            any.custom_bounds_reconciled |= outcome.custom_bounds_reconciled;
                        }
                        Err(error) => {
                            result = Err(error);
                            break;
                        }
                    }
                }
                result.map(|_| {
                    let c = compensation.unwrap();
                    [vec![fingerprint(&sum), fingerprint(&c)], flags(any)].concat()
                })
            }
            "mul" | "div" => {
                let factor = f64::from_bits(u64::from_str_radix(argument, 16).unwrap());
                let mut result = inputs[0].clone();
                if op == "mul" {
                    result.mul(factor);
                } else {
                    result.div(factor);
                }
                Ok(vec![fingerprint(&result)])
            }
            "compact" => {
                let mut result = inputs[0].clone();
                result.compact();
                Ok(vec![fingerprint(&result)])
            }
            "schema" => inputs[0]
                .copy_to_schema(argument.parse().unwrap())
                .map(|result| vec![fingerprint(&result)]),
            "reset" => inputs[0]
                .detect_reset(&inputs[1])
                .map(|reset| vec![reset.to_string()]),
            "equals" => Ok(vec![inputs[0].equals(&inputs[1]).to_string()]),
            other => panic!("unknown operation {other}"),
        };
        let expected: Vec<String> = match op {
            "add" | "sub" if outputs[0] != "error" => vec![
                fingerprint(&histogram(outputs[0])),
                outputs[1].to_string(),
                outputs[2].to_string(),
            ],
            "kahan" if outputs[0] != "error" => vec![
                fingerprint(&histogram(outputs[0])),
                fingerprint(&histogram(outputs[1])),
                outputs[2].to_string(),
                outputs[3].to_string(),
            ],
            "mul" | "div" | "compact" | "schema" => vec![fingerprint(&histogram(outputs[0]))],
            _ => outputs.iter().map(|output| output.to_string()).collect(),
        };
        match actual {
            Ok(actual) => assert_eq!(actual, expected, "{line}"),
            Err(error) => assert_eq!(expected, vec!["error".to_string()], "{line}: {error}"),
        }
        checked += 1;
    }
    assert_eq!(checked, 723);
}

/// Groups the input in its order as `sum`/`avg` with or without `by (job)` do, and compares the
/// aggregated histograms; float-only points belong to the float aggregation.
fn assert_aggregates(
    case: &GoldenCase,
    schema: &Arc<SeriesSchema>,
    input: &[Series],
    op: HistogramAggregate,
    by_job: bool,
) {
    assert!(
        case.outcome.is_ok(),
        "case {:?}: {:?}",
        case.spec,
        case.outcome
    );
    let mut groups: BTreeMap<Labels, Vec<Series>> = BTreeMap::new();
    for series in input {
        let mut labels = Labels::new();
        if by_job {
            labels.insert(
                "job".to_string(),
                series.metric.get("job").unwrap().to_string(),
            );
        }
        groups.entry(labels).or_default().push(series.clone());
    }
    let mut actual = BTreeMap::new();
    for (labels, group) in groups {
        if let Some(series) = aggregate_histograms(op, schema, &group, metric_of(&labels)).unwrap()
        {
            actual.insert(labels, histogram_points(&series));
        }
    }
    let expected: BTreeMap<_, _> = case
        .result_histograms
        .iter()
        .map(|(labels, points)| {
            let points: BTreeMap<_, _> = points
                .iter()
                .map(|(&index, text)| (index, fingerprint(&histogram(text))))
                .collect();
            (labels.clone(), points)
        })
        .collect();
    assert_eq!(actual, expected, "case {:?}", case.spec);
}

fn histogram_points(series: &Series) -> BTreeMap<usize, String> {
    let TimeAxis::Grid(range) = &series.time else {
        panic!("expected a grid series");
    };
    series.fields.as_slice()[0]
        .iter()
        .enumerate()
        .filter_map(|(row, sample)| match sample {
            SampleRef::Histogram(view) => Some((
                range.start() + row,
                fingerprint(&FloatHistogram::try_from_view(&view).unwrap()),
            )),
            _ => None,
        })
        .collect()
}
