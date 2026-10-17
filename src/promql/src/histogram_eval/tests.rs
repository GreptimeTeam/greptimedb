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
    FieldSpec, HistogramSamples, Samples, Series, SeriesFields, SeriesSchema,
};
use datatypes::series::time::{GridRange, TimeAxis};

use super::{
    HistogramFunction, NativeHistogramFunction, evaluate_histograms, map_native_histogram,
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
    assert_eq!(cases.len(), 420);
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
            other => panic!("unknown case {other}"),
        };
        assert_outcome(case, actual, &case.outcome);
    }
}

/// `histogram_quantile` drops all metadata labels; the single-histogram functions drop only
/// `__name__`, as Prometheus does.
#[test]
fn histogram_functions_drop_metadata_labels() {
    let grid = Arc::new(
        datatypes::series::time::EvalGrid::try_new(
            datatypes::series::time::GridSpec::try_new(0, 1_000, 1).unwrap(),
        )
        .unwrap(),
    );
    let schema = Arc::new(SeriesSchema {
        fields: vec![FieldSpec {
            name: Arc::from("value"),
        }]
        .into(),
        eval_grid: Some(grid),
    });
    let labels = |extra: &[(&str, &str)]| -> Labels {
        [
            ("__name__", "m"),
            ("__type__", "histogram"),
            ("__unit__", "seconds"),
            ("job", "x"),
        ]
        .iter()
        .chain(extra)
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect()
    };
    let one = BTreeMap::from([(0, 1f64.to_bits())]);
    let buckets = vec![
        mixed_series(&schema, &labels(&[("le", "1")]), &one, &BTreeMap::new()).unwrap(),
        mixed_series(&schema, &labels(&[("le", "+Inf")]), &one, &BTreeMap::new()).unwrap(),
    ];
    let quantile =
        evaluate_histograms(HistogramFunction::Quantile(0.5), &schema, &buckets).unwrap();
    assert_eq!(
        labels_of(&quantile[0].metric),
        Labels::from([("job".to_string(), "x".to_string())])
    );

    let native = mixed_series(
        &schema,
        &labels(&[]),
        &BTreeMap::new(),
        &BTreeMap::from([(
            0,
            "0/0/0/3ff0000000000000/3ff0000000000000/0/0_1/3ff0000000000000///".to_string(),
        )]),
    )
    .unwrap();
    let count = map_native_histogram(NativeHistogramFunction::Count, &schema, &native)
        .unwrap()
        .unwrap();
    let mut expected = labels(&[]);
    expected.remove("__name__");
    assert_eq!(labels_of(&count.metric), expected);
}
