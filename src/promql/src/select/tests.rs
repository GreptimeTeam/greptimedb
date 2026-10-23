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

use datafusion::arrow::array::Float64Array;
use datafusion::error::DataFusionError;
use datatypes::series::samples::{
    FieldSpec, SampleRef, Samples, Series, SeriesFields, SeriesSchema,
};
use datatypes::series::time::{EvalGrid, GridRange, GridSpec, TimeAxis};
use datatypes::series::{Label, SeriesLabels};

use super::{
    Grouping, LabelMatcher, MatchOp, SortOrder, absent, absent_labels, limit_ratio, limitk,
    natural_less, sample_offset, scalar, sort_instant, topk,
};
use crate::golden::fold_pieces;
use crate::grid_series::reference::{
    Dense, Labels, OwnedHistogram, Points, assert_pieces_eq, assert_result_eq, piece_ranges,
    run_ranges, sparse_series,
};
use crate::grid_series::{GridSample, GridSeriesBuilder};
use crate::histogram_eval::float_histogram::FloatHistogram;

fn schema(points: u64, fields: usize) -> SeriesSchema {
    SeriesSchema {
        fields: (0..fields)
            .map(|index| FieldSpec {
                name: Arc::from(format!("f{index}")),
            })
            .collect::<Vec<_>>()
            .into(),
        eval_grid: Some(Arc::new(
            EvalGrid::try_new(GridSpec::try_new(100_000, 10_000, points).unwrap()).unwrap(),
        )),
    }
}

/// Parses `name=value,...`, with `__name__` as the metric name; `-` is no labels.
fn metric(labels: &str) -> SeriesLabels {
    let mut name = None;
    let mut tags = Vec::new();
    for pair in labels.split(',').filter(|pair| *pair != "-") {
        let (label, value) = pair.split_once('=').unwrap();
        if label == "__name__" {
            name = Some(Arc::from(value));
        } else {
            tags.push(Label {
                name: Arc::from(label),
                value: Arc::from(value),
            });
        }
    }
    SeriesLabels::try_new(name, tags).unwrap()
}

fn dense(points: usize, values: &str) -> Dense {
    let mut dense = vec![vec![None]; points];
    for point in values.split_whitespace() {
        let (index, bits) = point.split_once(':').unwrap();
        dense[index.parse::<usize>().unwrap()][0] =
            Some(f64::from_bits(u64::from_str_radix(bits, 16).unwrap()));
    }
    dense
}

/// The `dense` matrix as one trimmed series, `None` when every row is missing.
///
/// An *input*, so the holes stay nulls inside one series: the builder would cut them into pieces,
/// and the operators under test take one series per input.
fn build(schema: &SeriesSchema, metric: SeriesLabels, dense: &Dense) -> Option<Series> {
    let start = dense
        .iter()
        .position(|row| row.iter().any(Option::is_some))?;
    let end = dense
        .iter()
        .rposition(|row| row.iter().any(Option::is_some))?
        + 1;
    let rows = &dense[start..end];
    let fields = (0..schema.fields.len())
        .map(|field| {
            let values: Float64Array = rows.iter().map(|row| row[field]).collect();
            Samples::from_float(values)
        })
        .collect::<Vec<_>>();
    let time = GridRange::try_new(schema.eval_grid.clone().unwrap(), start, rows.len()).unwrap();
    Some(
        Series::try_new(
            schema,
            metric,
            TimeAxis::Grid(time),
            SeriesFields::try_from_vec(fields).unwrap(),
        )
        .unwrap(),
    )
}

fn names(list: &str) -> Vec<Arc<str>> {
    list.split(',')
        .filter(|name| *name != "-")
        .map(Arc::from)
        .collect()
}

/// `topk`/`bottomk`, `scalar` and `absent` evaluated by the Prometheus v3.15.0 engine; see the
/// header of `prometheus_cases.txt`.
#[test]
fn select_operators_match_prometheus() {
    let checked = check_cases(
        include_str!("prometheus_cases.txt"),
        |spec, schema, series| match spec[0] {
            "topk" => {
                let k: f64 = spec[2].parse().unwrap();
                topk(
                    k,
                    spec[1] == "topk",
                    &grouping(spec[3], spec[4]),
                    schema,
                    series,
                )
            }
            "scalar" => Ok(vec![scalar(schema, series).unwrap()]),
            "absent" => {
                let matchers: Vec<LabelMatcher> = spec
                    .get(1)
                    .filter(|matchers| **matchers != "-")
                    .map(|matchers| {
                        matchers
                            .split(',')
                            .map(|matcher| {
                                let (name, op, value) = match matcher.split_once("!=") {
                                    Some((name, value)) => (name, MatchOp::NotEqual, value),
                                    None => {
                                        let (name, value) = matcher.split_once('=').unwrap();
                                        (name, MatchOp::Equal, value)
                                    }
                                };
                                let value = value.trim_matches('"');
                                LabelMatcher {
                                    name: Arc::from(name),
                                    op,
                                    value: Arc::from(value),
                                    matches_empty: (op == MatchOp::Equal) == value.is_empty(),
                                }
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                let selected: Vec<Series> = series
                    .iter()
                    .filter(|item| {
                        matchers.iter().all(|matcher| {
                            let value = item.metric.get(&matcher.name).unwrap_or("");
                            (value == &*matcher.value) == (matcher.op == MatchOp::Equal)
                        })
                    })
                    .cloned()
                    .collect();
                Ok(absent(schema, &selected, absent_labels(&matchers).unwrap())
                    .unwrap()
                    .into_iter()
                    .collect())
            }
            other => panic!("unknown case {other}"),
        },
    );
    assert!(checked > 200, "{checked} result series checked");
}

/// `limitk` and `limit_ratio` evaluated by the Prometheus v3.15.0 engine; see the header of
/// `limit_cases.txt`.
#[test]
fn limit_operators_match_prometheus() {
    let checked = check_cases(include_str!("limit_cases.txt"), |spec, schema, series| {
        let param: f64 = spec[2].parse().unwrap();
        match spec[1] {
            "limitk" => limitk(param, &grouping(spec[3], spec[4]), schema, series),
            _ => limit_ratio(param, schema, series),
        }
    });
    assert!(checked > 200, "{checked} result series checked");
}

/// The labels of the fixtures are short; these need the long length prefix of `stringlabels`.
/// Prometheus' offsets were computed with promql-compat `cmd/narthex-golden-labelhash`.
#[test]
fn sample_offset_hashes_labels_like_prometheus() {
    let cases = [
        ("__name__=a,job=x".to_string(), 0x3fe1c2f8b3f15aa8),
        (
            format!("__name__=a,long={}", "v".repeat(300)),
            0x3fca51ed01e04434,
        ),
        (
            format!(
                "__name__=a,{}={},zone={}",
                "n".repeat(254),
                "v".repeat(255),
                "z".repeat(70000)
            ),
            0x3fc3bf72b8300ba3,
        ),
        // A Prometheus label set has no empty value, so an empty label is no label.
        ("__name__=a,empty=,job=x".to_string(), 0x3fe1c2f8b3f15aa8),
    ];
    for (labels, offset) in cases {
        assert_eq!(sample_offset(&metric(&labels)).to_bits(), offset);
    }
}

/// `limitk` and `limit_ratio` select series, so histogram samples pass through, and
/// `limit_ratio` keeps every field of the series it keeps.
#[test]
fn limit_operators_keep_histogram_samples() {
    let histogram =
        OwnedHistogram::parse("0/0/0/3ff0000000000000/3ff0000000000000/0/1_1/3ff0000000000000///");
    let sample = || {
        GridSample::Histogram(Box::new(
            FloatHistogram::try_from_view(&histogram.view()).unwrap(),
        ))
    };
    let kinds = |series: &Series, field: usize| -> Vec<(usize, &str)> {
        let samples = &series.fields.as_slice()[field];
        series
            .time
            .positions()
            .map(|(point, row)| {
                let kind = match samples.sample(row) {
                    SampleRef::Missing => "missing",
                    SampleRef::Float(_) => "float",
                    SampleRef::Histogram(_) => "histogram",
                };
                (point, kind)
            })
            .collect()
    };

    let two_fields = schema(3, 2);
    let mut builder = GridSeriesBuilder::try_new(Arc::new(two_fields.clone())).unwrap();
    builder
        .push_samples(0, &[Some(GridSample::Float(1.0)), Some(sample())])
        .unwrap();
    builder.push_samples(2, &[None, Some(sample())]).unwrap();
    let series = builder.finish(metric("__name__=a")).unwrap().unwrap();
    let kept = limit_ratio(1.0, &two_fields, std::slice::from_ref(&series)).unwrap();
    assert_eq!(kept.len(), 1);
    assert_eq!(kinds(&kept[0], 0), [(0, "float"), (2, "missing")]);
    assert_eq!(kinds(&kept[0], 1), [(0, "histogram"), (2, "histogram")]);

    let one_field = schema(3, 1);
    let mut builder = GridSeriesBuilder::try_new(Arc::new(one_field.clone())).unwrap();
    builder.push_samples(1, &[Some(sample())]).unwrap();
    let series = builder.finish(metric("__name__=a")).unwrap().unwrap();
    let kept = limitk(1.0, &Grouping::By(Vec::new()), &one_field, &[series]).unwrap();
    assert_eq!(kept.len(), 1);
    assert_eq!(kinds(&kept[0], 0), [(1, "histogram")]);
}

/// `topk` ranks floats only: a histogram sample is skipped, as Prometheus ignores it, and the
/// float below it is kept in its place.
#[test]
fn topk_skips_histogram_samples() {
    let histogram =
        OwnedHistogram::parse("0/0/0/3ff0000000000000/3ff0000000000000/0/1_1/3ff0000000000000///");
    let schema = schema(1, 1);
    let mut builder = GridSeriesBuilder::try_new(Arc::new(schema.clone())).unwrap();
    builder
        .push_samples(
            0,
            &[Some(GridSample::Histogram(Box::new(
                FloatHistogram::try_from_view(&histogram.view()).unwrap(),
            )))],
        )
        .unwrap();
    let series = vec![
        builder.finish(metric("__name__=a")).unwrap().unwrap(),
        build(&schema, metric("__name__=b"), &vec![vec![Some(1.0)]]).unwrap(),
    ];
    let kept = topk(1.0, true, &Grouping::By(Vec::new()), &schema, &series).unwrap();
    assert_eq!(kept.len(), 1);
    assert_eq!(kept[0].metric.name(), Some("b"));
}

/// `sort` drops histogram samples at every step, as Prometheus' `filterFloats` does, while
/// `sort_by_label` keeps them.
#[test]
fn sort_drops_histogram_samples() {
    let histogram =
        OwnedHistogram::parse("0/0/0/3ff0000000000000/3ff0000000000000/0/1_1/3ff0000000000000///");
    let mixed = |schema: &SeriesSchema, name: &str| {
        let mut builder = GridSeriesBuilder::try_new(Arc::new(schema.clone())).unwrap();
        builder
            .push_samples(
                0,
                &[Some(GridSample::Histogram(Box::new(
                    FloatHistogram::try_from_view(&histogram.view()).unwrap(),
                )))],
            )
            .unwrap();
        if schema.eval_grid.as_ref().unwrap().spec().len > 1 {
            builder
                .push_samples(1, &[Some(GridSample::Float(2.0))])
                .unwrap();
        }
        builder
            .finish(metric(&format!("__name__={name}")))
            .unwrap()
            .unwrap()
    };

    let instant = schema(1, 1);
    let series = vec![
        mixed(&instant, "h"),
        build(&instant, metric("__name__=f"), &vec![vec![Some(1.0)]]).unwrap(),
    ];
    let sorted = sort_instant(&SortOrder::Asc, &instant, series.clone()).unwrap();
    assert_eq!(sorted.len(), 1);
    assert_eq!(sorted[0].metric.name(), Some("f"));
    let by_name = sort_instant(&SortOrder::ByLabel(names("__name__")), &instant, series).unwrap();
    assert_eq!(by_name.len(), 2);

    let range = schema(2, 1);
    let sorted = sort_instant(&SortOrder::Desc, &range, vec![mixed(&range, "h")]).unwrap();
    assert_eq!(sorted.len(), 1);
    let positions: Vec<usize> = sorted[0].time.positions().map(|(point, _)| point).collect();
    assert_eq!(positions, [1]);
}

/// A selection hands back the sample it selected, start timestamp included.
///
/// `limitk` and `limit_ratio` choose samples rather than compute them, so a histogram that arrives
/// with a start timestamp leaves with it. The count and the sum are compared too, so a computed
/// value that happened to carry a timestamp would not pass for the sample selected. What this pins
/// is that the selection passes the stored payload through, not a Prometheus result and not a
/// downstream rate that reads the field. A histogram computed from samples carries none, as
/// `histogram_eval::tests::a_start_timestamp_belongs_to_a_read_histogram_and_not_to_a_computed_one`
/// pins for the operations that build one.
#[test]
fn limit_operators_keep_the_start_timestamp_of_a_selected_sample() {
    let histogram =
        OwnedHistogram::parse("0/0/0/3ff0000000000000/3ff0000000000000/0/1_1/3ff0000000000000///");
    let start = 1_500;
    // The sample a scan read, which is what a selection hands on.
    let stamped = || {
        let mut read = FloatHistogram::try_from_view(&histogram.view()).unwrap();
        read.start_timestamp = Some(start);
        GridSample::Histogram(Box::new(read))
    };
    // What a limit operator keeps out of one series: the count and sum of the histogram it kept,
    // so the sample selected is named, and the start timestamp it kept with it.
    let kept = |series: &Series| -> Vec<(usize, f64, f64, Option<i64>)> {
        let samples = &series.fields.as_slice()[0];
        series
            .time
            .positions()
            .filter_map(|(point, row)| match samples.sample(row) {
                SampleRef::Histogram(view) => {
                    Some((point, view.count, view.sum, view.start_timestamp))
                }
                _ => None,
            })
            .collect()
    };
    let source = histogram.view();
    let expected = |point: usize| (point, source.count, source.sum, Some(start));

    let schema = schema(3, 1);
    let mut builder = GridSeriesBuilder::try_new(Arc::new(schema.clone())).unwrap();
    builder.push_samples(0, &[Some(stamped())]).unwrap();
    builder.push_samples(2, &[Some(stamped())]).unwrap();
    let series = builder.finish(metric("__name__=a")).unwrap().unwrap();

    let by_ratio = limit_ratio(1.0, &schema, std::slice::from_ref(&series)).unwrap();
    assert_eq!(by_ratio.len(), 1);
    assert_eq!(
        kept(&by_ratio[0]),
        [expected(0), expected(2)],
        "limit_ratio did not hand back the samples it selected"
    );

    let by_count = limitk(1.0, &Grouping::By(Vec::new()), &schema, &[series]).unwrap();
    assert_eq!(by_count.len(), 1);
    assert_eq!(
        kept(&by_count[0]),
        [expected(0), expected(2)],
        "limitk did not hand back the samples it selected"
    );
}

fn grouping(kind: &str, labels: &str) -> Grouping {
    match kind {
        "without" => Grouping::Without(names(labels)),
        "by" => Grouping::By(names(labels)),
        _ => Grouping::By(Vec::new()),
    }
}

/// Evaluates each case of a fixture with `eval` and returns how many result series matched; a
/// case with an `error` line must fail with that message.
fn check_cases(
    text: &str,
    eval: impl Fn(&[&str], &SeriesSchema, &[Series]) -> Result<Vec<Series>, DataFusionError>,
) -> usize {
    let mut checked = 0;
    for case in text.split("end\n").filter(|case| case.contains("case ")) {
        let lines: Vec<&str> = case.lines().filter(|line| !line.starts_with('#')).collect();
        let spec: Vec<&str> = lines[0].split_whitespace().skip(1).collect();
        let points: usize = lines[1].split_whitespace().nth(3).unwrap().parse().unwrap();
        let schema = schema(points as u64, 1);
        let mut inputs = Vec::new();
        let mut expected: BTreeMap<String, Dense> = BTreeMap::new();
        let mut error = None;
        for line in &lines[2..] {
            if let Some(message) = line.strip_prefix("error ") {
                error = Some(message);
                continue;
            }
            let (kind, rest) = line.split_once(' ').unwrap();
            let (labels, values) = rest.split_once(" ;").unwrap();
            match kind {
                "series" => inputs.push((metric(labels), dense(points, values))),
                "result" => {
                    expected.insert(labels.to_string(), dense(points, values));
                }
                other => panic!("unexpected line {other} in {case}"),
            }
        }
        let series: Vec<Series> = inputs
            .iter()
            .filter_map(|(metric, dense)| build(&schema, metric.clone(), dense))
            .collect();

        let actual = match (eval(&spec, &schema, &series), error) {
            (Ok(actual), None) => actual,
            (Err(actual), Some(expected)) => {
                assert_eq!(actual.message(), expected, "{case}");
                continue;
            }
            (actual, expected) => panic!("{case}\ngot {actual:?}, expected error {expected:?}"),
        };

        // A result arrives as one series over the runs of valued positions; the fixtures pin the
        // dense values, so each result is expanded over the grid before comparing.
        let actual: BTreeMap<String, Series> = fold_pieces(actual)
            .into_iter()
            .map(|series| (render(&series.metric), series))
            .collect();
        assert_eq!(
            actual.keys().collect::<Vec<_>>(),
            expected.keys().collect::<Vec<_>>(),
            "{case}"
        );
        for (labels, dense) in &expected {
            assert_result_eq(actual.get(labels), dense, &metric(labels));
            checked += 1;
        }
    }
    checked
}

/// The fixture's label rendering: `name=value` pairs in name order, `-` when empty.
fn render(metric: &SeriesLabels) -> String {
    let mut labels: Vec<(String, String)> = metric
        .tags()
        .iter()
        .map(|tag| (tag.name.to_string(), tag.value.to_string()))
        .collect();
    if let Some(name) = metric.name() {
        labels.push(("__name__".to_string(), name.to_string()));
    }
    labels.sort();
    if labels.is_empty() {
        return "-".to_string();
    }
    labels
        .iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn instant(values: &[(&str, Option<f64>)]) -> (SeriesSchema, Vec<Series>) {
    let schema = schema(1, 1);
    let series = values
        .iter()
        .map(|(labels, value)| build(&schema, metric(labels), &vec![vec![*value]]).unwrap())
        .collect();
    (schema, series)
}

fn rendered(series: &[Series]) -> Vec<String> {
    series.iter().map(|series| render(&series.metric)).collect()
}

#[test]
fn sorts_instant_results_like_prometheus() {
    let (schema, series) = instant(&[
        ("host=a", Some(2.0)),
        ("host=b", Some(f64::NAN)),
        ("host=c", Some(-1.0)),
    ]);
    let ascending = sort_instant(&SortOrder::Asc, &schema, series.clone()).unwrap();
    assert_eq!(rendered(&ascending), ["host=c", "host=a", "host=b"]);
    let descending = sort_instant(&SortOrder::Desc, &schema, series).unwrap();
    assert_eq!(rendered(&descending), ["host=a", "host=c", "host=b"]);

    // Go compares with `<`, so the zeros tie and keep their order, and `Less` holds for every
    // NaN on its left, so NaNs come out reversed.
    let (schema, series) = instant(&[("host=a", Some(0.0)), ("host=b", Some(-0.0))]);
    let ascending = sort_instant(&SortOrder::Asc, &schema, series).unwrap();
    assert_eq!(rendered(&ascending), ["host=a", "host=b"]);
    let (schema, series) = instant(&[
        ("host=a", Some(f64::NAN)),
        ("host=b", Some(1.0)),
        ("host=c", Some(f64::NAN)),
        ("host=d", Some(0.0)),
    ]);
    let ascending = sort_instant(&SortOrder::Asc, &schema, series.clone()).unwrap();
    assert_eq!(
        rendered(&ascending),
        ["host=d", "host=b", "host=c", "host=a"]
    );
    let descending = sort_instant(&SortOrder::Desc, &schema, series).unwrap();
    assert_eq!(
        rendered(&descending),
        ["host=b", "host=d", "host=c", "host=a"]
    );

    let (schema, series) = instant(&[
        ("host=x10", Some(1.0)),
        ("host=x9", Some(1.0)),
        ("host=x9,job=a", Some(1.0)),
        ("host=x", Some(1.0)),
    ]);
    let by_host = sort_instant(&SortOrder::ByLabel(names("host")), &schema, series.clone());
    assert_eq!(
        rendered(&by_host.unwrap()),
        ["host=x", "host=x9", "host=x9,job=a", "host=x10"]
    );
    let by_host_desc = sort_instant(&SortOrder::ByLabelDesc(names("host")), &schema, series);
    assert_eq!(
        rendered(&by_host_desc.unwrap()),
        ["host=x10", "host=x9,job=a", "host=x9", "host=x"]
    );
}

/// `natsort.Compare` holds both ways for "1" and "01"; sorting many such labels must not panic.
#[test]
fn sort_by_label_accepts_inconsistent_natural_order() {
    // This input makes `slice::sort_by` panic with this comparator on Rust 1.98.
    let hosts: Vec<String> = (0..100)
        .map(|i| format!("host={}{}", "0".repeat(i / 3 % 3), i * 7 % 10))
        .collect();
    let values: Vec<(&str, Option<f64>)> = hosts
        .iter()
        .map(|host| (host.as_str(), Some(1.0)))
        .collect();
    let (schema, series) = instant(&values);
    for order in [
        SortOrder::ByLabel(names("host")),
        SortOrder::ByLabelDesc(names("host")),
    ] {
        let sorted = sort_instant(&order, &schema, series.clone()).unwrap();
        assert_eq!(sorted.len(), 100);
    }
}

#[test]
fn natural_order_follows_natsort() {
    assert!(natural_less("a", "a1"));
    assert!(!natural_less("a1", "a"));
    assert!(natural_less("x9", "x10"));
    assert!(!natural_less("10", "9"));
    // A digit run too long for an i64 compares as a string, like Go's strconv.Atoi failing.
    assert!(natural_less("20000000000000000000", "3"));
}

#[test]
fn rejects_value_operators_over_several_fields() {
    let schema = schema(1, 2);
    assert!(matches!(
        topk(1.0, true, &Grouping::By(Vec::new()), &schema, &[]),
        Err(DataFusionError::NotImplemented(_))
    ));
    assert!(matches!(
        scalar(&schema, &[]),
        Err(DataFusionError::NotImplemented(_))
    ));
}

/// Prometheus returns nothing for `k` below 1 and rejects NaN and values out of int64.
#[test]
fn topk_validates_k_like_prometheus() {
    let schema = schema(1, 1);
    let series = vec![build(&schema, metric("__name__=a"), &vec![vec![Some(1.0)]]).unwrap()];
    // Prometheus' `maxInt64` is 9223372036854774784, one float step below 2^63.
    for k in [f64::NAN, f64::INFINITY, 9_223_372_036_854_774_784.0] {
        assert!(
            topk(k, true, &Grouping::By(Vec::new()), &schema, &series).is_err(),
            "{k}"
        );
    }
    for k in [0.5, f64::NEG_INFINITY] {
        let result = topk(k, false, &Grouping::By(Vec::new()), &schema, &series).unwrap();
        assert!(result.is_empty(), "{k}");
    }
}

/// Each field of `absent` is 1 where that field has no sample.
#[test]
fn absent_evaluates_fields_independently() {
    let schema = schema(2, 2);
    let input = vec![vec![Some(3.0), None], vec![None, Some(4.0)]];
    let series = vec![build(&schema, metric("__name__=a"), &input).unwrap()];
    let result = absent(&schema, &series, metric("-"))
        .unwrap()
        .expect("both positions are absent in one field");
    // The two fields take turns holding a value, but a row is kept when either field has one, so
    // both positions stay in one run; the missing field carries a null there.
    assert_eq!(piece_ranges(std::slice::from_ref(&result)), vec![(0, 2)]);
    let expected = vec![vec![None, Some(1.0)], vec![Some(1.0), None]];
    assert_pieces_eq(std::slice::from_ref(&result), &expected, &metric("-"));
}

/// `absent` and `scalar` read a series of several runs by its positions.
///
/// The rows of a sparse input are neither adjacent nor starting where the series says, so an
/// operator that counted rows from the first position would report presence at the wrong points.
#[test]
fn select_operators_read_a_sparse_axis_by_its_positions() {
    let schema = Arc::new(schema(8, 1));
    let labels = Labels::from([("host".to_string(), "a".to_string())]);
    // Positions 1, 2 and 6 hold a value; 0, 3, 4, 5 and 7 hold none.
    let mut points = Points::new();
    for position in [1, 2, 6] {
        points.insert(position, f64::to_bits(1.0));
    }
    let sparse = sparse_series(&schema, &labels, &points).unwrap();
    assert_eq!(sparse.time.len(), 3);

    // `absent` reports 1 at every point nothing is present: the five positions the axis skips.
    let absent_series = absent(&schema, std::slice::from_ref(&sparse), metric("-"))
        .unwrap()
        .expect("the sparse series holds five absent positions");
    let dense = crate::grid_series::reference::stitch(std::slice::from_ref(&absent_series));
    assert_eq!(
        dense
            .iter()
            .enumerate()
            .filter(|(_, row)| row[0].is_some())
            .map(|(index, _)| index)
            .collect::<Vec<_>>(),
        vec![0, 3, 4, 5, 7],
        "absent must hold a value exactly where the sparse series holds none"
    );

    // `scalar` reads the same sparse rows and reports NaN where nothing is present.
    let scalar_series = scalar(&schema, std::slice::from_ref(&sparse)).unwrap();
    assert_eq!(
        scalar_series.time.len(),
        8,
        "a scalar exists at every point"
    );
    let mut cursor = scalar_series.fields.as_slice()[0].cursor();
    let values = scalar_series
        .time
        .positions()
        .map(|(position, row)| {
            (
                position,
                cursor.at(row).expect("the axis and the fields are aligned"),
            )
        })
        .collect::<Vec<_>>();
    assert!(matches!(values[1].1, SampleRef::Float(value) if value == 1.0));
    assert!(matches!(values[6].1, SampleRef::Float(value) if value == 1.0));
    assert!(matches!(values[3].1, SampleRef::Float(value) if value.is_nan()));
}

/// F: `topk` selects a winner sitting in a later run, so its result is a fragmented axis of the
/// selected positions, not a contiguous piece from the earliest winner.
///
/// Three series each carry one position: a=7 at 1, b=9 at 4 and c=2 at 9. `topk` ranks per point,
/// so each point holds the one candidate its series carries -- and every survivor sits at its own
/// position, never shifted to the start of an envelope.
#[test]
fn topk_keeps_a_winner_sitting_in_a_later_run() {
    let schema = Arc::new(schema(10, 1));
    let operand = |name: &str, position: usize, value: f64| -> Series {
        let labels = Labels::from([("host".to_string(), name.to_string())]);
        let points = Points::from([(position, f64::to_bits(value))]);
        sparse_series(&schema, &labels, &points).expect("one position holds a value")
    };
    let series = vec![
        operand("a", 1, 7.0),
        operand("b", 4, 9.0),
        operand("c", 9, 2.0),
    ];
    for operand in &series {
        assert_eq!(
            run_ranges(operand),
            vec![(operand.time.first_position().expect("one row"), 1)]
        );
        assert_eq!(operand.time.len(), 1, "each series carries one row");
    }

    // `topk(1)` over the empty grouping: at each point the only candidate wins, so all three
    // series survive, each at its own position.
    let result = topk(1.0, true, &Grouping::By(Vec::new()), &schema, &series).unwrap();
    assert_eq!(result.len(), 3, "one candidate per point: {result:?}");
    let mut ranges = piece_ranges(&result);
    ranges.sort_unstable();
    assert_eq!(
        ranges,
        vec![(1, 1), (4, 1), (9, 1)],
        "every winner sits at its own position, not at the start of an envelope"
    );
    let mut expected = vec![vec![None]; 10];
    expected[4] = vec![Some(9.0)];
    let middle = result
        .iter()
        .find(|series| series.metric.get("host") == Some("b"))
        .expect("series b wins at position 4")
        .clone();
    assert_result_eq(Some(&middle), &expected, &metric("host=b"));

    // `topk(2)` is evaluated per point, and each point holds one candidate, so it selects the
    // same three series the `k=1` case did.
    let result = topk(2.0, true, &Grouping::By(Vec::new()), &schema, &series).unwrap();
    assert_eq!(result.len(), 3, "one candidate per point: {result:?}");
    let mut ranges = piece_ranges(&result);
    ranges.sort_unstable();
    assert_eq!(ranges, vec![(1, 1), (4, 1), (9, 1)]);
    for series in &result {
        assert_eq!(series.time.len(), 1, "one row, one position");
    }
}

/// F: `topk` ranks each point on its own, so a winner can sit in a later run, and a tie is broken
/// by label order rather than by position.
///
/// Series `a` holds 5 at 0 and 5 at 6; series `b` holds 5 at 0 and 4 at 6. The two tie at 0, and
/// the earlier label set wins there; at 6 the larger value wins. So `topk(1)` selects `a` twice, at
/// two positions with a hole between -- a winner in a later run. `bottomk(1)` flips the second
/// point to `b`, so the two winners sit in different runs of the two inputs.
#[test]
fn topk_breaks_ties_across_a_gap_by_label_order() {
    let schema = Arc::new(schema(10, 1));
    let operand = |name: &str, points: &[(usize, f64)]| -> Series {
        let labels = Labels::from([("host".to_string(), name.to_string())]);
        let points: Points = points
            .iter()
            .map(|&(position, value)| (position, f64::to_bits(value)))
            .collect();
        sparse_series(&schema, &labels, &points).expect("the series holds a value")
    };
    // Each series carries two positions with a hole between them.
    let a = operand("a", &[(0, 5.0), (6, 5.0)]);
    let b = operand("b", &[(0, 5.0), (6, 4.0)]);
    assert_eq!(run_ranges(&a), vec![(0, 1), (6, 1)]);
    assert_eq!(run_ranges(&b), vec![(0, 1), (6, 1)]);
    let series = vec![a, b];

    // `topk(1)`: `a` wins at both points -- the tie at 0 goes to the earlier label set, and 5 beats
    // 4 at 6. The winner is one series whose two rows are two runs.
    let result = topk(1.0, true, &Grouping::By(Vec::new()), &schema, &series).unwrap();
    assert_eq!(
        result.len(),
        1,
        "one series wins at both points: {result:?}"
    );
    assert_eq!(
        run_ranges(&result[0]),
        vec![(0, 1), (6, 1)],
        "the winner keeps the positions it won at, across the gap"
    );
    assert_eq!(result[0].time.len(), 2);
    let mut expected = vec![vec![None]; 10];
    expected[0] = vec![Some(5.0)];
    expected[6] = vec![Some(5.0)];
    assert_result_eq(Some(&result[0]), &expected, &metric("host=a"));

    // `topk(2)`: both series are selected wherever they hold a value.
    let result = topk(2.0, true, &Grouping::By(Vec::new()), &schema, &series).unwrap();
    assert_eq!(result.len(), 2, "both series are selected: {result:?}");
    let mut all = piece_ranges(&result);
    all.sort_unstable();
    assert_eq!(
        all,
        vec![(0, 1), (0, 1), (6, 1), (6, 1)],
        "both series keep both of their positions"
    );
    let by_label = |list: &[Series], name: &str| -> Series {
        list.iter()
            .find(|series| series.metric.get("host") == Some(name))
            .unwrap_or_else(|| panic!("no series {name}: {list:?}"))
            .clone()
    };
    let mut second = vec![vec![None]; 10];
    second[0] = vec![Some(5.0)];
    second[6] = vec![Some(4.0)];
    let winner = by_label(&result, "b");
    assert_result_eq(Some(&winner), &second, &metric("host=b"));

    // `bottomk(1)`: the tie at 0 still goes to `a`, but 4 beats 5 at 6, so `b` wins in the later
    // run. The two winners sit in different series and at different positions.
    let result = topk(1.0, false, &Grouping::By(Vec::new()), &schema, &series).unwrap();
    assert_eq!(result.len(), 2, "each series wins one point: {result:?}");
    let mut ranges = piece_ranges(&result);
    ranges.sort_unstable();
    assert_eq!(ranges, vec![(0, 1), (6, 1)]);
    let mut first = vec![vec![None]; 10];
    first[0] = vec![Some(5.0)];
    let smallest = by_label(&result, "a");
    assert_result_eq(Some(&smallest), &first, &metric("host=a"));
    let mut last = vec![vec![None]; 10];
    last[6] = vec![Some(4.0)];
    let later = by_label(&result, "b");
    assert_result_eq(Some(&later), &last, &metric("host=b"));
}
