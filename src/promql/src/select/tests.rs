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

use datafusion::error::DataFusionError;
use datatypes::series::samples::{FieldSpec, Series, SeriesSchema};
use datatypes::series::time::{EvalGrid, GridSpec};
use datatypes::series::{Label, MetricName};

use super::{
    Grouping, LabelMatcher, MatchOp, SortOrder, absent, absent_labels, natural_less, scalar,
    sort_instant, topk,
};
use crate::grid_series::GridSeriesBuilder;
use crate::grid_series::reference::{Dense, assert_series_eq};

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
fn metric(labels: &str) -> MetricName {
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
    MetricName::try_new(name, tags).unwrap()
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

fn build(schema: &SeriesSchema, metric: MetricName, dense: &Dense) -> Option<Series> {
    let mut builder = GridSeriesBuilder::try_new(Arc::new(schema.clone())).unwrap();
    for (index, row) in dense.iter().enumerate() {
        builder.push(index, row).unwrap();
    }
    builder.finish(metric).unwrap()
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
    let text = include_str!("prometheus_cases.txt");
    let mut checked = 0;
    for case in text.split("end\n").filter(|case| case.contains("case ")) {
        let lines: Vec<&str> = case.lines().filter(|line| !line.starts_with('#')).collect();
        let spec: Vec<&str> = lines[0].split_whitespace().skip(1).collect();
        let points: usize = lines[1].split_whitespace().nth(3).unwrap().parse().unwrap();
        let schema = schema(points as u64, 1);
        let mut inputs = Vec::new();
        let mut expected: BTreeMap<String, Dense> = BTreeMap::new();
        for line in &lines[2..] {
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

        let actual: Vec<Series> = match spec[0] {
            "topk" => {
                let grouping = match spec[3] {
                    "without" => Grouping::Without(names(spec[4])),
                    "by" => Grouping::By(names(spec[4])),
                    _ => Grouping::By(Vec::new()),
                };
                let k: f64 = spec[2].parse().unwrap();
                topk(k, spec[1] == "topk", &grouping, &schema, &series).unwrap()
            }
            "scalar" => vec![scalar(&schema, &series).unwrap()],
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
                absent(&schema, &selected, absent_labels(&matchers).unwrap())
                    .unwrap()
                    .into_iter()
                    .collect()
            }
            other => panic!("unknown case {other}"),
        };

        let actual: BTreeMap<String, Series> = actual
            .into_iter()
            .map(|series| (render(&series.metric), series))
            .collect();
        assert_eq!(
            actual.keys().collect::<Vec<_>>(),
            expected.keys().collect::<Vec<_>>(),
            "{case}"
        );
        for (labels, dense) in &expected {
            assert_series_eq(actual.get(labels), dense, &metric(labels));
            checked += 1;
        }
    }
    assert!(checked > 200, "{checked} result series checked");
}

/// The fixture's label rendering: `name=value` pairs in name order, `-` when empty.
fn render(metric: &MetricName) -> String {
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
    for k in [f64::NAN, f64::INFINITY, 2f64.powi(63)] {
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
    let result = absent(&schema, &series, metric("-")).unwrap();
    let expected = vec![vec![None, Some(1.0)], vec![Some(1.0), None]];
    assert_series_eq(result.as_ref(), &expected, &metric("-"));
}
