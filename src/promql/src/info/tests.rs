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

use datatypes::series::samples::{FieldSpec, SeriesSchema};
use datatypes::series::time::{EvalGrid, GridSpec};

use crate::grid_series::reference::{Labels, Points, labels_of, points_of, sparse_series};
use crate::info::{InfoSelector, info};
use crate::select::MatchOp;

fn parse_labels(text: &str) -> Labels {
    text.split(',')
        .filter(|pair| *pair != "-")
        .map(|pair| {
            let (name, value) = pair.split_once('=').unwrap();
            (name.to_string(), value.to_string())
        })
        .collect()
}

fn parse_points(text: &str) -> Points {
    text.split_whitespace()
        .map(|point| {
            let (index, bits) = point.split_once(':').unwrap();
            (
                index.parse().unwrap(),
                u64::from_str_radix(bits, 16).unwrap(),
            )
        })
        .collect()
}

fn list(text: &str) -> Vec<&str> {
    text.split(';').filter(|item| *item != "-").collect()
}

fn name_matcher(text: &str) -> (MatchOp, Arc<str>) {
    let rest = text.strip_prefix("__name__").unwrap();
    let (op, value) = [
        ("=~", MatchOp::Regex),
        ("!~", MatchOp::NotRegex),
        ("!=", MatchOp::NotEqual),
        ("=", MatchOp::Equal),
    ]
    .into_iter()
    .find_map(|(prefix, op)| rest.strip_prefix(prefix).map(|value| (op, value)))
    .unwrap();
    (op, Arc::from(value.trim_matches('"')))
}

/// `info` evaluated by the Prometheus v3.15.0 engine; see the header of `prometheus_cases.txt`.
#[test]
fn info_matches_prometheus() {
    let (mut matched, mut failed) = (0, 0);
    for case in include_str!("prometheus_cases.txt")
        .split("end\n")
        .filter(|case| case.contains("case "))
    {
        let lines: Vec<&str> = case.lines().filter(|line| !line.starts_with('#')).collect();
        let spec: Vec<&str> = lines[0].split_whitespace().skip(1).collect();
        let selector = InfoSelector {
            data_labels: list(spec[0]).into_iter().map(Arc::from).collect(),
            keep_unmatched: spec[1] == "true",
            name_matchers: list(spec[2]).into_iter().map(name_matcher).collect(),
        };
        let points: u64 = lines[2].split_whitespace().nth(3).unwrap().parse().unwrap();
        let schema = Arc::new(SeriesSchema {
            fields: vec![FieldSpec {
                name: Arc::from("value"),
            }]
            .into(),
            eval_grid: Some(Arc::new(
                EvalGrid::try_new(GridSpec::try_new(100_000, 10_000, points).unwrap()).unwrap(),
            )),
        });
        let (mut base, mut infos) = (Vec::new(), Vec::new());
        let mut expected: BTreeMap<Labels, Points> = BTreeMap::new();
        let mut error = None;
        for line in &lines[3..] {
            if let Some(message) = line.strip_prefix("error ") {
                error = Some(message.to_string());
                continue;
            }
            let (kind, rest) = line.split_once(' ').unwrap();
            let (labels, values) = rest.split_once(" ;").unwrap();
            let (labels, values) = (parse_labels(labels), parse_points(values));
            match kind {
                "base" => base.extend(sparse_series(&schema, &labels, &values)),
                "info" => infos.extend(sparse_series(&schema, &labels, &values)),
                "result" => {
                    expected.insert(labels, values);
                }
                other => panic!("unexpected line {other}"),
            }
        }
        match (info(&schema, &base, &infos, &selector), error) {
            (Ok(actual), None) => {
                let mut folded: BTreeMap<Labels, Points> = BTreeMap::new();
                for series in &actual {
                    folded
                        .entry(labels_of(&series.metric))
                        .or_default()
                        .extend(points_of(series));
                }
                assert_eq!(folded, expected, "{case}");
                matched += 1;
            }
            (Err(actual), Some(expected)) => {
                let class = |message: &str| {
                    [
                        "found duplicate series for info metric",
                        "conflicting label",
                        "same labelset",
                    ]
                    .into_iter()
                    .find(|class| message.contains(class))
                };
                assert_eq!(class(&actual.to_string()), class(&expected), "{case}");
                failed += 1;
            }
            (actual, expected) => panic!("{case}\ngot {actual:?}, expected error {expected:?}"),
        }
    }
    assert!(
        matched > 200 && failed > 5,
        "{matched} matched, {failed} failed"
    );
}
