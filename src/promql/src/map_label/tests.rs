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

use super::{label_join, label_replace};
use crate::grid_series::reference::{canonical_bits, golden_cases, points_by_labels, series_of};

#[test]
fn label_functions_match_prometheus() {
    let cases = golden_cases(include_str!("prometheus_cases.txt"));
    assert_eq!(cases.len(), 200);
    let (mut matched, mut merged, mut failed) = (0, 0, 0);
    for case in cases {
        let schema = Arc::new(SeriesSchema {
            fields: vec![FieldSpec {
                name: Arc::from("value"),
            }]
            .into(),
            eval_grid: Some(case.grid.clone()),
        });
        let input = case
            .inputs
            .iter()
            .filter_map(|(labels, points)| series_of(&schema, labels, points))
            .collect::<Vec<_>>();
        let inputs = input.len();
        let spec: Vec<&str> = case.spec.iter().map(String::as_str).collect();
        let actual = match spec[0] {
            "label_replace" => label_replace(&schema, input, spec[1], spec[2], spec[3], spec[4]),
            "label_join" => label_join(&schema, input, spec[1], spec[2], &spec[3..]),
            other => panic!("unknown case {other}"),
        };
        match (actual, case.outcome) {
            (Ok(actual), Ok(expected)) => {
                if expected.len() < inputs {
                    merged += 1;
                }
                // A result arrives as one piece per run of valued positions; a fixture pins
                // dense points, so the pieces of each label set are folded back together.
                let actual: BTreeMap<_, _> = points_by_labels(actual.clone());
                let expected: BTreeMap<_, _> = expected
                    .into_iter()
                    .map(|(labels, points)| {
                        let points = points
                            .into_iter()
                            .map(|(index, bits)| (index, canonical_bits(f64::from_bits(bits))))
                            .collect();
                        (labels, points)
                    })
                    .collect();
                assert_eq!(actual, expected, "case {spec:?}");
                matched += 1;
            }
            (Err(actual), Err(expected)) => {
                assert!(
                    actual.to_string().contains(&expected),
                    "case {spec:?}: {actual} is not {expected}"
                );
                failed += 1;
            }
            (actual, expected) => panic!("case {spec:?}: {actual:?} is not {expected:?}"),
        }
    }
    assert!(
        matched > 150 && merged > 2 && failed > 20,
        "{matched} matched, {merged} merged, {failed} failed"
    );
}

/// Replacement templates expand as Go's `regexp.Expand` does; expectations come from Go.
#[test]
fn replacements_expand_like_go() {
    let regex = regex::Regex::new("^(?s:(?P<word>[a-z]+)-([0-9]+))$").unwrap();
    let captures = regex.captures("abc-42").unwrap();
    for (template, expected) in [
        ("$01", ""),
        ("${01}", ""),
        ("$1", "abc"),
        ("${1}x", "abcx"),
        ("$1x", ""),
        ("$$", "$"),
        ("$", "$"),
        ("${1", "${1"),
        ("$word", "abc"),
        ("${word}!", "abc!"),
        ("$2$1", "42abc"),
        ("$-", "$-"),
        ("$0", "abc-42"),
        ("$9", ""),
        ("$1234567890", ""),
        ("$1²", "abc²"),
    ] {
        let mut out = String::new();
        super::expand(&regex, &captures, template, &mut out);
        assert_eq!(out, expected, "{template}");
    }
}

/// F: a label function over a series of several runs rewrites the labels and leaves the rows and
/// their positions untouched.
///
/// A label function has no kernel at all, so the only thing that can go wrong is the axis: it must
/// come back with the same runs and the same values, at the same absolute grid positions.
#[test]
fn a_label_function_keeps_the_runs_of_a_sparse_series() {
    use crate::grid_series::reference::{
        Dense, Labels, assert_result_eq, metric_of, run_ranges, sparse_series,
    };

    let grid = Arc::new(
        datatypes::series::time::EvalGrid::try_new(
            datatypes::series::time::GridSpec::try_new(0, 1_000, 10).unwrap(),
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
    let mut points = crate::grid_series::reference::Points::new();
    for (position, value) in [(1, 1.0), (2, 2.0), (6, 6.0)] {
        points.insert(position, f64::to_bits(value));
    }
    let labels = Labels::from([
        ("__name__".to_string(), "requests".to_string()),
        ("host".to_string(), "a.example.com".to_string()),
    ]);
    let input = sparse_series(&schema, &labels, &points).expect("three positions hold a value");
    assert_eq!(run_ranges(&input), vec![(1, 2), (6, 1)]);

    let output = label_replace(
        &schema,
        vec![input.clone()],
        "instance",
        "$1",
        "host",
        "(.*)\\.example\\.com",
    )
    .unwrap();
    assert_eq!(output.len(), 1, "one input label set, one output");
    assert_eq!(
        run_ranges(&output[0]),
        run_ranges(&input),
        "a label function moved the rows"
    );
    assert_eq!(output[0].time.len(), 3);
    assert_eq!(output[0].metric.get("instance"), Some("a"));
    // The value keeps its labels except the rewritten one, and the values are unchanged.
    let mut expected = vec![vec![None]; 10];
    expected[1] = vec![Some(1.0)];
    expected[2] = vec![Some(2.0)];
    expected[6] = vec![Some(6.0)];
    let expected: Dense = expected;
    let mut metric = labels.clone();
    metric.insert("instance".to_string(), "a".to_string());
    assert_result_eq(Some(&output[0]), &expected, &metric_of(&metric));

    // `label_join` likewise: the axis survives a function that only edits labels.
    let joined = label_join(&schema, vec![input], "id", "-", &["host", "instance"]).unwrap();
    assert_eq!(joined.len(), 1);
    assert_eq!(run_ranges(&joined[0]), vec![(1, 2), (6, 1)]);
    assert_eq!(joined[0].metric.get("id"), Some("a.example.com-"));
    assert_result_eq(
        Some(&joined[0]),
        &expected,
        &metric_of(&Labels::from([
            ("__name__".to_string(), "requests".to_string()),
            ("host".to_string(), "a.example.com".to_string()),
            ("id".to_string(), "a.example.com-".to_string()),
        ])),
    );
}
