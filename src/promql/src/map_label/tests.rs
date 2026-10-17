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
use crate::grid_series::reference::{
    canonical_bits, golden_cases, labels_of, points_of, series_of,
};

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
                let actual: BTreeMap<_, _> = actual
                    .iter()
                    .map(|series| (labels_of(&series.metric), points_of(series)))
                    .collect();
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
