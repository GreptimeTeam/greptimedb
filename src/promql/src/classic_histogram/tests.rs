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

use super::{ClassicHistogramFunction, classic_histogram};
use crate::grid_series::reference::{
    canonical_bits, golden_cases, labels_of, points_of, series_of,
};

#[test]
fn classic_histograms_match_prometheus() {
    let cases = golden_cases(include_str!("prometheus_cases.txt"));
    assert_eq!(cases.len(), 300);
    let (mut matched, mut failed) = (0, 0);
    for case in cases {
        let schema = Arc::new(SeriesSchema {
            fields: vec![FieldSpec {
                name: Arc::from("value"),
            }]
            .into(),
            eval_grid: Some(case.grid.clone()),
        });
        // The fixture lists buckets by bound; the kernel must not rely on input order.
        let input: Vec<_> = case
            .inputs
            .iter()
            .rev()
            .filter_map(|(labels, points)| series_of(&schema, labels, points))
            .collect();
        let argument =
            |index: usize| f64::from_bits(u64::from_str_radix(&case.spec[index], 16).unwrap());
        let function = match case.spec[0].as_str() {
            "quantile" => ClassicHistogramFunction::Quantile(argument(1)),
            "fraction" => ClassicHistogramFunction::Fraction {
                lower: argument(1),
                upper: argument(2),
            },
            other => panic!("unknown case {other}"),
        };
        match (classic_histogram(function, &schema, &input), case.outcome) {
            (Ok(actual), Ok(expected)) => {
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
                assert_eq!(actual, expected, "case {:?}", case.spec);
                matched += 1;
            }
            (Err(actual), Err(expected)) => {
                assert!(actual.to_string().contains(&expected), "{actual}");
                failed += 1;
            }
            (actual, expected) => panic!("case {:?}: {actual:?} is not {expected:?}", case.spec),
        }
    }
    assert!(
        matched > 250 && failed > 20,
        "{matched} matched, {failed} failed"
    );
}
