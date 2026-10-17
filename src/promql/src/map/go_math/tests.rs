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

use super::*;

const CASES: &str = include_str!("../go_math_cases.txt");

fn hex(s: &str) -> f64 {
    f64::from_bits(u64::from_str_radix(s, 16).unwrap())
}

fn evaluate(name: &str, args: &[f64]) -> f64 {
    match (name, args) {
        ("sin", [x]) => sin(*x),
        ("cos", [x]) => cos(*x),
        ("tan", [x]) => tan(*x),
        ("asin", [x]) => asin(*x),
        ("acos", [x]) => acos(*x),
        ("atan", [x]) => atan(*x),
        ("sinh", [x]) => sinh(*x),
        ("cosh", [x]) => cosh(*x),
        ("tanh", [x]) => tanh(*x),
        ("asinh", [x]) => asinh(*x),
        ("acosh", [x]) => acosh(*x),
        ("atanh", [x]) => atanh(*x),
        ("log1p", [x]) => log1p(*x),
        ("log", [x]) => log(*x),
        ("log2", [x]) => log2(*x),
        ("log10", [x]) => log10(*x),
        ("exp", [x]) => exp(*x),
        ("atan2", [y, x]) => atan2(*y, *x),
        ("mod", [x, y]) => fmod(*x, *y),
        ("pow", [x, y]) => pow(*x, *y),
        _ => panic!("unknown case {name} {args:?}"),
    }
}

#[test]
fn go_math_matches_go_on_amd64() {
    let mut counts = BTreeMap::<&str, usize>::new();
    for line in CASES.lines().filter(|line| !line.starts_with('#')) {
        let words = line.split(' ').collect::<Vec<_>>();
        let (name, expected) = (words[0], hex(words[words.len() - 1]));
        let args = words[1..words.len() - 1]
            .iter()
            .map(|word| hex(word))
            .collect::<Vec<_>>();
        let actual = evaluate(name, &args);
        assert!(
            actual.to_bits() == expected.to_bits() || (actual.is_nan() && expected.is_nan()),
            "{line}: got {:016x} ({actual:e})",
            actual.to_bits()
        );
        *counts.entry(name).or_default() += 1;
    }

    // Lower bounds keep coverage from silently shrinking when the cases are regenerated.
    let minimums = [
        ("sin", 42),
        ("cos", 42),
        ("tan", 42),
        ("asin", 32),
        ("acos", 32),
        ("atan", 36),
        ("sinh", 33),
        ("cosh", 29),
        ("tanh", 31),
        ("asinh", 36),
        ("acosh", 41),
        ("atanh", 34),
        ("log1p", 56),
        ("log", 40),
        ("log2", 32),
        ("log10", 32),
        ("exp", 40),
        ("atan2", 63),
        ("mod", 34),
        ("pow", 70),
    ];
    assert_eq!(counts.len(), minimums.len());
    for (name, minimum) in minimums {
        assert!(
            counts[name] >= minimum,
            "{name}: only {} cases",
            counts[name]
        );
    }
}
