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
use crate::golden::close;
use crate::map::{ArithmeticOp, MathOp};

const CASES: &str = include_str!("../go_math_cases.txt");

fn hex(s: &str) -> f64 {
    f64::from_bits(u64::from_str_radix(s, 16).unwrap())
}

/// Each function as the kernels evaluate it.
fn evaluate(name: &str, args: &[f64]) -> f64 {
    let op = match (name, args) {
        ("atan2", [y, x]) => return ArithmeticOp::Atan2.calculate(*y, *x),
        ("mod", [x, y]) => return ArithmeticOp::Modulo.calculate(*x, *y),
        ("pow", [x, y]) => return ArithmeticOp::Power.calculate(*x, *y),
        ("sin", [_]) => MathOp::Sin,
        ("cos", [_]) => MathOp::Cos,
        ("tan", [_]) => MathOp::Tan,
        ("asin", [_]) => MathOp::Asin,
        ("acos", [_]) => MathOp::Acos,
        ("atan", [_]) => MathOp::Atan,
        ("sinh", [_]) => MathOp::Sinh,
        ("cosh", [_]) => MathOp::Cosh,
        ("tanh", [_]) => MathOp::Tanh,
        ("asinh", [_]) => MathOp::Asinh,
        ("acosh", [_]) => MathOp::Acosh,
        ("atanh", [_]) => MathOp::Atanh,
        ("log", [_]) => MathOp::Ln,
        ("log2", [_]) => MathOp::Log2,
        ("log10", [_]) => MathOp::Log10,
        ("exp", [_]) => MathOp::Exp,
        _ => panic!("unknown case {name} {args:?}"),
    };
    op.calculate(args[0])
}

/// The standard library function each port replaces.
fn evaluate_std(name: &str, args: &[f64]) -> Option<f64> {
    Some(match (name, args) {
        ("sin", [x]) => x.sin(),
        ("cos", [x]) => x.cos(),
        ("tan", [x]) => x.tan(),
        ("acos", [x]) => x.acos(),
        ("sinh", [x]) => x.sinh(),
        ("cosh", [x]) => x.cosh(),
        ("log", [x]) => x.ln(),
        ("log2", [x]) => x.log2(),
        ("log10", [x]) => x.log10(),
        ("exp", [x]) => x.exp(),
        ("atan2", [y, x]) => y.atan2(*x),
        ("pow", [x, y]) => x.powf(*y),
        _ => return None,
    })
}

fn cases() -> impl Iterator<Item = (&'static str, &'static str, Vec<f64>, f64)> {
    CASES
        .lines()
        .filter(|line| !line.starts_with('#'))
        .map(|line| {
            let words = line.split(' ').collect::<Vec<_>>();
            let args = words[1..words.len() - 1]
                .iter()
                .map(|word| hex(word))
                .collect();
            (line, words[0], args, hex(words[words.len() - 1]))
        })
}

#[test]
fn kernels_match_go_math_on_amd64() {
    let mut counts = BTreeMap::<&str, usize>::new();
    for (line, name, args, expected) in cases() {
        let actual = evaluate(name, &args);
        assert!(
            close(actual, expected),
            "{line}: got {:016x} ({actual:e})",
            actual.to_bits()
        );
        *counts.entry(name).or_default() += 1;
    }

    // Lower bounds keep coverage from silently shrinking when the cases are regenerated.
    let minimums = [
        ("sin", 44),
        ("cos", 44),
        ("tan", 44),
        ("asin", 32),
        ("acos", 33),
        ("atan", 36),
        ("sinh", 33),
        ("cosh", 31),
        ("tanh", 31),
        ("asinh", 37),
        ("acosh", 41),
        ("atanh", 34),
        ("log", 40),
        ("log2", 33),
        ("log10", 33),
        ("exp", 40),
        ("atan2", 64),
        ("mod", 34),
        ("pow", 72),
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

/// Every port needs a case the standard library fails, or it should be the standard library.
#[test]
fn every_port_has_a_case_std_fails() {
    let mut failing = BTreeMap::<&str, usize>::new();
    for (_, name, args, expected) in cases() {
        if let Some(actual) = evaluate_std(name, &args) {
            let entry = failing.entry(name).or_default();
            if !close(actual, expected) {
                *entry += 1;
            }
        }
    }
    let unneeded = failing
        .iter()
        .filter(|(_, count)| **count == 0)
        .map(|(name, _)| *name)
        .collect::<Vec<_>>();
    assert_eq!(failing.len(), 12);
    assert!(unneeded.is_empty(), "std passes every case of {unneeded:?}");
}

/// Within the tolerance, but `floor(log10(1e15))` is 14 in Prometheus, so `log10` must not
/// become `f64::log10`, which returns exactly 15.
#[test]
fn log10_keeps_go_rounding_below_powers_of_ten() {
    assert_eq!(log10(1e15).floor(), 14.0);
}
