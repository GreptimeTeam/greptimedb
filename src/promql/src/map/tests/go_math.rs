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

/// The kernels are the standard library's functions, and these are the inputs on which Go's `math`
/// agrees with them within the comparison tolerance: NaN, the infinities, the zeros, subnormals and
/// the largest finite value among them.
#[test]
fn kernels_match_go_math() {
    let mut counts = BTreeMap::<&str, usize>::new();
    let mut failed = Vec::new();
    for line in CASES.lines().filter(|line| !line.starts_with('#')) {
        let words = line.split(' ').collect::<Vec<_>>();
        let args = words[1..words.len() - 1]
            .iter()
            .map(|word| hex(word))
            .collect::<Vec<_>>();
        let actual = evaluate(words[0], &args);
        if !close(actual, hex(words[words.len() - 1])) {
            failed.push(format!(
                "{line}: got {:016x} ({actual:e})",
                actual.to_bits()
            ));
        }
        *counts.entry(words[0]).or_default() += 1;
    }
    assert!(failed.is_empty(), "{}", failed.join("\n"));
    assert_eq!(counts.len(), 19, "{counts:?}");
}

#[test]
fn kernels_preserve_platform_math_differences() {
    let subnormal = f64::from_bits(1);
    assert!((MathOp::Ln.calculate(subnormal) - (-744.4400719213812)).abs() < 1e-10);
    let power = ArithmeticOp::Power.calculate(subnormal, 0.25);
    assert!((power / 1.4908919308538354e-81 - 1.0).abs() < 1e-10);
    assert!(MathOp::Exp.calculate(709.5).is_finite());
    assert_eq!(
        ArithmeticOp::Atan2.calculate(-1e-300, -1e300),
        -std::f64::consts::PI
    );
    assert_eq!(MathOp::Floor.calculate(MathOp::Log10.calculate(1e15)), 15.0);
}
