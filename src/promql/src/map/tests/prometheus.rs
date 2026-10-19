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

//! Map kernels against `prometheus_cases.txt`, evaluated by the Prometheus engine.

use std::collections::BTreeMap;

use datatypes::series::samples::Series;
use datatypes::series::time::GridRange;

use crate::golden::{Case, assert_result, cases, hex};
use crate::map::{
    ArithmeticOp, Comparison, ComparisonOp, MathOp, ScalarOperand, ScalarSeriesOperand,
    map_arithmetic, map_arithmetic_series, map_compare, map_compare_series, map_math, map_negate,
    scalar_arithmetic, scalar_compare,
};

const CASES: &str = include_str!("../prometheus_cases.txt");

fn math_op(header: &[String]) -> MathOp {
    let arg = |index: usize| hex(&header[index]);
    match header[1].as_str() {
        "abs" => MathOp::Abs,
        "ceil" => MathOp::Ceil,
        "floor" => MathOp::Floor,
        "sqrt" => MathOp::Sqrt,
        "exp" => MathOp::Exp,
        "ln" => MathOp::Ln,
        "log2" => MathOp::Log2,
        "log10" => MathOp::Log10,
        "sin" => MathOp::Sin,
        "cos" => MathOp::Cos,
        "tan" => MathOp::Tan,
        "asin" => MathOp::Asin,
        "acos" => MathOp::Acos,
        "atan" => MathOp::Atan,
        "sinh" => MathOp::Sinh,
        "cosh" => MathOp::Cosh,
        "tanh" => MathOp::Tanh,
        "asinh" => MathOp::Asinh,
        "acosh" => MathOp::Acosh,
        "atanh" => MathOp::Atanh,
        "deg" => MathOp::Deg,
        "rad" => MathOp::Rad,
        "sgn" => MathOp::Sgn,
        "round" => MathOp::Round { nearest: arg(2) },
        "clamp" => MathOp::Clamp {
            min: arg(2),
            max: arg(3),
        },
        "clamp_min" => MathOp::ClampMin { min: arg(2) },
        "clamp_max" => MathOp::ClampMax { max: arg(2) },
        other => panic!("unmapped math function {other}"),
    }
}

fn arithmetic_op(op: &str) -> ArithmeticOp {
    match op {
        "+" => ArithmeticOp::Add,
        "-" => ArithmeticOp::Subtract,
        "*" => ArithmeticOp::Multiply,
        "/" => ArithmeticOp::Divide,
        "%" => ArithmeticOp::Modulo,
        "^" => ArithmeticOp::Power,
        "atan2" => ArithmeticOp::Atan2,
        other => panic!("unmapped arithmetic {other}"),
    }
}

fn comparison(op: &str, mode: &str) -> Comparison {
    let op = match op {
        "==" => ComparisonOp::Equal,
        "!=" => ComparisonOp::NotEqual,
        ">" => ComparisonOp::Greater,
        "<" => ComparisonOp::Less,
        ">=" => ComparisonOp::GreaterEqual,
        "<=" => ComparisonOp::LessEqual,
        other => panic!("unmapped comparison {other}"),
    };
    match mode {
        "filter" => Comparison::Filter(op),
        "bool" => Comparison::Bool(op),
        other => panic!("unmapped comparison mode {other}"),
    }
}

fn scalar_operand(side: &str, value: f64) -> ScalarOperand {
    match side {
        "left" => ScalarOperand::Left(value),
        _ => ScalarOperand::Right(value),
    }
}

fn evaluate(case: &Case) -> Vec<Series> {
    let h = &case.header;
    let schema = &case.schema;
    let full_grid =
        || GridRange::try_new(schema.eval_grid.clone().unwrap(), 0, grid_len(case)).unwrap();
    let each = |f: &dyn Fn(&Series) -> Option<Series>| case.series.iter().filter_map(f).collect();
    let scalar_series = || {
        let scalar = case.scalar.as_ref().expect("scalar operand");
        match h[h.len() - 1].as_str() {
            "left" => ScalarSeriesOperand::Left(scalar),
            _ => ScalarSeriesOperand::Right(scalar),
        }
    };
    match h[0].as_str() {
        "math" => each(&|s| map_math(schema, s, math_op(h)).unwrap()),
        "neg" => each(&|s| Some(map_negate(schema, s).unwrap())),
        "arith" => {
            let operand = scalar_operand(&h[2], hex(&h[3]));
            each(&|s| Some(map_arithmetic(schema, s, arithmetic_op(&h[1]), operand).unwrap()))
        }
        "cmp" => {
            let operand = scalar_operand(&h[3], hex(&h[4]));
            each(&|s| map_compare(schema, s, comparison(&h[1], &h[2]), operand).unwrap())
        }
        "arith_series" => each(&|s| {
            map_arithmetic_series(schema, s, arithmetic_op(&h[1]), scalar_series()).unwrap()
        }),
        "cmp_series" => each(&|s| {
            map_compare_series(schema, s, comparison(&h[1], &h[2]), scalar_series()).unwrap()
        }),
        "scalar_arith" => vec![
            scalar_arithmetic(
                schema,
                full_grid(),
                arithmetic_op(&h[1]),
                [hex(&h[2]), hex(&h[3])],
            )
            .unwrap(),
        ],
        "scalar_cmp" => vec![
            scalar_compare(
                schema,
                full_grid(),
                match comparison(&h[1], "bool") {
                    Comparison::Bool(op) | Comparison::Filter(op) => op,
                },
                [hex(&h[2]), hex(&h[3])],
            )
            .unwrap(),
        ],
        other => panic!("unmapped case kind {other}"),
    }
}

fn grid_len(case: &Case) -> usize {
    case.schema.eval_grid.as_ref().unwrap().timestamps().len()
}

#[test]
fn map_kernels_match_prometheus() {
    let mut matched = BTreeMap::<String, usize>::new();
    for case in cases(CASES) {
        assert_result(&case, evaluate(&case));
        *matched.entry(case.header[0].clone()).or_default() += 1;
        if case.header[0] == "math" {
            *matched
                .entry(format!("math {}", case.header[1]))
                .or_default() += 1;
        }
    }
    // Floors keep coverage from shrinking silently when the fixture is regenerated.
    for (kind, floor) in [
        ("math", 180),
        ("neg", 10),
        ("arith", 70),
        ("cmp", 48),
        ("arith_series", 42),
        ("cmp_series", 36),
        ("scalar_arith", 42),
        ("scalar_cmp", 24),
    ] {
        let count = matched.get(kind).copied().unwrap_or_default();
        assert!(count >= floor, "{kind}: {count} < {floor}");
    }
    for function in [
        "abs",
        "ceil",
        "floor",
        "sqrt",
        "exp",
        "ln",
        "log2",
        "log10",
        "sin",
        "cos",
        "tan",
        "asin",
        "acos",
        "atan",
        "sinh",
        "cosh",
        "tanh",
        "asinh",
        "acosh",
        "atanh",
        "deg",
        "rad",
        "sgn",
        "round",
        "clamp",
        "clamp_min",
        "clamp_max",
    ] {
        let count = matched
            .get(&format!("math {function}"))
            .copied()
            .unwrap_or_default();
        assert!(count >= 4, "math {function}: {count}");
    }
}
