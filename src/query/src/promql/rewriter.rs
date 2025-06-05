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

use promql_parser::parser::token::{self};
use promql_parser::parser::{Expr, NumberLiteral};

/// PromQL expression rewriter.
pub struct PromRewriter;

impl PromRewriter {
    pub fn rewrite(expr: &mut Expr) {
        fold_constant_expr(expr);
    }
}

/// Fold constant expressions.
///
/// This function traverses the expression tree and evaluates any constant
/// sub-expressions. It supports basic arithmetic operations (+, -, *, /, %)
/// and unary negation.
pub fn fold_constant_expr(expr: &mut Expr) {
    match expr {
        Expr::Call(call) => {
            for arg in &mut call.args.args {
                if let Some(val) = eval_constant_expr(arg) {
                    *arg = Box::new(Expr::NumberLiteral(NumberLiteral { val }));
                } else {
                    fold_constant_expr(arg);
                }
            }
        }
        Expr::Binary(binary) => {
            fold_constant_expr(&mut binary.lhs);
            fold_constant_expr(&mut binary.rhs);
            if let Some(val) = eval_constant_expr(expr) {
                *expr = Expr::NumberLiteral(NumberLiteral { val });
            }
        }
        Expr::Paren(paren) => {
            fold_constant_expr(&mut paren.expr);
            if let Some(val) = eval_constant_expr(expr) {
                *expr = Expr::NumberLiteral(NumberLiteral { val });
            }
        }
        Expr::Unary(unary) => {
            fold_constant_expr(&mut unary.expr);
            if let Some(val) = eval_constant_expr(expr) {
                *expr = Expr::NumberLiteral(NumberLiteral { val });
            }
        }
        _ => {}
    }
}

fn eval_constant_expr(expr: &Expr) -> Option<f64> {
    match expr {
        Expr::NumberLiteral(n) => Some(n.val),
        Expr::Paren(p) => eval_constant_expr(&p.expr),
        Expr::Unary(u) => eval_constant_expr(&u.expr).map(|val| -val),
        Expr::Binary(b) => {
            let lhs = eval_constant_expr(&b.lhs)?;
            let rhs = eval_constant_expr(&b.rhs)?;
            match b.op.id() {
                token::T_ADD => Some(lhs + rhs),
                token::T_SUB => Some(lhs - rhs),
                token::T_MUL => Some(lhs * rhs),
                token::T_DIV => Some(lhs / rhs),
                token::T_MOD => Some(lhs % rhs),
                _ => None,
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use promql_parser::parser;

    use super::fold_constant_expr;

    #[test]
    fn test_eval_const_expr_basic() {
        let cases = vec![
            (r#"round(metric, 1 + 1)"#, r#"round(metric, 2)"#),
            (r#"round(metric, 2)"#, r#"round(metric, 2)"#),
            (r#"round(metric, -(-1))"#, r#"round(metric, 1)"#),
            (r#"round(metric, -(10 % 3) + 4)"#, r#"round(metric, 3)"#),
            (r#"metric + (-(10 % 3) + 4)"#, r#"metric + 3"#),
            (r#"round(metric, ((1)))"#, r#"round(metric, 1)"#),
            (
                r#"round(rate(foo[5m]), 1 + 2 * 3)"#,
                r#"round(rate(foo[5m]), 7)"#,
            ),
            (r#"round(metric)"#, r#"round(metric)"#),
            (
                r#"rate(http_requests_total[5m])"#,
                r#"rate(http_requests_total[5m])"#,
            ),
        ];
        for (case, expected) in cases {
            let mut prom_expr = parser::parse(case).unwrap_or_else(|_| panic!("case: {}", case));
            fold_constant_expr(&mut prom_expr);
            assert_eq!(
                prom_expr.to_string(),
                expected,
                "expected: {}, actual: {}",
                expected,
                prom_expr
            );
        }
    }

    #[test]
    fn test_eval_const_expr_arithmetic() {
        let cases = [
            (r#"1"#, r#"1"#),
            (r#"1 + 2"#, r#"3"#),
            (r#"1 + 2 * 3"#, r#"7"#),
            (r#"(1 + 2) * 3"#, r#"9"#),
            (r#"10 / 2 + 3"#, r#"8"#),
            (r#"10 / (2 + 3)"#, r#"2"#),
            (r#"((1 + 2) * (3 + 4)) / 7"#, r#"3"#),
            (r#"1.5 + 2.25"#, r#"3.75"#),
            (r#"-1 + 2"#, r#"1"#),
            (r#"-(1 + 2)"#, r#"-3"#),
            (r#" ( 1 + 2 ) * ( 3 + 4 ) "#, r#"21"#),
            (r#"1 + 2 * 3"#, r#"7"#),
            (r#"4 / 2 + 3"#, r#"5"#),
            (r#"6 - 4 / 2"#, r#"4"#),
            (r#"(1 + 2) * 3"#, r#"9"#),
            (r#"10 / (2 + 3)"#, r#"2"#),
            (r#"((1 + 2) * (3 + 4)) / 7"#, r#"3"#),
            (r#"1 + 2 - 3 + 4"#, r#"4"#),
            (r#"8 / 4 * 2 / 1"#, r#"4"#),
            (r#"1 + 2 * (3 + 4) / 7 - 1"#, r#"2"#),
            (r#"((2 + 3) * (4 - 2)) / (1 + 1)"#, r#"5"#),
            (r#"(5)"#, r#"5"#),
            (r#"-(-(5))"#, r#"5"#),
            // With modulo operation
            (r#"10 % 3"#, r#"1"#),
            (r#"10 % 5"#, r#"0"#),
            (r#"5 + 10 % 3"#, r#"6"#),
            (r#"-(1 + 2)"#, r#"-3"#),
            (r#"-(10 % 3)"#, r#"-1"#),
            (r#"10 % 3 + 2"#, r#"3"#),
            (r#"2 + 10 % 3"#, r#"3"#),
            (r#"10 % 3 * 2"#, r#"2"#),
            (r#"2 * 10 % 3"#, r#"2"#),
            (r#"(10 % 3) + 2"#, r#"3"#),
            (r#"10 % (3 + 2)"#, r#"0"#),
            (r#"(2 + 10) % 3"#, r#"0"#),
            (r#"1 + 10 % 3 * 4 - 5 / 2"#, r#"2.5"#),
            (r#"10 % 3 % 2"#, r#"1"#),
            (r#"-(10 % 3)"#, r#"-1"#),
            (r#"-(10 % 3) + 4"#, r#"3"#),
        ];

        for (case, expected) in cases {
            let mut prom_expr = parser::parse(case).unwrap_or_else(|_| panic!("case: {}", case));
            fold_constant_expr(&mut prom_expr);
            assert_eq!(
                prom_expr.to_string(),
                expected,
                "case: {}, expected: {}, actual: {}",
                case,
                expected,
                prom_expr
            );
        }
    }
}
