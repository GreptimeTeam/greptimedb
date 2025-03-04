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

use sqlparser::ast::{BinaryOperator, Expr, Ident, Value};

use crate::statements::create::Partitions;

macro_rules! between_string {
    ($col: expr, $left_incl: expr, $right_excl: expr) => {
        Expr::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(Expr::BinaryOp {
                op: BinaryOperator::GtEq,
                left: Box::new($col.clone()),
                right: Box::new(Expr::Value(Value::SingleQuotedString(
                    $left_incl.to_string(),
                ))),
            }),
            right: Box::new(Expr::BinaryOp {
                op: BinaryOperator::Lt,
                left: Box::new($col.clone()),
                right: Box::new(Expr::Value(Value::SingleQuotedString(
                    $right_excl.to_string(),
                ))),
            }),
        }
    };
}

macro_rules! or {
    ($left: expr, $right: expr) => {
        Expr::BinaryOp {
            op: BinaryOperator::Or,
            left: Box::new($left),
            right: Box::new($right),
        }
    };
}

pub fn partition_rule_for_hexstring(ident: &str) -> Partitions {
    let ident = Ident::new(ident);
    let ident_expr = Expr::Identifier(ident.clone());

    // rules are like:
    //
    // "trace_id < '1'",
    // "trace_id >= '1' AND trace_id < '2'",
    // "trace_id >= '2' AND trace_id < '3'",
    // "trace_id >= '3' AND trace_id < '4'",
    // "trace_id >= '4' AND trace_id < '5'",
    // "trace_id >= '5' AND trace_id < '6'",
    // "trace_id >= '6' AND trace_id < '7'",
    // "trace_id >= '7' AND trace_id < '8'",
    // "trace_id >= '8' AND trace_id < '9'",
    // "trace_id >= '9' AND trace_id < 'A'",
    // "trace_id >= 'A' AND trace_id < 'B' OR trace_id >= 'a' AND trace_id < 'b'",
    // "trace_id >= 'B' AND trace_id < 'C' OR trace_id >= 'b' AND trace_id < 'c'",
    // "trace_id >= 'C' AND trace_id < 'D' OR trace_id >= 'c' AND trace_id < 'd'",
    // "trace_id >= 'D' AND trace_id < 'E' OR trace_id >= 'd' AND trace_id < 'e'",
    // "trace_id >= 'E' AND trace_id < 'F' OR trace_id >= 'e' AND trace_id < 'f'",
    // "trace_id >= 'F' AND trace_id < 'a' OR trace_id >= 'f'",
    let rules = vec![
        Expr::BinaryOp {
            left: Box::new(ident_expr.clone()),
            op: BinaryOperator::Lt,
            right: Box::new(Expr::Value(Value::SingleQuotedString("1".to_string()))),
        },
        // [left, right)
        between_string!(ident_expr, "1", "2"),
        between_string!(ident_expr, "2", "3"),
        between_string!(ident_expr, "3", "4"),
        between_string!(ident_expr, "4", "5"),
        between_string!(ident_expr, "5", "6"),
        between_string!(ident_expr, "6", "7"),
        between_string!(ident_expr, "7", "8"),
        between_string!(ident_expr, "8", "9"),
        between_string!(ident_expr, "9", "A"),
        or!(
            between_string!(ident_expr, "A", "B"),
            between_string!(ident_expr, "a", "b")
        ),
        or!(
            between_string!(ident_expr, "B", "C"),
            between_string!(ident_expr, "b", "c")
        ),
        or!(
            between_string!(ident_expr, "C", "D"),
            between_string!(ident_expr, "c", "d")
        ),
        or!(
            between_string!(ident_expr, "D", "E"),
            between_string!(ident_expr, "d", "e")
        ),
        or!(
            between_string!(ident_expr, "E", "F"),
            between_string!(ident_expr, "e", "f")
        ),
        or!(
            between_string!(ident_expr, "F", "a"),
            Expr::BinaryOp {
                left: Box::new(ident_expr.clone()),
                op: BinaryOperator::GtEq,
                right: Box::new(Expr::Value(Value::SingleQuotedString("f".to_string()))),
            }
        ),
    ];

    Partitions {
        column_list: vec![ident],
        exprs: rules,
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::Expr;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    use super::*;

    #[test]
    fn test_rules() {
        let expr = vec![
            "trace_id < '1'",
            "trace_id >= '1' AND trace_id < '2'",
            "trace_id >= '2' AND trace_id < '3'",
            "trace_id >= '3' AND trace_id < '4'",
            "trace_id >= '4' AND trace_id < '5'",
            "trace_id >= '5' AND trace_id < '6'",
            "trace_id >= '6' AND trace_id < '7'",
            "trace_id >= '7' AND trace_id < '8'",
            "trace_id >= '8' AND trace_id < '9'",
            "trace_id >= '9' AND trace_id < 'A'",
            "trace_id >= 'A' AND trace_id < 'B' OR trace_id >= 'a' AND trace_id < 'b'",
            "trace_id >= 'B' AND trace_id < 'C' OR trace_id >= 'b' AND trace_id < 'c'",
            "trace_id >= 'C' AND trace_id < 'D' OR trace_id >= 'c' AND trace_id < 'd'",
            "trace_id >= 'D' AND trace_id < 'E' OR trace_id >= 'd' AND trace_id < 'e'",
            "trace_id >= 'E' AND trace_id < 'F' OR trace_id >= 'e' AND trace_id < 'f'",
            "trace_id >= 'F' AND trace_id < 'a' OR trace_id >= 'f'",
        ];

        let dialect = GenericDialect {};
        let results = expr
            .into_iter()
            .map(|s| {
                let mut parser = Parser::new(&dialect).try_with_sql(s).unwrap();
                parser.parse_expr().unwrap()
            })
            .collect::<Vec<Expr>>();

        assert_eq!(results, partition_rule_for_hexstring("trace_id").exprs);
    }
}
