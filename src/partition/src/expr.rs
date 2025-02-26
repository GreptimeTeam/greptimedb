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

use std::fmt::{Debug, Display, Formatter};

use datatypes::value::Value;
use serde::{Deserialize, Serialize};
use sql::statements::value_to_sql_value;
use sqlparser::ast::{BinaryOperator as ParserBinaryOperator, Expr as ParserExpr, Ident};

/// Struct for partition expression. This can be converted back to sqlparser's [Expr].
/// by [`Self::to_parser_expr`].
///
/// [Expr]: sqlparser::ast::Expr
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PartitionExpr {
    pub(crate) lhs: Box<Operand>,
    pub(crate) op: RestrictedOp,
    pub(crate) rhs: Box<Operand>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Operand {
    Column(String),
    Value(Value),
    Expr(PartitionExpr),
}

impl Display for Operand {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Column(v) => write!(f, "{v}"),
            Self::Value(v) => write!(f, "{v}"),
            Self::Expr(v) => write!(f, "{v}"),
        }
    }
}

/// A restricted set of [Operator](datafusion_expr::Operator) that can be used in
/// partition expressions.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum RestrictedOp {
    // Evaluate to binary
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,

    // Conjunction
    And,
    Or,
}

impl RestrictedOp {
    pub fn try_from_parser(op: &ParserBinaryOperator) -> Option<Self> {
        match op {
            ParserBinaryOperator::Eq => Some(Self::Eq),
            ParserBinaryOperator::NotEq => Some(Self::NotEq),
            ParserBinaryOperator::Lt => Some(Self::Lt),
            ParserBinaryOperator::LtEq => Some(Self::LtEq),
            ParserBinaryOperator::Gt => Some(Self::Gt),
            ParserBinaryOperator::GtEq => Some(Self::GtEq),
            ParserBinaryOperator::And => Some(Self::And),
            ParserBinaryOperator::Or => Some(Self::Or),
            _ => None,
        }
    }

    pub fn to_parser_op(&self) -> ParserBinaryOperator {
        match self {
            Self::Eq => ParserBinaryOperator::Eq,
            Self::NotEq => ParserBinaryOperator::NotEq,
            Self::Lt => ParserBinaryOperator::Lt,
            Self::LtEq => ParserBinaryOperator::LtEq,
            Self::Gt => ParserBinaryOperator::Gt,
            Self::GtEq => ParserBinaryOperator::GtEq,
            Self::And => ParserBinaryOperator::And,
            Self::Or => ParserBinaryOperator::Or,
        }
    }
}
impl Display for RestrictedOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Eq => write!(f, "="),
            Self::NotEq => write!(f, "<>"),
            Self::Lt => write!(f, "<"),
            Self::LtEq => write!(f, "<="),
            Self::Gt => write!(f, ">"),
            Self::GtEq => write!(f, ">="),
            Self::And => write!(f, "AND"),
            Self::Or => write!(f, "OR"),
        }
    }
}

impl PartitionExpr {
    pub fn new(lhs: Operand, op: RestrictedOp, rhs: Operand) -> Self {
        Self {
            lhs: Box::new(lhs),
            op,
            rhs: Box::new(rhs),
        }
    }

    /// Convert [Self] back to sqlparser's [Expr]
    ///
    /// [Expr]: ParserExpr
    pub fn to_parser_expr(&self) -> ParserExpr {
        // Safety: Partition rule won't contains unsupported value type.
        // Otherwise it will be rejected by the parser.
        let lhs = match &*self.lhs {
            Operand::Column(c) => ParserExpr::Identifier(Ident::new(c.clone())),
            Operand::Value(v) => ParserExpr::Value(value_to_sql_value(v).unwrap()),
            Operand::Expr(e) => e.to_parser_expr(),
        };

        let rhs = match &*self.rhs {
            Operand::Column(c) => ParserExpr::Identifier(Ident::new(c.clone())),
            Operand::Value(v) => ParserExpr::Value(value_to_sql_value(v).unwrap()),
            Operand::Expr(e) => e.to_parser_expr(),
        };

        ParserExpr::BinaryOp {
            left: Box::new(lhs),
            op: self.op.to_parser_op(),
            right: Box::new(rhs),
        }
    }
}

impl Display for PartitionExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} {}", self.lhs, self.op, self.rhs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_expr() {
        let cases = [
            (
                Operand::Column("a".to_string()),
                RestrictedOp::Eq,
                Operand::Value(Value::UInt32(10)),
                "a = 10",
            ),
            (
                Operand::Column("a".to_string()),
                RestrictedOp::NotEq,
                Operand::Value(Value::UInt32(10)),
                "a <> 10",
            ),
            (
                Operand::Column("a".to_string()),
                RestrictedOp::Lt,
                Operand::Value(Value::UInt32(10)),
                "a < 10",
            ),
            (
                Operand::Column("a".to_string()),
                RestrictedOp::LtEq,
                Operand::Value(Value::UInt32(10)),
                "a <= 10",
            ),
            (
                Operand::Column("a".to_string()),
                RestrictedOp::Gt,
                Operand::Value(Value::UInt32(10)),
                "a > 10",
            ),
            (
                Operand::Column("a".to_string()),
                RestrictedOp::GtEq,
                Operand::Value(Value::UInt32(10)),
                "a >= 10",
            ),
            (
                Operand::Column("a".to_string()),
                RestrictedOp::And,
                Operand::Column("b".to_string()),
                "a AND b",
            ),
            (
                Operand::Column("a".to_string()),
                RestrictedOp::Or,
                Operand::Column("b".to_string()),
                "a OR b",
            ),
            (
                Operand::Column("a".to_string()),
                RestrictedOp::Or,
                Operand::Expr(PartitionExpr::new(
                    Operand::Column("c".to_string()),
                    RestrictedOp::And,
                    Operand::Column("d".to_string()),
                )),
                "a OR c AND d",
            ),
        ];

        for case in cases {
            let expr = PartitionExpr::new(case.0, case.1.clone(), case.2);
            assert_eq!(case.3, expr.to_string());
        }
    }
}
