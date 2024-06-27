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

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use common_query::error::{
    GeneralDataFusionSnafu, IntoVectorSnafu, InvalidFuncArgsSnafu, InvalidInputTypeSnafu, Result,
};
use datafusion::common::DFSchema;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{self, Expr, Volatility};
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion::prelude::SessionConfig;
use datatypes::arrow::array::RecordBatch;
use datatypes::arrow::datatypes::{DataType, Field};
use datatypes::prelude::VectorRef;
use datatypes::vectors::BooleanVector;
use snafu::{ensure, ResultExt};
use store_api::storage::ConcreteDataType;

use crate::function::{Function, FunctionContext};

/// `matches` for full text search.
#[derive(Clone, Debug, Default)]
struct MatchesFunction;

impl fmt::Display for MatchesFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MATCHES")
    }
}

impl Function for MatchesFunction {
    fn name(&self) -> &str {
        "matches"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::boolean_datatype())
    }

    fn signature(&self) -> common_query::prelude::Signature {
        common_query::prelude::Signature::exact(
            vec![
                ConcreteDataType::string_datatype(),
                ConcreteDataType::string_datatype(),
            ],
            Volatility::Immutable,
        )
    }

    // TODO: read case-sensitive config
    fn eval(&self, _func_ctx: FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure!(
            columns.len() == 2,
            InvalidFuncArgsSnafu {
                err_msg: format!(
                    "The length of the args is not correct, expect exactly 2, have: {}",
                    columns.len()
                ),
            }
        );
        ensure!(
            columns[1].len() == 1,
            InvalidFuncArgsSnafu {
                err_msg: "The second argument shoudl be a string literal",
            }
        );
        let pattern_vector = &columns[1]
            .cast(&ConcreteDataType::string_datatype())
            .context(InvalidInputTypeSnafu {
                err_msg: "cannot cast `pattern` to string",
            })?;
        // Safety: both length and type are checked before
        let pattern = pattern_vector.get(0).as_string().unwrap();
        self.eval(columns[0].clone(), pattern)
    }
}

impl MatchesFunction {
    fn eval(&self, data: VectorRef, pattern: String) -> Result<VectorRef> {
        let col_name = "data";
        let parser_context = ParserContext::default();
        let ast = parser_context.parse_pattern(&pattern)?;
        let like_expr = ast.to_like_expr(col_name);

        let input_schema = Self::input_schema();
        let session_state =
            SessionState::new_with_config_rt(SessionConfig::default(), Arc::default());
        let planner = DefaultPhysicalPlanner::default();
        let physical_expr = planner
            .create_physical_expr(&like_expr, &input_schema, &session_state)
            .context(GeneralDataFusionSnafu)?;

        let data_array = data.to_arrow_array();
        let arrow_schema = Arc::new(input_schema.as_arrow().clone());
        let input_record_batch = RecordBatch::try_new(arrow_schema, vec![data_array]).unwrap();

        let num_rows = input_record_batch.num_rows();
        let result = physical_expr
            .evaluate(&input_record_batch)
            .context(GeneralDataFusionSnafu)?;
        let result_array = result
            .into_array(num_rows)
            .context(GeneralDataFusionSnafu)?;
        let result_vector =
            BooleanVector::try_from_arrow_array(result_array).context(IntoVectorSnafu {
                data_type: DataType::Boolean,
            })?;

        Ok(Arc::new(result_vector))
    }

    fn input_schema() -> DFSchema {
        DFSchema::from_unqualifed_fields(
            [Arc::new(Field::new("data", DataType::Utf8, true))].into(),
            HashMap::new(),
        )
        .unwrap()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PatternAst {
    Literal {
        op: UnaryOp,
        pattern: String,
    },
    Binary {
        lhs: Box<PatternAst>,
        op: BinaryOp,
        rhs: Box<PatternAst>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum UnaryOp {
    Must,
    Optional,
    Negative,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum BinaryOp {
    And,
    Or,
}

impl PatternAst {
    fn to_like_expr(&self, column: &str) -> Expr {
        match self {
            PatternAst::Literal { op, pattern } => {
                let expr = Self::convert_literal(column, pattern);
                match op {
                    UnaryOp::Must => expr,
                    UnaryOp::Optional => expr,
                    UnaryOp::Negative => logical_expr::not(expr),
                }
            }
            PatternAst::Binary { lhs, op, rhs } => {
                let lhs_expr = lhs.to_like_expr(column);
                let rhs_expr = rhs.to_like_expr(column);
                match op {
                    BinaryOp::And => lhs_expr.and(rhs_expr),
                    BinaryOp::Or => lhs_expr.or(rhs_expr),
                }
            }
        }
    }

    fn convert_literal(column: &str, pattern: &str) -> Expr {
        logical_expr::col(column).like(logical_expr::lit(format!(
            "%{}%",
            Self::escape_pattern(pattern)
        )))
    }

    fn escape_pattern(pattern: &str) -> String {
        pattern
            .replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_")
    }
}

struct PatternAstBuilder {
    ast: Option<PatternAst>,
}

impl PatternAstBuilder {
    fn from_existing(ast: PatternAst) -> Self {
        Self { ast: Some(ast) }
    }

    fn build(self) -> PatternAst {
        self.ast.unwrap()
    }

    fn or(&mut self, rhs: PatternAst) {
        let lhs = self.ast.take().unwrap();
        self.ast = Some(PatternAst::Binary {
            lhs: Box::new(lhs),
            op: BinaryOp::Or,
            rhs: Box::new(rhs),
        });
    }
}

#[derive(Default)]
struct ParserContext {
    stack: Vec<PatternAst>,
}

impl ParserContext {
    pub fn parse_pattern(mut self, pattern: &str) -> Result<PatternAst> {
        let tokenizer = Tokenizer::default();
        let raw_tokens = tokenizer.tokenize(pattern)?;
        let mut tokens = Self::to_rpn(raw_tokens)?;

        while !tokens.is_empty() {
            self.parse_one_impl(&mut tokens)?;
        }

        if self.stack.is_empty() {
            panic!("todo error");
        }

        // conjoin them together
        let mut builder = PatternAstBuilder::from_existing(self.stack.remove(0));
        for ast in self.stack.into_iter() {
            builder.or(ast);
        }

        Ok(builder.build())
    }

    /// Convert infix token stream to RPN
    fn to_rpn(mut raw_tokens: Vec<Token>) -> Result<Vec<Token>> {
        let mut operator_stack = vec![];
        let mut result = vec![];
        raw_tokens.reverse();

        while let Some(token) = raw_tokens.pop() {
            match token {
                Token::Phase(_) => result.push(token),
                Token::Must | Token::Negative => {
                    // unary operator with paren is not handled yet
                    let phase = raw_tokens.pop().expect("todo error");
                    result.push(phase);
                    result.push(token);
                }
                Token::OpenParen => operator_stack.push(token),
                Token::And | Token::Or => {
                    while let Some(op) = operator_stack.pop() {
                        if op == Token::OpenParen {
                            operator_stack.push(op);
                            break;
                        }
                        result.push(op);
                    }
                    operator_stack.push(token);
                }
                Token::CloseParen => {
                    while let Some(op) = operator_stack.pop() {
                        if op == Token::OpenParen {
                            break;
                        }
                        result.push(op);
                    }
                }
            }
        }

        for operand in operator_stack.into_iter() {
            if operand == Token::OpenParen {
                return InvalidFuncArgsSnafu {
                    err_msg: "Unmatched parentheses",
                }
                .fail();
            }
            result.push(operand);
        }

        Ok(result)
    }

    fn parse_one_impl(&mut self, tokens: &mut Vec<Token>) -> Result<()> {
        if let Some(token) = tokens.pop() {
            match token {
                Token::Must => {
                    let phase = tokens.pop().expect("todo error");
                    self.stack.push(PatternAst::Literal {
                        op: UnaryOp::Must,
                        pattern: Self::unwrap_phase(phase)?,
                    });
                    return Ok(());
                }
                Token::Negative => {
                    let phase = tokens.pop().expect("todo error");
                    self.stack.push(PatternAst::Literal {
                        op: UnaryOp::Negative,
                        pattern: Self::unwrap_phase(phase)?,
                    });
                    return Ok(());
                }
                Token::Phase(token) => {
                    let phase = token.clone();
                    self.stack.push(PatternAst::Literal {
                        op: UnaryOp::Optional,
                        pattern: phase,
                    });
                    return Ok(());
                }
                Token::And => {
                    if self.stack.is_empty() {
                        self.parse_one_impl(tokens)?
                    };
                    let rhs = self.stack.pop().expect("todo error");
                    if self.stack.is_empty() {
                        self.parse_one_impl(tokens)?
                    };
                    let lhs = self.stack.pop().expect("todo error");
                    self.stack.push(PatternAst::Binary {
                        lhs: Box::new(lhs),
                        op: BinaryOp::And,
                        rhs: Box::new(rhs),
                    });
                    return Ok(());
                }
                Token::Or => {
                    if self.stack.is_empty() {
                        self.parse_one_impl(tokens)?
                    };
                    let rhs = self.stack.pop().expect("todo error");
                    if self.stack.is_empty() {
                        self.parse_one_impl(tokens)?
                    };
                    let lhs = self.stack.pop().expect("todo error");
                    self.stack.push(PatternAst::Binary {
                        lhs: Box::new(lhs),
                        op: BinaryOp::Or,
                        rhs: Box::new(rhs),
                    });
                    return Ok(());
                }
                Token::OpenParen | Token::CloseParen => {
                    return InvalidFuncArgsSnafu {
                        err_msg: "Unexpected parentheses",
                    }
                    .fail();
                }
            }
        }

        Ok(())
    }

    fn unwrap_phase(token: Token) -> Result<String> {
        match token {
            Token::Phase(phase) => Ok(phase),
            _ => panic!("todo error {token:?}"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Token {
    /// "+"
    Must,
    /// "-"
    Negative,
    /// "AND"
    And,
    /// "OR"
    Or,
    /// "("
    OpenParen,
    /// ")"
    CloseParen,
    /// Any other phases
    Phase(String),
}

#[derive(Default)]
struct Tokenizer {
    cursor: usize,
}

impl Tokenizer {
    pub fn tokenize(mut self, pattern: &str) -> Result<Vec<Token>> {
        let mut tokens = vec![];
        while self.cursor < pattern.len() {
            let c = pattern.chars().nth(self.cursor).unwrap();
            match c {
                '+' => tokens.push(Token::Must),
                '-' => tokens.push(Token::Negative),
                '(' => tokens.push(Token::OpenParen),
                ')' => tokens.push(Token::CloseParen),
                ' ' => {}
                '\"' => {
                    self.step_next();
                    let phase = self.consume_next_phase(true, pattern)?;
                    tokens.push(Token::Phase(phase));
                }
                _ => {
                    let phase = self.consume_next_phase(false, pattern)?;
                    match phase.as_str() {
                        "AND" => tokens.push(Token::And),
                        "OR" => tokens.push(Token::Or),
                        _ => tokens.push(Token::Phase(phase)),
                    }
                }
            }
            self.cursor += 1;
        }
        Ok(tokens)
    }

    fn consume_next(&mut self, pattern: &str) -> Option<char> {
        self.cursor += 1;
        let c = pattern.chars().nth(self.cursor);
        c
    }

    fn step_next(&mut self) {
        self.cursor += 1;
    }

    fn rewind_one(&mut self) {
        self.cursor -= 1;
    }

    /// Current `cursor` points to the first character of the phase.
    /// If the phase is enclosed by double quotes, consume the start quote before calling this.
    fn consume_next_phase(&mut self, is_quoted: bool, pattern: &str) -> Result<String> {
        let mut phase = String::new();
        let mut is_quote_present = false;

        while self.cursor < pattern.len() {
            let mut c = pattern.chars().nth(self.cursor).unwrap();

            match c {
                '\"' => {
                    is_quote_present = true;
                    break;
                }
                ' ' => {
                    if !is_quoted {
                        break;
                    }
                }
                '(' | ')' | '+' | '-' => {
                    if !is_quoted {
                        self.rewind_one();
                        break;
                    }
                }
                '\\' => {
                    let Some(next) = self.consume_next(pattern) else {
                        return InvalidFuncArgsSnafu {
                            err_msg: "Unexpected end of pattern, expected a character after escape ('\\')",
                        }.fail();
                    };
                    // it doesn't check whether the escaped character is valid or not
                    c = next;
                }
                _ => {}
            }

            phase.push(c);
            self.cursor += 1;
        }

        if is_quoted ^ is_quote_present {
            return InvalidFuncArgsSnafu {
                err_msg: "Unclosed quotes ('\"')",
            }
            .fail();
        }

        Ok(phase)
    }
}

#[cfg(test)]
mod test {
    use datatypes::vectors::StringVector;

    use super::*;

    #[test]
    fn valid_matches_tokenizer() {
        use Token::*;
        let cases = [
            (
                "a + b - c",
                vec![
                    Phase("a".to_string()),
                    Must,
                    Phase("b".to_string()),
                    Negative,
                    Phase("c".to_string()),
                ],
            ),
            (
                "+a(b-c)",
                vec![
                    Must,
                    Phase("a".to_string()),
                    OpenParen,
                    Phase("b".to_string()),
                    Negative,
                    Phase("c".to_string()),
                    CloseParen,
                ],
            ),
            (
                r#"Barack Obama"#,
                vec![Phase("Barack".to_string()), Phase("Obama".to_string())],
            ),
            (
                r#"+apple +fruit"#,
                vec![
                    Must,
                    Phase("apple".to_string()),
                    Must,
                    Phase("fruit".to_string()),
                ],
            ),
            (
                r#""He said \"hello\"""#,
                vec![Phase("He said \"hello\"".to_string())],
            ),
            (
                r#"a AND b OR c"#,
                vec![
                    Phase("a".to_string()),
                    And,
                    Phase("b".to_string()),
                    Or,
                    Phase("c".to_string()),
                ],
            ),
        ];

        for (query, expected) in cases {
            let tokenizer = Tokenizer::default();
            let tokens = tokenizer.tokenize(query).unwrap();
            assert_eq!(expected, tokens, "{query}");
        }
    }

    #[test]
    fn invalid_matches_tokenizer() {
        let cases = [
            (r#""He said "hello""#, "Unclosed quotes ('\"')"),
            (r#""He said hello"#, "Unclosed quotes ('\"')"),
        ];

        for (query, expected) in cases {
            let tokenizer = Tokenizer::default();
            let result = tokenizer.tokenize(query);
            assert!(result.is_err(), "{query}");
            assert!(
                result.unwrap_err().to_string().contains(expected),
                "{query}"
            );
        }
    }

    #[test]
    fn valid_matches_parser() {
        let cases = [
            (
                "a AND b OR c",
                PatternAst::Binary {
                    lhs: Box::new(PatternAst::Binary {
                        lhs: Box::new(PatternAst::Literal {
                            op: UnaryOp::Optional,
                            pattern: "a".to_string(),
                        }),
                        op: BinaryOp::And,
                        rhs: Box::new(PatternAst::Literal {
                            op: UnaryOp::Optional,
                            pattern: "b".to_string(),
                        }),
                    }),
                    op: BinaryOp::Or,
                    rhs: Box::new(PatternAst::Literal {
                        op: UnaryOp::Optional,
                        pattern: "c".to_string(),
                    }),
                },
            ),
            (
                "(a AND b) OR c",
                PatternAst::Binary {
                    lhs: Box::new(PatternAst::Binary {
                        lhs: Box::new(PatternAst::Literal {
                            op: UnaryOp::Optional,
                            pattern: "a".to_string(),
                        }),
                        op: BinaryOp::And,
                        rhs: Box::new(PatternAst::Literal {
                            op: UnaryOp::Optional,
                            pattern: "b".to_string(),
                        }),
                    }),
                    op: BinaryOp::Or,
                    rhs: Box::new(PatternAst::Literal {
                        op: UnaryOp::Optional,
                        pattern: "c".to_string(),
                    }),
                },
            ),
            (
                "a AND (b OR c)",
                PatternAst::Binary {
                    lhs: Box::new(PatternAst::Literal {
                        op: UnaryOp::Optional,
                        pattern: "a".to_string(),
                    }),

                    op: BinaryOp::And,
                    rhs: Box::new(PatternAst::Binary {
                        lhs: Box::new(PatternAst::Literal {
                            op: UnaryOp::Optional,
                            pattern: "b".to_string(),
                        }),
                        op: BinaryOp::Or,
                        rhs: Box::new(PatternAst::Literal {
                            op: UnaryOp::Optional,
                            pattern: "c".to_string(),
                        }),
                    }),
                },
            ),
            (
                "a + b - c",
                PatternAst::Binary {
                    lhs: Box::new(PatternAst::Binary {
                        lhs: Box::new(PatternAst::Literal {
                            op: UnaryOp::Negative,
                            pattern: "c".to_string(),
                        }),
                        op: BinaryOp::Or,
                        rhs: Box::new(PatternAst::Literal {
                            op: UnaryOp::Must,
                            pattern: "b".to_string(),
                        }),
                    }),
                    op: BinaryOp::Or,
                    rhs: Box::new(PatternAst::Literal {
                        op: UnaryOp::Optional,
                        pattern: "a".to_string(),
                    }),
                },
            ),
            (
                "(+a +b) c",
                PatternAst::Binary {
                    lhs: Box::new(PatternAst::Binary {
                        lhs: Box::new(PatternAst::Literal {
                            op: UnaryOp::Optional,
                            pattern: "c".to_string(),
                        }),
                        op: BinaryOp::Or,
                        rhs: Box::new(PatternAst::Literal {
                            op: UnaryOp::Must,
                            pattern: "b".to_string(),
                        }),
                    }),
                    op: BinaryOp::Or,
                    rhs: Box::new(PatternAst::Literal {
                        op: UnaryOp::Must,
                        pattern: "a".to_string(),
                    }),
                },
            ),
        ];

        for (query, expected) in cases {
            let parser = ParserContext { stack: vec![] };
            let ast = parser.parse_pattern(query).unwrap();
            assert_eq!(expected, ast, "{query}");
        }
    }

    #[test]
    fn evaluate_matches_without_parenthesis() {
        let input_data = vec![
            "The quick brown fox jumps over the lazy dog",
            "The             fox jumps over the lazy dog",
            "The quick brown     jumps over the lazy dog",
            "The quick brown fox       over the lazy dog",
            "The quick brown fox jumps      the lazy dog",
            "The quick brown fox jumps over          dog",
            "The quick brown fox jumps over the      dog",
        ];
        let input_vector = Arc::new(StringVector::from(input_data));
        let cases = [
            ("quick", vec![true, false, true, true, true, true, true]),
            (
                "\"quick brown\"",
                vec![true, false, true, true, true, true, true],
            ),
            (
                "\"fox jumps\"",
                vec![true, true, false, false, true, true, true],
            ),
            (
                "fox +jumps -over",
                vec![true, true, true, true, true, true, true],
            ),
            (
                "fox AND +jumps AND -over",
                vec![false, false, false, false, true, false, false],
            ),
            (
                "fox OR lazy",
                vec![true, true, true, true, true, true, true],
            ),
            (
                "fox AND lazy",
                vec![true, true, false, true, true, false, false],
            ),
            (
                "-over -lazy",
                vec![false, false, false, false, true, true, true],
            ),
            (
                "-over AND -lazy",
                vec![false, false, false, false, false, false, false],
            ),
        ];

        let f = MatchesFunction;
        for (pattern, expected) in cases {
            let actual: VectorRef = f.eval(input_vector.clone(), pattern.to_string()).unwrap();
            let expected: VectorRef = Arc::new(BooleanVector::from(expected)) as _;
            assert_eq!(expected, actual, "{pattern}");
        }
    }
}
