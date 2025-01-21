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
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeIterator, TreeNodeRecursion};
use datafusion::common::{DFSchema, Result as DfResult};
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{self, Expr, Volatility};
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datatypes::arrow::array::RecordBatch;
use datatypes::arrow::datatypes::{DataType, Field};
use datatypes::prelude::VectorRef;
use datatypes::vectors::BooleanVector;
use snafu::{ensure, OptionExt, ResultExt};
use store_api::storage::ConcreteDataType;

use crate::function::{Function, FunctionContext};
use crate::function_registry::FunctionRegistry;

/// `matches` for full text search.
///
/// Usage: matches(`<col>`, `<pattern>`) -> boolean
#[derive(Clone, Debug, Default)]
pub(crate) struct MatchesFunction;

impl MatchesFunction {
    pub fn register(registry: &FunctionRegistry) {
        registry.register(Arc::new(MatchesFunction));
    }
}

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
        let raw_ast = parser_context.parse_pattern(&pattern)?;
        let ast = raw_ast.transform_ast()?;

        let like_expr = ast.into_like_expr(col_name);

        let input_schema = Self::input_schema();
        let session_state = SessionStateBuilder::new().with_default_features().build();
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
        DFSchema::from_unqualified_fields(
            [Arc::new(Field::new("data", DataType::Utf8, true))].into(),
            HashMap::new(),
        )
        .unwrap()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PatternAst {
    // Distinguish this with `Group` for simplicity
    /// A leaf node that matches a column with `pattern`
    Literal { op: UnaryOp, pattern: String },
    /// Flattened binary chains
    Binary {
        op: BinaryOp,
        children: Vec<PatternAst>,
    },
    /// A sub-tree enclosed by parenthesis
    Group { op: UnaryOp, child: Box<PatternAst> },
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum UnaryOp {
    Must,
    Optional,
    Negative,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum BinaryOp {
    And,
    Or,
}

impl PatternAst {
    fn into_like_expr(self, column: &str) -> Expr {
        match self {
            PatternAst::Literal { op, pattern } => {
                let expr = Self::convert_literal(column, &pattern);
                match op {
                    UnaryOp::Must => expr,
                    UnaryOp::Optional => expr,
                    UnaryOp::Negative => logical_expr::not(expr),
                }
            }
            PatternAst::Binary { op, children } => {
                if children.is_empty() {
                    return logical_expr::lit(true);
                }
                let exprs = children
                    .into_iter()
                    .map(|child| child.into_like_expr(column));
                // safety: children is not empty
                match op {
                    BinaryOp::And => exprs.reduce(Expr::and).unwrap(),
                    BinaryOp::Or => exprs.reduce(Expr::or).unwrap(),
                }
            }
            PatternAst::Group { op, child } => {
                let child = child.into_like_expr(column);
                match op {
                    UnaryOp::Must => child,
                    UnaryOp::Optional => child,
                    UnaryOp::Negative => logical_expr::not(child),
                }
            }
        }
    }

    fn convert_literal(column: &str, pattern: &str) -> Expr {
        logical_expr::col(column).like(logical_expr::lit(format!(
            "%{}%",
            crate::utils::escape_like_pattern(pattern)
        )))
    }

    /// Transform this AST with preset rules to make it correct.
    fn transform_ast(self) -> Result<Self> {
        self.transform_up(Self::collapse_binary_branch_fn)
            .context(GeneralDataFusionSnafu)
            .map(|data| data.data)?
            .transform_up(Self::eliminate_optional_fn)
            .context(GeneralDataFusionSnafu)
            .map(|data| data.data)?
            .transform_down(Self::eliminate_single_child_fn)
            .context(GeneralDataFusionSnafu)
            .map(|data| data.data)
    }

    /// Collapse binary branch with the same operator. I.e., this transformer
    /// changes the binary-tree AST into a multiple branching AST.
    ///
    /// This function is expected to be called in a bottom-up manner as
    /// it won't recursion.
    fn collapse_binary_branch_fn(self) -> DfResult<Transformed<Self>> {
        let PatternAst::Binary {
            op: parent_op,
            children,
        } = self
        else {
            return Ok(Transformed::no(self));
        };

        let mut collapsed = vec![];
        let mut remains = vec![];

        for child in children {
            match child {
                PatternAst::Literal { .. } | PatternAst::Group { .. } => {
                    collapsed.push(child);
                }
                PatternAst::Binary { op, children } => {
                    // no need to recursion because this function is expected to be called
                    // in a bottom-up manner
                    if op == parent_op {
                        collapsed.extend(children);
                    } else {
                        remains.push(PatternAst::Binary { op, children });
                    }
                }
            }
        }

        if collapsed.is_empty() {
            Ok(Transformed::no(PatternAst::Binary {
                op: parent_op,
                children: remains,
            }))
        } else {
            collapsed.extend(remains);
            Ok(Transformed::yes(PatternAst::Binary {
                op: parent_op,
                children: collapsed,
            }))
        }
    }

    /// Eliminate optional pattern. An optional pattern can always be
    /// omitted or transformed into a must pattern follows the following rules:
    /// - If there is only one pattern and it's optional, change it to must
    /// - If there is any must pattern, remove all other optional patterns
    fn eliminate_optional_fn(self) -> DfResult<Transformed<Self>> {
        let PatternAst::Binary {
            op: parent_op,
            children,
        } = self
        else {
            return Ok(Transformed::no(self));
        };

        if parent_op == BinaryOp::Or {
            let mut must_list = vec![];
            let mut must_not_list = vec![];
            let mut optional_list = vec![];
            let mut compound_list = vec![];

            for child in children {
                match child {
                    PatternAst::Literal { op, .. } | PatternAst::Group { op, .. } => match op {
                        UnaryOp::Must => must_list.push(child),
                        UnaryOp::Optional => optional_list.push(child),
                        UnaryOp::Negative => must_not_list.push(child),
                    },
                    PatternAst::Binary { .. } => {
                        compound_list.push(child);
                    }
                }
            }

            // Eliminate optional list if there is MUST.
            if !must_list.is_empty() {
                optional_list.clear();
            }

            let children_this_level = optional_list.into_iter().chain(compound_list).collect();
            let new_node = if !must_list.is_empty() || !must_not_list.is_empty() {
                let new_children = must_list
                    .into_iter()
                    .chain(must_not_list)
                    .chain(Some(PatternAst::Binary {
                        op: BinaryOp::Or,
                        children: children_this_level,
                    }))
                    .collect();
                PatternAst::Binary {
                    op: BinaryOp::And,
                    children: new_children,
                }
            } else {
                PatternAst::Binary {
                    op: BinaryOp::Or,
                    children: children_this_level,
                }
            };

            return Ok(Transformed::yes(new_node));
        }

        Ok(Transformed::no(PatternAst::Binary {
            op: parent_op,
            children,
        }))
    }

    /// Eliminate single child [`PatternAst::Binary`] node. If a binary node has only one child, it can be
    /// replaced by its only child.
    ///
    /// This function prefers to be applied in a top-down manner. But it's not required.
    fn eliminate_single_child_fn(self) -> DfResult<Transformed<Self>> {
        let PatternAst::Binary { op, mut children } = self else {
            return Ok(Transformed::no(self));
        };

        // remove empty grand children
        children.retain(|child| match child {
            PatternAst::Binary {
                children: grand_children,
                ..
            } => !grand_children.is_empty(),
            PatternAst::Literal { .. } | PatternAst::Group { .. } => true,
        });

        if children.len() == 1 {
            Ok(Transformed::yes(children.into_iter().next().unwrap()))
        } else {
            Ok(Transformed::no(PatternAst::Binary { op, children }))
        }
    }
}

impl TreeNode for PatternAst {
    fn apply_children<'n, F: FnMut(&'n Self) -> DfResult<TreeNodeRecursion>>(
        &'n self,
        mut f: F,
    ) -> DfResult<TreeNodeRecursion> {
        match self {
            PatternAst::Literal { .. } => Ok(TreeNodeRecursion::Continue),
            PatternAst::Binary { op: _, children } => {
                for child in children {
                    if TreeNodeRecursion::Stop == f(child)? {
                        return Ok(TreeNodeRecursion::Stop);
                    }
                }
                Ok(TreeNodeRecursion::Continue)
            }
            PatternAst::Group { op: _, child } => f(child),
        }
    }

    fn map_children<F: FnMut(Self) -> DfResult<Transformed<Self>>>(
        self,
        mut f: F,
    ) -> DfResult<Transformed<Self>> {
        match self {
            PatternAst::Literal { .. } => Ok(Transformed::no(self)),
            PatternAst::Binary { op, children } => children
                .into_iter()
                .map_until_stop_and_collect(&mut f)?
                .map_data(|new_children| {
                    Ok(PatternAst::Binary {
                        op,
                        children: new_children,
                    })
                }),
            PatternAst::Group { op, child } => f(*child)?.map_data(|new_child| {
                Ok(PatternAst::Group {
                    op,
                    child: Box::new(new_child),
                })
            }),
        }
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
        let raw_tokens = Self::accomplish_optional_unary_op(raw_tokens)?;
        let mut tokens = Self::to_rpn(raw_tokens)?;

        while !tokens.is_empty() {
            self.parse_one_impl(&mut tokens)?;
        }

        ensure!(
            !self.stack.is_empty(),
            InvalidFuncArgsSnafu {
                err_msg: "Empty pattern",
            }
        );

        // conjoin them together
        if self.stack.len() == 1 {
            Ok(self.stack.pop().unwrap())
        } else {
            Ok(PatternAst::Binary {
                op: BinaryOp::Or,
                children: self.stack,
            })
        }
    }

    /// Add [`Token::Optional`] for all bare [`Token::Phase`] and [`Token::Or`]
    /// for all adjacent [`Token::Phase`]s.
    ///
    /// This function also does some checks by the way. Like if two unary ops are
    /// adjacent.
    fn accomplish_optional_unary_op(raw_tokens: Vec<Token>) -> Result<Vec<Token>> {
        let mut is_prev_unary_op = false;
        // The first one doesn't need binary op
        let mut is_binary_op_before = true;
        let mut is_unary_op_before = false;
        let mut new_tokens = Vec::with_capacity(raw_tokens.len());
        for token in raw_tokens {
            // fill `Token::Or`
            if !is_binary_op_before
                && matches!(
                    token,
                    Token::Phase(_)
                        | Token::OpenParen
                        | Token::Must
                        | Token::Optional
                        | Token::Negative
                )
            {
                is_binary_op_before = true;
                new_tokens.push(Token::Or);
            }
            if matches!(
                token,
                Token::OpenParen // treat open paren as begin of new group
                | Token::And | Token::Or
            ) {
                is_binary_op_before = true;
            } else if matches!(token, Token::Phase(_) | Token::CloseParen) {
                // need binary op next time
                is_binary_op_before = false;
            }

            // fill `Token::Optional`
            if !is_prev_unary_op && matches!(token, Token::Phase(_) | Token::OpenParen) {
                new_tokens.push(Token::Optional);
            } else {
                is_prev_unary_op = matches!(token, Token::Must | Token::Negative);
            }

            // check if unary ops are adjacent by the way
            if matches!(token, Token::Must | Token::Optional | Token::Negative) {
                if is_unary_op_before {
                    return InvalidFuncArgsSnafu {
                        err_msg: "Invalid pattern, unary operators should not be adjacent",
                    }
                    .fail();
                }
                is_unary_op_before = true;
            } else {
                is_unary_op_before = false;
            }

            new_tokens.push(token);
        }

        Ok(new_tokens)
    }

    /// Convert infix token stream to RPN
    fn to_rpn(mut raw_tokens: Vec<Token>) -> Result<Vec<Token>> {
        let mut operator_stack = vec![];
        let mut result = vec![];
        raw_tokens.reverse();

        while let Some(token) = raw_tokens.pop() {
            match token {
                Token::Phase(_) => result.push(token),
                Token::Must | Token::Negative | Token::Optional => {
                    operator_stack.push(token);
                }
                Token::OpenParen => operator_stack.push(token),
                Token::And | Token::Or => {
                    // - Or has lower priority than And
                    // - Binary op have lower priority than unary op
                    while let Some(stack_top) = operator_stack.last()
                        && ((*stack_top == Token::And && token == Token::Or)
                            || matches!(
                                *stack_top,
                                Token::Must | Token::Optional | Token::Negative
                            ))
                    {
                        result.push(operator_stack.pop().unwrap());
                    }
                    operator_stack.push(token);
                }
                Token::CloseParen => {
                    let mut is_open_paren_found = false;
                    while let Some(op) = operator_stack.pop() {
                        if op == Token::OpenParen {
                            is_open_paren_found = true;
                            break;
                        }
                        result.push(op);
                    }
                    if !is_open_paren_found {
                        return InvalidFuncArgsSnafu {
                            err_msg: "Unmatched close parentheses",
                        }
                        .fail();
                    }
                }
            }
        }

        while let Some(operator) = operator_stack.pop() {
            if operator == Token::OpenParen {
                return InvalidFuncArgsSnafu {
                    err_msg: "Unmatched parentheses",
                }
                .fail();
            }
            result.push(operator);
        }

        Ok(result)
    }

    fn parse_one_impl(&mut self, tokens: &mut Vec<Token>) -> Result<()> {
        if let Some(token) = tokens.pop() {
            match token {
                Token::Must => {
                    if self.stack.is_empty() {
                        self.parse_one_impl(tokens)?;
                    }
                    let phase_or_group = self.stack.pop().context(InvalidFuncArgsSnafu {
                        err_msg: "Invalid pattern, \"+\" operator should have one operand",
                    })?;
                    match phase_or_group {
                        PatternAst::Literal { op: _, pattern } => {
                            self.stack.push(PatternAst::Literal {
                                op: UnaryOp::Must,
                                pattern,
                            });
                        }
                        PatternAst::Binary { .. } | PatternAst::Group { .. } => {
                            self.stack.push(PatternAst::Group {
                                op: UnaryOp::Must,
                                child: Box::new(phase_or_group),
                            })
                        }
                    }
                    return Ok(());
                }
                Token::Negative => {
                    if self.stack.is_empty() {
                        self.parse_one_impl(tokens)?;
                    }
                    let phase_or_group = self.stack.pop().context(InvalidFuncArgsSnafu {
                        err_msg: "Invalid pattern, \"-\" operator should have one operand",
                    })?;
                    match phase_or_group {
                        PatternAst::Literal { op: _, pattern } => {
                            self.stack.push(PatternAst::Literal {
                                op: UnaryOp::Negative,
                                pattern,
                            });
                        }
                        PatternAst::Binary { .. } | PatternAst::Group { .. } => {
                            self.stack.push(PatternAst::Group {
                                op: UnaryOp::Negative,
                                child: Box::new(phase_or_group),
                            })
                        }
                    }
                    return Ok(());
                }
                Token::Optional => {
                    if self.stack.is_empty() {
                        self.parse_one_impl(tokens)?;
                    }
                    let phase_or_group = self.stack.pop().context(InvalidFuncArgsSnafu {
                        err_msg:
                            "Invalid pattern, OPTIONAL(space) operator should have one operand",
                    })?;
                    match phase_or_group {
                        PatternAst::Literal { op: _, pattern } => {
                            self.stack.push(PatternAst::Literal {
                                op: UnaryOp::Optional,
                                pattern,
                            });
                        }
                        PatternAst::Binary { .. } | PatternAst::Group { .. } => {
                            self.stack.push(PatternAst::Group {
                                op: UnaryOp::Optional,
                                child: Box::new(phase_or_group),
                            })
                        }
                    }
                    return Ok(());
                }
                Token::Phase(pattern) => {
                    self.stack.push(PatternAst::Literal {
                        // Op here is a placeholder
                        op: UnaryOp::Optional,
                        pattern,
                    })
                }
                Token::And => {
                    if self.stack.is_empty() {
                        self.parse_one_impl(tokens)?;
                    };
                    let rhs = self.stack.pop().context(InvalidFuncArgsSnafu {
                        err_msg: "Invalid pattern, \"AND\" operator should have two operands",
                    })?;
                    if self.stack.is_empty() {
                        self.parse_one_impl(tokens)?
                    };
                    let lhs = self.stack.pop().context(InvalidFuncArgsSnafu {
                        err_msg: "Invalid pattern, \"AND\" operator should have two operands",
                    })?;
                    self.stack.push(PatternAst::Binary {
                        op: BinaryOp::And,
                        children: vec![lhs, rhs],
                    });
                    return Ok(());
                }
                Token::Or => {
                    if self.stack.is_empty() {
                        self.parse_one_impl(tokens)?
                    };
                    let rhs = self.stack.pop().context(InvalidFuncArgsSnafu {
                        err_msg: "Invalid pattern, \"OR\" operator should have two operands",
                    })?;
                    if self.stack.is_empty() {
                        self.parse_one_impl(tokens)?
                    };
                    let lhs = self.stack.pop().context(InvalidFuncArgsSnafu {
                        err_msg: "Invalid pattern, \"OR\" operator should have two operands",
                    })?;
                    self.stack.push(PatternAst::Binary {
                        op: BinaryOp::Or,
                        children: vec![lhs, rhs],
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

    /// This is not a token from user input, but a placeholder for internal use.
    /// It's used to accomplish the unary operator class with Must and Negative.
    /// In user provided pattern, optional is expressed by a bare phase or group
    /// (simply nothing or writespace).
    Optional,
}

#[derive(Default)]
struct Tokenizer {
    cursor: usize,
}

impl Tokenizer {
    pub fn tokenize(mut self, pattern: &str) -> Result<Vec<Token>> {
        let mut tokens = vec![];
        while self.cursor < pattern.len() {
            // TODO: collect pattern into Vec<char> if this tokenizer is bottleneck in the future
            let c = pattern.chars().nth(self.cursor).unwrap();
            match c {
                '+' => tokens.push(Token::Must),
                '-' => tokens.push(Token::Negative),
                '(' => tokens.push(Token::OpenParen),
                ')' => tokens.push(Token::CloseParen),
                ' ' => {
                    if let Some(last_token) = tokens.last() {
                        match last_token {
                            Token::Must | Token::Negative => {
                                return InvalidFuncArgsSnafu {
                                    err_msg: format!("Unexpected space after {:?}", last_token),
                                }
                                .fail();
                            }
                            _ => {}
                        }
                    }
                }
                '\"' => {
                    self.step_next();
                    let phase = self.consume_next_phase(true, pattern)?;
                    tokens.push(Token::Phase(phase));
                    // consume a writespace (or EOF) after quotes
                    if let Some(ending_separator) = self.consume_next(pattern) {
                        if ending_separator != ' ' {
                            return InvalidFuncArgsSnafu {
                                err_msg: "Expect a space after quotes ('\"')",
                            }
                            .fail();
                        }
                    }
                }
                _ => {
                    let phase = self.consume_next_phase(false, pattern)?;
                    match phase.to_uppercase().as_str() {
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
                "a +b -c",
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
            (r#""He said "hello""#, "Expect a space after quotes"),
            (r#""He said hello"#, "Unclosed quotes"),
            (r#"a + b - c"#, "Unexpected space after"),
            (r#"ab "c"def"#, "Expect a space after quotes"),
        ];

        for (query, expected) in cases {
            let tokenizer = Tokenizer::default();
            let result = tokenizer.tokenize(query);
            assert!(result.is_err(), "{query}");
            let actual_error = result.unwrap_err().to_string();
            assert!(actual_error.contains(expected), "{query}, {actual_error}");
        }
    }

    #[test]
    fn valid_ast_transformer() {
        let cases = [
            (
                "a AND b OR c",
                PatternAst::Binary {
                    op: BinaryOp::Or,
                    children: vec![
                        PatternAst::Literal {
                            op: UnaryOp::Optional,
                            pattern: "c".to_string(),
                        },
                        PatternAst::Binary {
                            op: BinaryOp::And,
                            children: vec![
                                PatternAst::Literal {
                                    op: UnaryOp::Optional,
                                    pattern: "a".to_string(),
                                },
                                PatternAst::Literal {
                                    op: UnaryOp::Optional,
                                    pattern: "b".to_string(),
                                },
                            ],
                        },
                    ],
                },
            ),
            (
                "a -b",
                PatternAst::Binary {
                    op: BinaryOp::And,
                    children: vec![
                        PatternAst::Literal {
                            op: UnaryOp::Negative,
                            pattern: "b".to_string(),
                        },
                        PatternAst::Literal {
                            op: UnaryOp::Optional,
                            pattern: "a".to_string(),
                        },
                    ],
                },
            ),
            (
                "a +b",
                PatternAst::Literal {
                    op: UnaryOp::Must,
                    pattern: "b".to_string(),
                },
            ),
            (
                "a b c d",
                PatternAst::Binary {
                    op: BinaryOp::Or,
                    children: vec![
                        PatternAst::Literal {
                            op: UnaryOp::Optional,
                            pattern: "a".to_string(),
                        },
                        PatternAst::Literal {
                            op: UnaryOp::Optional,
                            pattern: "b".to_string(),
                        },
                        PatternAst::Literal {
                            op: UnaryOp::Optional,
                            pattern: "c".to_string(),
                        },
                        PatternAst::Literal {
                            op: UnaryOp::Optional,
                            pattern: "d".to_string(),
                        },
                    ],
                },
            ),
            (
                "a b c AND d",
                PatternAst::Binary {
                    op: BinaryOp::Or,
                    children: vec![
                        PatternAst::Literal {
                            op: UnaryOp::Optional,
                            pattern: "a".to_string(),
                        },
                        PatternAst::Literal {
                            op: UnaryOp::Optional,
                            pattern: "b".to_string(),
                        },
                        PatternAst::Binary {
                            op: BinaryOp::And,
                            children: vec![
                                PatternAst::Literal {
                                    op: UnaryOp::Optional,
                                    pattern: "c".to_string(),
                                },
                                PatternAst::Literal {
                                    op: UnaryOp::Optional,
                                    pattern: "d".to_string(),
                                },
                            ],
                        },
                    ],
                },
            ),
        ];

        for (query, expected) in cases {
            let parser = ParserContext { stack: vec![] };
            let ast = parser.parse_pattern(query).unwrap();
            let ast = ast.transform_ast().unwrap();
            assert_eq!(expected, ast, "{query}");
        }
    }

    #[test]
    fn invalid_ast() {
        let cases = [
            (r#"a b (c"#, "Unmatched parentheses"),
            (r#"a b) c"#, "Unmatched close parentheses"),
            (r#"a +-b"#, "unary operators should not be adjacent"),
        ];

        for (query, expected) in cases {
            let result: Result<()> = try {
                let parser = ParserContext { stack: vec![] };
                let ast = parser.parse_pattern(query)?;
                let _ast = ast.transform_ast()?;
            };

            assert!(result.is_err(), "{query}");
            let actual_error = result.unwrap_err().to_string();
            assert!(actual_error.contains(expected), "{query}, {actual_error}");
        }
    }

    #[test]
    fn valid_matches_parser() {
        let cases = [
            (
                "a AND b OR c",
                PatternAst::Binary {
                    op: BinaryOp::Or,
                    children: vec![
                        PatternAst::Binary {
                            op: BinaryOp::And,
                            children: vec![
                                PatternAst::Literal {
                                    op: UnaryOp::Optional,
                                    pattern: "a".to_string(),
                                },
                                PatternAst::Literal {
                                    op: UnaryOp::Optional,
                                    pattern: "b".to_string(),
                                },
                            ],
                        },
                        PatternAst::Literal {
                            op: UnaryOp::Optional,
                            pattern: "c".to_string(),
                        },
                    ],
                },
            ),
            (
                "(a AND b) OR c",
                PatternAst::Binary {
                    op: BinaryOp::Or,
                    children: vec![
                        PatternAst::Group {
                            op: UnaryOp::Optional,
                            child: Box::new(PatternAst::Binary {
                                op: BinaryOp::And,
                                children: vec![
                                    PatternAst::Literal {
                                        op: UnaryOp::Optional,
                                        pattern: "a".to_string(),
                                    },
                                    PatternAst::Literal {
                                        op: UnaryOp::Optional,
                                        pattern: "b".to_string(),
                                    },
                                ],
                            }),
                        },
                        PatternAst::Literal {
                            op: UnaryOp::Optional,
                            pattern: "c".to_string(),
                        },
                    ],
                },
            ),
            (
                "a AND (b OR c)",
                PatternAst::Binary {
                    op: BinaryOp::And,
                    children: vec![
                        PatternAst::Literal {
                            op: UnaryOp::Optional,
                            pattern: "a".to_string(),
                        },
                        PatternAst::Group {
                            op: UnaryOp::Optional,
                            child: Box::new(PatternAst::Binary {
                                op: BinaryOp::Or,
                                children: vec![
                                    PatternAst::Literal {
                                        op: UnaryOp::Optional,
                                        pattern: "b".to_string(),
                                    },
                                    PatternAst::Literal {
                                        op: UnaryOp::Optional,
                                        pattern: "c".to_string(),
                                    },
                                ],
                            }),
                        },
                    ],
                },
            ),
            (
                "a +b -c",
                PatternAst::Binary {
                    op: BinaryOp::Or,
                    children: vec![
                        PatternAst::Literal {
                            op: UnaryOp::Optional,
                            pattern: "a".to_string(),
                        },
                        PatternAst::Binary {
                            op: BinaryOp::Or,
                            children: vec![
                                PatternAst::Literal {
                                    op: UnaryOp::Must,
                                    pattern: "b".to_string(),
                                },
                                PatternAst::Literal {
                                    op: UnaryOp::Negative,
                                    pattern: "c".to_string(),
                                },
                            ],
                        },
                    ],
                },
            ),
            (
                "(+a +b) c",
                PatternAst::Binary {
                    op: BinaryOp::Or,
                    children: vec![
                        PatternAst::Group {
                            op: UnaryOp::Optional,
                            child: Box::new(PatternAst::Binary {
                                op: BinaryOp::Or,
                                children: vec![
                                    PatternAst::Literal {
                                        op: UnaryOp::Must,
                                        pattern: "a".to_string(),
                                    },
                                    PatternAst::Literal {
                                        op: UnaryOp::Must,
                                        pattern: "b".to_string(),
                                    },
                                ],
                            }),
                        },
                        PatternAst::Literal {
                            op: UnaryOp::Optional,
                            pattern: "c".to_string(),
                        },
                    ],
                },
            ),
            (
                "\"AND\" AnD \"OR\"",
                PatternAst::Binary {
                    op: BinaryOp::And,
                    children: vec![
                        PatternAst::Literal {
                            op: UnaryOp::Optional,
                            pattern: "AND".to_string(),
                        },
                        PatternAst::Literal {
                            op: UnaryOp::Optional,
                            pattern: "OR".to_string(),
                        },
                    ],
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
    fn evaluate_matches() {
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
            // basic cases
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
                "fox OR lazy",
                vec![true, true, true, true, true, true, true],
            ),
            (
                "fox AND lazy",
                vec![true, true, false, true, true, false, false],
            ),
            (
                "-over -lazy",
                vec![false, false, false, false, false, false, false],
            ),
            (
                "-over AND -lazy",
                vec![false, false, false, false, false, false, false],
            ),
            // priority between AND & OR
            (
                "fox AND jumps OR over",
                vec![true, true, true, true, true, true, true],
            ),
            (
                "fox OR brown AND quick",
                vec![true, true, true, true, true, true, true],
            ),
            (
                "(fox OR brown) AND quick",
                vec![true, false, true, true, true, true, true],
            ),
            (
                "brown AND quick OR fox",
                vec![true, true, true, true, true, true, true],
            ),
            (
                "brown AND (quick OR fox)",
                vec![true, false, true, true, true, true, true],
            ),
            (
                "brown AND quick AND fox  OR  jumps AND over AND lazy",
                vec![true, true, true, true, true, true, true],
            ),
            // optional & must conversion
            (
                "quick brown fox +jumps",
                vec![true, true, true, false, true, true, true],
            ),
            (
                "fox +jumps -over",
                vec![false, false, false, false, true, false, false],
            ),
            (
                "fox AND +jumps AND -over",
                vec![false, false, false, false, true, false, false],
            ),
            // weird parentheses cases
            (
                "(+fox +jumps) over",
                vec![true, true, true, true, true, true, true],
            ),
            (
                "+(fox jumps) AND over",
                vec![true, true, true, true, false, true, true],
            ),
            (
                "over -(fox jumps)",
                vec![false, false, false, false, false, false, false],
            ),
            (
                "over -(fox AND jumps)",
                vec![false, false, true, true, false, false, false],
            ),
            (
                "over AND -(-(fox OR jumps))",
                vec![true, true, true, true, false, true, true],
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
