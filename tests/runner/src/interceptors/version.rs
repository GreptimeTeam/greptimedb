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

use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case, take_while1};
use nom::character::complete::multispace0;
use nom::combinator::{all_consuming, map};
use nom::error::Error;
use nom::multi::fold_many0;
use nom::sequence::{delimited, preceded};
use nom::{IResult, Parser as NomParser};
use sqlness::interceptor::{Interceptor, InterceptorFactory, InterceptorRef};
use sqlness::{SKIP_MARKER_PREFIX, SqlnessError};

use crate::version::Version;

pub const PREFIX: &str = "VERSION";

pub struct VersionInterceptor {
    target_version: Version,
    expression: Expression,
    raw_expression: String,
}

impl VersionInterceptor {
    pub fn new(target_version: Version, expression: Expression, raw_expression: String) -> Self {
        Self {
            target_version,
            expression,
            raw_expression,
        }
    }

    fn skip_reason(&self) -> String {
        format!(
            "target version {} does not satisfy expression: {}",
            self.target_version, self.raw_expression
        )
    }

    fn maybe_rewrite_to_skip_sql(&self, sql: &mut Vec<String>) -> bool {
        if self.expression.eval(&self.target_version) {
            return false;
        }

        let skip_marker = format!("{} {}", SKIP_MARKER_PREFIX, self.skip_reason());
        sql.clear();
        sql.push(format!("SELECT '{}';", skip_marker));
        true
    }

    fn normalize_skip_result(&self, result: &mut String) {
        let reason = self.skip_reason();
        if result.contains(SKIP_MARKER_PREFIX) && result.contains(reason.as_str()) {
            *result = format!("{} {}", SKIP_MARKER_PREFIX, self.skip_reason());
        }
    }
}

impl Interceptor for VersionInterceptor {
    fn before_execute(&self, sql: &mut Vec<String>, _ctx: &mut sqlness::QueryContext) {
        self.maybe_rewrite_to_skip_sql(sql);
    }

    fn after_execute(&self, result: &mut String) {
        self.normalize_skip_result(result);
    }
}

pub struct VersionInterceptorFactory {
    target_version: Version,
}

impl VersionInterceptorFactory {
    pub fn new(target_version: Version) -> Self {
        Self { target_version }
    }
}

impl InterceptorFactory for VersionInterceptorFactory {
    fn try_new(&self, ctx: &str) -> Result<InterceptorRef, SqlnessError> {
        let raw_expression = ctx.trim();
        if raw_expression.is_empty() {
            return Err(SqlnessError::InvalidContext {
                prefix: PREFIX.to_string(),
                msg: "Expression cannot be empty".to_string(),
            });
        }

        let expression =
            Parser::parse(raw_expression).map_err(|e| SqlnessError::InvalidContext {
                prefix: PREFIX.to_string(),
                msg: e,
            })?;

        Ok(Box::new(VersionInterceptor::new(
            self.target_version.clone(),
            expression,
            raw_expression.to_string(),
        )))
    }
}

#[derive(Debug, Clone)]
pub enum Expression {
    Comparison(ComparisonOp, Version),
    And(Box<Expression>, Box<Expression>),
    Or(Box<Expression>, Box<Expression>),
    Not(Box<Expression>),
}

impl Expression {
    fn eval(&self, target: &Version) -> bool {
        match self {
            Expression::Comparison(op, rhs) => op.eval(target, rhs),
            Expression::And(lhs, rhs) => lhs.eval(target) && rhs.eval(target),
            Expression::Or(lhs, rhs) => lhs.eval(target) || rhs.eval(target),
            Expression::Not(inner) => !inner.eval(target),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ComparisonOp {
    Lt,
    Le,
    Gt,
    Ge,
    Eq,
    Ne,
}

impl ComparisonOp {
    fn eval(&self, lhs: &Version, rhs: &Version) -> bool {
        match self {
            ComparisonOp::Lt => lhs < rhs,
            ComparisonOp::Le => lhs <= rhs,
            ComparisonOp::Gt => lhs > rhs,
            ComparisonOp::Ge => lhs >= rhs,
            ComparisonOp::Eq => lhs == rhs,
            ComparisonOp::Ne => lhs != rhs,
        }
    }
}

struct Parser {
    _private: (),
}

impl Parser {
    fn parse(input: &str) -> Result<Expression, String> {
        match all_consuming(Self::ws(Self::parse_or_expr))(input) {
            Ok((_, expression)) => Ok(expression),
            Err(err) => Err(format!("Failed to parse VERSION expression: {err:?}")),
        }
    }

    fn ws<'a, O, F>(inner: F) -> impl NomParser<&'a str, O, Error<&'a str>>
    where
        F: NomParser<&'a str, O, Error<&'a str>>,
    {
        delimited(multispace0, inner, multispace0)
    }

    fn parse_or_expr(input: &str) -> IResult<&str, Expression> {
        let (input, left) = Self::parse_and_expr(input)?;
        fold_many0(
            preceded(Self::ws(tag_no_case("OR")), Self::parse_and_expr),
            move || left.clone(),
            |acc, rhs| Expression::Or(Box::new(acc), Box::new(rhs)),
        )
        .parse(input)
    }

    fn parse_and_expr(input: &str) -> IResult<&str, Expression> {
        let (input, left) = Self::parse_unary_expr(input)?;
        fold_many0(
            preceded(Self::ws(tag_no_case("AND")), Self::parse_unary_expr),
            move || left.clone(),
            |acc, rhs| Expression::And(Box::new(acc), Box::new(rhs)),
        )
        .parse(input)
    }

    fn parse_unary_expr(input: &str) -> IResult<&str, Expression> {
        let not_parser = map(
            preceded(Self::ws(tag_no_case("NOT")), Self::parse_unary_expr),
            |expr| Expression::Not(Box::new(expr)),
        );
        alt((not_parser, Self::parse_primary_expr)).parse(input)
    }

    fn parse_primary_expr(input: &str) -> IResult<&str, Expression> {
        alt((
            delimited(Self::ws(tag("(")), Self::parse_or_expr, Self::ws(tag(")"))),
            Self::parse_comparison,
        ))
        .parse(input)
    }

    fn parse_comparison(input: &str) -> IResult<&str, Expression> {
        let (input, identifier) = Self::ws(Self::parse_identifier).parse(input)?;
        if !identifier.eq_ignore_ascii_case("version") {
            return Err(nom::Err::Failure(Error::new(
                input,
                nom::error::ErrorKind::Tag,
            )));
        }

        let (input, op) = Self::ws(Self::parse_comparison_op).parse(input)?;
        let (input, version_token) = Self::ws(Self::parse_version_literal).parse(input)?;
        let version = Version::parse(version_token)
            .map_err(|_| nom::Err::Failure(Error::new(input, nom::error::ErrorKind::Fail)))?;

        Ok((input, Expression::Comparison(op, version)))
    }

    fn parse_identifier(input: &str) -> IResult<&str, &str> {
        take_while1(|c: char| c.is_ascii_alphabetic() || c == '_').parse(input)
    }

    fn parse_version_literal(input: &str) -> IResult<&str, &str> {
        take_while1(|c: char| !c.is_whitespace() && c != ')').parse(input)
    }

    fn parse_comparison_op(input: &str) -> IResult<&str, ComparisonOp> {
        map(
            alt((
                tag(">="),
                tag("<="),
                tag("=="),
                tag("!="),
                tag(">"),
                tag("<"),
            )),
            |op| match op {
                ">=" => ComparisonOp::Ge,
                "<=" => ComparisonOp::Le,
                "==" => ComparisonOp::Eq,
                "!=" => ComparisonOp::Ne,
                ">" => ComparisonOp::Gt,
                "<" => ComparisonOp::Lt,
                _ => unreachable!(),
            },
        )
        .parse(input)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parser_allows_range_expression() {
        let expr = Parser::parse("version > 1.0.0 AND version < 1.1.0").unwrap();
        let target = Version::parse("1.0.5").unwrap();
        assert!(expr.eval(&target));

        let target = Version::parse("1.1.0").unwrap();
        assert!(!expr.eval(&target));
    }

    #[test]
    fn test_parser_honors_parentheses_and_not() {
        let expr = Parser::parse("NOT (version < 1.0.0 OR version >= 2.0.0)").unwrap();

        assert!(expr.eval(&Version::parse("1.5.0").unwrap()));
        assert!(!expr.eval(&Version::parse("0.9.9").unwrap()));
        assert!(!expr.eval(&Version::parse("2.0.0").unwrap()));
    }

    #[test]
    fn test_parser_supports_current_literal() {
        let expr = Parser::parse("version == current").unwrap();
        assert!(expr.eval(&Version::Current));
    }

    #[test]
    fn test_parser_rejects_invalid_identifier() {
        assert!(Parser::parse("target > 1.0.0").is_err());
    }

    #[test]
    fn test_parser_rejects_invalid_version_literal() {
        assert!(Parser::parse("version > nope").is_err());
    }

    #[test]
    fn test_before_execute_keeps_sql_when_expression_matches() {
        let expr = Parser::parse("version >= 0.15.0").unwrap();
        let interceptor = VersionInterceptor::new(
            Version::parse("0.15.0").unwrap(),
            expr,
            "version >= 0.15.0".to_string(),
        );

        let mut sql = vec!["SELECT 1;".to_string()];
        let skipped = interceptor.maybe_rewrite_to_skip_sql(&mut sql);

        assert!(!skipped);
        assert_eq!(sql, vec!["SELECT 1;"]);
    }

    #[test]
    fn test_before_execute_rewrites_sql_when_expression_not_match() {
        let expr = Parser::parse("version >= 0.16.0").unwrap();
        let interceptor = VersionInterceptor::new(
            Version::parse("0.15.0").unwrap(),
            expr,
            "version >= 0.16.0".to_string(),
        );

        let mut sql = vec!["SELECT 1;".to_string()];
        let skipped = interceptor.maybe_rewrite_to_skip_sql(&mut sql);

        assert!(skipped);
        assert_eq!(sql.len(), 1);
        assert!(sql[0].contains(SKIP_MARKER_PREFIX));
        assert!(
            sql[0].contains("target version 0.15.0 does not satisfy expression: version >= 0.16.0")
        );
    }

    #[test]
    fn test_after_execute_normalizes_skip_result_when_skipped() {
        let expr = Parser::parse("version >= 0.16.0").unwrap();
        let interceptor = VersionInterceptor::new(
            Version::parse("0.15.0").unwrap(),
            expr,
            "version >= 0.16.0".to_string(),
        );
        let mut result = format!(
            "+----------------+\n| {} {} |\n+----------------+",
            SKIP_MARKER_PREFIX,
            interceptor.skip_reason()
        );
        interceptor.after_execute(&mut result);

        assert_eq!(
            result,
            format!(
                "{} target version 0.15.0 does not satisfy expression: version >= 0.16.0",
                SKIP_MARKER_PREFIX
            )
        );
    }

    #[test]
    fn test_after_execute_does_not_rewrite_when_not_skipped() {
        let expr = Parser::parse("version <= 0.15.0").unwrap();
        let interceptor = VersionInterceptor::new(
            Version::parse("0.15.0").unwrap(),
            expr,
            "version <= 0.15.0".to_string(),
        );

        let expected = format!("{} some other reason", SKIP_MARKER_PREFIX);
        let mut result = expected.clone();
        interceptor.after_execute(&mut result);

        assert_eq!(result, expected);
    }

    #[test]
    fn test_factory_rejects_empty_expression() {
        let factory = VersionInterceptorFactory::new(Version::parse("0.15.0").unwrap());
        let err = match factory.try_new("  ") {
            Ok(_) => panic!("expected empty expression to fail"),
            Err(err) => err,
        };
        let msg = err.to_string();
        assert!(msg.contains("Expression cannot be empty"));
    }
}
