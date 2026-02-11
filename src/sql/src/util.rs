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

use std::collections::HashSet;
use std::fmt::{Display, Formatter};

use itertools::Itertools;
use promql_parser::label::{METRIC_NAME, MatchOp};
use promql_parser::parser::{
    AggregateExpr as PromAggregateExpr, BinaryExpr as PromBinaryExpr, Call as PromCall,
    Expr as PromExpr, MatrixSelector as PromMatrixSelector, ParenExpr as PromParenExpr,
    SubqueryExpr as PromSubqueryExpr, UnaryExpr as PromUnaryExpr,
    VectorSelector as PromVectorSelector,
};
use serde::Serialize;
use snafu::ensure;
use sqlparser::ast::{
    Array, Expr, Ident, ObjectName, ObjectNamePart, SetExpr, SqlOption, StructField, TableFactor,
    Value, ValueWithSpan,
};
use sqlparser_derive::{Visit, VisitMut};

use crate::ast::ObjectNamePartExt;
use crate::error::{InvalidExprAsOptionValueSnafu, InvalidSqlSnafu, Result};
use crate::statements::create::SqlOrTql;
use crate::statements::tql::Tql;

const SCHEMA_MATCHER: &str = "__schema__";
const DATABASE_MATCHER: &str = "__database__";

/// Format an [ObjectName] without any quote of its idents.
pub fn format_raw_object_name(name: &ObjectName) -> String {
    struct Inner<'a> {
        name: &'a ObjectName,
    }

    impl Display for Inner<'_> {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            let mut delim = "";
            for ident in self.name.0.iter() {
                write!(f, "{delim}")?;
                delim = ".";
                write!(f, "{}", ident.to_string_unquoted())?;
            }
            Ok(())
        }
    }

    format!("{}", Inner { name })
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Visit, VisitMut)]
pub struct OptionValue(Expr);

impl OptionValue {
    pub(crate) fn try_new(expr: Expr) -> Result<Self> {
        ensure!(
            matches!(
                expr,
                Expr::Value(_) | Expr::Identifier(_) | Expr::Array(_) | Expr::Struct { .. }
            ),
            InvalidExprAsOptionValueSnafu {
                error: format!("{expr} not accepted")
            }
        );
        Ok(Self(expr))
    }

    fn expr_as_string(expr: &Expr) -> Option<&str> {
        match expr {
            Expr::Value(ValueWithSpan { value, .. }) => match value {
                Value::SingleQuotedString(s)
                | Value::DoubleQuotedString(s)
                | Value::TripleSingleQuotedString(s)
                | Value::TripleDoubleQuotedString(s)
                | Value::SingleQuotedByteStringLiteral(s)
                | Value::DoubleQuotedByteStringLiteral(s)
                | Value::TripleSingleQuotedByteStringLiteral(s)
                | Value::TripleDoubleQuotedByteStringLiteral(s)
                | Value::SingleQuotedRawStringLiteral(s)
                | Value::DoubleQuotedRawStringLiteral(s)
                | Value::TripleSingleQuotedRawStringLiteral(s)
                | Value::TripleDoubleQuotedRawStringLiteral(s)
                | Value::EscapedStringLiteral(s)
                | Value::UnicodeStringLiteral(s)
                | Value::NationalStringLiteral(s)
                | Value::HexStringLiteral(s) => Some(s),
                Value::DollarQuotedString(s) => Some(&s.value),
                Value::Number(s, _) => Some(s),
                Value::Boolean(b) => Some(if *b { "true" } else { "false" }),
                _ => None,
            },
            Expr::Identifier(ident) => Some(&ident.value),
            _ => None,
        }
    }

    /// Convert the option value to a string.
    ///
    /// Notes: Not all values can be converted to a string, refer to [Self::expr_as_string] for more details.
    pub fn as_string(&self) -> Option<&str> {
        Self::expr_as_string(&self.0)
    }

    pub fn as_list(&self) -> Option<Vec<&str>> {
        let expr = &self.0;
        match expr {
            Expr::Value(_) | Expr::Identifier(_) => self.as_string().map(|s| vec![s]),
            Expr::Array(array) => array
                .elem
                .iter()
                .map(Self::expr_as_string)
                .collect::<Option<Vec<_>>>(),
            _ => None,
        }
    }

    pub(crate) fn as_struct_fields(&self) -> Option<&[StructField]> {
        match &self.0 {
            Expr::Struct { fields, .. } => Some(fields),
            _ => None,
        }
    }
}

impl From<String> for OptionValue {
    fn from(value: String) -> Self {
        Self(Expr::Identifier(Ident::new(value)))
    }
}

impl From<&str> for OptionValue {
    fn from(value: &str) -> Self {
        Self(Expr::Identifier(Ident::new(value)))
    }
}

impl From<Vec<&str>> for OptionValue {
    fn from(value: Vec<&str>) -> Self {
        Self(Expr::Array(Array {
            elem: value
                .into_iter()
                .map(|x| Expr::Identifier(Ident::new(x)))
                .collect(),
            named: false,
        }))
    }
}

impl Display for OptionValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(s) = self.as_string() {
            write!(f, "'{s}'")
        } else if let Some(s) = self.as_list() {
            write!(
                f,
                "[{}]",
                s.into_iter().map(|x| format!("'{x}'")).join(", ")
            )
        } else {
            write!(f, "'{}'", self.0)
        }
    }
}

pub fn parse_option_string(option: SqlOption) -> Result<(String, OptionValue)> {
    let SqlOption::KeyValue { key, value } = option else {
        return InvalidSqlSnafu {
            msg: "Expecting a key-value pair in the option",
        }
        .fail();
    };
    let v = OptionValue::try_new(value)?;
    let k = key.value.to_lowercase();
    Ok((k, v))
}

/// Walk through a [Query] and extract all the tables referenced in it.
pub fn extract_tables_from_query(query: &SqlOrTql) -> impl Iterator<Item = ObjectName> {
    let mut names = HashSet::new();

    match query {
        SqlOrTql::Sql(query, _) => extract_tables_from_set_expr(&query.body, &mut names),
        SqlOrTql::Tql(tql, _) => extract_tables_from_tql(tql, &mut names),
    }

    names.into_iter()
}

fn extract_tables_from_tql(tql: &Tql, names: &mut HashSet<ObjectName>) {
    let promql = match tql {
        Tql::Eval(eval) => &eval.query,
        Tql::Explain(explain) => &explain.query,
        Tql::Analyze(analyze) => &analyze.query,
    };

    if let Ok(expr) = promql_parser::parser::parse(promql) {
        extract_tables_from_prom_expr(&expr, names);
    }
}

fn extract_tables_from_prom_expr(expr: &PromExpr, names: &mut HashSet<ObjectName>) {
    match expr {
        PromExpr::Aggregate(PromAggregateExpr { expr, .. }) => {
            extract_tables_from_prom_expr(expr, names);
        }
        PromExpr::Unary(PromUnaryExpr { expr, .. }) => {
            extract_tables_from_prom_expr(expr, names);
        }
        PromExpr::Binary(PromBinaryExpr { lhs, rhs, .. }) => {
            extract_tables_from_prom_expr(lhs, names);
            extract_tables_from_prom_expr(rhs, names);
        }
        PromExpr::Paren(PromParenExpr { expr }) => {
            extract_tables_from_prom_expr(expr, names);
        }
        PromExpr::Subquery(PromSubqueryExpr { expr, .. }) => {
            extract_tables_from_prom_expr(expr, names);
        }
        PromExpr::VectorSelector(selector) => {
            extract_metric_name_from_vector_selector(selector, names);
        }
        PromExpr::MatrixSelector(PromMatrixSelector { vs, .. }) => {
            extract_metric_name_from_vector_selector(vs, names);
        }
        PromExpr::Call(PromCall { args, .. }) => {
            for arg in &args.args {
                extract_tables_from_prom_expr(arg, names);
            }
        }
        PromExpr::NumberLiteral(_) | PromExpr::StringLiteral(_) | PromExpr::Extension(_) => {}
    }
}

fn extract_metric_name_from_vector_selector(
    selector: &PromVectorSelector,
    names: &mut HashSet<ObjectName>,
) {
    let metric_name = selector.name.clone().or_else(|| {
        let mut metric_name_matchers = selector.matchers.find_matchers(METRIC_NAME);
        if metric_name_matchers.len() == 1 && metric_name_matchers[0].op == MatchOp::Equal {
            metric_name_matchers.pop().map(|matcher| matcher.value)
        } else {
            None
        }
    });
    let Some(metric_name) = metric_name else {
        return;
    };

    let schema_matcher = selector.matchers.matchers.iter().rev().find(|matcher| {
        matcher.op == MatchOp::Equal
            && (matcher.name == SCHEMA_MATCHER || matcher.name == DATABASE_MATCHER)
    });

    if let Some(schema) = schema_matcher {
        names.insert(ObjectName(vec![
            ObjectNamePart::Identifier(Ident::new(&schema.value)),
            ObjectNamePart::Identifier(Ident::new(metric_name)),
        ]));
    } else {
        names.insert(ObjectName(vec![ObjectNamePart::Identifier(Ident::new(
            metric_name,
        ))]));
    }
}

/// translate the start location to the index in the sql string
pub fn location_to_index(sql: &str, location: &sqlparser::tokenizer::Location) -> usize {
    let mut index = 0;
    for (lno, line) in sql.lines().enumerate() {
        if lno + 1 == location.line as usize {
            index += location.column as usize;
            break;
        } else {
            index += line.len() + 1; // +1 for the newline
        }
    }
    // -1 because the index is 0-based
    // and the location is 1-based
    index - 1
}

/// Helper function for [extract_tables_from_query].
///
/// Handle [SetExpr].
fn extract_tables_from_set_expr(set_expr: &SetExpr, names: &mut HashSet<ObjectName>) {
    match set_expr {
        SetExpr::Select(select) => {
            for from in &select.from {
                table_factor_to_object_name(&from.relation, names);
                for join in &from.joins {
                    table_factor_to_object_name(&join.relation, names);
                }
            }
        }
        SetExpr::Query(query) => {
            extract_tables_from_set_expr(&query.body, names);
        }
        SetExpr::SetOperation { left, right, .. } => {
            extract_tables_from_set_expr(left, names);
            extract_tables_from_set_expr(right, names);
        }
        _ => {}
    };
}

/// Helper function for [extract_tables_from_query].
///
/// Handle [TableFactor].
fn table_factor_to_object_name(table_factor: &TableFactor, names: &mut HashSet<ObjectName>) {
    if let TableFactor::Table { name, .. } = table_factor {
        names.insert(name.to_owned());
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::tokenizer::Token;

    use super::*;
    use crate::dialect::GreptimeDbDialect;
    use crate::parser::{ParseOptions, ParserContext};
    use crate::statements::statement::Statement;

    #[test]
    fn test_location_to_index() {
        let testcases = vec![
            "SELECT * FROM t WHERE a = 1",
            // start or end with newline
            r"
SELECT *
FROM
t
WHERE a =
1
",
            r"SELECT *
FROM
t
WHERE a =
1
",
            r"
SELECT *
FROM
t
WHERE a =
1",
        ];

        for sql in testcases {
            let mut parser = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
            loop {
                let token = parser.parser.next_token();
                if token == Token::EOF {
                    break;
                }
                let span = token.span;
                let subslice =
                    &sql[location_to_index(sql, &span.start)..location_to_index(sql, &span.end)];
                assert_eq!(token.to_string(), subslice);
            }
        }
    }

    #[test]
    fn test_extract_tables_from_tql_query() {
        let testcases = vec![
            (
                r#"
CREATE FLOW calc_reqs SINK TO cnt_reqs AS
TQL EVAL (now() - '15s'::interval, now(), '5s') count_values("status_code", http_requests);"#,
                vec!["http_requests".to_string()],
            ),
            (
                r#"
CREATE FLOW calc_reqs SINK TO cnt_reqs AS
TQL EVAL (now() - '15s'::interval, now(), '5s') count_values("status_code", {__name__="http_requests"});"#,
                vec!["http_requests".to_string()],
            ),
        ];

        for (sql, expected_tables) in testcases {
            let mut stmts = ParserContext::create_with_dialect(
                sql,
                &GreptimeDbDialect {},
                ParseOptions::default(),
            )
            .unwrap();
            let Statement::CreateFlow(create_flow) = stmts.pop().unwrap() else {
                unreachable!()
            };

            let mut tables = extract_tables_from_query(&create_flow.query)
                .map(|table| format_raw_object_name(&table))
                .collect_vec();
            tables.sort();
            assert_eq!(expected_tables, tables);
        }
    }

    #[test]
    fn test_extract_tables_from_tql_query_with_schema_matcher() {
        let sql = r#"
CREATE FLOW calc_reqs SINK TO cnt_reqs AS
TQL EVAL (now() - '15s'::interval, now(), '5s') count_values("status_code", http_requests{__schema__="greptime_private"});"#;
        let mut stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        let Statement::CreateFlow(create_flow) = stmts.pop().unwrap() else {
            unreachable!()
        };

        let mut tables = extract_tables_from_query(&create_flow.query)
            .map(|table| format_raw_object_name(&table))
            .collect_vec();
        tables.sort();
        assert_eq!(vec!["greptime_private.http_requests".to_string()], tables);
    }
}
