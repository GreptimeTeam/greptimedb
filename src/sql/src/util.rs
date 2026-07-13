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
use std::ops::ControlFlow;

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
    Array, Expr, Ident, ObjectName, ObjectNamePart, Query as SqlQuery, SetExpr, SqlOption,
    TableFactor, TableWithJoins, Value, ValueWithSpan, Visit as AstVisit, Visitor,
};
use sqlparser_derive::{Visit, VisitMut};

use crate::ast::ObjectNamePartExt;
use crate::error::{InvalidExprAsOptionValueSnafu, InvalidSqlSnafu, Result};
use crate::parser::ParserContext;
use crate::parsers::with_tql_parser::CteContent;
use crate::statements::create::SqlOrTql;
use crate::statements::query::Query;
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
    extract_tables_from_query_inner(query).0.into_iter()
}

/// Walk through a [Query] and extract its referenced tables, returning `None`
/// if any table reference cannot be resolved statically.
pub fn extract_tables_from_query_checked(
    query: &SqlOrTql,
) -> Option<impl Iterator<Item = ObjectName>> {
    let (names, complete) = extract_tables_from_query_inner(query);
    complete.then_some(names.into_iter())
}

fn extract_tables_from_query_inner(query: &SqlOrTql) -> (HashSet<ObjectName>, bool) {
    let mut names = HashSet::new();

    let complete = match query {
        SqlOrTql::Sql(query, _) => {
            extract_tables_from_sql_query(&query.inner, &mut names);
            extract_tables_from_hybrid_cte_query(query, &mut names)
        }
        SqlOrTql::Tql(tql, _) => extract_tables_from_tql(tql, &mut names),
    };

    (names, complete)
}

fn extract_tables_from_hybrid_cte_query(
    query: &Query,
    sql_names: &mut HashSet<ObjectName>,
) -> bool {
    let Some(hybrid_cte) = &query.hybrid_cte else {
        return true;
    };

    let mut complete = true;
    let cte_names: HashSet<String> = hybrid_cte
        .cte_tables
        .iter()
        .map(|cte| ParserContext::canonicalize_identifier(cte.name.clone()).value)
        .collect();
    remove_cte_names(sql_names, &cte_names);

    for cte in &hybrid_cte.cte_tables {
        let mut cte_query_names = HashSet::new();
        match &cte.content {
            CteContent::Sql(cte_query) => {
                extract_tables_from_sql_query(cte_query, &mut cte_query_names)
            }
            CteContent::Tql(tql) => complete &= extract_tables_from_tql(tql, &mut cte_query_names),
        }
        sql_names.extend(cte_query_names);
    }

    complete
}

fn remove_cte_names(names: &mut HashSet<ObjectName>, cte_names: &HashSet<String>) {
    if cte_names.is_empty() {
        return;
    }

    names.retain(|name| {
        if name.0.len() != 1 {
            return true;
        }
        let Some(ident) = name.0[0].as_ident() else {
            return true;
        };

        let canonical = ParserContext::canonicalize_identifier(ident.clone()).value;
        !cte_names.contains(&canonical)
    });
}

fn extract_tables_from_tql(tql: &Tql, names: &mut HashSet<ObjectName>) -> bool {
    let promql = match tql {
        Tql::Eval(eval) => &eval.query,
        Tql::Explain(explain) => &explain.query,
        Tql::Analyze(analyze) => &analyze.query,
    };

    let Ok(expr) = promql_parser::parser::parse(promql) else {
        return false;
    };
    extract_tables_from_prom_expr(&expr, names)
}

fn extract_tables_from_prom_expr(expr: &PromExpr, names: &mut HashSet<ObjectName>) -> bool {
    match expr {
        PromExpr::Aggregate(PromAggregateExpr { expr, .. }) => {
            extract_tables_from_prom_expr(expr, names)
        }
        PromExpr::Unary(PromUnaryExpr { expr, .. }) => extract_tables_from_prom_expr(expr, names),
        PromExpr::Binary(PromBinaryExpr { lhs, rhs, .. }) => {
            extract_tables_from_prom_expr(lhs, names) & extract_tables_from_prom_expr(rhs, names)
        }
        PromExpr::Paren(PromParenExpr { expr }) => extract_tables_from_prom_expr(expr, names),
        PromExpr::Subquery(PromSubqueryExpr { expr, .. }) => {
            extract_tables_from_prom_expr(expr, names)
        }
        PromExpr::VectorSelector(selector) => {
            extract_metric_name_from_vector_selector(selector, names)
        }
        PromExpr::MatrixSelector(PromMatrixSelector { vs, .. }) => {
            extract_metric_name_from_vector_selector(vs, names)
        }
        PromExpr::Call(PromCall { args, .. }) => {
            let mut complete = true;
            for arg in &args.args {
                complete &= extract_tables_from_prom_expr(arg, names);
            }
            complete
        }
        PromExpr::NumberLiteral(_) | PromExpr::StringLiteral(_) => true,
        PromExpr::Extension(_) => false,
    }
}

fn extract_metric_name_from_vector_selector(
    selector: &PromVectorSelector,
    names: &mut HashSet<ObjectName>,
) -> bool {
    let metric_name = selector.name.clone().or_else(|| {
        let mut metric_name_matchers = selector.matchers.find_matchers(METRIC_NAME);
        if metric_name_matchers.len() == 1 && metric_name_matchers[0].op == MatchOp::Equal {
            metric_name_matchers.pop().map(|matcher| matcher.value)
        } else {
            None
        }
    });
    let Some(metric_name) = metric_name else {
        return false;
    };

    if selector.matchers.matchers.iter().any(|matcher| {
        (matcher.name == SCHEMA_MATCHER || matcher.name == DATABASE_MATCHER)
            && matcher.op != MatchOp::Equal
    }) {
        return false;
    }

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
    true
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
/// Handle [sqlparser::ast::Query].
fn extract_tables_from_sql_query(query: &sqlparser::ast::Query, names: &mut HashSet<ObjectName>) {
    extract_tables_from_sql_query_inner(query, names, &mut HashSet::new());
}

fn extract_tables_from_sql_query_inner(
    query: &SqlQuery,
    names: &mut HashSet<ObjectName>,
    visited: &mut HashSet<*const SqlQuery>,
) {
    if !visited.insert(query) {
        return;
    }

    let mut cte_names = HashSet::new();
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            let cte_name = ParserContext::canonicalize_identifier(cte.alias.name.clone()).value;
            let mut cte_query_names = HashSet::new();
            extract_tables_from_sql_query_inner(&cte.query, &mut cte_query_names, visited);
            if with.recursive {
                cte_names.insert(cte_name.clone());
            }
            remove_cte_names(&mut cte_query_names, &cte_names);
            names.extend(cte_query_names);
            if !with.recursive {
                cte_names.insert(cte_name);
            }
        }
    }

    let mut body_names = HashSet::new();
    extract_tables_from_set_expr(&query.body, &mut body_names, visited);
    extract_tables_from_expression_subqueries(query, &mut body_names, visited);
    remove_cte_names(&mut body_names, &cte_names);
    names.extend(body_names);
}

fn extract_tables_from_expression_subqueries(
    query: &SqlQuery,
    names: &mut HashSet<ObjectName>,
    visited: &mut HashSet<*const SqlQuery>,
) {
    struct SubqueryCollector<'a, 'b> {
        names: &'a mut HashSet<ObjectName>,
        visited: &'b mut HashSet<*const SqlQuery>,
        depth: usize,
    }

    impl Visitor for SubqueryCollector<'_, '_> {
        type Break = ();

        fn pre_visit_query(&mut self, query: &SqlQuery) -> ControlFlow<Self::Break> {
            if self.depth == 1 {
                extract_tables_from_sql_query_inner(query, self.names, self.visited);
            }
            self.depth += 1;
            ControlFlow::Continue(())
        }

        fn post_visit_query(&mut self, _query: &SqlQuery) -> ControlFlow<Self::Break> {
            self.depth -= 1;
            ControlFlow::Continue(())
        }
    }

    let _ = query.visit(&mut SubqueryCollector {
        names,
        visited,
        depth: 0,
    });
}

/// Helper function for [extract_tables_from_query].
///
/// Handle [SetExpr].
fn extract_tables_from_set_expr(
    set_expr: &SetExpr,
    names: &mut HashSet<ObjectName>,
    visited: &mut HashSet<*const SqlQuery>,
) {
    match set_expr {
        SetExpr::Select(select) => {
            for from in &select.from {
                extract_tables_from_table_with_joins(from, names, visited);
            }
        }
        SetExpr::Query(query) => {
            extract_tables_from_sql_query_inner(query, names, visited);
        }
        SetExpr::SetOperation { left, right, .. } => {
            extract_tables_from_set_expr(left, names, visited);
            extract_tables_from_set_expr(right, names, visited);
        }
        _ => {}
    };
}

/// Helper function for [extract_tables_from_query].
///
/// Handle [TableWithJoins].
fn extract_tables_from_table_with_joins(
    table_with_joins: &TableWithJoins,
    names: &mut HashSet<ObjectName>,
    visited: &mut HashSet<*const SqlQuery>,
) {
    table_factor_to_object_name(&table_with_joins.relation, names, visited);
    for join in &table_with_joins.joins {
        table_factor_to_object_name(&join.relation, names, visited);
    }
}

/// Helper function for [extract_tables_from_query].
///
/// Handle [TableFactor].
fn table_factor_to_object_name(
    table_factor: &TableFactor,
    names: &mut HashSet<ObjectName>,
    visited: &mut HashSet<*const SqlQuery>,
) {
    match table_factor {
        TableFactor::Table { name, .. } => {
            names.insert(name.to_owned());
        }
        TableFactor::Derived { subquery, .. } => {
            extract_tables_from_sql_query_inner(subquery, names, visited);
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            extract_tables_from_table_with_joins(table_with_joins, names, visited);
        }
        TableFactor::Pivot { table, .. }
        | TableFactor::Unpivot { table, .. }
        | TableFactor::MatchRecognize { table, .. } => {
            table_factor_to_object_name(table, names, visited);
        }
        TableFactor::TableFunction { .. }
        | TableFactor::Function { .. }
        | TableFactor::UNNEST { .. }
        | TableFactor::JsonTable { .. }
        | TableFactor::OpenJsonTable { .. }
        | TableFactor::XmlTable { .. }
        | TableFactor::SemanticView { .. } => {}
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
    fn test_extract_tables_from_tql_query_completeness() {
        let testcases = [
            ("cpu", true, vec!["cpu"]),
            (r#"{__name__="cpu"}"#, true, vec!["cpu"]),
            (r#"cpu + {job="api"}"#, false, vec!["cpu"]),
            (r#"{__name__=~"cpu.*"}"#, false, vec![]),
            (r#"cpu{__schema__=~"private.*"}"#, false, vec![]),
        ];

        for (promql, expected_complete, expected_tables) in testcases {
            let sql = format!("TQL EVAL (0, 10, '5s') {promql}");
            let mut stmts = ParserContext::create_with_dialect(
                &sql,
                &GreptimeDbDialect {},
                ParseOptions::default(),
            )
            .unwrap();
            let Statement::Tql(tql) = stmts.pop().unwrap() else {
                unreachable!()
            };

            let query = SqlOrTql::Tql(tql, sql);
            let (tables, complete) = extract_tables_from_query_inner(&query);
            let mut tables = tables
                .into_iter()
                .map(|table| format_raw_object_name(&table))
                .collect_vec();
            tables.sort();
            assert_eq!(expected_complete, complete, "{promql}");
            assert_eq!(expected_tables, tables, "{promql}");
            assert_eq!(
                expected_complete,
                extract_tables_from_query_checked(&query).is_some(),
                "{promql}"
            );
        }
    }

    #[test]
    fn test_extract_tables_from_chained_tql_ctes() {
        let sql = "WITH denied AS (TQL EVAL (0, 10, '5s') allowed), \
                   leak AS (TQL EVAL (0, 10, '5s') denied) \
                   SELECT * FROM leak";
        let mut stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        let Statement::Query(query) = stmts.pop().unwrap() else {
            unreachable!()
        };

        let mut tables = extract_tables_from_query_checked(&SqlOrTql::Sql(*query, sql.to_string()))
            .unwrap()
            .map(|table| format_raw_object_name(&table))
            .collect_vec();
        tables.sort();
        assert_eq!(vec!["allowed".to_string(), "denied".to_string()], tables);
    }

    #[test]
    fn test_extract_tables_from_sql_query_with_derived_join() {
        let sql = r#"
CREATE FLOW flow_batch_join_subquery SINK TO flow_batch_join_sink
EVAL INTERVAL '1m' AS
SELECT a.symbol, b.mark_price
FROM (
    SELECT inst_id AS symbol, max(ts) AS mark_iv_ts
    FROM flow_batch_join_opt_summary
    GROUP BY inst_id
) a
LEFT JOIN (
    SELECT symbol, max(mark_price) AS mark_price
    FROM flow_batch_join_market_v5
    WHERE "type" = 'OPTION_MARK'
    GROUP BY symbol
) b ON a.symbol = b.symbol;
"#;
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
        assert_eq!(
            vec![
                "flow_batch_join_market_v5".to_string(),
                "flow_batch_join_opt_summary".to_string(),
            ],
            tables
        );
    }

    #[test]
    fn test_extract_tables_from_deeply_nested_derived_queries() {
        let mut sql = "SELECT * FROM physical_source".to_string();
        for depth in 0..20 {
            sql = format!("SELECT * FROM ({sql}) nested_{depth}");
        }

        let mut stmts = ParserContext::create_with_dialect(
            &sql,
            &GreptimeDbDialect {},
            ParseOptions::default(),
        )
        .unwrap();
        let Statement::Query(query) = stmts.pop().unwrap() else {
            unreachable!()
        };

        let tables = extract_tables_from_query(&SqlOrTql::Sql(*query, sql))
            .map(|table| format_raw_object_name(&table))
            .collect_vec();
        assert_eq!(vec!["physical_source"], tables);
    }

    #[test]
    fn test_extract_tables_from_sql_query_with_expression_subqueries() {
        let sql = r#"
SELECT
    (SELECT max(value) FROM scalar_source)
FROM outer_source
WHERE EXISTS (SELECT 1 FROM exists_source)
ORDER BY (SELECT max(value) FROM order_source);
"#;
        let mut stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        let Statement::Query(query) = stmts.pop().unwrap() else {
            unreachable!()
        };

        let mut tables = extract_tables_from_query(&SqlOrTql::Sql(*query, sql.to_string()))
            .map(|table| format_raw_object_name(&table))
            .collect_vec();
        tables.sort();
        assert_eq!(
            vec![
                "exists_source".to_string(),
                "order_source".to_string(),
                "outer_source".to_string(),
                "scalar_source".to_string(),
            ],
            tables
        );
    }

    #[test]
    fn test_extract_tables_from_sql_query_with_cte_scopes() {
        let testcases = vec![
            (
                r#"
WITH source AS (
    SELECT * FROM source
)
SELECT * FROM source;
"#,
                vec!["source".to_string()],
            ),
            (
                r#"
WITH first_cte AS (
    SELECT * FROM physical_source
), second_cte AS (
    SELECT * FROM first_cte
)
SELECT * FROM second_cte;
"#,
                vec!["physical_source".to_string()],
            ),
            (
                r#"
SELECT * FROM (
    WITH nested_cte AS (SELECT * FROM nested_source)
    SELECT * FROM nested_cte
);
"#,
                vec!["nested_source".to_string()],
            ),
            (
                r#"
SELECT * FROM (
    WITH nested_cte AS (SELECT * FROM nested_source)
    SELECT (SELECT * FROM nested_cte)
);
"#,
                vec!["nested_source".to_string()],
            ),
        ];

        for (sql, expected_tables) in testcases {
            let mut stmts = ParserContext::create_with_dialect(
                sql,
                &GreptimeDbDialect {},
                ParseOptions::default(),
            )
            .unwrap();
            let Statement::Query(query) = stmts.pop().unwrap() else {
                unreachable!()
            };

            let mut tables = HashSet::new();
            extract_tables_from_sql_query(&query.inner, &mut tables);
            let mut tables = tables
                .into_iter()
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
