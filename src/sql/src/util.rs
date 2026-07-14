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
    Expr as PromExpr, MatrixSelector as PromMatrixSelector, VectorSelector as PromVectorSelector,
};
use promql_parser::util::{ExprVisitor, walk_expr};
use serde::Serialize;
use snafu::ensure;
use sqlparser::ast::{
    Array, Expr, Ident, ObjectName, ObjectNamePart, SqlOption, Value, ValueWithSpan,
    Visit as AstVisit, Visitor,
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
    struct TableCollector<'a> {
        names: &'a mut HashSet<ObjectName>,
        complete: bool,
    }

    impl ExprVisitor for TableCollector<'_> {
        type Error = ();

        fn pre_visit(&mut self, expr: &PromExpr) -> std::result::Result<bool, Self::Error> {
            self.complete &= match expr {
                PromExpr::VectorSelector(selector) => {
                    extract_metric_name_from_vector_selector(selector, self.names)
                }
                PromExpr::MatrixSelector(PromMatrixSelector { vs, .. }) => {
                    extract_metric_name_from_vector_selector(vs, self.names)
                }
                PromExpr::Extension(_) => false,
                _ => true,
            };
            Ok(true)
        }
    }

    let mut collector = TableCollector {
        names,
        complete: true,
    };
    let _ = walk_expr(&mut collector, expr);
    collector.complete
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

// Tracks CTE aliases as sqlparser's visitor enters their query bodies.
struct QueryScope {
    cte_names: Vec<String>,
    next_cte: usize,
    recursive: bool,
    scope_start: usize,
    add_to_parent_after: Option<String>,
}

struct RelationCollector<'a> {
    names: &'a mut HashSet<ObjectName>,
    ctes_in_scope: Vec<String>,
    query_scopes: Vec<QueryScope>,
    #[cfg(test)]
    query_visits: usize,
}

impl<'a> RelationCollector<'a> {
    fn new(names: &'a mut HashSet<ObjectName>) -> Self {
        Self {
            names,
            ctes_in_scope: Vec::new(),
            query_scopes: Vec::new(),
            #[cfg(test)]
            query_visits: 0,
        }
    }
}

impl Visitor for RelationCollector<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, query: &sqlparser::ast::Query) -> ControlFlow<Self::Break> {
        #[cfg(test)]
        {
            self.query_visits += 1;
        }

        // Query::visit enters WITH definitions in declaration order before its body.
        let parent_cte = self.query_scopes.last_mut().and_then(|scope| {
            let cte_name = scope.cte_names.get(scope.next_cte)?.clone();
            scope.next_cte += 1;
            Some((cte_name, scope.recursive))
        });

        let add_to_parent_after = match parent_cte {
            Some((cte_name, true)) => {
                self.ctes_in_scope.push(cte_name);
                None
            }
            Some((cte_name, false)) => Some(cte_name),
            None => None,
        };

        let scope_start = self.ctes_in_scope.len();
        let (cte_names, recursive) = if let Some(with) = &query.with {
            (
                with.cte_tables
                    .iter()
                    .map(|cte| ParserContext::canonicalize_identifier(cte.alias.name.clone()).value)
                    .collect(),
                with.recursive,
            )
        } else {
            (Vec::new(), false)
        };

        self.query_scopes.push(QueryScope {
            cte_names,
            next_cte: 0,
            recursive,
            scope_start,
            add_to_parent_after,
        });
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &sqlparser::ast::Query) -> ControlFlow<Self::Break> {
        let Some(scope) = self.query_scopes.pop() else {
            return ControlFlow::Break(());
        };
        self.ctes_in_scope.truncate(scope.scope_start);
        if let Some(cte_name) = scope.add_to_parent_after {
            self.ctes_in_scope.push(cte_name);
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<Self::Break> {
        let is_cte = matches!(
            relation.0.as_slice(),
            [part]
                if part.as_ident().is_some_and(|ident| {
                    self.ctes_in_scope.contains(
                        &ParserContext::canonicalize_identifier(ident.clone()).value,
                    )
                })
        );
        if !is_cte {
            self.names.insert(relation.clone());
        }
        ControlFlow::Continue(())
    }
}

/// Helper function for [extract_tables_from_query].
///
/// Handle [sqlparser::ast::Query].
fn extract_tables_from_sql_query(query: &sqlparser::ast::Query, names: &mut HashSet<ObjectName>) {
    let _ = query.visit(&mut RelationCollector::new(names));
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
WITH RECURSIVE source AS (
    SELECT * FROM source
)
SELECT * FROM source;
"#,
                vec![],
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
    fn test_extract_tables_from_deeply_nested_ctes_visits_each_query_once() {
        let mut sql = "SELECT * FROM physical_source".to_string();
        for depth in 0..20 {
            sql = format!("WITH cte_{depth} AS ({sql}) SELECT * FROM cte_{depth}");
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

        let mut tables = HashSet::new();
        let query_visits = {
            let mut collector = RelationCollector::new(&mut tables);
            let _ = query.inner.visit(&mut collector);
            collector.query_visits
        };

        assert_eq!(21, query_visits);
        assert_eq!(
            vec!["physical_source"],
            tables
                .into_iter()
                .map(|table| format_raw_object_name(&table))
                .collect_vec()
        );
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
