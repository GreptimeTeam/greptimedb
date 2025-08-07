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

use sqlparser::ast::{Expr, ObjectName, SetExpr, SqlOption, TableFactor, Value, ValueWithSpan};

use crate::ast::ObjectNamePartExt;
use crate::error::{InvalidSqlSnafu, InvalidTableOptionValueSnafu, Result};
use crate::statements::create::SqlOrTql;

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

pub fn parse_option_string(option: SqlOption) -> Result<(String, String)> {
    let SqlOption::KeyValue { key, value } = option else {
        return InvalidSqlSnafu {
            msg: "Expecting a key-value pair in the option",
        }
        .fail();
    };
    let v = match value {
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(v),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::DoubleQuotedString(v),
            ..
        }) => v,
        Expr::Identifier(v) => v.value,
        Expr::Value(ValueWithSpan {
            value: Value::Number(v, _),
            ..
        }) => v.to_string(),
        value => return InvalidTableOptionValueSnafu { key, value }.fail(),
    };
    let k = key.value.to_lowercase();
    Ok((k, v))
}

/// Walk through a [Query] and extract all the tables referenced in it.
pub fn extract_tables_from_query(query: &SqlOrTql) -> impl Iterator<Item = ObjectName> {
    let mut names = HashSet::new();

    match query {
        SqlOrTql::Sql(query, _) => extract_tables_from_set_expr(&query.body, &mut names),
        SqlOrTql::Tql(_tql, _) => {
            // since tql have sliding time window, so we don't need to extract tables from it
            // (because we are going to eval it fully anyway)
        }
    }

    names.into_iter()
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
        SetExpr::Values(_) | SetExpr::Insert(_) | SetExpr::Update(_) | SetExpr::Table(_) => {}
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
    use crate::parser::ParserContext;

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
}
