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

use std::fmt::Display;

use serde::Serialize;
use sqlparser::ast::{Expr, ObjectName, ObjectNamePart, Value, ValueWithSpan};
use sqlparser_derive::{Visit, VisitMut};

/// SET variables statement.
#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub struct SetVariables {
    pub variable: ObjectName,
    pub value: Vec<Expr>,
}

impl SetVariables {
    /// Returns the schema candidates for a supported `search_path` assignment.
    pub fn search_path(&self) -> Option<Vec<String>> {
        let [ObjectNamePart::Identifier(variable)] = self.variable.0.as_slice() else {
            return None;
        };
        if !variable.value.eq_ignore_ascii_case("search_path") {
            return None;
        }

        if let [Expr::Identifier(value)] = self.value.as_slice()
            && value.quote_style.is_none()
            && value.value.eq_ignore_ascii_case("default")
        {
            return Some(vec!["$user".to_string(), "public".to_string()]);
        }

        self.value
            .iter()
            .map(|value| match value {
                Expr::Value(ValueWithSpan {
                    value: Value::SingleQuotedString(value) | Value::DoubleQuotedString(value),
                    ..
                }) => Some(value.clone()),
                Expr::Identifier(value) if value.quote_style.is_none() => {
                    Some(value.value.to_lowercase())
                }
                Expr::Identifier(value) => Some(value.value.clone()),
                _ => None,
            })
            .collect()
    }
}

impl Display for SetVariables {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let variable = &self.variable;
        let value = &self
            .value
            .iter()
            .map(|expr| format!("{}", expr))
            .collect::<Vec<_>>()
            .join(", ");

        write!(f, r#"SET {variable} = {value}"#)
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use crate::dialect::GreptimeDbDialect;
    use crate::parser::{ParseOptions, ParserContext};
    use crate::statements::statement::Statement;

    #[test]
    fn test_display_show_variables() {
        let sql = r"set delayed_insert_timeout=300;";
        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::SetVariables { .. });

        match &stmts[0] {
            Statement::SetVariables(set) => {
                let new_sql = format!("\n{}", set);
                assert_eq!(
                    r#"
SET delayed_insert_timeout = 300"#,
                    &new_sql
                );
            }
            _ => {
                unreachable!();
            }
        }
    }

    #[test]
    fn test_search_path() {
        for (sql, expected) in [
            ("SET search_path TO public", Some(vec!["public"])),
            ("SET search_path TO DEFAULT", Some(vec!["$user", "public"])),
            (r#"SET search_path TO "default""#, Some(vec!["default"])),
            ("SET search_path TO 'default'", Some(vec!["default"])),
            ("SET SEARCH_PATH TO 'private'", Some(vec!["private"])),
            ("SET search_path TO \"quoted\"", Some(vec!["quoted"])),
            ("SET \"search_path\" TO private", Some(vec!["private"])),
            ("SET timezone TO 'UTC'", None),
            (
                r#"SET search_path TO "$user", MISSING, "Mixed", 'a,b'"#,
                Some(vec!["$user", "missing", "Mixed", "a,b"]),
            ),
            ("SET search_path TO ''", Some(vec![""])),
            ("SET search_path TO public, 42", None),
        ] {
            let statements = ParserContext::create_with_dialect(
                sql,
                &GreptimeDbDialect {},
                ParseOptions::default(),
            )
            .unwrap();
            let [Statement::SetVariables(set)] = statements.as_slice() else {
                unreachable!()
            };
            assert_eq!(
                expected.map(|v| v.into_iter().map(str::to_owned).collect::<Vec<_>>()),
                set.search_path()
            );
        }
    }
}
