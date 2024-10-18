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

use snafu::ResultExt;
use sqlparser::ast::Statement as SpStatement;

use crate::ast::{Ident, ObjectName};
use crate::error::{self, Result};
use crate::parser::ParserContext;
use crate::statements::set_variables::SetVariables;
use crate::statements::statement::Statement;

/// SET variables statement parser implementation
impl ParserContext<'_> {
    pub(crate) fn parse_set_variables(&mut self) -> Result<Statement> {
        let _ = self.parser.next_token();
        let spstatement = self.parser.parse_set().context(error::SyntaxSnafu)?;
        match spstatement {
            SpStatement::SetVariable {
                variable,
                value,
                hivevar,
                ..
            } if !hivevar => Ok(Statement::SetVariables(SetVariables { variable, value })),

            SpStatement::SetTimeZone { value, .. } => Ok(Statement::SetVariables(SetVariables {
                variable: ObjectName(vec![Ident {
                    value: "TIMEZONE".to_string(),
                    quote_style: None,
                }]),
                value: vec![value],
            })),

            unexp => error::UnsupportedSnafu {
                keyword: unexp.to_string(),
            }
            .fail(),
        }
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::{Expr, Ident, ObjectName, Value};

    use super::*;
    use crate::dialect::GreptimeDbDialect;
    use crate::parser::ParseOptions;

    fn assert_mysql_parse_result(sql: &str) {
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let mut stmts = result.unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::SetVariables(SetVariables {
                variable: ObjectName(vec![Ident::new("time_zone")]),
                value: vec![Expr::Value(Value::SingleQuotedString("UTC".to_string()))]
            })
        );
    }

    fn assert_pg_parse_result(sql: &str) {
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let mut stmts = result.unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::SetVariables(SetVariables {
                variable: ObjectName(vec![Ident::new("TIMEZONE")]),
                value: vec![Expr::Value(Value::SingleQuotedString("UTC".to_string()))],
            })
        );
    }

    #[test]
    pub fn test_set_timezone() {
        // mysql style
        let sql = "SET time_zone = 'UTC'";
        assert_mysql_parse_result(sql);
        // session or local style
        let sql = "SET LOCAL time_zone = 'UTC'";
        assert_mysql_parse_result(sql);
        let sql = "SET SESSION time_zone = 'UTC'";
        assert_mysql_parse_result(sql);

        // postgresql style
        let sql = "SET TIMEZONE TO 'UTC'";
        assert_pg_parse_result(sql);
        let sql = "SET TIMEZONE 'UTC'";
        assert_pg_parse_result(sql);
    }
}
