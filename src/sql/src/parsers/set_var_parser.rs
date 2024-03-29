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

use crate::error::{self, Result};
use crate::parser::ParserContext;
use crate::statements::set_variables::SetVariables;
use crate::statements::statement::Statement;

/// SET variables statement parser implementation
impl<'a> ParserContext<'a> {
    pub(crate) fn parse_set_variables(&mut self) -> Result<Statement> {
        let _ = self.parser.next_token();
        let spstatement = self.parser.parse_set().context(error::SyntaxSnafu)?;
        match spstatement {
            SpStatement::SetVariable {
                variable,
                value,
                local,
                hivevar,
            } if !local && !hivevar => {
                Ok(Statement::SetVariables(SetVariables { variable, value }))
            }
            unexp => error::UnsupportedSnafu {
                sql: self.sql.to_string(),
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

    #[test]
    pub fn test_set_timezone() {
        // mysql style
        let sql = "SET time_zone = 'UTC'";
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
        // postgresql style
        let sql = "SET TIMEZONE TO 'UTC'";
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
}
