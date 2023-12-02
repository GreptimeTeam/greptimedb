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

use snafu::{ensure, ResultExt};
use sqlparser::keywords::Keyword;

use crate::error::{self, InvalidTableNameSnafu, Result};
use crate::parser::ParserContext;
use crate::statements::describe::DescribeTable;
use crate::statements::statement::Statement;

/// DESCRIBE statement parser implementation
impl<'a> ParserContext<'a> {
    pub(crate) fn parse_describe(&mut self) -> Result<Statement> {
        if self.matches_keyword(Keyword::TABLE) {
            let _ = self.parser.next_token();
        }
        self.parse_describe_table()
    }

    fn parse_describe_table(&mut self) -> Result<Statement> {
        let raw_table_idents =
            self.parser
                .parse_object_name()
                .with_context(|_| error::UnexpectedSnafu {
                    sql: self.sql,
                    expected: "a table name",
                    actual: self.peek_token_as_string(),
                })?;
        let table_idents = Self::canonicalize_object_name(raw_table_idents);
        ensure!(
            !table_idents.0.is_empty(),
            InvalidTableNameSnafu {
                name: table_idents.to_string(),
            }
        );
        Ok(Statement::DescribeTable(DescribeTable::new(table_idents)))
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::Expr;

    use super::*;
    use crate::dialect::GreptimeDbDialect;

    #[test]
    fn test_parse_function() {
        let expr =
            ParserContext::parse_function("current_timestamp()", &GreptimeDbDialect {}).unwrap();
        assert!(matches!(expr, Expr::Function(_)));
    }

    fn assert_describe_table(sql: &str) {
        let stmt = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {})
            .unwrap()
            .pop()
            .unwrap();
        assert!(matches!(stmt, Statement::DescribeTable(_)))
    }

    #[test]
    fn test_parse_describe_table() {
        assert_describe_table("desc table t;");
        assert_describe_table("describe table t;");
        assert_describe_table("desc t;");
        assert_describe_table("describe t;");
    }
}
