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
use crate::statements::drop::DropTable;
use crate::statements::statement::Statement;

/// DROP statement parser implementation
impl<'a> ParserContext<'a> {
    pub(crate) fn parse_drop(&mut self) -> Result<Statement> {
        let _ = self.parser.next_token();
        if !self.matches_keyword(Keyword::TABLE) {
            return self.unsupported(self.peek_token_as_string());
        }
        let _ = self.parser.next_token();

        let if_exists = self.parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let raw_table_ident =
            self.parser
                .parse_object_name()
                .with_context(|_| error::UnexpectedSnafu {
                    sql: self.sql,
                    expected: "a table name",
                    actual: self.peek_token_as_string(),
                })?;
        let table_ident = Self::canonicalize_object_name(raw_table_ident);
        ensure!(
            !table_ident.0.is_empty(),
            InvalidTableNameSnafu {
                name: table_ident.to_string()
            }
        );

        Ok(Statement::DropTable(DropTable::new(table_ident, if_exists)))
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::{Ident, ObjectName};

    use super::*;
    use crate::dialect::GreptimeDbDialect;

    #[test]
    pub fn test_drop_table() {
        let sql = "DROP TABLE foo";
        let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {});
        let mut stmts = result.unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::DropTable(DropTable::new(ObjectName(vec![Ident::new("foo")]), false))
        );

        let sql = "DROP TABLE IF EXISTS foo";
        let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {});
        let mut stmts = result.unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::DropTable(DropTable::new(ObjectName(vec![Ident::new("foo")]), true))
        );

        let sql = "DROP TABLE my_schema.foo";
        let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {});
        let mut stmts = result.unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::DropTable(DropTable::new(
                ObjectName(vec![Ident::new("my_schema"), Ident::new("foo")]),
                false
            ))
        );

        let sql = "DROP TABLE my_catalog.my_schema.foo";
        let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {});
        let mut stmts = result.unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::DropTable(DropTable::new(
                ObjectName(vec![
                    Ident::new("my_catalog"),
                    Ident::new("my_schema"),
                    Ident::new("foo")
                ]),
                false
            ))
        )
    }
}
