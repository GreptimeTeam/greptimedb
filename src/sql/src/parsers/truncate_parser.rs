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
use crate::statements::statement::Statement;
use crate::statements::truncate::TruncateTable;

/// `TRUNCATE [TABLE] table_name;`
impl<'a> ParserContext<'a> {
    pub(crate) fn parse_truncate(&mut self) -> Result<Statement> {
        let _ = self.parser.next_token();
        let _ = self.parser.parse_keyword(Keyword::TABLE);

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

        Ok(Statement::TruncateTable(TruncateTable::new(table_ident)))
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::{Ident, ObjectName};

    use super::*;
    use crate::dialect::GreptimeDbDialect;

    #[test]
    pub fn test_parse_truncate() {
        let sql = "TRUNCATE foo";
        let mut stmts = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}).unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::TruncateTable(TruncateTable::new(ObjectName(vec![Ident::new("foo")])))
        );

        let sql = "TRUNCATE TABLE foo";
        let mut stmts = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}).unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::TruncateTable(TruncateTable::new(ObjectName(vec![Ident::new("foo")])))
        );

        let sql = "TRUNCATE TABLE my_schema.foo";
        let mut stmts = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}).unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::TruncateTable(TruncateTable::new(ObjectName(vec![
                Ident::new("my_schema"),
                Ident::new("foo")
            ])))
        );

        let sql = "TRUNCATE my_schema.foo";
        let mut stmts = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}).unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::TruncateTable(TruncateTable::new(ObjectName(vec![
                Ident::new("my_schema"),
                Ident::new("foo")
            ])))
        );

        let sql = "TRUNCATE TABLE my_catalog.my_schema.foo";
        let mut stmts = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}).unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::TruncateTable(TruncateTable::new(ObjectName(vec![
                Ident::new("my_catalog"),
                Ident::new("my_schema"),
                Ident::new("foo")
            ])))
        );

        let sql = "TRUNCATE drop";
        let mut stmts = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}).unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::TruncateTable(TruncateTable::new(ObjectName(vec![Ident::new("drop")])))
        );

        let sql = "TRUNCATE `drop`";
        let mut stmts = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}).unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::TruncateTable(TruncateTable::new(ObjectName(vec![Ident::with_quote(
                '`', "drop"
            ),])))
        );

        let sql = "TRUNCATE \"drop\"";
        let mut stmts = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}).unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::TruncateTable(TruncateTable::new(ObjectName(vec![Ident::with_quote(
                '"', "drop"
            ),])))
        );
    }

    #[test]
    pub fn test_parse_invalid_truncate() {
        let sql = "TRUNCATE SCHEMA foo";
        let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {});
        assert!(result.is_err(), "result is: {result:?}");

        let sql = "TRUNCATE";
        let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {});
        assert!(result.is_err(), "result is: {result:?}");
    }
}
