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
use sqlparser::dialect::keywords::Keyword;
use sqlparser::tokenizer::Token;

use crate::error::{self, InvalidTableNameSnafu, Result};
use crate::parser::{ParserContext, FLOW};
use crate::statements::drop::{DropDatabase, DropFlow, DropTable};
use crate::statements::statement::Statement;

/// DROP statement parser implementation
impl<'a> ParserContext<'a> {
    pub(crate) fn parse_drop(&mut self) -> Result<Statement> {
        let _ = self.parser.next_token();
        match self.parser.peek_token().token {
            Token::Word(w) => match w.keyword {
                Keyword::TABLE => self.parse_drop_table(),
                Keyword::SCHEMA | Keyword::DATABASE => self.parse_drop_database(),
                Keyword::NoKeyword => {
                    let uppercase = w.value.to_uppercase();
                    match uppercase.as_str() {
                        FLOW => self.parse_drop_flow(),
                        _ => self.unsupported(w.to_string()),
                    }
                }
                _ => self.unsupported(w.to_string()),
            },
            unexpected => self.unsupported(unexpected.to_string()),
        }
    }

    fn parse_drop_flow(&mut self) -> Result<Statement> {
        let _ = self.parser.next_token();

        let if_exists = self.parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let raw_flow_ident =
            self.parse_object_name()
                .with_context(|_| error::UnexpectedSnafu {
                    sql: self.sql,
                    expected: "a flow name",
                    actual: self.peek_token_as_string(),
                })?;
        let flow_ident = Self::canonicalize_object_name(raw_flow_ident);
        ensure!(
            !flow_ident.0.is_empty(),
            InvalidTableNameSnafu {
                name: flow_ident.to_string()
            }
        );

        Ok(Statement::DropFlow(DropFlow::new(flow_ident, if_exists)))
    }

    fn parse_drop_table(&mut self) -> Result<Statement> {
        let _ = self.parser.next_token();

        let if_exists = self.parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let raw_table_ident =
            self.parse_object_name()
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

    fn parse_drop_database(&mut self) -> Result<Statement> {
        let _ = self.parser.next_token();

        let if_exists = self.parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let database_name = self
            .parse_object_name()
            .with_context(|_| error::UnexpectedSnafu {
                sql: self.sql,
                expected: "a database name",
                actual: self.peek_token_as_string(),
            })?;
        let database_name = Self::canonicalize_object_name(database_name);

        Ok(Statement::DropDatabase(DropDatabase::new(
            database_name,
            if_exists,
        )))
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::{Ident, ObjectName};

    use super::*;
    use crate::dialect::GreptimeDbDialect;
    use crate::parser::ParseOptions;

    #[test]
    pub fn test_drop_table() {
        let sql = "DROP TABLE foo";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let mut stmts = result.unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::DropTable(DropTable::new(ObjectName(vec![Ident::new("foo")]), false))
        );

        let sql = "DROP TABLE IF EXISTS foo";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let mut stmts = result.unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::DropTable(DropTable::new(ObjectName(vec![Ident::new("foo")]), true))
        );

        let sql = "DROP TABLE my_schema.foo";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let mut stmts = result.unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::DropTable(DropTable::new(
                ObjectName(vec![Ident::new("my_schema"), Ident::new("foo")]),
                false
            ))
        );

        let sql = "DROP TABLE my_catalog.my_schema.foo";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
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

    #[test]
    pub fn test_drop_database() {
        let sql = "DROP DATABASE public";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let mut stmts = result.unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::DropDatabase(DropDatabase::new(
                ObjectName(vec![Ident::new("public")]),
                false
            ))
        );

        let sql = "DROP DATABASE IF EXISTS public";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let mut stmts = result.unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::DropDatabase(DropDatabase::new(
                ObjectName(vec![Ident::new("public")]),
                true
            ))
        );

        let sql = "DROP DATABASE `fOo`";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let mut stmts = result.unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::DropDatabase(DropDatabase::new(
                ObjectName(vec![Ident::with_quote('`', "fOo"),]),
                false
            ))
        );
    }

    #[test]
    pub fn test_drop_flow() {
        let sql = "DROP FLOW foo";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let mut stmts: Vec<Statement> = result.unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::DropFlow(DropFlow::new(ObjectName(vec![Ident::new("foo")]), false))
        );

        let sql = "DROP FLOW IF EXISTS foo";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let mut stmts = result.unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::DropFlow(DropFlow::new(ObjectName(vec![Ident::new("foo")]), true))
        );

        let sql = "DROP FLOW my_schema.foo";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let mut stmts = result.unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::DropFlow(DropFlow::new(
                ObjectName(vec![Ident::new("my_schema"), Ident::new("foo")]),
                false
            ))
        );

        let sql = "DROP FLOW my_catalog.my_schema.foo";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let mut stmts = result.unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::DropFlow(DropFlow::new(
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
