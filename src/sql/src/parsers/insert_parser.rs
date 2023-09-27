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
use crate::statements::insert::Insert;
use crate::statements::statement::Statement;

/// INSERT statement parser implementation
impl<'a> ParserContext<'a> {
    pub(crate) fn parse_insert(&mut self) -> Result<Statement> {
        let _ = self.parser.next_token();
        let spstatement = self.parser.parse_insert().context(error::SyntaxSnafu)?;

        match spstatement {
            SpStatement::Insert { .. } => {
                Ok(Statement::Insert(Box::new(Insert { inner: spstatement })))
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
    use std::assert_matches::assert_matches;

    use super::*;
    use crate::dialect::GreptimeDbDialect;

    #[test]
    pub fn test_parse_insert() {
        let sql = r"INSERT INTO table_1 VALUES (
            'test1',1,'true',
            'test2',2,'false')
         ";
        let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}).unwrap();
        assert_eq!(1, result.len());
        assert_matches!(result[0], Statement::Insert { .. })
    }

    #[test]
    pub fn test_parse_invalid_insert() {
        let sql = r"INSERT INTO table_1 VALUES ("; // intentionally a bad sql
        let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {});
        assert!(result.is_err(), "result is: {result:?}");
    }
}
