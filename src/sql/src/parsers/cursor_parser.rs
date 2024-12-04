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
use sqlparser::keywords::Keyword;

use crate::error::{self, Result};
use crate::parser::ParserContext;
use crate::statements::cursor::{CloseCursor, DeclareCursor, FetchCursor};
use crate::statements::statement::Statement;

impl ParserContext<'_> {
    pub(crate) fn parse_declare_cursor(&mut self) -> Result<Statement> {
        let _ = self.parser.expect_keyword(Keyword::DECLARE);
        let cursor_name = self
            .parser
            .parse_object_name(false)
            .context(error::SyntaxSnafu)?;
        let _ = self
            .parser
            .expect_keywords(&[Keyword::CURSOR, Keyword::FOR]);

        let query_stmt = self.parse_query()?;
        match query_stmt {
            Statement::Query(query) => Ok(Statement::DeclareCursor(DeclareCursor {
                cursor_name,
                query,
            })),
            _ => error::InvalidSqlSnafu {
                msg: format!("Expect query, found {}", query_stmt),
            }
            .fail(),
        }
    }

    pub(crate) fn parse_fetch_cursor(&mut self) -> Result<Statement> {
        let _ = self.parser.expect_keyword(Keyword::FETCH);

        let fetch_size = self
            .parser
            .parse_literal_uint()
            .context(error::SyntaxSnafu)?;
        let _ = self.parser.parse_keyword(Keyword::FROM);

        let cursor_name = self
            .parser
            .parse_object_name(false)
            .context(error::SyntaxSnafu)?;

        Ok(Statement::FetchCursor(FetchCursor {
            cursor_name,
            fetch_size,
        }))
    }

    pub(crate) fn parse_close_cursor(&mut self) -> Result<Statement> {
        let _ = self.parser.expect_keyword(Keyword::CLOSE);
        let cursor_name = self
            .parser
            .parse_object_name(false)
            .context(error::SyntaxSnafu)?;

        Ok(Statement::CloseCursor(CloseCursor { cursor_name }))
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::dialect::GreptimeDbDialect;
    use crate::parser::ParseOptions;

    #[test]
    fn test_parse_declare_cursor() {
        let sql = "DECLARE c1 CURSOR FOR\nSELECT * FROM numbers";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();

        if let Statement::DeclareCursor(dc) = &result[0] {
            assert_eq!("c1", dc.cursor_name.to_string());
        } else {
            panic!("Unexpected statement");
        }
    }

    #[test]
    fn test_parese_fetch_cursor() {
        let sql = "FETCH 1000 FROM c1";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();

        if let Statement::FetchCursor(fc) = &result[0] {
            assert_eq!("c1", fc.cursor_name.to_string());
            assert_eq!("1000", fc.fetch_size.to_string());
        } else {
            panic!("Unexpected statement")
        }
    }

    #[test]
    fn test_close_fetch_cursor() {
        let sql = "CLOSE c1";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();

        if let Statement::CloseCursor(cc) = &result[0] {
            assert_eq!("c1", cc.cursor_name.to_string());
        } else {
            panic!("Unexpected statement")
        }
    }
}