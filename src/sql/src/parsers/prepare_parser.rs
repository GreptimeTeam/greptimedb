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
use sqlparser::tokenizer::Token;

use crate::error::{Result, SyntaxSnafu};
use crate::parser::ParserContext;

impl<'a> ParserContext<'a> {
    /// Parses MySQL style 'PREPARE stmt_name FROM stmt' into a (stmt_name, stmt) tuple.
    /// Only use for MySQL. for PostgreSQL, use `sqlparser::parser::Parser::parse_prepare` instead.
    pub(crate) fn parse_mysql_prepare(&mut self) -> Result<(String, String)> {
        self.parser
            .expect_keyword(Keyword::PREPARE)
            .context(SyntaxSnafu)?;
        let stmt_name = self.parser.parse_identifier(false).context(SyntaxSnafu)?;
        self.parser
            .expect_keyword(Keyword::FROM)
            .context(SyntaxSnafu)?;
        let next_token = self.parser.peek_token();
        let stmt = match next_token.token {
            Token::SingleQuotedString(s) | Token::DoubleQuotedString(s) => {
                let _ = self.parser.next_token();
                s
            }
            _ => self
                .parser
                .expected("string literal", next_token)
                .context(SyntaxSnafu)?,
        };
        Ok((stmt_name.value, stmt))
    }
}
