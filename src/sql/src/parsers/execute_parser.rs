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
use sqlparser::ast::Expr;
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;

use crate::error::{Result, SyntaxSnafu};
use crate::parser::ParserContext;

impl ParserContext<'_> {
    /// Parses MySQL style 'EXECUTE stmt_name USING param_list' into a stmt_name string and a list of parameters.
    /// Only use for MySQL. for PostgreSQL, use `sqlparser::parser::Parser::parse_execute` instead.
    pub(crate) fn parse_mysql_execute(&mut self) -> Result<(String, Vec<Expr>)> {
        self.parser
            .expect_keyword(Keyword::EXECUTE)
            .context(SyntaxSnafu)?;
        let stmt_name = self.parser.parse_identifier(false).context(SyntaxSnafu)?;
        if self.parser.parse_keyword(Keyword::USING) {
            let param_list = self
                .parser
                .parse_comma_separated(Parser::parse_expr)
                .context(SyntaxSnafu)?;
            Ok((stmt_name.value, param_list))
        } else {
            Ok((stmt_name.value, vec![]))
        }
    }
}
