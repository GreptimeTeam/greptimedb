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

use snafu::prelude::*;

use crate::error::{self, Result};
use crate::parser::ParserContext;
use crate::statements::query::Query;
use crate::statements::statement::Statement;

impl<'a> ParserContext<'a> {
    /// Parses select and it's variants.
    pub(crate) fn parse_query(&mut self) -> Result<Statement> {
        let spquery = self
            .parser
            .parse_query()
            .context(error::SyntaxSnafu { sql: self.sql })?;

        Ok(Statement::Query(Box::new(Query::try_from(spquery)?)))
    }
}

#[cfg(test)]
mod tests {
    use common_error::ext::ErrorExt;

    use crate::dialect::GreptimeDbDialect;
    use crate::parser::ParserContext;

    #[test]
    pub fn test_parse_query() {
        let sql = "SELECT a, b, 123, myfunc(b) \
           FROM table_1 \
           WHERE a > b AND b < 100 \
           ORDER BY a DESC, b";

        let _ = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}).unwrap();
    }

    #[test]
    pub fn test_parse_invalid_query() {
        let sql = "SELECT * FROM table_1 WHERE";
        let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {});
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .output_msg()
            .contains("Expected an expression"));
    }
}
