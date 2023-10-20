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

use crate::error::{self, Result};
use crate::parser::ParserContext;
use crate::statements::explain::Explain;
use crate::statements::statement::Statement;

/// EXPLAIN statement parser implementation
impl<'a> ParserContext<'a> {
    pub(crate) fn parse_explain(&mut self) -> Result<Statement> {
        let explain_statement =
            self.parser
                .parse_explain(false)
                .with_context(|_| error::UnexpectedSnafu {
                    sql: self.sql,
                    expected: "a query statement",
                    actual: self.peek_token_as_string(),
                })?;

        Ok(Statement::Explain(Explain::try_from(explain_statement)?))
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::{
        GroupByExpr, Query as SpQuery, Statement as SpStatement, WildcardAdditionalOptions,
    };

    use super::*;
    use crate::dialect::GreptimeDbDialect;

    #[test]
    pub fn test_explain() {
        let sql = "EXPLAIN select * from foo";
        let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {});
        let stmts = result.unwrap();
        assert_eq!(1, stmts.len());

        let select = sqlparser::ast::Select {
            distinct: None,
            top: None,
            projection: vec![sqlparser::ast::SelectItem::Wildcard(
                WildcardAdditionalOptions::default(),
            )],
            into: None,
            from: vec![sqlparser::ast::TableWithJoins {
                relation: sqlparser::ast::TableFactor::Table {
                    name: sqlparser::ast::ObjectName(vec![sqlparser::ast::Ident::new("foo")]),
                    alias: None,
                    args: None,
                    with_hints: vec![],
                    partitions: vec![],
                    version: None,
                },
                joins: vec![],
            }],
            lateral_views: vec![],
            selection: None,
            group_by: GroupByExpr::Expressions(vec![]),
            cluster_by: vec![],
            distribute_by: vec![],
            sort_by: vec![],
            having: None,
            qualify: None,
            named_window: vec![],
        };

        let sp_statement = SpStatement::Query(Box::new(SpQuery {
            with: None,
            body: Box::new(sqlparser::ast::SetExpr::Select(Box::new(select))),
            order_by: vec![],
            limit: None,
            offset: None,
            fetch: None,
            locks: vec![],
        }));

        let explain = Explain::try_from(SpStatement::Explain {
            describe_alias: false,
            analyze: false,
            verbose: false,
            statement: Box::new(sp_statement),
            format: None,
        })
        .unwrap();

        assert_eq!(stmts[0], Statement::Explain(explain))
    }
}
