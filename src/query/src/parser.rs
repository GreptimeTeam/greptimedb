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

use common_error::prelude::BoxedError;
use common_telemetry::timer;
use promql_parser::parser::EvalStmt;
use snafu::ResultExt;
use sql::dialect::GenericDialect;
use sql::parser::ParserContext;
use sql::statements::statement::Statement;

use crate::error::{MultipleStatementsSnafu, QueryParseSnafu, Result};
use crate::metric::METRIC_PARSE_SQL_ELAPSED;

#[derive(Debug, Clone)]
pub enum QueryStatement {
    Sql(Statement),
    Promql(EvalStmt),
}

pub struct QueryLanguageParser {}

impl QueryLanguageParser {
    pub fn parse_sql(sql: &str) -> Result<QueryStatement> {
        let _timer = timer!(METRIC_PARSE_SQL_ELAPSED);
        let mut statement = ParserContext::create_with_dialect(sql, &GenericDialect {})
            .map_err(BoxedError::new)
            .context(QueryParseSnafu {
                query: sql.to_string(),
            })?;
        if statement.len() != 1 {
            MultipleStatementsSnafu {
                query: sql.to_string(),
            }
            .fail()
        } else {
            Ok(QueryStatement::Sql(statement.pop().unwrap()))
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    // Detailed logic tests are covered in the parser crate.
    #[test]
    fn parse_sql_simple() {
        let sql = "select * from t1";
        let stmt = QueryLanguageParser::parse_sql(sql).unwrap();
        let expected = String::from("SQL(Query(Query { \
            inner: Query { \
                with: None, body: Select(Select { \
                    distinct: false, \
                    top: None, \
                    projection: \
                    [Wildcard(WildcardAdditionalOptions { opt_exclude: None, opt_except: None })], \
                    into: None, \
                    from: [TableWithJoins { relation: Table { name: ObjectName([Ident { value: \"t1\", quote_style: None }]\
                ), \
                alias: None, \
                args: None, \
                with_hints: [] \
            }, \
            joins: [] }], \
            lateral_views: [], \
            selection: None, \
            group_by: [], \
            cluster_by: [], \
            distribute_by: [], \
            sort_by: [], \
            having: None, \
            qualify: None \
        }), order_by: [], limit: None, offset: None, fetch: None, lock: None } }))");

        assert_eq!(format!("{stmt:?}"), expected);
    }
}
