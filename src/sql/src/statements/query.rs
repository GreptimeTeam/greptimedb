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

use std::fmt;

use serde::Serialize;
use sqlparser::ast::Query as SpQuery;
use sqlparser_derive::{Visit, VisitMut};

use crate::error::Error;
use crate::parsers::with_tql_parser::HybridCteWith;

/// A wrapper around [`Query`] from sqlparser-rs to add support for hybrid CTEs
#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub struct Query {
    pub inner: SpQuery,
    /// Hybrid CTE containing both SQL and TQL CTEs
    pub hybrid_cte: Option<HybridCteWith>,
}

impl TryFrom<SpQuery> for Query {
    type Error = Error;

    fn try_from(inner: SpQuery) -> Result<Self, Self::Error> {
        Ok(Self {
            inner,
            hybrid_cte: None,
        })
    }
}

impl TryFrom<Query> for SpQuery {
    type Error = Error;

    fn try_from(value: Query) -> Result<Self, Self::Error> {
        Ok(value.inner)
    }
}

impl fmt::Display for Query {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(hybrid_cte) = &self.hybrid_cte {
            // Delegate the WITH clause rendering to `HybridCteWith`
            write!(f, "{} ", hybrid_cte)?;

            // Display the main query without its WITH clause since we handled it above
            let mut main_query = self.inner.clone();
            main_query.with = None;
            write!(f, "{}", main_query)
        } else {
            write!(f, "{}", self.inner)
        }
    }
}

#[cfg(test)]
mod test {

    use super::Query;
    use crate::dialect::GreptimeDbDialect;
    use crate::parser::{ParseOptions, ParserContext};
    use crate::statements::statement::Statement;

    fn create_query(sql: &str) -> Option<Box<Query>> {
        match ParserContext::create_with_dialect(
            sql,
            &GreptimeDbDialect {},
            ParseOptions::default(),
        )
        .unwrap()
        .remove(0)
        {
            Statement::Query(query) => Some(query),
            _ => None,
        }
    }

    #[test]
    fn test_query_display() {
        assert_eq!(
            create_query("select * from abc where x = 1 and y = 7")
                .unwrap()
                .to_string(),
            "SELECT * FROM abc WHERE x = 1 AND y = 7"
        );
        assert_eq!(
            create_query(
                "select * from abc left join bcd where abc.a = 1 and bcd.d = 7 and abc.id = bcd.id"
            )
            .unwrap()
            .to_string(),
            "SELECT * FROM abc LEFT JOIN bcd WHERE abc.a = 1 AND bcd.d = 7 AND abc.id = bcd.id"
        );
        assert_eq!(
            create_query("WITH tql_cte AS (TQL EVAL (0, 100, '5s') up) SELECT * FROM tql_cte")
                .unwrap()
                .to_string(),
            "WITH tql_cte AS (TQL EVAL (0, 100, '5s') up) SELECT * FROM tql_cte"
        );
    }
}
