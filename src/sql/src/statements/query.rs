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

use sqlparser::ast::Query as SpQuery;
use sqlparser_derive::{Visit, VisitMut};

use crate::error::Error;

/// Query statement instance.
#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut)]
pub struct Query {
    pub inner: SpQuery,
}

/// Automatically converts from sqlparser Query instance to SqlQuery.
impl TryFrom<SpQuery> for Query {
    type Error = Error;

    fn try_from(q: SpQuery) -> Result<Self, Self::Error> {
        Ok(Query { inner: q })
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
        write!(f, "{}", self.inner)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {

    use super::Query;
    use crate::dialect::GreptimeDbDialect;
    use crate::parser::ParserContext;
    use crate::statements::statement::Statement;

    fn create_query(sql: &str) -> Option<Box<Query>> {
        match ParserContext::create_with_dialect(sql, &GreptimeDbDialect {})
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
    }
}
