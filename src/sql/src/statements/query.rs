use sqlparser::ast::Query as SpQuery;

use crate::error::Error;

/// Query statement instance.
#[derive(Debug, Clone, PartialEq, Eq)]
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
