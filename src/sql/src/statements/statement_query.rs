use sqlparser::ast::Query as SpQuery;

use crate::errors::ParserError;

/// Query statement instance.
#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    pub inner: SpQuery,
}

/// Automatically converts from sqlparser Query instance to SqlQuery.
impl TryFrom<SpQuery> for Query {
    type Error = ParserError;

    fn try_from(q: SpQuery) -> Result<Self, Self::Error> {
        Ok(Query { inner: q })
    }
}

impl TryInto<SpQuery> for Query {
    type Error = ParserError;

    fn try_into(self) -> Result<SpQuery, Self::Error> {
        Ok(self.inner)
    }
}
