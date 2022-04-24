use sqlparser::ast::{
    Expr, Offset, OrderByExpr, Query, Select, SelectItem, SetExpr, TableWithJoins,
};

use crate::errors::ParserError;

/// Query statement instance.
#[derive(Debug, Clone, PartialEq)]
pub struct SqlQuery {
    pub from: Vec<TableWithJoins>,
    // selected items
    pub projection: Vec<SelectItem>,
    // selection conditions
    pub selection: Option<Expr>,
    // GROUP BY expressions
    pub group_by: Vec<Expr>,
    // HAVING expressions
    pub having: Option<Expr>,
    // ORDER BY expressions
    pub order_by: Vec<OrderByExpr>,
    // LIMIT expressions
    pub limit: Option<Expr>,
    // OFFSET
    pub offset: Option<Offset>,
    // query body (now only support SELECT variant)
    pub set_expr: SetExpr,
}

impl SqlQuery {
    /// The body of SqlQuery now only supports `SELECT`
    pub fn get_body(query: &Query) -> Result<&Select, ParserError> {
        match &query.body {
            SetExpr::Select(query) => Ok(query),
            other => Err(ParserError::Unsupported {
                sql: query.to_string(),
                keyword: other.to_string(),
            }),
        }
    }
}

/// Automatically converts from sqlparser Query instance to SqlQuery.
impl TryFrom<sqlparser::ast::Query> for SqlQuery {
    type Error = ParserError;

    fn try_from(q: Query) -> Result<Self, Self::Error> {
        let body = Self::get_body(&q)?;

        if q.with.is_some() {
            return Err(ParserError::Unsupported {
                sql: q.to_string(),
                keyword: "WITH".to_string(),
            });
        }

        if q.fetch.is_some() {
            return Err(ParserError::Unsupported {
                sql: "".to_string(),
                keyword: "FETCH".to_string(),
            });
        }

        if body.projection.is_empty() {
            return Err(ParserError::SyntaxError {
                msg: "Statement projection is empty".to_string(),
            });
        }

        if body.top.is_some() {
            return Err(ParserError::Unsupported {
                sql: "".to_string(),
                keyword: "TOP".to_string(),
            });
        }

        if !body.sort_by.is_empty() {
            return Err(ParserError::Unsupported {
                sql: "".to_string(),
                keyword: "ORDER BY".to_string(),
            });
        }

        if !body.cluster_by.is_empty() {
            return Err(ParserError::Unsupported {
                sql: "".to_string(),
                keyword: "CLUSTER BY".to_string(),
            });
        }

        if !body.distribute_by.is_empty() {
            return Err(ParserError::Unsupported {
                sql: "".to_string(),
                keyword: "DISTRIBUTE BY".to_string(),
            });
        }

        Ok(SqlQuery {
            from: body.from.clone(),
            projection: body.projection.clone(),
            selection: body.selection.clone(),
            group_by: body.group_by.clone(),
            having: body.having.clone(),
            order_by: q.order_by.clone(),
            limit: q.limit.clone(),
            offset: q.offset.clone(),
            set_expr: q.body,
        })
    }
}
