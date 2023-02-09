use std::collections::HashMap;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;

use common_query::Output;
use common_telemetry::warn;
use datatypes::prelude::VectorRef;
use datatypes::vectors::{BooleanVector, Float64Vector, StringVector, TimestampMillisecondVector};
use session::context::QueryContextRef;
use snafu::ResultExt;
use sql::ast::{BinaryOperator, Expr, Value};
use sql::statements::delete::Delete;
use table::engine::TableReference;
use table::requests::DeleteRequest;

use crate::error::{DeleteSnafu, InvalidSqlSnafu, Result};
use crate::instance::sql::table_idents_to_full_name;
use crate::sql::{SqlHandler, SqlRequest};

impl SqlHandler {
    pub(crate) async fn delete(&self, req: DeleteRequest) -> Result<Output> {
        let table_ref = TableReference {
            catalog: &req.catalog_name.to_string(),
            schema: &req.schema_name.to_string(),
            table: &req.table_name.to_string(),
        };

        let table = self.get_table(&table_ref)?;

        let affected_rows = table.delete(req).await.with_context(|_| DeleteSnafu {
            table_name: table_ref.to_string(),
        })?;

        Ok(Output::AffectedRows(affected_rows))
    }

    pub(crate) fn delete_to_request(
        &self,
        stmt: Delete,
        query_ctx: QueryContextRef,
    ) -> Result<SqlRequest> {
        let (catalog_name, schema_name, table_name) =
            table_idents_to_full_name(stmt.table_name(), query_ctx.clone())?;
        let key_column_values = parser_selection(stmt.selection())?;
        Ok(SqlRequest::Delete(DeleteRequest {
            key_column_values,
            catalog_name,
            schema_name,
            table_name,
        }))
    }
}

/// parser selection, currently supported format is `tagkey1 = 'tagvalue1' and tagkey2 = 'tagvalue2'`.
/// (only uses =, and in the where clause and provides all columns needed by the key.)
fn parser_selection(selection: &Option<Expr>) -> Result<HashMap<String, VectorRef>> {
    let mut key_column_values = HashMap::new();
    match selection {
        Some(expr) => {
            parser_expr(&expr, &mut key_column_values)?;
        }
        _ => {}
    }
    return Ok(key_column_values);
}

fn parser_expr(expr: &Expr, key_column_values: &mut HashMap<String, VectorRef>) -> Result<()> {
    // match BinaryOp
    if let Expr::BinaryOp { left, op, right } = expr {
        // match And operator
        if let BinaryOperator::And = op {
            if let Expr::BinaryOp { .. } = left.deref() {
                if let Expr::BinaryOp { .. } = right.deref() {
                    parser_expr(left.deref(), key_column_values)?;
                    parser_expr(right.deref(), key_column_values)?;
                    return Ok(());
                }
            }
        }
        // match eq operator
        else if let BinaryOperator::Eq = op {
            if let Expr::Identifier(column_name) = left.deref() {
                if let Expr::Value(value) = right.deref() {
                    key_column_values.insert(
                        column_name.to_string(),
                        value_to_vector(&column_name.to_string(), &value)?,
                    );
                    return Ok(());
                } else if let Expr::Identifier(value) = right.deref() {
                    key_column_values.insert(
                        column_name.to_string(),
                        Arc::new(StringVector::from(vec![value.to_string()])),
                    );
                    return Ok(());
                }
            }
        }
    }
    return InvalidSqlSnafu {
        msg: format!("Failed to parse expr:{expr}"),
    }
    .fail();
}

/// parse value to vector
fn value_to_vector(column_name: &String, value: &Value) -> Result<VectorRef> {
    match value {
        Value::Number(n, _) => {
            return if column_name == "ts" {
                Ok(Arc::new(TimestampMillisecondVector::from_vec(vec![
                    i64::from_str(n).unwrap(),
                ])))
            } else {
                Ok(Arc::new(Float64Vector::from_vec(vec![
                    f64::from_str(n).unwrap()
                ])))
            };
        }
        Value::SingleQuotedString(s) | sql::ast::Value::DoubleQuotedString(s) => {
            Ok(Arc::new(StringVector::from(vec![s.to_string()])))
        }
        Value::Boolean(b) => Ok(Arc::new(BooleanVector::from(vec![*b]))),
        _ => {
            warn!("Current value type is not supported, value:{value}");
            return InvalidSqlSnafu {
                msg: format!("Failed to parse value:{value}"),
            }
            .fail();
        }
    }
}
