use std::collections::HashMap;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;
use snafu::ResultExt;
use catalog::CatalogManagerRef;
use table::requests::DeleteRequest;
use table::engine::TableReference;
use common_query::Output;
use datatypes::prelude::LogicalTypeId::TimestampMillisecond;
use datatypes::prelude::VectorRef;
use datatypes::vectors::{BooleanVector, Float64Vector, StringVector, TimestampMillisecondVector};
use sql::ast::ColumnOption::Default;
use sql::ast::{BinaryOperator, Expr, Value};
use sql::statements::delete::Delete;

use crate::sql::{SqlHandler, SqlRequest};

use crate::error::{
    Result,
    DeleteSnafu,
};

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
        table_ref: TableReference,
        stmt: Delete,
    ) -> Result<SqlRequest> {
        Ok(SqlRequest::Delete(DeleteRequest {
            key_column_values: parser_selection(stmt.selection()),
            catalog_name: table_ref.catalog.to_string(),
            schema_name: table_ref.schema.to_string(),
            table_name: table_ref.table.to_string(),
        }))
    }
}

fn parser_selection(selection: &Option<Expr>) -> HashMap<String, VectorRef> {
    println!("selection: {:?}", selection);
    let mut key_column_values = HashMap::new();
    match selection {
        Some(expr) => {
            parser_expr(&expr, &mut key_column_values);
        }
        _ => {}
    }
    println!("key_column_values: {:?}", key_column_values);
    return key_column_values;
}

fn parser_expr(expr: &Expr, key_column_values: &mut HashMap<String, VectorRef>) {
    if let Expr::BinaryOp { left, op, right } = expr {
        if let BinaryOperator::And = op {
            if let Expr::BinaryOp { .. } = left.deref() {
                if let Expr::BinaryOp { .. } = right.deref() {
                    parser_expr(left.deref(), key_column_values);
                    parser_expr(right.deref(), key_column_values);
                }
            }
        }
        if let BinaryOperator::Eq = op {
            if let Expr::Identifier(column_name) = left.deref() {
                if let Expr::Value(value) = right.deref() {
                    key_column_values.insert(column_name.to_string(), value_to_vector(&column_name.to_string(), &value));
                }
                if let Expr::Identifier(value) = right.deref() {
                    key_column_values.insert(column_name.to_string(),
                                             Arc::new(StringVector::from(vec![value.to_string()])));
                }
            }
        }
    }
}

fn value_to_vector(column_name: &String, value: &Value) -> VectorRef {
    match value {
        Value::Number(n, _) => {
            if column_name == "ts" {
                Arc::new(TimestampMillisecondVector::from_vec(vec![i64::from_str(n).unwrap()]))
            } else {
                Arc::new(Float64Vector::from_vec(vec![f64::from_str(n).unwrap()]))
            }
        }
        Value::SingleQuotedString(s) | sql::ast::Value::DoubleQuotedString(s) => {
            Arc::new(StringVector::from(vec![s.to_string()]))
        }
        Value::Boolean(b) => {
            Arc::new(BooleanVector::from(vec![*b]))
        }
        _ => {
            //TODO not support
            Arc::new(StringVector::from(vec!["1"]))
        }
    }
}