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

use std::collections::HashMap;
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

use crate::error::{DeleteSnafu, NotSupportSqlSnafu, Result};
use crate::instance::sql::table_idents_to_full_name;
use crate::sql::SqlHandler;

impl SqlHandler {
    pub(crate) async fn delete(&self, query_ctx: QueryContextRef, stmt: Delete) -> Result<Output> {
        let (catalog_name, schema_name, table_name) =
            table_idents_to_full_name(stmt.table_name(), query_ctx)?;
        let table_ref = TableReference {
            catalog: &catalog_name.to_string(),
            schema: &schema_name.to_string(),
            table: &table_name.to_string(),
        };

        let table = self.get_table(&table_ref)?;

        let req = DeleteRequest {
            key_column_values: parse_selection(stmt.selection())?,
        };

        let affected_rows = table.delete(req).await.with_context(|_| DeleteSnafu {
            table_name: table_ref.to_string(),
        })?;

        Ok(Output::AffectedRows(affected_rows))
    }
}

/// parse selection, currently supported format is `tagkey1 = 'tagvalue1' and 'ts' = 'value'`.
/// (only uses =, and in the where clause and provides all columns needed by the key.)
fn parse_selection(selection: &Option<Expr>) -> Result<HashMap<String, VectorRef>> {
    let mut key_column_values = HashMap::new();
    if let Some(expr) = selection {
        parse_expr(expr, &mut key_column_values)?;
    }
    Ok(key_column_values)
}

fn parse_expr(expr: &Expr, key_column_values: &mut HashMap<String, VectorRef>) -> Result<()> {
    // match BinaryOp
    if let Expr::BinaryOp { left, op, right } = expr {
        match (&**left, op, &**right) {
            // match And operator
            (Expr::BinaryOp { .. }, BinaryOperator::And, Expr::BinaryOp { .. }) => {
                parse_expr(left, key_column_values)?;
                parse_expr(right, key_column_values)?;
                return Ok(());
            }
            // match Eq operator
            (Expr::Identifier(column_name), BinaryOperator::Eq, Expr::Value(value)) => {
                key_column_values.insert(
                    column_name.to_string(),
                    value_to_vector(&column_name.to_string(), value)?,
                );
                return Ok(());
            }
            (Expr::Identifier(column_name), BinaryOperator::Eq, Expr::Identifier(value)) => {
                key_column_values.insert(
                    column_name.to_string(),
                    Arc::new(StringVector::from(vec![value.to_string()])),
                );
                return Ok(());
            }
            _ => {}
        }
    }
    return NotSupportSqlSnafu {
        msg: format!(
            "Not support sql expr:{expr},correct format is tagkey1 = tagvalue1 and ts = value"
        ),
    }
    .fail();
}

/// parse value to vector
fn value_to_vector(column_name: &String, value: &Value) -> Result<VectorRef> {
    match value {
        Value::Number(n, _) => {
            if column_name == "ts" {
                Ok(Arc::new(TimestampMillisecondVector::from_vec(vec![
                    i64::from_str(n).unwrap(),
                ])))
            } else {
                Ok(Arc::new(Float64Vector::from_vec(vec![
                    f64::from_str(n).unwrap()
                ])))
            }
        }
        Value::SingleQuotedString(s) | sql::ast::Value::DoubleQuotedString(s) => {
            Ok(Arc::new(StringVector::from(vec![s.to_string()])))
        }
        Value::Boolean(b) => Ok(Arc::new(BooleanVector::from(vec![*b]))),
        _ => {
            warn!("Current value type is not supported, value:{value}");
            return NotSupportSqlSnafu {
                msg: format!("Failed to parse value:{value}, current value type is not supported"),
            }
            .fail();
        }
    }
}
