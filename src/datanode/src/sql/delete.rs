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
use std::sync::Arc;

use common_query::Output;
use datatypes::data_type::DataType;
use datatypes::prelude::VectorRef;
use datatypes::vectors::StringVector;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt};
use sql::ast::{BinaryOperator, Expr, Value};
use sql::statements::delete::Delete;
use sql::statements::sql_value_to_value;
use table::engine::TableReference;
use table::requests::DeleteRequest;
use table::TableRef;

use crate::error::{ColumnNotFoundSnafu, DeleteSnafu, InvalidSqlSnafu, NotSupportSqlSnafu, Result};
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
            key_column_values: parse_selection(stmt.selection(), &table)?,
        };

        let affected_rows = table.delete(req).await.with_context(|_| DeleteSnafu {
            table_name: table_ref.to_string(),
        })?;

        Ok(Output::AffectedRows(affected_rows))
    }
}

/// parse selection, currently supported format is `tagkey1 = 'tagvalue1' and 'ts' = 'value'`.
/// (only uses =, and in the where clause and provides all columns needed by the key.)
fn parse_selection(
    selection: &Option<Expr>,
    table: &TableRef,
) -> Result<HashMap<String, VectorRef>> {
    let mut key_column_values = HashMap::new();
    if let Some(expr) = selection {
        parse_expr(expr, &mut key_column_values, table)?;
    }
    Ok(key_column_values)
}

fn parse_expr(
    expr: &Expr,
    key_column_values: &mut HashMap<String, VectorRef>,
    table: &TableRef,
) -> Result<()> {
    // match BinaryOp
    if let Expr::BinaryOp { left, op, right } = expr {
        match (&**left, op, &**right) {
            // match And operator
            (Expr::BinaryOp { .. }, BinaryOperator::And, Expr::BinaryOp { .. }) => {
                parse_expr(left, key_column_values, table)?;
                parse_expr(right, key_column_values, table)?;
                return Ok(());
            }
            // match Eq operator
            (Expr::Identifier(column_name), BinaryOperator::Eq, Expr::Value(value)) => {
                key_column_values.insert(
                    column_name.to_string(),
                    value_to_vector(&column_name.to_string(), value, table)?,
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
    NotSupportSqlSnafu {
        msg: format!(
            "Not support sql expr:{expr},correct format is tagkey1 = tagvalue1 and ts = value"
        ),
    }
    .fail()
}

/// parse value to vector
fn value_to_vector(column_name: &String, sql_value: &Value, table: &TableRef) -> Result<VectorRef> {
    let schema = table.schema();
    let column_schema =
        schema
            .column_schema_by_name(column_name)
            .with_context(|| ColumnNotFoundSnafu {
                table_name: table.table_info().name.clone(),
                column_name: column_name.to_string(),
            })?;
    let data_type = &column_schema.data_type;
    let value = sql_value_to_value(column_name, data_type, sql_value);
    match value {
        Ok(value) => {
            let mut vec = data_type.create_mutable_vector(1);
            if vec.try_push_value_ref(value.as_value_ref()).is_err() {
                return InvalidSqlSnafu {
                    msg: format!(
                        "invalid sql, column name is {column_name}, value is {sql_value}",
                    ),
                }
                    .fail();
            }
            Ok(vec.to_vector())
        }
        _ => InvalidSqlSnafu {
            msg: format!("invalid sql, column name is {column_name}, value is {sql_value}",),
        }
        .fail(),
    }
}
