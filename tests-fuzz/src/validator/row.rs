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

use chrono::{DateTime as ChronoDateTime, NaiveDate, NaiveDateTime, Utc};
use common_time::date::Date;
use common_time::Timestamp;
use datatypes::value::Value;
use snafu::{ensure, ResultExt};
use sqlx::mysql::MySqlRow;
use sqlx::{Column, ColumnIndex, Database, MySqlPool, Row, TypeInfo, ValueRef};

use crate::error::{self, Result};
use crate::ir::insert_expr::{RowValue, RowValues};

/// Asserts fetched_rows are equal to rows
pub fn assert_eq<'a, DB>(
    columns: &[crate::ir::Column],
    fetched_rows: &'a [<DB as Database>::Row],
    rows: &[RowValues],
) -> Result<()>
where
    DB: Database,
    usize: ColumnIndex<<DB as Database>::Row>,
    bool: sqlx::Type<DB> + sqlx::Decode<'a, DB>,
    i8: sqlx::Type<DB> + sqlx::Decode<'a, DB>,
    i16: sqlx::Type<DB> + sqlx::Decode<'a, DB>,
    i32: sqlx::Type<DB> + sqlx::Decode<'a, DB>,
    i64: sqlx::Type<DB> + sqlx::Decode<'a, DB>,
    f32: sqlx::Type<DB> + sqlx::Decode<'a, DB>,
    f64: sqlx::Type<DB> + sqlx::Decode<'a, DB>,
    String: sqlx::Type<DB> + sqlx::Decode<'a, DB>,
    Vec<u8>: sqlx::Type<DB> + sqlx::Decode<'a, DB>,
    ChronoDateTime<Utc>: sqlx::Type<DB> + sqlx::Decode<'a, DB>,
    NaiveDateTime: sqlx::Type<DB> + sqlx::Decode<'a, DB>,
    NaiveDate: sqlx::Type<DB> + sqlx::Decode<'a, DB>,
{
    ensure!(
        fetched_rows.len() == rows.len(),
        error::AssertSnafu {
            reason: format!(
                "Expected values length: {}, got: {}",
                rows.len(),
                fetched_rows.len(),
            )
        }
    );

    for (idx, fetched_row) in fetched_rows.iter().enumerate() {
        let row = &rows[idx];

        ensure!(
            fetched_row.len() == row.len(),
            error::AssertSnafu {
                reason: format!(
                    "Expected row length: {}, got: {}",
                    row.len(),
                    fetched_row.len(),
                )
            }
        );

        for (idx, value) in row.iter().enumerate() {
            let fetched_value = if fetched_row.try_get_raw(idx).unwrap().is_null() {
                RowValue::Value(Value::Null)
            } else {
                let value_type = fetched_row.column(idx).type_info().name();
                match value_type {
                    "BOOL" | "BOOLEAN" => RowValue::Value(Value::Boolean(
                        fetched_row.try_get::<bool, usize>(idx).unwrap(),
                    )),
                    "TINYINT" => {
                        RowValue::Value(Value::Int8(fetched_row.try_get::<i8, usize>(idx).unwrap()))
                    }
                    "SMALLINT" => RowValue::Value(Value::Int16(
                        fetched_row.try_get::<i16, usize>(idx).unwrap(),
                    )),
                    "INT" => RowValue::Value(Value::Int32(
                        fetched_row.try_get::<i32, usize>(idx).unwrap(),
                    )),
                    "BIGINT" => RowValue::Value(Value::Int64(
                        fetched_row.try_get::<i64, usize>(idx).unwrap(),
                    )),
                    "FLOAT" => RowValue::Value(Value::Float32(datatypes::value::OrderedFloat(
                        fetched_row.try_get::<f32, usize>(idx).unwrap(),
                    ))),
                    "DOUBLE" => RowValue::Value(Value::Float64(datatypes::value::OrderedFloat(
                        fetched_row.try_get::<f64, usize>(idx).unwrap(),
                    ))),
                    "VARCHAR" | "CHAR" | "TEXT" => RowValue::Value(Value::String(
                        fetched_row.try_get::<String, usize>(idx).unwrap().into(),
                    )),
                    "VARBINARY" | "BINARY" | "BLOB" => RowValue::Value(Value::Binary(
                        fetched_row.try_get::<Vec<u8>, usize>(idx).unwrap().into(),
                    )),
                    "TIMESTAMP" => RowValue::Value(Value::Timestamp(
                        Timestamp::from_chrono_datetime(
                            fetched_row
                                .try_get::<ChronoDateTime<Utc>, usize>(idx)
                                .unwrap()
                                .naive_utc(),
                        )
                        .unwrap(),
                    )),
                    "DATETIME" => RowValue::Value(Value::Timestamp(
                        Timestamp::from_chrono_datetime(
                            fetched_row
                                .try_get::<ChronoDateTime<Utc>, usize>(idx)
                                .unwrap()
                                .naive_utc(),
                        )
                        .unwrap(),
                    )),
                    "DATE" => RowValue::Value(Value::Date(Date::from(
                        fetched_row.try_get::<NaiveDate, usize>(idx).unwrap(),
                    ))),
                    _ => panic!("Unsupported type: {}", value_type),
                }
            };

            let value = match value {
                // In MySQL, boolean is stored as TINYINT(1)
                RowValue::Value(Value::Boolean(v)) => RowValue::Value(Value::Int8(*v as i8)),
                RowValue::Default => match columns[idx].default_value().unwrap().clone() {
                    Value::Boolean(v) => RowValue::Value(Value::Int8(v as i8)),
                    default_value => RowValue::Value(default_value),
                },
                _ => value.clone(),
            };
            ensure!(
                value == fetched_value,
                error::AssertSnafu {
                    reason: format!("Expected value: {:?}, got: {:?}", value, fetched_value)
                }
            )
        }
    }

    Ok(())
}

#[derive(Debug, sqlx::FromRow)]
pub struct ValueCount {
    pub count: i64,
}

pub async fn count_values(db: &MySqlPool, sql: &str) -> Result<ValueCount> {
    sqlx::query_as::<_, ValueCount>(sql)
        .fetch_one(db)
        .await
        .context(error::ExecuteQuerySnafu { sql })
}

/// Returns all [RowEntry] of the `table_name`.
pub async fn fetch_values(db: &MySqlPool, sql: &str) -> Result<Vec<MySqlRow>> {
    sqlx::query(sql)
        .fetch_all(db)
        .await
        .context(error::ExecuteQuerySnafu { sql })
}
