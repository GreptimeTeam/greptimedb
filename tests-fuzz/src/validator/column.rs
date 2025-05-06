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

use common_telemetry::debug;
use datatypes::data_type::DataType;
use snafu::{ensure, ResultExt};
use sqlx::MySqlPool;

use crate::error::{self, Result, UnexpectedSnafu};
use crate::ir::create_expr::ColumnOption;
use crate::ir::{Column, Ident};

#[derive(Debug, sqlx::FromRow)]
pub struct ColumnEntry {
    pub table_schema: String,
    pub table_name: String,
    pub column_name: String,
    pub data_type: String,
    pub semantic_type: String,
    pub column_default: Option<String>,
    pub is_nullable: String,
}

fn is_nullable(str: &str) -> bool {
    str.to_uppercase() == "YES"
}

enum SemanticType {
    Timestamp,
    Field,
    Tag,
}

fn semantic_type(str: &str) -> Option<SemanticType> {
    match str {
        "TIMESTAMP" => Some(SemanticType::Timestamp),
        "FIELD" => Some(SemanticType::Field),
        "TAG" => Some(SemanticType::Tag),
        _ => None,
    }
}

impl PartialEq<Column> for ColumnEntry {
    fn eq(&self, other: &Column) -> bool {
        // Checks `table_name`
        if other.name.value != self.column_name {
            debug!(
                "expected name: {}, got: {}",
                other.name.value, self.column_name
            );
            return false;
        }
        // Checks `data_type`
        if other.column_type.name() != self.data_type {
            debug!(
                "expected column_type: {}, got: {}",
                other.column_type.name(),
                self.data_type
            );
            return false;
        }
        // Checks `column_default`
        match &self.column_default {
            Some(value) => {
                let default_value_opt = other.options.iter().find(|opt| {
                    matches!(
                        opt,
                        ColumnOption::DefaultFn(_) | ColumnOption::DefaultValue(_)
                    )
                });
                if default_value_opt.is_none() {
                    debug!("default value options is not found");
                    return false;
                }
                let default_value = match default_value_opt.unwrap() {
                    ColumnOption::DefaultValue(v) => v.to_string(),
                    ColumnOption::DefaultFn(f) => f.to_string(),
                    _ => unreachable!(),
                };
                if &default_value != value {
                    debug!("expected default value: {default_value}, got: {value}");
                    return false;
                }
            }
            None => {
                if other.options.iter().any(|opt| {
                    matches!(
                        opt,
                        ColumnOption::DefaultFn(_) | ColumnOption::DefaultValue(_)
                    )
                }) {
                    return false;
                }
            }
        };
        // Checks `is_nullable`
        if is_nullable(&self.is_nullable) {
            // Null is the default value. Therefore, we only ensure there is no `ColumnOption::NotNull` option.
            if other
                .options
                .iter()
                .any(|opt| matches!(opt, ColumnOption::NotNull))
            {
                debug!("ColumnOption::NotNull is found");
                return false;
            }
        } else {
            // `ColumnOption::TimeIndex` imply means the field is not nullable.
            if !other
                .options
                .iter()
                .any(|opt| matches!(opt, ColumnOption::NotNull | ColumnOption::TimeIndex))
            {
                debug!("ColumnOption::NotNull or ColumnOption::TimeIndex is not found");
                return false;
            }
        }
        //TODO: Checks `semantic_type`
        match semantic_type(&self.semantic_type) {
            Some(SemanticType::Tag) => {
                if !other
                    .options
                    .iter()
                    .any(|opt| matches!(opt, ColumnOption::PrimaryKey))
                {
                    debug!("ColumnOption::PrimaryKey is not found");
                    return false;
                }
            }
            Some(SemanticType::Field) => {
                if other
                    .options
                    .iter()
                    .any(|opt| matches!(opt, ColumnOption::PrimaryKey | ColumnOption::TimeIndex))
                {
                    debug!("unexpected ColumnOption::PrimaryKey or ColumnOption::TimeIndex");
                    return false;
                }
            }
            Some(SemanticType::Timestamp) => {
                if !other
                    .options
                    .iter()
                    .any(|opt| matches!(opt, ColumnOption::TimeIndex))
                {
                    debug!("ColumnOption::TimeIndex is not found");
                    return false;
                }
            }
            None => {
                debug!("unknown semantic type: {}", self.semantic_type);
                return false;
            }
        };

        true
    }
}

/// Asserts [&[ColumnEntry]] is equal to [&[Column]]
pub fn assert_eq(fetched_columns: &[ColumnEntry], columns: &[Column]) -> Result<()> {
    ensure!(
        columns.len() == fetched_columns.len(),
        error::AssertSnafu {
            reason: format!(
                "Expected columns length: {}, got: {}",
                columns.len(),
                fetched_columns.len(),
            )
        }
    );

    for (idx, fetched) in fetched_columns.iter().enumerate() {
        ensure!(
            fetched == &columns[idx],
            error::AssertSnafu {
                reason: format!(
                    "ColumnEntry {fetched:?} is not equal to Column {:?}",
                    columns[idx]
                )
            }
        );
    }

    Ok(())
}

/// Returns all [ColumnEntry] of the `table_name` from `information_schema`.
pub async fn fetch_columns(
    db: &MySqlPool,
    schema_name: Ident,
    table_name: Ident,
) -> Result<Vec<ColumnEntry>> {
    let sql = "SELECT table_schema, table_name, column_name, greptime_data_type as data_type, semantic_type, column_default, is_nullable FROM information_schema.columns WHERE table_schema = ? AND table_name = ?";
    sqlx::query_as::<_, ColumnEntry>(sql)
        .bind(schema_name.value.to_string())
        .bind(table_name.value.to_string())
        .fetch_all(db)
        .await
        .context(error::ExecuteQuerySnafu { sql })
}

/// validate that apart from marker column(if it still exists, every thing else is just default value)
#[allow(unused)]
pub async fn valid_marker_value(
    db: &MySqlPool,
    _schema_name: Ident,
    table_name: Ident,
) -> Result<bool> {
    let sql = format!("SELECT * FROM {table_name}");
    // cache is useless and buggy anyway since alter happens all the time
    // TODO(discord9): invalid prepared sql after alter
    let res = sqlx::query(&sql)
        .persistent(false)
        .fetch_all(db)
        .await
        .context(error::ExecuteQuerySnafu { sql })?;
    if res.len() != 1 {
        UnexpectedSnafu {
            violated: format!(
                "Expected one row after alter table, found {}:{:?}",
                res.len(),
                res
            ),
        }
        .fail()?
    }
    // TODO(discord9): make sure marker value is set
    Ok(true)
}

#[cfg(test)]
mod tests {
    use datatypes::data_type::{ConcreteDataType, DataType};
    use datatypes::value::Value;

    use super::ColumnEntry;
    use crate::ir::create_expr::ColumnOption;
    use crate::ir::{Column, Ident};

    #[test]
    fn test_column_eq() {
        common_telemetry::init_default_ut_logging();
        let column_entry = ColumnEntry {
            table_schema: String::new(),
            table_name: String::new(),
            column_name: "test".to_string(),
            data_type: ConcreteDataType::int8_datatype().name(),
            semantic_type: "FIELD".to_string(),
            column_default: None,
            is_nullable: "Yes".to_string(),
        };
        // Naive
        let column = Column {
            name: Ident::new("test"),
            column_type: ConcreteDataType::int8_datatype(),
            options: vec![],
        };
        assert!(column_entry == column);
        // With quote
        let column = Column {
            name: Ident::with_quote('\'', "test"),
            column_type: ConcreteDataType::int8_datatype(),
            options: vec![],
        };
        assert!(column_entry == column);
        // With default value
        let column_entry = ColumnEntry {
            table_schema: String::new(),
            table_name: String::new(),
            column_name: "test".to_string(),
            data_type: ConcreteDataType::int8_datatype().to_string(),
            semantic_type: "FIELD".to_string(),
            column_default: Some("1".to_string()),
            is_nullable: "Yes".to_string(),
        };
        let column = Column {
            name: Ident::with_quote('\'', "test"),
            column_type: ConcreteDataType::int8_datatype(),
            options: vec![ColumnOption::DefaultValue(Value::from(1))],
        };
        assert!(column_entry == column);
        // With default function
        let column_entry = ColumnEntry {
            table_schema: String::new(),
            table_name: String::new(),
            column_name: "test".to_string(),
            data_type: ConcreteDataType::int8_datatype().to_string(),
            semantic_type: "FIELD".to_string(),
            column_default: Some("Hello()".to_string()),
            is_nullable: "Yes".to_string(),
        };
        let column = Column {
            name: Ident::with_quote('\'', "test"),
            column_type: ConcreteDataType::int8_datatype(),
            options: vec![ColumnOption::DefaultFn("Hello()".to_string())],
        };
        assert!(column_entry == column);
    }
}
