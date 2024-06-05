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

//! The intermediate representation

pub(crate) mod alter_expr;
pub(crate) mod create_expr;
pub(crate) mod insert_expr;
pub(crate) mod select_expr;

use core::fmt;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

pub use alter_expr::AlterTableExpr;
use common_time::{Date, DateTime, Timestamp};
pub use create_expr::{CreateDatabaseExpr, CreateTableExpr};
use datatypes::data_type::ConcreteDataType;
use datatypes::types::TimestampType;
use datatypes::value::Value;
use derive_builder::Builder;
pub use insert_expr::InsertIntoExpr;
use lazy_static::lazy_static;
use rand::seq::SliceRandom;
use rand::Rng;
use serde::{Deserialize, Serialize};

use self::insert_expr::{RowValue, RowValues};
use crate::context::TableContextRef;
use crate::generator::Random;
use crate::impl_random;
use crate::ir::create_expr::ColumnOption;

lazy_static! {
    pub static ref DATA_TYPES: Vec<ConcreteDataType> = vec![
        ConcreteDataType::boolean_datatype(),
        ConcreteDataType::int16_datatype(),
        ConcreteDataType::int32_datatype(),
        ConcreteDataType::int64_datatype(),
        ConcreteDataType::float32_datatype(),
        ConcreteDataType::float64_datatype(),
    ];
    pub static ref TS_DATA_TYPES: Vec<ConcreteDataType> = vec![
        ConcreteDataType::timestamp_nanosecond_datatype(),
        ConcreteDataType::timestamp_microsecond_datatype(),
        ConcreteDataType::timestamp_millisecond_datatype(),
        ConcreteDataType::timestamp_second_datatype(),
    ];
    pub static ref PARTIBLE_DATA_TYPES: Vec<ConcreteDataType> = vec![
        ConcreteDataType::int16_datatype(),
        ConcreteDataType::int32_datatype(),
        ConcreteDataType::int64_datatype(),
        ConcreteDataType::float32_datatype(),
        ConcreteDataType::float64_datatype(),
        ConcreteDataType::string_datatype(),
        ConcreteDataType::date_datatype(),
        ConcreteDataType::datetime_datatype(),
    ];
    pub static ref STRING_DATA_TYPES: Vec<ConcreteDataType> =
        vec![ConcreteDataType::string_datatype()];
    pub static ref MYSQL_TS_DATA_TYPES: Vec<ConcreteDataType> = vec![
        // MySQL only permits fractional seconds with up to microseconds (6 digits) precision.
        ConcreteDataType::timestamp_microsecond_datatype(),
        ConcreteDataType::timestamp_millisecond_datatype(),
        ConcreteDataType::timestamp_second_datatype(),
    ];
}

impl_random!(ConcreteDataType, ColumnTypeGenerator, DATA_TYPES);
impl_random!(ConcreteDataType, TsColumnTypeGenerator, TS_DATA_TYPES);
impl_random!(
    ConcreteDataType,
    MySQLTsColumnTypeGenerator,
    MYSQL_TS_DATA_TYPES
);
impl_random!(
    ConcreteDataType,
    PartibleColumnTypeGenerator,
    PARTIBLE_DATA_TYPES
);
impl_random!(
    ConcreteDataType,
    StringColumnTypeGenerator,
    STRING_DATA_TYPES
);

pub struct ColumnTypeGenerator;
pub struct TsColumnTypeGenerator;
pub struct MySQLTsColumnTypeGenerator;
pub struct PartibleColumnTypeGenerator;
pub struct StringColumnTypeGenerator;

/// Generates a random [Value].
pub fn generate_random_value<R: Rng>(
    rng: &mut R,
    datatype: &ConcreteDataType,
    random_str: Option<&dyn Random<Ident, R>>,
) -> Value {
    match datatype {
        &ConcreteDataType::Boolean(_) => Value::from(rng.gen::<bool>()),
        ConcreteDataType::Int16(_) => Value::from(rng.gen::<i16>()),
        ConcreteDataType::Int32(_) => Value::from(rng.gen::<i32>()),
        ConcreteDataType::Int64(_) => Value::from(rng.gen::<i64>()),
        ConcreteDataType::Float32(_) => Value::from(rng.gen::<f32>()),
        ConcreteDataType::Float64(_) => Value::from(rng.gen::<f64>()),
        ConcreteDataType::String(_) => match random_str {
            Some(random) => Value::from(random.gen(rng).value),
            None => Value::from(rng.gen::<char>().to_string()),
        },
        ConcreteDataType::Date(_) => generate_random_date(rng),
        ConcreteDataType::DateTime(_) => generate_random_datetime(rng),

        _ => unimplemented!("unsupported type: {datatype}"),
    }
}

/// Generate monotonically increasing timestamps for MySQL.
pub fn generate_unique_timestamp_for_mysql<R: Rng>(
    base: i64,
) -> impl Fn(&mut R, TimestampType) -> Value {
    let base = Arc::new(AtomicI64::new(base));

    move |_rng, ts_type| -> Value {
        let value = base.fetch_add(1, Ordering::Relaxed);
        let v = match ts_type {
            TimestampType::Second(_) => Timestamp::new_second(1 + value),
            TimestampType::Millisecond(_) => Timestamp::new_millisecond(1000 + value),
            TimestampType::Microsecond(_) => Timestamp::new_microsecond(1_000_000 + value),
            TimestampType::Nanosecond(_) => Timestamp::new_nanosecond(1_000_000_000 + value),
        };
        Value::from(v)
    }
}

/// Generate random timestamps.
pub fn generate_random_timestamp<R: Rng>(rng: &mut R, ts_type: TimestampType) -> Value {
    let v = match ts_type {
        TimestampType::Second(_) => {
            let min = i64::from(Timestamp::MIN_SECOND);
            let max = i64::from(Timestamp::MAX_SECOND);
            let value = rng.gen_range(min..=max);
            Timestamp::new_second(value)
        }
        TimestampType::Millisecond(_) => {
            let min = i64::from(Timestamp::MIN_MILLISECOND);
            let max = i64::from(Timestamp::MAX_MILLISECOND);
            let value = rng.gen_range(min..=max);
            Timestamp::new_millisecond(value)
        }
        TimestampType::Microsecond(_) => {
            let min = i64::from(Timestamp::MIN_MICROSECOND);
            let max = i64::from(Timestamp::MAX_MICROSECOND);
            let value = rng.gen_range(min..=max);
            Timestamp::new_microsecond(value)
        }
        TimestampType::Nanosecond(_) => {
            let min = i64::from(Timestamp::MIN_NANOSECOND);
            let max = i64::from(Timestamp::MAX_NANOSECOND);
            let value = rng.gen_range(min..=max);
            Timestamp::new_nanosecond(value)
        }
    };
    Value::from(v)
}

// MySQL supports timestamp from '1970-01-01 00:00:01.000000' to '2038-01-19 03:14:07.499999'
pub fn generate_random_timestamp_for_mysql<R: Rng>(rng: &mut R, ts_type: TimestampType) -> Value {
    let v = match ts_type {
        TimestampType::Second(_) => {
            let min = 1;
            let max = 2_147_483_647;
            let value = rng.gen_range(min..=max);
            Timestamp::new_second(value)
        }
        TimestampType::Millisecond(_) => {
            let min = 1000;
            let max = 2_147_483_647_499;
            let value = rng.gen_range(min..=max);
            Timestamp::new_millisecond(value)
        }
        TimestampType::Microsecond(_) => {
            let min = 1_000_000;
            let max = 2_147_483_647_499_999;
            let value = rng.gen_range(min..=max);
            Timestamp::new_microsecond(value)
        }
        TimestampType::Nanosecond(_) => {
            let min = 1_000_000_000;
            let max = 2_147_483_647_499_999_000;
            let value = rng.gen_range(min..=max);
            Timestamp::new_nanosecond(value)
        }
    };
    Value::from(v)
}

fn generate_random_datetime<R: Rng>(rng: &mut R) -> Value {
    let min = i64::from(Timestamp::MIN_MILLISECOND);
    let max = i64::from(Timestamp::MAX_MILLISECOND);
    let value = rng.gen_range(min..=max);
    let datetime = Timestamp::new_millisecond(value)
        .to_chrono_datetime()
        .unwrap();
    Value::from(DateTime::from(datetime))
}

fn generate_random_date<R: Rng>(rng: &mut R) -> Value {
    let min = i64::from(Timestamp::MIN_MILLISECOND);
    let max = i64::from(Timestamp::MAX_MILLISECOND);
    let value = rng.gen_range(min..=max);
    let date = Timestamp::new_millisecond(value).to_chrono_date().unwrap();
    Value::from(Date::from(date))
}

/// An identifier.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct Ident {
    pub value: String,
    pub quote_style: Option<char>,
}

impl Ident {
    /// Creates a new identifier with the given value and no quotes.
    pub fn new<S>(value: S) -> Self
    where
        S: Into<String>,
    {
        Ident {
            value: value.into(),
            quote_style: None,
        }
    }

    /// Creates a new quoted identifier with the given quote and value.
    pub fn with_quote<S>(quote: char, value: S) -> Self
    where
        S: Into<String>,
    {
        Ident {
            value: value.into(),
            quote_style: Some(quote),
        }
    }
}

impl From<&str> for Ident {
    fn from(value: &str) -> Self {
        Ident {
            value: value.to_string(),
            quote_style: None,
        }
    }
}

impl From<String> for Ident {
    fn from(value: String) -> Self {
        Ident {
            value,
            quote_style: None,
        }
    }
}

impl fmt::Display for Ident {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.quote_style {
            Some(q) => write!(f, "{q}{}{q}", self.value),
            None => f.write_str(&self.value),
        }
    }
}

/// The IR column.
#[derive(Debug, Builder, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Column {
    #[builder(setter(into))]
    pub name: Ident,
    pub column_type: ConcreteDataType,
    #[builder(default, setter(into))]
    pub options: Vec<ColumnOption>,
}

impl Column {
    /// Returns [TimestampType] if it's [ColumnOption::TimeIndex] [Column].
    pub fn timestamp_type(&self) -> Option<TimestampType> {
        if let ConcreteDataType::Timestamp(ts_type) = self.column_type {
            Some(ts_type)
        } else {
            None
        }
    }

    /// Returns true if it's [ColumnOption::TimeIndex] [Column].
    pub fn is_time_index(&self) -> bool {
        self.options
            .iter()
            .any(|opt| opt == &ColumnOption::TimeIndex)
    }

    /// Returns true if it's the [ColumnOption::PrimaryKey] [Column].
    pub fn is_primary_key(&self) -> bool {
        self.options
            .iter()
            .any(|opt| opt == &ColumnOption::PrimaryKey)
    }

    /// Returns true if it's nullable.
    pub fn is_nullable(&self) -> bool {
        !self
            .options
            .iter()
            .any(|opt| matches!(opt, ColumnOption::NotNull | ColumnOption::TimeIndex))
    }

    // Returns true if it has default value.
    pub fn has_default_value(&self) -> bool {
        self.options.iter().any(|opt| {
            matches!(
                opt,
                ColumnOption::DefaultValue(_) | ColumnOption::DefaultFn(_)
            )
        })
    }

    // Returns default value if it has.
    pub fn default_value(&self) -> Option<&Value> {
        self.options.iter().find_map(|opt| match opt {
            ColumnOption::DefaultValue(value) => Some(value),
            _ => None,
        })
    }
}

/// Returns droppable columns. i.e., non-primary key columns, non-ts columns.
pub fn droppable_columns(columns: &[Column]) -> Vec<&Column> {
    columns
        .iter()
        .filter(|column| {
            !column.options.iter().any(|option| {
                option == &ColumnOption::PrimaryKey || option == &ColumnOption::TimeIndex
            })
        })
        .collect::<Vec<_>>()
}

/// Returns columns that can use the alter table modify command
pub fn modifiable_columns(columns: &[Column]) -> Vec<&Column> {
    columns
        .iter()
        .filter(|column| {
            !column.options.iter().any(|option| {
                option == &ColumnOption::PrimaryKey
                    || option == &ColumnOption::TimeIndex
                    || option == &ColumnOption::NotNull
            })
        })
        .collect::<Vec<_>>()
}

/// Generates [ColumnOption] for [Column].
pub fn column_options_generator<R: Rng>(
    rng: &mut R,
    column_type: &ConcreteDataType,
) -> Vec<ColumnOption> {
    // 0 -> NULL
    // 1 -> NOT NULL
    // 2 -> DEFAULT VALUE
    // 3 -> PRIMARY KEY
    // 4 -> EMPTY
    let option_idx = rng.gen_range(0..5);
    match option_idx {
        0 => vec![ColumnOption::Null],
        1 => vec![ColumnOption::NotNull],
        2 => vec![ColumnOption::DefaultValue(generate_random_value(
            rng,
            column_type,
            None,
        ))],
        3 => vec![ColumnOption::PrimaryKey],
        _ => vec![],
    }
}

/// Generates [ColumnOption] for Partible [Column].
pub fn partible_column_options_generator<R: Rng + 'static>(
    rng: &mut R,
    column_type: &ConcreteDataType,
) -> Vec<ColumnOption> {
    // 0 -> NULL
    // 1 -> NOT NULL
    // 2 -> DEFAULT VALUE
    // 3 -> PRIMARY KEY
    let option_idx = rng.gen_range(0..4);
    match option_idx {
        0 => vec![ColumnOption::PrimaryKey, ColumnOption::Null],
        1 => vec![ColumnOption::PrimaryKey, ColumnOption::NotNull],
        2 => vec![
            ColumnOption::PrimaryKey,
            ColumnOption::DefaultValue(generate_random_value(rng, column_type, None)),
        ],
        3 => vec![ColumnOption::PrimaryKey],
        _ => unreachable!(),
    }
}

/// Generates [ColumnOption] for ts [Column].
pub fn ts_column_options_generator<R: Rng + 'static>(
    _: &mut R,
    _: &ConcreteDataType,
) -> Vec<ColumnOption> {
    vec![ColumnOption::TimeIndex]
}

pub fn primary_key_and_not_null_column_options_generator<R: Rng + 'static>(
    _: &mut R,
    _: &ConcreteDataType,
) -> Vec<ColumnOption> {
    vec![ColumnOption::PrimaryKey, ColumnOption::NotNull]
}

pub fn primary_key_options_generator<R: Rng + 'static>(
    _: &mut R,
    _: &ConcreteDataType,
) -> Vec<ColumnOption> {
    vec![ColumnOption::PrimaryKey]
}

/// Generates columns with given `names`.
pub fn generate_columns<R: Rng + 'static>(
    rng: &mut R,
    names: impl IntoIterator<Item = Ident>,
    types: &(impl Random<ConcreteDataType, R> + ?Sized),
    options: impl Fn(&mut R, &ConcreteDataType) -> Vec<ColumnOption>,
) -> Vec<Column> {
    names
        .into_iter()
        .map(|name| {
            let column_type = types.gen(rng);
            let options = options(rng, &column_type);
            Column {
                name,
                options,
                column_type,
            }
        })
        .collect()
}

/// Replace Value::Default with the corresponding default value in the rows for comparison.
pub fn replace_default(
    rows: &[RowValues],
    table_ctx_ref: &TableContextRef,
    insert_expr: &InsertIntoExpr,
) -> Vec<RowValues> {
    let index_map: HashMap<usize, usize> = insert_expr
        .columns
        .iter()
        .enumerate()
        .map(|(insert_idx, insert_column)| {
            let create_idx = table_ctx_ref
                .columns
                .iter()
                .position(|create_column| create_column.name == insert_column.name)
                .expect("Column not found in create_expr");
            (insert_idx, create_idx)
        })
        .collect();

    let mut new_rows = Vec::new();
    for row in rows {
        let mut new_row = Vec::new();
        for (idx, value) in row.iter().enumerate() {
            if let RowValue::Default = value {
                let column = &table_ctx_ref.columns[index_map[&idx]];
                new_row.push(RowValue::Value(column.default_value().unwrap().clone()));
            } else {
                new_row.push(value.clone());
            }
        }
        new_rows.push(new_row);
    }
    new_rows
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_droppable_columns() {
        let columns = vec![
            Column {
                name: "hi".into(),
                column_type: ConcreteDataType::uint64_datatype(),
                options: vec![ColumnOption::PrimaryKey],
            },
            Column {
                name: "foo".into(),
                column_type: ConcreteDataType::uint64_datatype(),
                options: vec![ColumnOption::TimeIndex],
            },
        ];
        let droppable = droppable_columns(&columns);
        assert!(droppable.is_empty());

        let columns = vec![
            Column {
                name: "hi".into(),
                column_type: ConcreteDataType::uint64_datatype(),
                options: vec![],
            },
            Column {
                name: "foo".into(),
                column_type: ConcreteDataType::uint64_datatype(),
                options: vec![],
            },
        ];
        let droppable = droppable_columns(&columns);
        assert_eq!(droppable.len(), 2);
    }
}
