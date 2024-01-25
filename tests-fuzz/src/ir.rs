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

pub use alter_expr::AlterTableExpr;
pub use create_expr::CreateTableExpr;
use datatypes::data_type::ConcreteDataType;
use datatypes::value::Value;
use derive_builder::Builder;
use lazy_static::lazy_static;
use rand::seq::SliceRandom;
use rand::Rng;
use serde::{Deserialize, Serialize};

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
}

impl_random!(ConcreteDataType, ColumnTypeGenerator, DATA_TYPES);
impl_random!(ConcreteDataType, TsColumnTypeGenerator, TS_DATA_TYPES);
impl_random!(
    ConcreteDataType,
    PartibleColumnTypeGenerator,
    PARTIBLE_DATA_TYPES
);

pub struct ColumnTypeGenerator;
pub struct TsColumnTypeGenerator;
pub struct PartibleColumnTypeGenerator;

/// Generates a random [Value].
pub fn generate_random_value<R: Rng>(
    rng: &mut R,
    datatype: &ConcreteDataType,
    random_str: Option<&dyn Random<String, R>>,
) -> Value {
    match datatype {
        &ConcreteDataType::Boolean(_) => Value::from(rng.gen::<bool>()),
        ConcreteDataType::Int16(_) => Value::from(rng.gen::<i16>()),
        ConcreteDataType::Int32(_) => Value::from(rng.gen::<i32>()),
        ConcreteDataType::Int64(_) => Value::from(rng.gen::<i64>()),
        ConcreteDataType::Float32(_) => Value::from(rng.gen::<f32>()),
        ConcreteDataType::Float64(_) => Value::from(rng.gen::<f64>()),
        ConcreteDataType::String(_) => match random_str {
            Some(random) => Value::from(random.gen(rng)),
            None => Value::from(rng.gen::<char>().to_string()),
        },
        ConcreteDataType::Date(_) => Value::from(rng.gen::<i32>()),
        ConcreteDataType::DateTime(_) => Value::from(rng.gen::<i64>()),
        &ConcreteDataType::Timestamp(_) => Value::from(rng.gen::<u64>()),

        _ => unimplemented!("unsupported type: {datatype}"),
    }
}

/// The IR column.
#[derive(Debug, Builder, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Column {
    #[builder(setter(into))]
    pub name: String,
    pub column_type: ConcreteDataType,
    #[builder(default, setter(into))]
    pub options: Vec<ColumnOption>,
}

impl Column {
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

/// Generates columns with given `names`.
pub fn generate_columns<R: Rng + 'static>(
    rng: &mut R,
    names: impl IntoIterator<Item = String>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_droppable_columns() {
        let columns = vec![
            Column {
                name: "hi".to_string(),
                column_type: ConcreteDataType::uint64_datatype(),
                options: vec![ColumnOption::PrimaryKey],
            },
            Column {
                name: "foo".to_string(),
                column_type: ConcreteDataType::uint64_datatype(),
                options: vec![ColumnOption::TimeIndex],
            },
        ];
        let droppable = droppable_columns(&columns);
        assert!(droppable.is_empty());

        let columns = vec![
            Column {
                name: "hi".to_string(),
                column_type: ConcreteDataType::uint64_datatype(),
                options: vec![],
            },
            Column {
                name: "foo".to_string(),
                column_type: ConcreteDataType::uint64_datatype(),
                options: vec![],
            },
        ];
        let droppable = droppable_columns(&columns);
        assert_eq!(droppable.len(), 2);
    }
}
