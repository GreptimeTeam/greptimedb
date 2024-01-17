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

pub(crate) mod create_expr;

pub use create_expr::CreateTableExpr;
use datatypes::data_type::ConcreteDataType;
use datatypes::value::Value;
use derive_builder::Builder;
use faker_rand::lorem::Word;
use lazy_static::lazy_static;
use rand::distributions::{Distribution, Standard};

use crate::ir::create_expr::ColumnOption;

lazy_static! {
    static ref DATA_TYPES: Vec<ConcreteDataType> = vec![
        ConcreteDataType::boolean_datatype(),
        ConcreteDataType::int16_datatype(),
        ConcreteDataType::int32_datatype(),
        ConcreteDataType::int64_datatype(),
        ConcreteDataType::float32_datatype(),
        ConcreteDataType::float64_datatype(),
    ];
    static ref TS_DATA_TYPES: Vec<ConcreteDataType> = vec![
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

/// Generates a random [Value].
pub fn generate_random_value(datatype: &ConcreteDataType) -> Value {
    match datatype {
        &ConcreteDataType::Boolean(_) => Value::from(rand::random::<bool>()),
        ConcreteDataType::Int16(_) => Value::from(rand::random::<i16>()),
        ConcreteDataType::Int32(_) => Value::from(rand::random::<i32>()),
        ConcreteDataType::Int64(_) => Value::from(rand::random::<i64>()),
        ConcreteDataType::Float32(_) => Value::from(rand::random::<f32>()),
        ConcreteDataType::Float64(_) => Value::from(rand::random::<f64>()),
        ConcreteDataType::String(_) => Value::from(rand::random::<char>().to_string()),
        ConcreteDataType::Date(_) => Value::from(rand::random::<i32>()),
        ConcreteDataType::DateTime(_) => Value::from(rand::random::<i64>()),

        _ => unimplemented!("unsupported type: {datatype}"),
    }
}

/// The IR column.
#[derive(Debug, Builder, Clone)]
pub struct Column {
    #[builder(setter(into))]
    pub name: String,
    pub column_type: ConcreteDataType,
    #[builder(default, setter(into))]
    pub options: Vec<ColumnOption>,
}

impl Distribution<Column> for Standard {
    fn sample<R: rand::prelude::Rng + ?Sized>(&self, rng: &mut R) -> Column {
        let column_type = DATA_TYPES[rng.gen_range(0..DATA_TYPES.len())].clone();
        // 0 -> NULL
        // 1 -> NOT NULL
        // 2 -> DEFAULT VALUE
        // 3 -> PRIMARY KEY
        // 4 -> EMPTY
        let option_idx = rng.gen_range(0..5);
        let options = match option_idx {
            0 => vec![ColumnOption::Null],
            1 => vec![ColumnOption::NotNull],
            2 => vec![ColumnOption::DefaultValue(generate_random_value(
                &column_type,
            ))],
            3 => vec![ColumnOption::PrimaryKey],
            _ => vec![],
        };

        Column {
            name: rng.gen::<Word>().to_string(),
            column_type,
            options,
        }
    }
}

/// The IR ts column.
pub struct TsColumn(pub Column);

impl Distribution<TsColumn> for Standard {
    fn sample<R: rand::prelude::Rng + ?Sized>(&self, rng: &mut R) -> TsColumn {
        let column_type = TS_DATA_TYPES[rng.gen_range(0..TS_DATA_TYPES.len())].clone();
        TsColumn(Column {
            name: rng.gen::<Word>().to_string(),
            column_type,
            options: vec![],
        })
    }
}

/// The IR partible column.
pub struct PartibleColumn(pub Column);

impl Distribution<PartibleColumn> for Standard {
    fn sample<R: rand::prelude::Rng + ?Sized>(&self, rng: &mut R) -> PartibleColumn {
        let column_type = PARTIBLE_DATA_TYPES[rng.gen_range(0..PARTIBLE_DATA_TYPES.len())].clone();
        PartibleColumn(Column {
            name: rng.gen::<Word>().to_string(),
            column_type,
            options: vec![],
        })
    }
}
