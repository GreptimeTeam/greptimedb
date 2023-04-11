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

use std::sync::Arc;

use datatypes::prelude::*;
use datatypes::schema::ColumnSchema;
use datatypes::vectors::{
    BinaryVector, BooleanVector, Float32Vector, Float64Vector, Int16Vector, Int32Vector,
    Int64Vector, Int8Vector, NullVector, StringVector, UInt16Vector, UInt32Vector, UInt64Vector,
    UInt8Vector,
};
use mysql_async::prelude::FromRow;
use mysql_async::{FromRowError, Value as MysqlValue};
use opensrv_mysql::ColumnType;

mod mysql_server_test;
mod mysql_writer_test;

pub struct TestingData {
    column_schemas: Vec<ColumnSchema>,
    mysql_columns_def: Vec<ColumnType>,
    columns: Vec<VectorRef>,
    mysql_text_output_rows: Vec<Vec<Value>>,
}

impl TestingData {
    fn new(
        column_schemas: Vec<ColumnSchema>,
        mysql_columns_def: Vec<ColumnType>,
        columns: Vec<VectorRef>,
        mysql_text_output_rows: Vec<Vec<Value>>,
    ) -> Self {
        // Check input columns have same size,
        assert_eq!(column_schemas.len(), mysql_columns_def.len());
        assert_eq!(column_schemas.len(), columns.len());
        // and all columns length are equal
        assert!(columns.windows(2).all(|x| x[0].len() == x[1].len()));
        // and all output rows width are equal
        assert!(mysql_text_output_rows
            .windows(2)
            .all(|x| x[0].len() == x[1].len()));
        // and the rows' columns size equals to input columns size.
        assert_eq!(columns.first().unwrap().len(), mysql_text_output_rows.len());

        TestingData {
            column_schemas,
            mysql_columns_def,
            columns,
            mysql_text_output_rows,
        }
    }
}

#[derive(Debug)]
struct MysqlTextRow {
    values: Vec<Value>,
}

impl FromRow for MysqlTextRow {
    fn from_row_opt(row: mysql_async::Row) -> Result<Self, FromRowError>
    where
        Self: Sized,
    {
        let mut values = Vec::with_capacity(row.len());
        for i in 0..row.len() {
            let value = if let Some(mysql_value) = row.as_ref(i) {
                match mysql_value {
                    MysqlValue::NULL => Value::Null,
                    MysqlValue::Bytes(v) => Value::from(v.clone()),
                    _ => unreachable!(),
                }
            } else {
                Value::Null
            };
            values.push(value);
        }
        Ok(MysqlTextRow { values })
    }
}

pub fn all_datatype_testing_data() -> TestingData {
    let column_schemas = vec![
        ColumnSchema::new("nulls", ConcreteDataType::null_datatype(), true),
        ColumnSchema::new("bools", ConcreteDataType::boolean_datatype(), true),
        ColumnSchema::new("int8s", ConcreteDataType::int8_datatype(), true),
        ColumnSchema::new("int16s", ConcreteDataType::int16_datatype(), true),
        ColumnSchema::new("int32s", ConcreteDataType::int32_datatype(), true),
        ColumnSchema::new("int64s", ConcreteDataType::int64_datatype(), true),
        ColumnSchema::new("uint8s", ConcreteDataType::uint8_datatype(), true),
        ColumnSchema::new("uint16s", ConcreteDataType::uint16_datatype(), true),
        ColumnSchema::new("uint32s", ConcreteDataType::uint32_datatype(), true),
        ColumnSchema::new("uint64s", ConcreteDataType::uint64_datatype(), true),
        ColumnSchema::new("float32s", ConcreteDataType::float32_datatype(), true),
        ColumnSchema::new("float64s", ConcreteDataType::float64_datatype(), true),
        ColumnSchema::new("binaries", ConcreteDataType::binary_datatype(), true),
        ColumnSchema::new("strings", ConcreteDataType::string_datatype(), true),
    ];
    let mysql_columns_def = vec![
        ColumnType::MYSQL_TYPE_NULL,
        ColumnType::MYSQL_TYPE_TINY,
        ColumnType::MYSQL_TYPE_TINY,
        ColumnType::MYSQL_TYPE_SHORT,
        ColumnType::MYSQL_TYPE_LONG,
        ColumnType::MYSQL_TYPE_LONGLONG,
        ColumnType::MYSQL_TYPE_TINY,
        ColumnType::MYSQL_TYPE_SHORT,
        ColumnType::MYSQL_TYPE_LONG,
        ColumnType::MYSQL_TYPE_LONGLONG,
        ColumnType::MYSQL_TYPE_FLOAT,
        ColumnType::MYSQL_TYPE_DOUBLE,
        ColumnType::MYSQL_TYPE_VARCHAR,
        ColumnType::MYSQL_TYPE_VARCHAR,
    ];
    let columns: Vec<VectorRef> = vec![
        Arc::new(NullVector::new(4)),
        Arc::new(BooleanVector::from(vec![
            Some(true),
            None,
            Some(false),
            None,
        ])),
        Arc::new(Int8Vector::from(vec![
            Some(i8::MIN),
            None,
            Some(i8::MAX),
            None,
        ])),
        Arc::new(Int16Vector::from(vec![
            Some(i16::MIN),
            None,
            Some(i16::MAX),
            None,
        ])),
        Arc::new(Int32Vector::from(vec![
            Some(i32::MIN),
            None,
            Some(i32::MAX),
            None,
        ])),
        Arc::new(Int64Vector::from(vec![
            Some(i64::MIN),
            None,
            Some(i64::MAX),
            None,
        ])),
        Arc::new(UInt8Vector::from(vec![
            Some(u8::MIN),
            None,
            Some(u8::MAX),
            None,
        ])),
        Arc::new(UInt16Vector::from(vec![
            Some(u16::MIN),
            None,
            Some(u16::MAX),
            None,
        ])),
        Arc::new(UInt32Vector::from(vec![
            Some(u32::MIN),
            None,
            Some(u32::MAX),
            None,
        ])),
        Arc::new(UInt64Vector::from(vec![
            Some(u64::MIN),
            None,
            Some(u64::MAX),
            None,
        ])),
        Arc::new(Float32Vector::from(vec![
            Some(-1.123456_f32),
            None,
            Some(1.654321),
            None,
        ])),
        Arc::new(Float64Vector::from(vec![
            Some(-10.123456_f64),
            None,
            Some(10.654321),
            None,
        ])),
        Arc::new(BinaryVector::from(vec![
            None,
            Some("hello".as_bytes().to_vec()),
            Some("greptime".as_bytes().to_vec()),
            None,
        ])),
        Arc::new(StringVector::from(vec![
            Some("hola"),
            None,
            None,
            Some("GT"),
        ])),
    ];

    // Because we can only use MySQL text protocol (binary protocol requires prepared statement,
    // which we are not implemented yet), every MysqlValue is of type "Bytes"
    let mysql_text_output_rows = vec![
        vec![
            Value::Null,
            Value::from("1".as_bytes()),
            Value::from(i8::MIN.to_string().as_bytes()),
            Value::from(i16::MIN.to_string().as_bytes()),
            Value::from(i32::MIN.to_string().as_bytes()),
            Value::from(i64::MIN.to_string().as_bytes()),
            Value::from(u8::MIN.to_string().as_bytes()),
            Value::from(u16::MIN.to_string().as_bytes()),
            Value::from(u32::MIN.to_string().as_bytes()),
            Value::from(u64::MIN.to_string().as_bytes()),
            Value::from((-1.123456_f32).to_string().as_bytes()),
            Value::from((-10.123456_f64).to_string().as_bytes()),
            Value::Null,
            Value::from("hola".as_bytes()),
        ],
        vec![
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::from("hello".as_bytes()),
            Value::Null,
        ],
        vec![
            Value::Null,
            Value::from("0".as_bytes()),
            Value::from(i8::MAX.to_string().as_bytes()),
            Value::from(i16::MAX.to_string().as_bytes()),
            Value::from(i32::MAX.to_string().as_bytes()),
            Value::from(i64::MAX.to_string().as_bytes()),
            Value::from(u8::MAX.to_string().as_bytes()),
            Value::from(u16::MAX.to_string().as_bytes()),
            Value::from(u32::MAX.to_string().as_bytes()),
            Value::from(u64::MAX.to_string().as_bytes()),
            Value::from(1.654321_f32.to_string().as_bytes()),
            Value::from(10.654321_f64.to_string().as_bytes()),
            Value::from("greptime".as_bytes()),
            Value::Null,
        ],
        vec![
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::from("GT".as_bytes()),
        ],
    ];
    TestingData::new(
        column_schemas,
        mysql_columns_def,
        columns,
        mysql_text_output_rows,
    )
}
