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

use common_macro::{as_aggr_func_creator, AggrFuncTypeStore, ToRow};
use greptime_proto::v1::value::ValueData;
use greptime_proto::v1::{ColumnDataType, ColumnSchema, Row, SemanticType, Value};
use static_assertions::{assert_fields, assert_impl_all};

#[as_aggr_func_creator]
#[derive(Debug, Default, AggrFuncTypeStore)]
struct Foo {}

#[test]
#[allow(clippy::extra_unused_type_parameters)]
fn test_derive() {
    let _ = Foo::default();
    assert_fields!(Foo: input_types);
    assert_impl_all!(Foo: std::fmt::Debug, Default, common_query::logical_plan::accumulator::AggrFuncTypeStore);
}

#[derive(ToRow)]
struct ToRowOwned {
    my_value: i32,
    #[col(name = "string_value", datatype = "string", semantic = "tag")]
    my_string: String,
    my_bool: bool,
    my_float: f32,
    #[col(
        name = "timestamp_value",
        semantic = "Timestamp",
        datatype = "TimestampMillisecond"
    )]
    my_timestamp: i64,
}

#[test]
fn test_to_row() {
    let test = ToRowOwned {
        my_value: 1,
        my_string: "test".to_string(),
        my_bool: true,
        my_float: 1.0,
        my_timestamp: 1718563200000,
    };
    let row = test.to_row();
    assert_row(&row);
    let schema = test.schema();
    assert_schema(&schema);
}

#[derive(ToRow)]
struct ToRowRef<'a> {
    my_value: &'a i32,
    #[col(name = "string_value", datatype = "string", semantic = "tag")]
    my_string: &'a String,
    my_bool: &'a bool,
    my_float: &'a f32,
    #[col(
        name = "timestamp_value",
        semantic = "Timestamp",
        datatype = "TimestampMillisecond"
    )]
    my_timestamp: &'a i64,
}

#[test]
fn test_to_row_ref() {
    let string = "test".to_string();
    let test = ToRowRef {
        my_value: &1,
        my_string: &string,
        my_bool: &true,
        my_float: &1.0,
        my_timestamp: &1718563200000,
    };
    let row = test.to_row();
    assert_row(&row);
    let schema = test.schema();
    assert_schema(&schema);
}

#[derive(ToRow)]
struct ToRowOptional {
    my_value: Option<i32>,
    #[col(name = "string_value", datatype = "string", semantic = "tag")]
    my_string: Option<String>,
    my_bool: Option<bool>,
    my_float: Option<f32>,
    #[col(
        name = "timestamp_value",
        semantic = "Timestamp",
        datatype = "TimestampMillisecond"
    )]
    my_timestamp: i64,
}

#[test]
fn test_to_row_optional() {
    let test = ToRowOptional {
        my_value: None,
        my_string: None,
        my_bool: None,
        my_float: None,
        my_timestamp: 1718563200000,
    };

    let row = test.to_row();
    assert_eq!(row.values.len(), 5);
    assert_eq!(row.values[0].value_data, None);
    assert_eq!(row.values[1].value_data, None);
    assert_eq!(row.values[2].value_data, None);
    assert_eq!(row.values[3].value_data, None);
    assert_eq!(
        row.values[4].value_data,
        Some(greptime_proto::v1::value::ValueData::TimestampMillisecondValue(1718563200000))
    );
}

fn assert_row(row: &Row) {
    assert_eq!(row.values.len(), 5);
    assert_eq!(
        row.values[0].value_data,
        Some(greptime_proto::v1::value::ValueData::I32Value(1))
    );
    assert_eq!(
        row.values[1].value_data,
        Some(greptime_proto::v1::value::ValueData::StringValue(
            "test".to_string()
        ))
    );
    assert_eq!(
        row.values[2].value_data,
        Some(greptime_proto::v1::value::ValueData::BoolValue(true))
    );
    assert_eq!(
        row.values[3].value_data,
        Some(greptime_proto::v1::value::ValueData::F32Value(1.0))
    );
    assert_eq!(
        row.values[4].value_data,
        Some(greptime_proto::v1::value::ValueData::TimestampMillisecondValue(1718563200000))
    );
}

fn assert_schema(schema: &[ColumnSchema]) {
    assert_eq!(schema.len(), 5);
    assert_eq!(schema[0].column_name, "my_value");
    assert_eq!(schema[0].datatype, ColumnDataType::Int32 as i32);
    assert_eq!(schema[0].semantic_type, SemanticType::Field as i32);
    assert_eq!(schema[1].column_name, "string_value");
    assert_eq!(schema[1].datatype, ColumnDataType::String as i32);
    assert_eq!(schema[1].semantic_type, SemanticType::Tag as i32);
    assert_eq!(schema[2].column_name, "my_bool");
    assert_eq!(schema[2].datatype, ColumnDataType::Boolean as i32);
    assert_eq!(schema[2].semantic_type, SemanticType::Field as i32);
    assert_eq!(schema[3].column_name, "my_float");
    assert_eq!(schema[3].datatype, ColumnDataType::Float32 as i32);
    assert_eq!(schema[3].semantic_type, SemanticType::Field as i32);
    assert_eq!(schema[4].column_name, "timestamp_value");
    assert_eq!(
        schema[4].datatype,
        ColumnDataType::TimestampMillisecond as i32
    );
    assert_eq!(schema[4].semantic_type, SemanticType::Timestamp as i32);
}
