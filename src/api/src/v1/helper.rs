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

use greptime_proto::v1::value::ValueData;
use greptime_proto::v1::{ColumnDataType, ColumnSchema, Row, SemanticType, Value};

/// Create a time index [ColumnSchema] with column's name and datatype.
/// Other fields are left default.
/// Useful when you just want to create a simple [ColumnSchema] without providing much struct fields.
pub fn time_index_column_schema(name: &str, datatype: ColumnDataType) -> ColumnSchema {
    ColumnSchema {
        column_name: name.to_string(),
        datatype: datatype as i32,
        semantic_type: SemanticType::Timestamp as i32,
        ..Default::default()
    }
}

/// Create a tag [ColumnSchema] with column's name and datatype.
/// Other fields are left default.
/// Useful when you just want to create a simple [ColumnSchema] without providing much struct fields.
pub fn tag_column_schema(name: &str, datatype: ColumnDataType) -> ColumnSchema {
    ColumnSchema {
        column_name: name.to_string(),
        datatype: datatype as i32,
        semantic_type: SemanticType::Tag as i32,
        ..Default::default()
    }
}

/// Create a field [ColumnSchema] with column's name and datatype.
/// Other fields are left default.
/// Useful when you just want to create a simple [ColumnSchema] without providing much struct fields.
pub fn field_column_schema(name: &str, datatype: ColumnDataType) -> ColumnSchema {
    ColumnSchema {
        column_name: name.to_string(),
        datatype: datatype as i32,
        semantic_type: SemanticType::Field as i32,
        ..Default::default()
    }
}

/// Create a [Row] from [ValueData]s.
/// Useful when you don't want to write much verbose codes.
pub fn row(values: Vec<ValueData>) -> Row {
    Row {
        values: values
            .into_iter()
            .map(|x| Value {
                value_data: Some(x),
            })
            .collect::<Vec<_>>(),
    }
}
