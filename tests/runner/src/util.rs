// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use client::api::v1::column::Values;
use client::api::v1::ColumnDataType;

pub fn values_to_string(data_type: ColumnDataType, values: Values) -> Vec<String> {
    match data_type {
        ColumnDataType::Int64 => values
            .i64_values
            .into_iter()
            .map(|val| val.to_string())
            .collect(),
        ColumnDataType::Float64 => values
            .f64_values
            .into_iter()
            .map(|val| val.to_string())
            .collect(),
        ColumnDataType::String => values.string_values,
        ColumnDataType::Boolean => values
            .bool_values
            .into_iter()
            .map(|val| val.to_string())
            .collect(),
        ColumnDataType::Int8 => values
            .i8_values
            .into_iter()
            .map(|val| val.to_string())
            .collect(),
        ColumnDataType::Int16 => values
            .i16_values
            .into_iter()
            .map(|val| val.to_string())
            .collect(),
        ColumnDataType::Int32 => values
            .i32_values
            .into_iter()
            .map(|val| val.to_string())
            .collect(),
        ColumnDataType::Uint8 => values
            .u8_values
            .into_iter()
            .map(|val| val.to_string())
            .collect(),
        ColumnDataType::Uint16 => values
            .u16_values
            .into_iter()
            .map(|val| val.to_string())
            .collect(),
        ColumnDataType::Uint32 => values
            .u32_values
            .into_iter()
            .map(|val| val.to_string())
            .collect(),
        ColumnDataType::Uint64 => values
            .u64_values
            .into_iter()
            .map(|val| val.to_string())
            .collect(),
        ColumnDataType::Float32 => values
            .f32_values
            .into_iter()
            .map(|val| val.to_string())
            .collect(),
        ColumnDataType::Binary => values
            .binary_values
            .into_iter()
            .map(|val| format!("{:?}", val))
            .collect(),
        ColumnDataType::Datetime => values
            .i64_values
            .into_iter()
            .map(|v| v.to_string())
            .collect(),
        ColumnDataType::Date => values
            .i32_values
            .into_iter()
            .map(|v| v.to_string())
            .collect(),
        ColumnDataType::Timestamp => values
            .ts_millis_values
            .into_iter()
            .map(|v| v.to_string())
            .collect(),
    }
}
