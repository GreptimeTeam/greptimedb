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

use api::v1::Row;
use common_time::Timestamp;

/// Extracts the timestamp value from a row at the given column index.
/// Returns None if the index is invalid or the value is not a timestamp type.
pub fn extract_timestamp_from_row(row: &Row, timestamp_index: usize) -> Option<Timestamp> {
    if timestamp_index >= row.values.len() {
        return None;
    }

    let value = &row.values[timestamp_index];
    let value_data = value.value_data.as_ref()?;

    use api::v1::value::ValueData;
    match value_data {
        ValueData::TimestampSecondValue(v) => {
            Some(Timestamp::new(*v, common_time::timestamp::TimeUnit::Second))
        }
        ValueData::TimestampMillisecondValue(v) => {
            Some(Timestamp::new(*v, common_time::timestamp::TimeUnit::Millisecond))
        }
        ValueData::TimestampMicrosecondValue(v) => {
            Some(Timestamp::new(*v, common_time::timestamp::TimeUnit::Microsecond))
        }
        ValueData::TimestampNanosecondValue(v) => {
            Some(Timestamp::new(*v, common_time::timestamp::TimeUnit::Nanosecond))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use api::v1::value::ValueData;
    use api::v1::Value;
    use common_time::timestamp::TimeUnit;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use std::sync::Arc;

    #[test]
    fn test_extract_timestamp_from_row() {
        let _schema = build_test_schema();
        let timestamp_index = 1; // Second column is timestamp

        // Create a row with timestamp 1000ms
        let row = Row {
            values: vec![
                Value {
                    value_data: Some(ValueData::I64Value(1)),
                },
                Value {
                    value_data: Some(ValueData::TimestampMillisecondValue(1000)),
                },
            ],
        };

        let result = extract_timestamp_from_row(&row, timestamp_index);
        assert!(result.is_some());
        let ts = result.unwrap();
        assert_eq!(ts.value(), 1000);
        assert_eq!(ts.unit(), TimeUnit::Millisecond);
    }

    #[test]
    fn test_extract_timestamp_invalid_index() {
        let _schema = build_test_schema();
        let row = Row {
            values: vec![
                Value {
                    value_data: Some(ValueData::I64Value(1)),
                },
            ],
        };

        // Index out of bounds
        let result = extract_timestamp_from_row(&row, 10);
        assert!(result.is_none());
    }

    #[test]
    fn test_extract_timestamp_wrong_type() {
        let row = Row {
            values: vec![
                Value {
                    value_data: Some(ValueData::I64Value(1)),
                },
                Value {
                    value_data: Some(ValueData::StringValue("not a timestamp".to_string())),
                },
            ],
        };

        let result = extract_timestamp_from_row(&row, 1);
        assert!(result.is_none());
    }

    fn build_test_schema() -> Arc<Schema> {
        let columns = vec![
            ColumnSchema::new("id", ConcreteDataType::int64_datatype(), false),
            ColumnSchema::new("ts", ConcreteDataType::timestamp_millisecond_datatype(), false),
        ];
        Arc::new(Schema::new(columns))
    }
}
