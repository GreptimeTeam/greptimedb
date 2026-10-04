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

use api::v1::{ColumnSchema, Row};

use crate::Event;

/// Asserts the type, additional schema, and additional rows of an event.
pub fn assert_event_contract<E>(
    event: &E,
    expected_event_type: &str,
    expected_schema: &[ColumnSchema],
    expected_rows: &[Row],
) where
    E: Event + ?Sized,
{
    assert_eq!(event.event_type(), expected_event_type);
    assert_eq!(event.extra_schema(), expected_schema);
    assert_eq!(event.extra_rows().expect("event rows"), expected_rows);
}

#[cfg(test)]
mod tests {
    use std::any::Any;

    use api::v1::Row;
    use api::v1::value::ValueData;

    use super::*;
    use crate::error::Result;
    use crate::event_table::{CATALOG_NAME_COLUMN, column_schemas, nullable_string};

    #[derive(Debug)]
    struct TestEvent;

    impl Event for TestEvent {
        fn event_type(&self) -> &str {
            "test_event_table"
        }

        fn extra_schema(&self) -> Vec<ColumnSchema> {
            column_schemas([&CATALOG_NAME_COLUMN])
        }

        fn extra_rows(&self) -> Result<Vec<Row>> {
            Ok(vec![Row {
                values: vec![nullable_string(Some("value"))],
            }])
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[test]
    fn event_contract_assertions_cover_type_schema_and_rows() {
        let event = TestEvent;
        let schema = vec![CATALOG_NAME_COLUMN.column_schema()];
        let rows = vec![Row {
            values: vec![ValueData::StringValue("value".to_string()).into()],
        }];

        assert_event_contract(&event, "test_event_table", &schema, &rows);
    }
}
