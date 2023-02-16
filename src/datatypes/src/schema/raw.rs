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

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::schema::{ColumnSchema, Schema, SchemaBuilder};

/// Struct used to serialize and deserialize [`Schema`](crate::schema::Schema).
///
/// This struct only contains necessary data to recover the Schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawSchema {
    /// Schema of columns.
    pub column_schemas: Vec<ColumnSchema>,
    /// Index of the timestamp column.
    pub timestamp_index: Option<usize>,
    /// Schema version.
    pub version: u32,
}

impl RawSchema {
    /// Creates a new [RawSchema] from specific `column_schemas`.
    ///
    /// Sets [RawSchema::timestamp_index] to the first index of the timestamp
    /// column. It doesn't check whether time index column is duplicate.
    pub fn new(column_schemas: Vec<ColumnSchema>) -> RawSchema {
        let timestamp_index = column_schemas
            .iter()
            .position(|column_schema| column_schema.is_time_index());

        RawSchema {
            column_schemas,
            timestamp_index,
            version: 0,
        }
    }
}

impl TryFrom<RawSchema> for Schema {
    type Error = Error;

    fn try_from(raw: RawSchema) -> Result<Schema> {
        // While building Schema, we don't trust the fields, such as timestamp_index,
        // in RawSchema. We use SchemaBuilder to perform the validation.
        SchemaBuilder::try_from(raw.column_schemas)?
            .version(raw.version)
            .build()
    }
}

impl From<&Schema> for RawSchema {
    fn from(schema: &Schema) -> RawSchema {
        RawSchema {
            column_schemas: schema.column_schemas.clone(),
            timestamp_index: schema.timestamp_index,
            version: schema.version,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_type::ConcreteDataType;

    #[test]
    fn test_raw_convert() {
        let column_schemas = vec![
            ColumnSchema::new("col1", ConcreteDataType::int32_datatype(), true),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
        ];
        let schema = SchemaBuilder::try_from(column_schemas)
            .unwrap()
            .version(123)
            .build()
            .unwrap();

        let raw = RawSchema::from(&schema);
        let schema_new = Schema::try_from(raw).unwrap();

        assert_eq!(schema, schema_new);
    }

    #[test]
    fn test_new_raw_schema_with_time_index() {
        let column_schemas = vec![
            ColumnSchema::new("col1", ConcreteDataType::int32_datatype(), true),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
        ];
        let schema = RawSchema::new(column_schemas);
        assert_eq!(1, schema.timestamp_index.unwrap());
    }

    #[test]
    fn test_new_raw_schema_without_time_index() {
        let column_schemas = vec![
            ColumnSchema::new("col1", ConcreteDataType::int32_datatype(), true),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
        ];
        let schema = RawSchema::new(column_schemas);
        assert!(schema.timestamp_index.is_none());
    }
}
