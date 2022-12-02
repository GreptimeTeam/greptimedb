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

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::schema::{ColumnSchema, Schema, SchemaBuilder};

/// Struct used to serialize and deserialize [`Schema`](crate::schema::Schema).
///
/// This struct only contains necessary data to recover the Schema.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RawSchema {
    pub column_schemas: Vec<ColumnSchema>,
    pub timestamp_index: Option<usize>,
    pub version: u32,
}

impl TryFrom<RawSchema> for Schema {
    type Error = Error;

    fn try_from(raw: RawSchema) -> Result<Schema> {
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
}
