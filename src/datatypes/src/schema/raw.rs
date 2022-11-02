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
        let mut builder = SchemaBuilder::try_from(raw.column_schemas)?.version(raw.version);
        if let Some(idx) = raw.timestamp_index {
            builder = builder.timestamp_index(idx);
        }
        builder.build()
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
            ColumnSchema::new("ts", ConcreteDataType::timestamp_millis_datatype(), false),
        ];
        let schema = SchemaBuilder::try_from(column_schemas)
            .unwrap()
            .timestamp_index(1)
            .version(123)
            .build()
            .unwrap();

        let raw = RawSchema::from(&schema);
        let schema_new = Schema::try_from(raw).unwrap();

        assert_eq!(schema, schema_new);
    }
}
