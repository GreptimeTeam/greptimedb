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

mod column_schema;
mod constraint;
mod raw;

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{Field, Schema as ArrowSchema};
use datafusion_common::DFSchemaRef;
use snafu::{ensure, ResultExt};

use crate::data_type::DataType;
use crate::error::{self, Error, Result};
pub use crate::schema::column_schema::{ColumnSchema, Metadata};
pub use crate::schema::constraint::ColumnDefaultConstraint;
pub use crate::schema::raw::RawSchema;

/// Key used to store version number of the schema in metadata.
const VERSION_KEY: &str = "greptime:version";

/// A common schema, should be immutable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    column_schemas: Vec<ColumnSchema>,
    name_to_index: HashMap<String, usize>,
    arrow_schema: Arc<ArrowSchema>,
    /// Index of the timestamp key column.
    ///
    /// Timestamp key column is the column holds the timestamp and forms part of
    /// the primary key. None means there is no timestamp key column.
    timestamp_index: Option<usize>,
    /// Version of the schema.
    ///
    /// Initial value is zero. The version should bump after altering schema.
    version: u32,
}

impl Schema {
    /// Initial version of the schema.
    pub const INITIAL_VERSION: u32 = 0;

    /// Create a schema from a vector of [ColumnSchema].
    ///
    /// # Panics
    /// Panics when ColumnSchema's `default_constraint` can't be serialized into json.
    pub fn new(column_schemas: Vec<ColumnSchema>) -> Schema {
        // Builder won't fail in this case
        SchemaBuilder::try_from(column_schemas)
            .unwrap()
            .build()
            .unwrap()
    }

    /// Try to Create a schema from a vector of [ColumnSchema].
    pub fn try_new(column_schemas: Vec<ColumnSchema>) -> Result<Schema> {
        SchemaBuilder::try_from(column_schemas)?.build()
    }

    #[inline]
    pub fn arrow_schema(&self) -> &Arc<ArrowSchema> {
        &self.arrow_schema
    }

    #[inline]
    pub fn column_schemas(&self) -> &[ColumnSchema] {
        &self.column_schemas
    }

    pub fn column_schema_by_name(&self, name: &str) -> Option<&ColumnSchema> {
        self.name_to_index
            .get(name)
            .map(|index| &self.column_schemas[*index])
    }

    /// Retrieve the column's name by index
    /// # Panics
    /// This method **may** panic if the index is out of range of column schemas.
    #[inline]
    pub fn column_name_by_index(&self, idx: usize) -> &str {
        &self.column_schemas[idx].name
    }

    #[inline]
    pub fn column_index_by_name(&self, name: &str) -> Option<usize> {
        self.name_to_index.get(name).copied()
    }

    #[inline]
    pub fn contains_column(&self, name: &str) -> bool {
        self.name_to_index.contains_key(name)
    }

    #[inline]
    pub fn num_columns(&self) -> usize {
        self.column_schemas.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.column_schemas.is_empty()
    }

    /// Returns index of the timestamp key column.
    #[inline]
    pub fn timestamp_index(&self) -> Option<usize> {
        self.timestamp_index
    }

    #[inline]
    pub fn timestamp_column(&self) -> Option<&ColumnSchema> {
        self.timestamp_index.map(|idx| &self.column_schemas[idx])
    }

    #[inline]
    pub fn version(&self) -> u32 {
        self.version
    }

    #[inline]
    pub fn metadata(&self) -> &HashMap<String, String> {
        &self.arrow_schema.metadata
    }
}

#[derive(Default)]
pub struct SchemaBuilder {
    column_schemas: Vec<ColumnSchema>,
    name_to_index: HashMap<String, usize>,
    fields: Vec<Field>,
    timestamp_index: Option<usize>,
    version: u32,
    metadata: HashMap<String, String>,
}

impl TryFrom<Vec<ColumnSchema>> for SchemaBuilder {
    type Error = Error;

    fn try_from(column_schemas: Vec<ColumnSchema>) -> Result<SchemaBuilder> {
        SchemaBuilder::try_from_columns(column_schemas)
    }
}

impl SchemaBuilder {
    pub fn try_from_columns(column_schemas: Vec<ColumnSchema>) -> Result<Self> {
        let FieldsAndIndices {
            fields,
            name_to_index,
            timestamp_index,
        } = collect_fields(&column_schemas)?;

        Ok(Self {
            column_schemas,
            name_to_index,
            fields,
            timestamp_index,
            ..Default::default()
        })
    }

    pub fn version(mut self, version: u32) -> Self {
        self.version = version;
        self
    }

    /// Add key value pair to metadata.
    ///
    /// Old metadata with same key would be overwritten.
    pub fn add_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    pub fn build(mut self) -> Result<Schema> {
        if let Some(timestamp_index) = self.timestamp_index {
            validate_timestamp_index(&self.column_schemas, timestamp_index)?;
        }

        self.metadata
            .insert(VERSION_KEY.to_string(), self.version.to_string());

        let arrow_schema = ArrowSchema::new(self.fields).with_metadata(self.metadata);

        Ok(Schema {
            column_schemas: self.column_schemas,
            name_to_index: self.name_to_index,
            arrow_schema: Arc::new(arrow_schema),
            timestamp_index: self.timestamp_index,
            version: self.version,
        })
    }
}

struct FieldsAndIndices {
    fields: Vec<Field>,
    name_to_index: HashMap<String, usize>,
    timestamp_index: Option<usize>,
}

fn collect_fields(column_schemas: &[ColumnSchema]) -> Result<FieldsAndIndices> {
    let mut fields = Vec::with_capacity(column_schemas.len());
    let mut name_to_index = HashMap::with_capacity(column_schemas.len());
    let mut timestamp_index = None;
    for (index, column_schema) in column_schemas.iter().enumerate() {
        if column_schema.is_time_index() {
            ensure!(
                timestamp_index.is_none(),
                error::DuplicateTimestampIndexSnafu {
                    exists: timestamp_index.unwrap(),
                    new: index,
                }
            );
            timestamp_index = Some(index);
        }
        let field = Field::try_from(column_schema)?;
        fields.push(field);
        name_to_index.insert(column_schema.name.clone(), index);
    }

    Ok(FieldsAndIndices {
        fields,
        name_to_index,
        timestamp_index,
    })
}

fn validate_timestamp_index(column_schemas: &[ColumnSchema], timestamp_index: usize) -> Result<()> {
    ensure!(
        timestamp_index < column_schemas.len(),
        error::InvalidTimestampIndexSnafu {
            index: timestamp_index,
        }
    );

    let column_schema = &column_schemas[timestamp_index];
    ensure!(
        column_schema.data_type.is_timestamp_compatible(),
        error::InvalidTimestampIndexSnafu {
            index: timestamp_index,
        }
    );
    ensure!(
        column_schema.is_time_index(),
        error::InvalidTimestampIndexSnafu {
            index: timestamp_index,
        }
    );

    Ok(())
}

pub type SchemaRef = Arc<Schema>;

impl TryFrom<Arc<ArrowSchema>> for Schema {
    type Error = Error;

    fn try_from(arrow_schema: Arc<ArrowSchema>) -> Result<Schema> {
        let mut column_schemas = Vec::with_capacity(arrow_schema.fields.len());
        let mut name_to_index = HashMap::with_capacity(arrow_schema.fields.len());
        for field in &arrow_schema.fields {
            let column_schema = ColumnSchema::try_from(field)?;
            name_to_index.insert(field.name().to_string(), column_schemas.len());
            column_schemas.push(column_schema);
        }

        let mut timestamp_index = None;
        for (index, column_schema) in column_schemas.iter().enumerate() {
            if column_schema.is_time_index() {
                validate_timestamp_index(&column_schemas, index)?;
                ensure!(
                    timestamp_index.is_none(),
                    error::DuplicateTimestampIndexSnafu {
                        exists: timestamp_index.unwrap(),
                        new: index,
                    }
                );
                timestamp_index = Some(index);
            }
        }

        let version = try_parse_version(&arrow_schema.metadata, VERSION_KEY)?;

        Ok(Self {
            column_schemas,
            name_to_index,
            arrow_schema,
            timestamp_index,
            version,
        })
    }
}

impl TryFrom<ArrowSchema> for Schema {
    type Error = Error;

    fn try_from(arrow_schema: ArrowSchema) -> Result<Schema> {
        let arrow_schema = Arc::new(arrow_schema);

        Schema::try_from(arrow_schema)
    }
}

impl TryFrom<DFSchemaRef> for Schema {
    type Error = Error;

    fn try_from(value: DFSchemaRef) -> Result<Self> {
        let s: ArrowSchema = value.as_ref().into();
        s.try_into()
    }
}

fn try_parse_version(metadata: &HashMap<String, String>, key: &str) -> Result<u32> {
    if let Some(value) = metadata.get(key) {
        let version = value
            .parse()
            .context(error::ParseSchemaVersionSnafu { value })?;

        Ok(version)
    } else {
        Ok(Schema::INITIAL_VERSION)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_type::ConcreteDataType;

    #[test]
    fn test_build_empty_schema() {
        let schema = SchemaBuilder::default().build().unwrap();
        assert_eq!(0, schema.num_columns());
        assert!(schema.is_empty());
    }

    #[test]
    fn test_schema_no_timestamp() {
        let column_schemas = vec![
            ColumnSchema::new("col1", ConcreteDataType::int32_datatype(), false),
            ColumnSchema::new("col2", ConcreteDataType::float64_datatype(), true),
        ];
        let schema = Schema::new(column_schemas.clone());

        assert_eq!(2, schema.num_columns());
        assert!(!schema.is_empty());
        assert!(schema.timestamp_index().is_none());
        assert!(schema.timestamp_column().is_none());
        assert_eq!(Schema::INITIAL_VERSION, schema.version());

        for column_schema in &column_schemas {
            let found = schema.column_schema_by_name(&column_schema.name).unwrap();
            assert_eq!(column_schema, found);
        }
        assert!(schema.column_schema_by_name("col3").is_none());

        let new_schema = Schema::try_from(schema.arrow_schema().clone()).unwrap();

        assert_eq!(schema, new_schema);
        assert_eq!(column_schemas, schema.column_schemas());
    }

    #[test]
    fn test_metadata() {
        let column_schemas = vec![ColumnSchema::new(
            "col1",
            ConcreteDataType::int32_datatype(),
            false,
        )];
        let schema = SchemaBuilder::try_from(column_schemas)
            .unwrap()
            .add_metadata("k1", "v1")
            .build()
            .unwrap();

        assert_eq!("v1", schema.metadata().get("k1").unwrap());
    }

    #[test]
    fn test_schema_with_timestamp() {
        let column_schemas = vec![
            ColumnSchema::new("col1", ConcreteDataType::int32_datatype(), true),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
        ];
        let schema = SchemaBuilder::try_from(column_schemas.clone())
            .unwrap()
            .version(123)
            .build()
            .unwrap();

        assert_eq!(1, schema.timestamp_index().unwrap());
        assert_eq!(&column_schemas[1], schema.timestamp_column().unwrap());
        assert_eq!(123, schema.version());

        let new_schema = Schema::try_from(schema.arrow_schema().clone()).unwrap();
        assert_eq!(1, schema.timestamp_index().unwrap());
        assert_eq!(schema, new_schema);
    }

    #[test]
    fn test_schema_wrong_timestamp() {
        let column_schemas = vec![
            ColumnSchema::new("col1", ConcreteDataType::int32_datatype(), true)
                .with_time_index(true),
            ColumnSchema::new("col2", ConcreteDataType::float64_datatype(), false),
        ];
        assert!(SchemaBuilder::try_from(column_schemas)
            .unwrap()
            .build()
            .is_err());

        let column_schemas = vec![
            ColumnSchema::new("col1", ConcreteDataType::int32_datatype(), true),
            ColumnSchema::new("col2", ConcreteDataType::float64_datatype(), false)
                .with_time_index(true),
        ];

        assert!(SchemaBuilder::try_from(column_schemas)
            .unwrap()
            .build()
            .is_err());
    }
}
