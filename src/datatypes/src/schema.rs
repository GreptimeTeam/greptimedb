mod constraint;
mod raw;

use std::collections::HashMap;
use std::sync::Arc;

pub use arrow::datatypes::Metadata;
use arrow::datatypes::{Field, Schema as ArrowSchema};
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};

use crate::data_type::{ConcreteDataType, DataType};
use crate::error::{self, DeserializeSnafu, Error, Result, SerializeSnafu};
pub use crate::schema::constraint::ColumnDefaultConstraint;
pub use crate::schema::raw::RawSchema;
use crate::vectors::VectorRef;

/// Key used to store whether the column is time index in arrow field's metadata.
const TIME_INDEX_KEY: &str = "greptime:time_index";
/// Key used to store version number of the schema in metadata.
const VERSION_KEY: &str = "greptime:version";
/// Key used to store default constraint in arrow field's metadata.
const ARROW_FIELD_DEFAULT_CONSTRAINT_KEY: &str = "greptime:default_constraint";

/// Schema of a column, used as an immutable struct.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: ConcreteDataType,
    is_nullable: bool,
    is_time_index: bool,
    default_constraint: Option<ColumnDefaultConstraint>,
    metadata: Metadata,
}

impl ColumnSchema {
    pub fn new<T: Into<String>>(
        name: T,
        data_type: ConcreteDataType,
        is_nullable: bool,
    ) -> ColumnSchema {
        ColumnSchema {
            name: name.into(),
            data_type,
            is_nullable,
            is_time_index: false,
            default_constraint: None,
            metadata: Metadata::new(),
        }
    }

    #[inline]
    pub fn is_time_index(&self) -> bool {
        self.is_time_index
    }

    #[inline]
    pub fn is_nullable(&self) -> bool {
        self.is_nullable
    }

    #[inline]
    pub fn default_constraint(&self) -> Option<&ColumnDefaultConstraint> {
        self.default_constraint.as_ref()
    }

    #[inline]
    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    pub fn with_time_index(mut self, is_time_index: bool) -> Self {
        self.is_time_index = is_time_index;
        if is_time_index {
            self.metadata
                .insert(TIME_INDEX_KEY.to_string(), "true".to_string());
        } else {
            self.metadata.remove(TIME_INDEX_KEY);
        }
        self
    }

    pub fn with_default_constraint(
        mut self,
        default_constraint: Option<ColumnDefaultConstraint>,
    ) -> Result<Self> {
        if let Some(constraint) = &default_constraint {
            constraint.validate(&self.data_type, self.is_nullable)?;
        }

        self.default_constraint = default_constraint;
        Ok(self)
    }

    /// Creates a new [`ColumnSchema`] with given metadata.
    pub fn with_metadata(mut self, metadata: Metadata) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn create_default_vector(&self, num_rows: usize) -> Result<Option<VectorRef>> {
        match &self.default_constraint {
            Some(c) => c
                .create_default_vector(&self.data_type, self.is_nullable, num_rows)
                .map(Some),
            None => {
                if self.is_nullable {
                    // No default constraint, use null as default value.
                    // TODO(yingwen): Use NullVector once it supports setting logical type.
                    ColumnDefaultConstraint::null_value()
                        .create_default_vector(&self.data_type, self.is_nullable, num_rows)
                        .map(Some)
                } else {
                    Ok(None)
                }
            }
        }
    }
}

/// A common schema, should be immutable.
#[derive(Debug, Clone, PartialEq)]
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
    /// Panics when ColumnSchema's `default_constrait` can't be serialized into json.
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
    pub fn metadata(&self) -> &Metadata {
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
    metadata: Metadata,
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

        let arrow_schema = ArrowSchema::from(self.fields).with_metadata(self.metadata);

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
        column_schema.data_type.is_timestamp(),
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

impl TryFrom<&Field> for ColumnSchema {
    type Error = Error;

    fn try_from(field: &Field) -> Result<ColumnSchema> {
        let data_type = ConcreteDataType::try_from(&field.data_type)?;
        let mut metadata = field.metadata.clone();
        let default_constraint = match metadata.remove(ARROW_FIELD_DEFAULT_CONSTRAINT_KEY) {
            Some(json) => Some(serde_json::from_str(&json).context(DeserializeSnafu { json })?),
            None => None,
        };
        let is_time_index = metadata.contains_key(TIME_INDEX_KEY);

        Ok(ColumnSchema {
            name: field.name.clone(),
            data_type,
            is_nullable: field.is_nullable,
            is_time_index,
            default_constraint,
            metadata,
        })
    }
}

impl TryFrom<&ColumnSchema> for Field {
    type Error = Error;

    fn try_from(column_schema: &ColumnSchema) -> Result<Field> {
        let mut metadata = column_schema.metadata.clone();
        if let Some(value) = &column_schema.default_constraint {
            // Adds an additional metadata to store the default constraint.
            let old = metadata.insert(
                ARROW_FIELD_DEFAULT_CONSTRAINT_KEY.to_string(),
                serde_json::to_string(&value).context(SerializeSnafu)?,
            );

            ensure!(
                old.is_none(),
                error::DuplicateMetaSnafu {
                    key: ARROW_FIELD_DEFAULT_CONSTRAINT_KEY,
                }
            );
        }

        Ok(Field::new(
            column_schema.name.clone(),
            column_schema.data_type.as_arrow_type(),
            column_schema.is_nullable(),
        )
        .with_metadata(metadata))
    }
}

impl TryFrom<Arc<ArrowSchema>> for Schema {
    type Error = Error;

    fn try_from(arrow_schema: Arc<ArrowSchema>) -> Result<Schema> {
        let mut column_schemas = Vec::with_capacity(arrow_schema.fields.len());
        let mut name_to_index = HashMap::with_capacity(arrow_schema.fields.len());
        for field in &arrow_schema.fields {
            let column_schema = ColumnSchema::try_from(field)?;
            name_to_index.insert(field.name.clone(), column_schemas.len());
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

fn try_parse_version(metadata: &Metadata, key: &str) -> Result<u32> {
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
    use arrow::datatypes::DataType as ArrowDataType;

    use super::*;
    use crate::value::Value;

    #[test]
    fn test_column_schema() {
        let column_schema = ColumnSchema::new("test", ConcreteDataType::int32_datatype(), true);
        let field = Field::try_from(&column_schema).unwrap();
        assert_eq!("test", field.name);
        assert_eq!(ArrowDataType::Int32, field.data_type);
        assert!(field.is_nullable);

        let new_column_schema = ColumnSchema::try_from(&field).unwrap();
        assert_eq!(column_schema, new_column_schema);
    }

    #[test]
    fn test_column_schema_with_default_constraint() {
        let column_schema = ColumnSchema::new("test", ConcreteDataType::int32_datatype(), true)
            .with_default_constraint(Some(ColumnDefaultConstraint::Value(Value::from(99))))
            .unwrap();
        assert!(column_schema
            .metadata()
            .get(ARROW_FIELD_DEFAULT_CONSTRAINT_KEY)
            .is_none());

        let field = Field::try_from(&column_schema).unwrap();
        assert_eq!("test", field.name);
        assert_eq!(ArrowDataType::Int32, field.data_type);
        assert!(field.is_nullable);
        assert_eq!(
            "{\"Value\":{\"Int32\":99}}",
            field
                .metadata
                .get(ARROW_FIELD_DEFAULT_CONSTRAINT_KEY)
                .unwrap()
        );

        let new_column_schema = ColumnSchema::try_from(&field).unwrap();
        assert_eq!(column_schema, new_column_schema);
    }

    #[test]
    fn test_column_schema_with_metadata() {
        let mut metadata = Metadata::new();
        metadata.insert("k1".to_string(), "v1".to_string());
        let column_schema = ColumnSchema::new("test", ConcreteDataType::int32_datatype(), true)
            .with_metadata(metadata)
            .with_default_constraint(Some(ColumnDefaultConstraint::null_value()))
            .unwrap();
        assert_eq!("v1", column_schema.metadata().get("k1").unwrap());
        assert!(column_schema
            .metadata()
            .get(ARROW_FIELD_DEFAULT_CONSTRAINT_KEY)
            .is_none());

        let field = Field::try_from(&column_schema).unwrap();
        assert_eq!("v1", field.metadata.get("k1").unwrap());
        assert!(field
            .metadata
            .get(ARROW_FIELD_DEFAULT_CONSTRAINT_KEY)
            .is_some());

        let new_column_schema = ColumnSchema::try_from(&field).unwrap();
        assert_eq!(column_schema, new_column_schema);
    }

    #[test]
    fn test_column_schema_with_duplicate_metadata() {
        let mut metadata = Metadata::new();
        metadata.insert(
            ARROW_FIELD_DEFAULT_CONSTRAINT_KEY.to_string(),
            "v1".to_string(),
        );
        let column_schema = ColumnSchema::new("test", ConcreteDataType::int32_datatype(), true)
            .with_metadata(metadata)
            .with_default_constraint(Some(ColumnDefaultConstraint::null_value()))
            .unwrap();
        Field::try_from(&column_schema).unwrap_err();
    }

    #[test]
    fn test_column_schema_invalid_default_constraint() {
        ColumnSchema::new("test", ConcreteDataType::int32_datatype(), false)
            .with_default_constraint(Some(ColumnDefaultConstraint::null_value()))
            .unwrap_err();
    }

    #[test]
    fn test_column_default_constraint_try_into_from() {
        let default_constraint = ColumnDefaultConstraint::Value(Value::from(42i64));

        let bytes: Vec<u8> = default_constraint.clone().try_into().unwrap();
        let from_value = ColumnDefaultConstraint::try_from(&bytes[..]).unwrap();

        assert_eq!(default_constraint, from_value);
    }

    #[test]
    fn test_column_schema_create_default_null() {
        // Implicit default null.
        let column_schema = ColumnSchema::new("test", ConcreteDataType::int32_datatype(), true);
        let v = column_schema.create_default_vector(5).unwrap().unwrap();
        assert_eq!(5, v.len());
        assert!(v.only_null());

        // Explicit default null.
        let column_schema = ColumnSchema::new("test", ConcreteDataType::int32_datatype(), true)
            .with_default_constraint(Some(ColumnDefaultConstraint::null_value()))
            .unwrap();
        let v = column_schema.create_default_vector(5).unwrap().unwrap();
        assert_eq!(5, v.len());
        assert!(v.only_null());
    }

    #[test]
    fn test_column_schema_no_default() {
        let column_schema = ColumnSchema::new("test", ConcreteDataType::int32_datatype(), false);
        assert!(column_schema.create_default_vector(5).unwrap().is_none());
    }

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
            ColumnSchema::new("ts", ConcreteDataType::timestamp_millis_datatype(), false)
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
