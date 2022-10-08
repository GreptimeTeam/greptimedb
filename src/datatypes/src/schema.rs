mod constraint;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

pub use arrow::datatypes::Metadata;
use arrow::datatypes::{Field, Schema as ArrowSchema};
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt};

use crate::data_type::{ConcreteDataType, DataType};
use crate::error::{self, DeserializeSnafu, Error, Result, SerializeSnafu};
pub use crate::schema::constraint::ColumnDefaultConstraint;

/// Key used to store column name of the timestamp column in metadata.
///
/// Instead of storing the column index, we store the column name as the
/// query engine may modify the column order of the arrow schema, then
/// we would fail to recover the correct timestamp column when converting
/// the arrow schema back to our schema.
const TIMESTAMP_COLUMN_KEY: &str = "greptime:timestamp_column";
/// Key used to store version number of the schema in metadata.
const VERSION_KEY: &str = "greptime:version";
/// Key used to store default constraint in arrow field's metadata.
const ARROW_FIELD_DEFAULT_CONSTRAINT_KEY: &str = "greptime:default_constraint";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: ConcreteDataType,
    pub is_nullable: bool,
    pub default_constraint: Option<ColumnDefaultConstraint>,
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
            default_constraint: None,
        }
    }

    pub fn with_default_constraint(
        mut self,
        default_constraint: Option<ColumnDefaultConstraint>,
    ) -> Self {
        self.default_constraint = default_constraint;
        self
    }
}

/// A common schema, should be immutable.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    /// # Panics
    /// Panics when ColumnSchema's `default_constrait` can't be serialized into json.
    pub fn new(column_schemas: Vec<ColumnSchema>) -> Schema {
        // Builder won't fail
        SchemaBuilder::try_from(column_schemas)
            .unwrap()
            .build()
            .unwrap()
    }

    pub fn try_new(column_schemas: Vec<ColumnSchema>) -> Result<Schema> {
        // Builder won't fail
        Ok(SchemaBuilder::try_from(column_schemas)?.build().unwrap())
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
        let (fields, name_to_index) = collect_fields(&column_schemas)?;

        Ok(Self {
            column_schemas,
            name_to_index,
            fields,
            ..Default::default()
        })
    }

    /// Set timestamp index.
    ///
    /// The validation of timestamp column is done in `build()`.
    pub fn timestamp_index(mut self, timestamp_index: usize) -> Self {
        self.timestamp_index = Some(timestamp_index);
        self
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
            let timestamp_name = self.column_schemas[timestamp_index].name.clone();
            self.metadata
                .insert(TIMESTAMP_COLUMN_KEY.to_string(), timestamp_name);
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

fn collect_fields(column_schemas: &[ColumnSchema]) -> Result<(Vec<Field>, HashMap<String, usize>)> {
    let mut fields = Vec::with_capacity(column_schemas.len());
    let mut name_to_index = HashMap::with_capacity(column_schemas.len());
    for (index, column_schema) in column_schemas.iter().enumerate() {
        let field = Field::try_from(column_schema)?;
        fields.push(field);
        name_to_index.insert(column_schema.name.clone(), index);
    }

    Ok((fields, name_to_index))
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

    Ok(())
}

pub type SchemaRef = Arc<Schema>;

impl TryFrom<&Field> for ColumnSchema {
    type Error = Error;

    fn try_from(field: &Field) -> Result<ColumnSchema> {
        let data_type = ConcreteDataType::try_from(&field.data_type)?;
        let default_constraint = match field.metadata.get(ARROW_FIELD_DEFAULT_CONSTRAINT_KEY) {
            Some(json) => Some(serde_json::from_str(json).context(DeserializeSnafu { json })?),
            None => None,
        };

        Ok(ColumnSchema {
            name: field.name.clone(),
            data_type,
            is_nullable: field.is_nullable,
            default_constraint,
        })
    }
}

impl TryFrom<&ColumnSchema> for Field {
    type Error = Error;

    fn try_from(column_schema: &ColumnSchema) -> Result<Field> {
        let metadata = if let Some(value) = &column_schema.default_constraint {
            let mut m = BTreeMap::new();
            m.insert(
                ARROW_FIELD_DEFAULT_CONSTRAINT_KEY.to_string(),
                serde_json::to_string(&value).context(SerializeSnafu)?,
            );
            m
        } else {
            BTreeMap::default()
        };

        Ok(Field::new(
            column_schema.name.clone(),
            column_schema.data_type.as_arrow_type(),
            column_schema.is_nullable,
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

        let timestamp_name = arrow_schema.metadata.get(TIMESTAMP_COLUMN_KEY);
        let mut timestamp_index = None;
        if let Some(name) = timestamp_name {
            let index = name_to_index
                .get(name)
                .context(error::TimestampNotFoundSnafu { name })?;
            validate_timestamp_index(&column_schemas, *index)?;
            timestamp_index = Some(*index);
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
            .with_default_constraint(Some(ColumnDefaultConstraint::Value(Value::from(99))));
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
    fn test_column_default_constraint_try_into_from() {
        let default_constraint = ColumnDefaultConstraint::Value(Value::from(42i64));

        let bytes: Vec<u8> = default_constraint.clone().try_into().unwrap();
        let from_value = ColumnDefaultConstraint::try_from(&bytes[..]).unwrap();

        assert_eq!(default_constraint, from_value);
    }

    #[test]
    fn test_build_empty_schema() {
        let schema = SchemaBuilder::default().build().unwrap();
        assert_eq!(0, schema.num_columns());
        assert!(schema.is_empty());

        assert!(SchemaBuilder::default().timestamp_index(0).build().is_err());
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
            ColumnSchema::new("ts", ConcreteDataType::timestamp_millis_datatype(), false),
        ];
        let schema = SchemaBuilder::try_from(column_schemas.clone())
            .unwrap()
            .timestamp_index(1)
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
            ColumnSchema::new("col1", ConcreteDataType::int32_datatype(), true),
            ColumnSchema::new("col2", ConcreteDataType::float64_datatype(), false),
        ];
        assert!(SchemaBuilder::try_from(column_schemas.clone())
            .unwrap()
            .timestamp_index(0)
            .build()
            .is_err());
        assert!(SchemaBuilder::try_from(column_schemas.clone())
            .unwrap()
            .timestamp_index(1)
            .build()
            .is_err());
        assert!(SchemaBuilder::try_from(column_schemas)
            .unwrap()
            .timestamp_index(1)
            .build()
            .is_err());
    }
}
