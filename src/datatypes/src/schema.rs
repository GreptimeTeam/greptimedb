use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{Field, Schema as ArrowSchema};
use serde::{Deserialize, Serialize};

use crate::data_type::{ConcreteDataType, DataType};
use crate::error::{Error, Result};

// TODO(yingwen): consider assign a version to schema so compare schema can be
// done by compare version.

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: ConcreteDataType,
    pub is_nullable: bool,
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
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Schema {
    column_schemas: Vec<ColumnSchema>,
    name_to_index: HashMap<String, usize>,
    arrow_schema: Arc<ArrowSchema>,
}

impl Schema {
    pub fn new(column_schemas: Vec<ColumnSchema>) -> Schema {
        let mut fields = Vec::with_capacity(column_schemas.len());
        let mut name_to_index = HashMap::with_capacity(column_schemas.len());
        for (index, column_schema) in column_schemas.iter().enumerate() {
            let field = Field::from(column_schema);
            fields.push(field);
            name_to_index.insert(column_schema.name.clone(), index);
        }
        let arrow_schema = Arc::new(ArrowSchema::from(fields));

        Schema {
            column_schemas,
            name_to_index,
            arrow_schema,
        }
    }

    pub fn arrow_schema(&self) -> &Arc<ArrowSchema> {
        &self.arrow_schema
    }

    pub fn column_schemas(&self) -> &[ColumnSchema] {
        &self.column_schemas
    }

    pub fn column_schema_by_name(&self, name: &str) -> Option<&ColumnSchema> {
        self.name_to_index
            .get(name)
            .map(|index| &self.column_schemas[*index])
    }

    #[inline]
    pub fn num_columns(&self) -> usize {
        self.column_schemas.len()
    }
}

pub type SchemaRef = Arc<Schema>;

impl TryFrom<&Field> for ColumnSchema {
    type Error = Error;

    fn try_from(field: &Field) -> Result<ColumnSchema> {
        let data_type = ConcreteDataType::try_from(&field.data_type)?;

        Ok(ColumnSchema {
            name: field.name.clone(),
            data_type,
            is_nullable: field.is_nullable,
        })
    }
}

impl From<&ColumnSchema> for Field {
    fn from(column_schema: &ColumnSchema) -> Field {
        Field::new(
            column_schema.name.clone(),
            column_schema.data_type.as_arrow_type(),
            column_schema.is_nullable,
        )
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

        Ok(Self {
            column_schemas,
            name_to_index,
            arrow_schema,
        })
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType as ArrowDataType;

    use super::*;

    #[test]
    fn test_column_schema() {
        let column_schema = ColumnSchema::new("test", ConcreteDataType::int32_datatype(), true);
        let field = Field::from(&column_schema);
        assert_eq!("test", field.name);
        assert_eq!(ArrowDataType::Int32, field.data_type);
        assert!(field.is_nullable);

        let new_column_schema = ColumnSchema::try_from(&field).unwrap();
        assert_eq!(column_schema, new_column_schema);
    }

    #[test]
    fn test_schema() {
        let column_schemas = vec![
            ColumnSchema::new("col1", ConcreteDataType::int32_datatype(), false),
            ColumnSchema::new("col2", ConcreteDataType::float64_datatype(), true),
        ];
        let schema = Schema::new(column_schemas.clone());

        assert_eq!(2, schema.num_columns());

        for column_schema in &column_schemas {
            let found = schema.column_schema_by_name(&column_schema.name).unwrap();
            assert_eq!(column_schema, found);
        }
        assert!(schema.column_schema_by_name("col3").is_none());

        let fields: Vec<_> = column_schemas.iter().map(Field::from).collect();
        let arrow_schema = Arc::new(ArrowSchema::from(fields));

        let new_schema = Schema::try_from(arrow_schema.clone()).unwrap();

        assert_eq!(schema, new_schema);
        assert_eq!(column_schemas, schema.column_schemas());
        assert_eq!(arrow_schema, *schema.arrow_schema());
        assert_eq!(arrow_schema, *new_schema.arrow_schema());
    }
}
