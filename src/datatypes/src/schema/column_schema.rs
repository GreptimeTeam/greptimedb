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

use std::collections::BTreeMap;

use arrow::datatypes::Field;
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};

use crate::data_type::{ConcreteDataType, DataType};
use crate::error::{self, Error, Result};
use crate::schema::constraint::ColumnDefaultConstraint;
use crate::vectors::VectorRef;

pub type Metadata = BTreeMap<String, String>;

/// Key used to store whether the column is time index in arrow field's metadata.
const TIME_INDEX_KEY: &str = "greptime:time_index";
/// Key used to store default constraint in arrow field's metadata.
const DEFAULT_CONSTRAINT_KEY: &str = "greptime:default_constraint";

/// Schema of a column, used as an immutable struct.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

impl TryFrom<&Field> for ColumnSchema {
    type Error = Error;

    fn try_from(field: &Field) -> Result<ColumnSchema> {
        let data_type = ConcreteDataType::try_from(field.data_type())?;
        let mut metadata = field.metadata().cloned().unwrap_or_default();
        let default_constraint = match metadata.remove(DEFAULT_CONSTRAINT_KEY) {
            Some(json) => {
                Some(serde_json::from_str(&json).context(error::DeserializeSnafu { json })?)
            }
            None => None,
        };
        let is_time_index = metadata.contains_key(TIME_INDEX_KEY);

        Ok(ColumnSchema {
            name: field.name().clone(),
            data_type,
            is_nullable: field.is_nullable(),
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
                DEFAULT_CONSTRAINT_KEY.to_string(),
                serde_json::to_string(&value).context(error::SerializeSnafu)?,
            );

            ensure!(
                old.is_none(),
                error::DuplicateMetaSnafu {
                    key: DEFAULT_CONSTRAINT_KEY,
                }
            );
        }

        Ok(Field::new(
            &column_schema.name,
            column_schema.data_type.as_arrow_type(),
            column_schema.is_nullable(),
        )
        .with_metadata(Some(metadata)))
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
        assert_eq!("test", field.name());
        assert_eq!(ArrowDataType::Int32, *field.data_type());
        assert!(field.is_nullable());

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
            .get(DEFAULT_CONSTRAINT_KEY)
            .is_none());

        let field = Field::try_from(&column_schema).unwrap();
        assert_eq!("test", field.name());
        assert_eq!(ArrowDataType::Int32, *field.data_type());
        assert!(field.is_nullable());
        assert_eq!(
            "{\"Value\":{\"Int32\":99}}",
            field
                .metadata()
                .unwrap()
                .get(DEFAULT_CONSTRAINT_KEY)
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
            .get(DEFAULT_CONSTRAINT_KEY)
            .is_none());

        let field = Field::try_from(&column_schema).unwrap();
        assert_eq!("v1", field.metadata().unwrap().get("k1").unwrap());
        assert!(field
            .metadata()
            .unwrap()
            .get(DEFAULT_CONSTRAINT_KEY)
            .is_some());

        let new_column_schema = ColumnSchema::try_from(&field).unwrap();
        assert_eq!(column_schema, new_column_schema);
    }

    #[test]
    fn test_column_schema_with_duplicate_metadata() {
        let mut metadata = Metadata::new();
        metadata.insert(DEFAULT_CONSTRAINT_KEY.to_string(), "v1".to_string());
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
}
