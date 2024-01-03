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

use std::collections::HashMap;
use std::fmt;

use arrow::datatypes::Field;
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};

use crate::data_type::{ConcreteDataType, DataType};
use crate::error::{self, Error, Result};
use crate::schema::constraint::ColumnDefaultConstraint;
use crate::value::Value;
use crate::vectors::VectorRef;

pub type Metadata = HashMap<String, String>;

/// Key used to store whether the column is time index in arrow field's metadata.
pub const TIME_INDEX_KEY: &str = "greptime:time_index";
pub const COMMENT_KEY: &str = "greptime:storage:comment";
/// Key used to store default constraint in arrow field's metadata.
const DEFAULT_CONSTRAINT_KEY: &str = "greptime:default_constraint";

/// Schema of a column, used as an immutable struct.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: ConcreteDataType,
    is_nullable: bool,
    is_time_index: bool,
    default_constraint: Option<ColumnDefaultConstraint>,
    metadata: Metadata,
}

impl fmt::Debug for ColumnSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {} {}",
            self.name,
            self.data_type,
            if self.is_nullable { "null" } else { "not null" },
        )?;

        // Add default constraint if present
        if let Some(default_constraint) = &self.default_constraint {
            write!(f, " default={:?}", default_constraint)?;
        }

        // Add metadata if present
        if !self.metadata.is_empty() {
            write!(f, " metadata={:?}", self.metadata)?;
        }

        Ok(())
    }
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

    #[inline]
    pub fn mut_metadata(&mut self) -> &mut Metadata {
        &mut self.metadata
    }

    /// Retrieve the column comment
    pub fn column_comment(&self) -> Option<&String> {
        self.metadata.get(COMMENT_KEY)
    }

    pub fn with_time_index(mut self, is_time_index: bool) -> Self {
        self.is_time_index = is_time_index;
        if is_time_index {
            let _ = self
                .metadata
                .insert(TIME_INDEX_KEY.to_string(), "true".to_string());
        } else {
            let _ = self.metadata.remove(TIME_INDEX_KEY);
        }
        self
    }

    /// Set default constraint.
    ///
    /// If a default constraint exists for the column, this method will
    /// validate it against the column's data type and nullability.
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

    /// Set the nullablity to `true` of the column.
    pub fn with_nullable_set(mut self) -> Self {
        self.is_nullable = true;
        self
    }

    /// Creates a new [`ColumnSchema`] with given metadata.
    pub fn with_metadata(mut self, metadata: Metadata) -> Self {
        self.metadata = metadata;
        self
    }

    /// Creates a vector with default value for this column.
    ///
    /// If the column is `NOT NULL` but doesn't has `DEFAULT` value supplied, returns `Ok(None)`.
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

    /// Creates a vector for padding.
    ///
    /// This method always returns a vector since it uses [DataType::default_value]
    /// to fill the vector. Callers should only use the created vector for padding
    /// and never read its content.
    pub fn create_default_vector_for_padding(&self, num_rows: usize) -> VectorRef {
        let padding_value = if self.is_nullable {
            Value::Null
        } else {
            // If the column is not null, use the data type's default value as it is
            // more efficient to acquire.
            self.data_type.default_value()
        };
        let value_ref = padding_value.as_value_ref();
        let mut mutable_vector = self.data_type.create_mutable_vector(num_rows);
        for _ in 0..num_rows {
            mutable_vector.push_value_ref(value_ref);
        }
        mutable_vector.to_vector()
    }

    /// Creates a default value for this column.
    ///
    /// If the column is `NOT NULL` but doesn't has `DEFAULT` value supplied, returns `Ok(None)`.
    pub fn create_default(&self) -> Result<Option<Value>> {
        match &self.default_constraint {
            Some(c) => c
                .create_default(&self.data_type, self.is_nullable)
                .map(Some),
            None => {
                if self.is_nullable {
                    // No default constraint, use null as default value.
                    ColumnDefaultConstraint::null_value()
                        .create_default(&self.data_type, self.is_nullable)
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
        let mut metadata = field.metadata().clone();
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
        .with_metadata(metadata))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::DataType as ArrowDataType;

    use super::*;
    use crate::value::Value;
    use crate::vectors::Int32Vector;

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
            field.metadata().get(DEFAULT_CONSTRAINT_KEY).unwrap()
        );

        let new_column_schema = ColumnSchema::try_from(&field).unwrap();
        assert_eq!(column_schema, new_column_schema);
    }

    #[test]
    fn test_column_schema_with_metadata() {
        let metadata = Metadata::from([
            ("k1".to_string(), "v1".to_string()),
            (COMMENT_KEY.to_string(), "test comment".to_string()),
        ]);
        let column_schema = ColumnSchema::new("test", ConcreteDataType::int32_datatype(), true)
            .with_metadata(metadata)
            .with_default_constraint(Some(ColumnDefaultConstraint::null_value()))
            .unwrap();
        assert_eq!("v1", column_schema.metadata().get("k1").unwrap());
        assert_eq!("test comment", column_schema.column_comment().unwrap());
        assert!(column_schema
            .metadata()
            .get(DEFAULT_CONSTRAINT_KEY)
            .is_none());

        let field = Field::try_from(&column_schema).unwrap();
        assert_eq!("v1", field.metadata().get("k1").unwrap());
        let _ = field.metadata().get(DEFAULT_CONSTRAINT_KEY).unwrap();

        let new_column_schema = ColumnSchema::try_from(&field).unwrap();
        assert_eq!(column_schema, new_column_schema);
    }

    #[test]
    fn test_column_schema_with_duplicate_metadata() {
        let metadata = Metadata::from([(DEFAULT_CONSTRAINT_KEY.to_string(), "v1".to_string())]);
        let column_schema = ColumnSchema::new("test", ConcreteDataType::int32_datatype(), true)
            .with_metadata(metadata)
            .with_default_constraint(Some(ColumnDefaultConstraint::null_value()))
            .unwrap();
        assert!(Field::try_from(&column_schema).is_err());
    }

    #[test]
    fn test_column_schema_invalid_default_constraint() {
        assert!(
            ColumnSchema::new("test", ConcreteDataType::int32_datatype(), false)
                .with_default_constraint(Some(ColumnDefaultConstraint::null_value()))
                .is_err()
        );
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
    fn test_create_default_vector_for_padding() {
        let column_schema = ColumnSchema::new("test", ConcreteDataType::int32_datatype(), true);
        let vector = column_schema.create_default_vector_for_padding(4);
        assert!(vector.only_null());
        assert_eq!(4, vector.len());

        let column_schema = ColumnSchema::new("test", ConcreteDataType::int32_datatype(), false);
        let vector = column_schema.create_default_vector_for_padding(4);
        assert_eq!(4, vector.len());
        let expect: VectorRef = Arc::new(Int32Vector::from_slice([0, 0, 0, 0]));
        assert_eq!(expect, vector);
    }

    #[test]
    fn test_column_schema_single_create_default_null() {
        // Implicit default null.
        let column_schema = ColumnSchema::new("test", ConcreteDataType::int32_datatype(), true);
        let v = column_schema.create_default().unwrap().unwrap();
        assert!(v.is_null());

        // Explicit default null.
        let column_schema = ColumnSchema::new("test", ConcreteDataType::int32_datatype(), true)
            .with_default_constraint(Some(ColumnDefaultConstraint::null_value()))
            .unwrap();
        let v = column_schema.create_default().unwrap().unwrap();
        assert!(v.is_null());
    }

    #[test]
    fn test_column_schema_single_create_default_not_null() {
        let column_schema = ColumnSchema::new("test", ConcreteDataType::int32_datatype(), true)
            .with_default_constraint(Some(ColumnDefaultConstraint::Value(Value::Int32(6))))
            .unwrap();
        let v = column_schema.create_default().unwrap().unwrap();
        assert_eq!(v, Value::Int32(6));
    }

    #[test]
    fn test_column_schema_single_no_default() {
        let column_schema = ColumnSchema::new("test", ConcreteDataType::int32_datatype(), false);
        assert!(column_schema.create_default().unwrap().is_none());
    }

    #[test]
    fn test_debug_for_column_schema() {
        let column_schema_int8 =
            ColumnSchema::new("test_column_1", ConcreteDataType::int8_datatype(), true);

        let column_schema_int32 =
            ColumnSchema::new("test_column_2", ConcreteDataType::int32_datatype(), false);

        let formatted_int8 = format!("{:?}", column_schema_int8);
        let formatted_int32 = format!("{:?}", column_schema_int32);
        assert_eq!(formatted_int8, "test_column_1 Int8 null");
        assert_eq!(formatted_int32, "test_column_2 Int32 not null");
    }
}
