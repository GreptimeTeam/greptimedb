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

use arrow::datatypes::{DataType as ArrowDataType, Field};
use arrow_schema::Fields;
use serde::{Deserialize, Serialize};

use crate::prelude::{ConcreteDataType, DataType, LogicalTypeId};
use crate::value::Value;
use crate::vectors::StructVectorBuilder;

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct StructType {
    fields: Vec<StructField>,
}

impl TryFrom<&Fields> for StructType {
    type Error = crate::error::Error;
    fn try_from(value: &Fields) -> Result<Self, Self::Error> {
        let fields = value
            .iter()
            .map(|field| {
                Ok(StructField::new(
                    field.name().clone(),
                    ConcreteDataType::try_from(field.data_type())?,
                    field.is_nullable(),
                ))
            })
            .collect::<Result<Vec<StructField>, Self::Error>>()?;
        Ok(StructType { fields })
    }
}

impl From<Vec<StructField>> for StructType {
    fn from(fields: Vec<StructField>) -> Self {
        StructType { fields }
    }
}

impl DataType for StructType {
    fn name(&self) -> String {
        format!(
            "Struct<{}>",
            self.fields
                .iter()
                .map(|f| f.name())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }

    fn logical_type_id(&self) -> LogicalTypeId {
        LogicalTypeId::Struct
    }

    fn default_value(&self) -> Value {
        Value::Null
    }

    fn as_arrow_type(&self) -> ArrowDataType {
        let fields = self.as_arrow_fields();
        ArrowDataType::Struct(fields)
    }

    fn create_mutable_vector(&self, capacity: usize) -> Box<dyn crate::prelude::MutableVector> {
        Box::new(StructVectorBuilder::with_type_and_capacity(
            self.clone(),
            capacity,
        ))
    }

    fn try_cast(&self, _from: Value) -> Option<Value> {
        // TODO(discord9): what is the meaning of casting from Value to StructFields?
        None
    }
}

impl StructType {
    pub fn new(fields: Vec<StructField>) -> Self {
        StructType { fields }
    }

    pub fn fields(&self) -> &[StructField] {
        &self.fields
    }

    pub fn take_fields(self) -> Vec<StructField> {
        self.fields
    }

    pub fn as_arrow_fields(&self) -> Fields {
        self.fields
            .iter()
            .map(|f| Field::new(f.name.clone(), f.data_type.as_arrow_type(), f.nullable))
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct StructField {
    name: String,
    data_type: ConcreteDataType,
    nullable: bool,
}

impl StructField {
    pub fn new(name: String, data_type: ConcreteDataType, nullable: bool) -> Self {
        StructField {
            name,
            data_type,
            nullable,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn take_name(self) -> String {
        self.name
    }

    pub fn data_type(&self) -> &ConcreteDataType {
        &self.data_type
    }

    pub fn is_nullable(&self) -> bool {
        self.nullable
    }

    pub fn to_df_field(&self) -> Field {
        Field::new(
            self.name.clone(),
            self.data_type.as_arrow_type(),
            self.nullable,
        )
    }
}
