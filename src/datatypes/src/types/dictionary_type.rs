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

use arrow::datatypes::DataType as ArrowDataType;
use serde::{Deserialize, Serialize};

use crate::data_type::{ConcreteDataType, DataType};
use crate::type_id::LogicalTypeId;
use crate::value::Value;
use crate::vectors::MutableVector;

/// Used to represent the Dictionary datatype.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DictionaryType {
    // Use Box to avoid recursive dependency, as enum ConcreteDataType depends on DictionaryType.
    /// The type of Dictionary key.
    key_type: Box<ConcreteDataType>,
    /// The type of Dictionary value.
    value_type: Box<ConcreteDataType>,
}

impl Default for DictionaryType {
    fn default() -> Self {
        DictionaryType::new(
            ConcreteDataType::null_datatype(),
            ConcreteDataType::null_datatype(),
        )
    }
}

impl DictionaryType {
    /// Create a new `DictionaryType` whose item's data type is `item_type`.
    pub fn new(key_type: ConcreteDataType, value_type: ConcreteDataType) -> Self {
        DictionaryType {
            key_type: Box::new(key_type),
            value_type: Box::new(value_type),
        }
    }

    /// Returns the key data type.
    #[inline]
    pub fn key_type(&self) -> &ConcreteDataType {
        &self.key_type
    }

    /// Returns the value data type.
    #[inline]
    pub fn value_type(&self) -> &ConcreteDataType {
        &self.value_type
    }
}

impl DataType for DictionaryType {
    fn name(&self) -> String {
        format!(
            "Dictionary<{}, {}>",
            self.key_type.name(),
            self.value_type.name()
        )
    }

    fn logical_type_id(&self) -> LogicalTypeId {
        LogicalTypeId::Dictionary
    }

    fn default_value(&self) -> Value {
        unimplemented!()
    }

    fn as_arrow_type(&self) -> ArrowDataType {
        ArrowDataType::Dictionary(
            Box::new(self.key_type.as_arrow_type()),
            Box::new(self.value_type.as_arrow_type()),
        )
    }

    fn create_mutable_vector(&self, _capacity: usize) -> Box<dyn MutableVector> {
        unimplemented!()
    }

    fn try_cast(&self, _: Value) -> Option<Value> {
        None
    }
}
