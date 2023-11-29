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

use std::sync::Arc;

use arrow::datatypes::{DataType as ArrowDataType, Field};
use serde::{Deserialize, Serialize};

use crate::data_type::{ConcreteDataType, DataType};
use crate::type_id::LogicalTypeId;
use crate::value::{ListValue, Value};
use crate::vectors::{ListVectorBuilder, MutableVector};

/// Used to represent the List datatype.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ListType {
    /// The type of List's item.
    // Use Box to avoid recursive dependency, as enum ConcreteDataType depends on ListType.
    item_type: Box<ConcreteDataType>,
}

impl Default for ListType {
    fn default() -> Self {
        ListType::new(ConcreteDataType::null_datatype())
    }
}

impl ListType {
    /// Create a new `ListType` whose item's data type is `item_type`.
    pub fn new(item_type: ConcreteDataType) -> Self {
        ListType {
            item_type: Box::new(item_type),
        }
    }

    /// Returns the item data type.
    #[inline]
    pub fn item_type(&self) -> &ConcreteDataType {
        &self.item_type
    }
}

impl DataType for ListType {
    fn name(&self) -> String {
        format!("List<{}>", self.item_type.name())
    }

    fn logical_type_id(&self) -> LogicalTypeId {
        LogicalTypeId::List
    }

    fn default_value(&self) -> Value {
        Value::List(ListValue::new(None, *self.item_type.clone()))
    }

    fn as_arrow_type(&self) -> ArrowDataType {
        let field = Arc::new(Field::new("item", self.item_type.as_arrow_type(), true));
        ArrowDataType::List(field)
    }

    fn create_mutable_vector(&self, capacity: usize) -> Box<dyn MutableVector> {
        Box::new(ListVectorBuilder::with_type_capacity(
            *self.item_type.clone(),
            capacity,
        ))
    }

    fn try_cast(&self, from: Value) -> Option<Value> {
        match from {
            Value::List(v) => Some(Value::List(v)),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::ListValue;

    #[test]
    fn test_list_type() {
        let t = ListType::new(ConcreteDataType::boolean_datatype());
        assert_eq!("List<Boolean>", t.name());
        assert_eq!(LogicalTypeId::List, t.logical_type_id());
        assert_eq!(
            Value::List(ListValue::new(None, ConcreteDataType::boolean_datatype())),
            t.default_value()
        );
        assert_eq!(
            ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Boolean, true))),
            t.as_arrow_type()
        );
        assert_eq!(ConcreteDataType::boolean_datatype(), *t.item_type());
    }
}
