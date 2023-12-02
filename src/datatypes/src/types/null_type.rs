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

use arrow::datatypes::DataType as ArrowDataType;
use serde::{Deserialize, Serialize};

use crate::data_type::{DataType, DataTypeRef};
use crate::type_id::LogicalTypeId;
use crate::value::Value;
use crate::vectors::{MutableVector, NullVectorBuilder};

#[derive(Debug, Default, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NullType;

impl NullType {
    pub fn arc() -> DataTypeRef {
        Arc::new(NullType)
    }
}

impl DataType for NullType {
    fn name(&self) -> String {
        "Null".to_string()
    }

    fn logical_type_id(&self) -> LogicalTypeId {
        LogicalTypeId::Null
    }

    fn default_value(&self) -> Value {
        Value::Null
    }

    fn as_arrow_type(&self) -> ArrowDataType {
        ArrowDataType::Null
    }

    fn create_mutable_vector(&self, _capacity: usize) -> Box<dyn MutableVector> {
        Box::<NullVectorBuilder>::default()
    }

    // Unconditional cast other type to Value::Null
    fn try_cast(&self, _from: Value) -> Option<Value> {
        Some(Value::Null)
    }
}
