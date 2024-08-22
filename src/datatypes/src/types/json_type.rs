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
use crate::scalars::ScalarVectorBuilder;
use crate::type_id::LogicalTypeId;
use crate::value::{JsonbValue, Value};
use crate::vectors::{JsonVectorBuilder, MutableVector};

#[derive(Debug, Default, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct JsonType;

impl JsonType {
    pub fn arc() -> DataTypeRef {
        Arc::new(Self)
    }
}

impl DataType for JsonType {
    fn name(&self) -> String {
        "Json".to_string()
    }

    fn logical_type_id(&self) -> LogicalTypeId {
        LogicalTypeId::Json
    }

    fn default_value(&self) -> Value {
        Value::Json(JsonbValue::default())
    }

    fn as_arrow_type(&self) -> ArrowDataType {
        ArrowDataType::Binary
    }

    fn create_mutable_vector(&self, capacity: usize) -> Box<dyn MutableVector> {
        Box::new(JsonVectorBuilder::with_capacity(capacity))
    }

    fn try_cast(&self, from: Value) -> Option<Value> {
        match from {
            Value::Json(v) => Some(Value::Json(v)),
            _ => None,
        }
    }
}
