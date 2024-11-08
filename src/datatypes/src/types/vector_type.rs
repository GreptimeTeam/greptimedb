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
use common_base::bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::data_type::DataType;
use crate::scalars::ScalarVectorBuilder;
use crate::type_id::LogicalTypeId;
use crate::value::Value;
use crate::vectors::{BinaryVectorBuilder, MutableVector};

/// `VectorType` is a data type for vector data with a fixed dimension.
/// The type of items in the vector is float32.
/// It is stored as binary data that contains the concatenated float32 values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct VectorType {
    pub dim: u32,
}

impl VectorType {
    pub fn new(dim: u32) -> Self {
        Self { dim }
    }
}

impl DataType for VectorType {
    fn name(&self) -> String {
        format!("Vector({})", self.dim)
    }

    fn logical_type_id(&self) -> LogicalTypeId {
        LogicalTypeId::Vector
    }

    fn default_value(&self) -> Value {
        Bytes::default().into()
    }

    fn as_arrow_type(&self) -> ArrowDataType {
        ArrowDataType::Binary
    }

    fn create_mutable_vector(&self, capacity: usize) -> Box<dyn MutableVector> {
        Box::new(BinaryVectorBuilder::with_capacity(capacity))
    }

    fn try_cast(&self, from: Value) -> Option<Value> {
        match from {
            Value::Binary(v) => Some(Value::Binary(v)),
            _ => None,
        }
    }
}
