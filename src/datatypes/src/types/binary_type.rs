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
use common_base::bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::data_type::{DataType, DataTypeRef};
use crate::scalars::ScalarVectorBuilder;
use crate::type_id::LogicalTypeId;
use crate::value::Value;
use crate::vectors::{BinaryVectorBuilder, MutableVector};

#[derive(Debug, Default, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BinaryType;

impl BinaryType {
    pub fn arc() -> DataTypeRef {
        Arc::new(Self)
    }
}

impl DataType for BinaryType {
    fn name(&self) -> String {
        "Binary".to_string()
    }

    fn logical_type_id(&self) -> LogicalTypeId {
        LogicalTypeId::Binary
    }

    fn default_value(&self) -> Value {
        Bytes::default().into()
    }

    fn as_arrow_type(&self) -> ArrowDataType {
        ArrowDataType::LargeBinary
    }

    fn create_mutable_vector(&self, capacity: usize) -> Box<dyn MutableVector> {
        Box::new(BinaryVectorBuilder::with_capacity(capacity))
    }

    fn try_cast(&self, from: Value) -> Option<Value> {
        match from {
            Value::Binary(v) => Some(Value::Binary(v)),
            Value::String(v) => Some(Value::Binary(Bytes::from(v.as_utf8().as_bytes()))),
            _ => None,
        }
    }
}
