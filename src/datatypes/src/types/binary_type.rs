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

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default,
)]
pub enum BinaryReprType {
    #[default]
    Binary,
    BinaryView,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
pub struct BinaryType {
    #[serde(default)]
    repr_type: BinaryReprType,
}

/// Custom deserialization to support both old and new formats.
impl<'de> serde::Deserialize<'de> for BinaryType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        struct Helper {
            #[serde(default)]
            repr_type: BinaryReprType,
        }

        let opt = Option::<Helper>::deserialize(deserializer)?;
        Ok(match opt {
            Some(helper) => Self {
                repr_type: helper.repr_type,
            },
            None => Self::default(),
        })
    }
}

impl Default for BinaryType {
    fn default() -> Self {
        Self {
            repr_type: BinaryReprType::Binary,
        }
    }
}

impl BinaryType {
    pub fn binary() -> Self {
        Self {
            repr_type: BinaryReprType::Binary,
        }
    }

    pub fn binary_view() -> Self {
        Self {
            repr_type: BinaryReprType::BinaryView,
        }
    }

    pub fn is_view(&self) -> bool {
        matches!(self.repr_type, BinaryReprType::BinaryView)
    }

    pub fn arc() -> DataTypeRef {
        Arc::new(Self::default())
    }

    pub fn view_arc() -> DataTypeRef {
        Arc::new(Self::binary_view())
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
        match self.repr_type {
            BinaryReprType::Binary => ArrowDataType::Binary,
            BinaryReprType::BinaryView => ArrowDataType::BinaryView,
        }
    }

    fn create_mutable_vector(&self, capacity: usize) -> Box<dyn MutableVector> {
        match self.repr_type {
            BinaryReprType::Binary => Box::new(BinaryVectorBuilder::with_capacity(capacity)),
            BinaryReprType::BinaryView => {
                Box::new(BinaryVectorBuilder::with_view_capacity(capacity))
            }
        }
    }

    fn try_cast(&self, from: Value) -> Option<Value> {
        match from {
            Value::Binary(v) => Some(Value::Binary(v)),
            Value::String(v) => Some(Value::Binary(Bytes::from(v.as_utf8().as_bytes()))),
            _ => None,
        }
    }
}
