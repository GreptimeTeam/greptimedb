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
use crate::error::{InvalidVectorSnafu, Result};
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

/// Converts a vector type value to string
/// for example: [1.0, 2.0, 3.0] -> "[1.0,2.0,3.0]"
pub fn vector_type_value_to_string(val: &[u8], dim: u32) -> Result<String> {
    if dim as usize * std::mem::size_of::<f32>() != val.len() {
        return InvalidVectorSnafu {
            msg: format!(
                "Failed to convert Vector value to string: wrong byte size, expected {}, got {}",
                dim as usize * std::mem::size_of::<f32>(),
                val.len()
            ),
        }
        .fail();
    }

    let elements = unsafe {
        std::slice::from_raw_parts(
            val.as_ptr() as *const f32,
            val.len() / std::mem::size_of::<f32>(),
        )
    };

    let mut s = String::from("[");
    for (i, e) in elements.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&e.to_string());
    }
    s.push(']');
    Ok(s)
}

/// Parses a string to a vector type value
/// Valid input format: "[1.0,2.0,3.0]", "[1.0, 2.0, 3.0]"
pub fn parse_string_to_vector_type_value(s: &str, dim: u32) -> Result<Vec<u8>> {
    // Trim the brackets
    let trimmed = s.trim();
    if !trimmed.starts_with('[') || !trimmed.ends_with(']') {
        return InvalidVectorSnafu {
            msg: format!("Failed to parse {s} to Vector value: not properly enclosed in brackets"),
        }
        .fail();
    }
    // Remove the brackets
    let content = &trimmed[1..trimmed.len() - 1];

    let elements = content
        .split(',')
        .map(|s| {
            s.trim().parse::<f32>().map_err(|_| {
                InvalidVectorSnafu {
                    msg: format!(
                        "Failed to parse {s} to Vector value: elements are not all float32"
                    ),
                }
                .build()
            })
        })
        .collect::<Result<Vec<f32>>>()?;

    // Check dimension
    if elements.len() != dim as usize {
        return InvalidVectorSnafu {
            msg: format!("Failed to parse {s} to Vector value: wrong dimension"),
        }
        .fail();
    }

    // Convert Vec<f32> to Vec<u8>
    let bytes = unsafe {
        std::slice::from_raw_parts(
            elements.as_ptr() as *const u8,
            elements.len() * std::mem::size_of::<f32>(),
        )
        .to_vec()
    };

    Ok(bytes)
}
