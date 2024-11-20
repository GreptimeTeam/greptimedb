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
    let expected_len = dim as usize * std::mem::size_of::<f32>();
    if val.len() != expected_len {
        return InvalidVectorSnafu {
            msg: format!(
                "Failed to convert Vector value to string: wrong byte size, expected {}, got {}",
                expected_len,
                val.len()
            ),
        }
        .fail();
    }

    if dim == 0 {
        return Ok("[]".to_string());
    }

    let elements = val
        .chunks_exact(std::mem::size_of::<f32>())
        .map(|e| f32::from_le_bytes(e.try_into().unwrap()));

    let mut s = String::from("[");
    for (i, e) in elements.enumerate() {
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
pub fn parse_string_to_vector_type_value(s: &str, dim: Option<u32>) -> Result<Vec<u8>> {
    // Trim the brackets
    let trimmed = s.trim();
    if !trimmed.starts_with('[') || !trimmed.ends_with(']') {
        return InvalidVectorSnafu {
            msg: format!("Failed to parse {s} to Vector value: not properly enclosed in brackets"),
        }
        .fail();
    }
    // Remove the brackets
    let content = trimmed[1..trimmed.len() - 1].trim();

    if content.is_empty() {
        if dim.map_or(false, |d| d != 0) {
            return InvalidVectorSnafu {
                msg: format!("Failed to parse {s} to Vector value: wrong dimension"),
            }
            .fail();
        }
        return Ok(vec![]);
    }

    let elements = content
        .split(',')
        .map(|e| {
            e.trim().parse::<f32>().map_err(|_| {
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
    if dim.map_or(false, |d| d as usize != elements.len()) {
        return InvalidVectorSnafu {
            msg: format!("Failed to parse {s} to Vector value: wrong dimension"),
        }
        .fail();
    }

    // Convert Vec<f32> to Vec<u8>
    let bytes = if cfg!(target_endian = "little") {
        unsafe {
            std::slice::from_raw_parts(
                elements.as_ptr() as *const u8,
                elements.len() * std::mem::size_of::<f32>(),
            )
            .to_vec()
        }
    } else {
        elements
            .iter()
            .flat_map(|e| e.to_le_bytes())
            .collect::<Vec<u8>>()
    };

    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_conversion_between_string_and_vector_type_value() {
        let dim = 3;

        let cases = [
            ("[1.0,2.0,3]", "[1,2,3]"),
            ("[0.0 , 0.0 , 0.0]", "[0,0,0]"),
            ("[3.4028235e38, -3.4028235e38, 1.1754944e-38]", "[340282350000000000000000000000000000000,-340282350000000000000000000000000000000,0.000000000000000000000000000000000000011754944]"),
        ];

        for (s, expected) in cases.iter() {
            let val = parse_string_to_vector_type_value(s, Some(dim)).unwrap();
            let s = vector_type_value_to_string(&val, dim).unwrap();
            assert_eq!(s, *expected);
        }

        let dim = 0;
        let cases = [("[]", "[]"), ("[ ]", "[]"), ("[  ]", "[]")];
        for (s, expected) in cases.iter() {
            let val = parse_string_to_vector_type_value(s, Some(dim)).unwrap();
            let s = vector_type_value_to_string(&val, dim).unwrap();
            assert_eq!(s, *expected);
        }
    }

    #[test]
    fn test_vector_type_value_to_string_wrong_byte_size() {
        let dim = 3;
        let val = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];
        let res = vector_type_value_to_string(&val, dim);
        assert!(res.is_err());

        let dim = 0;
        let val = vec![1];
        let res = vector_type_value_to_string(&val, dim);
        assert!(res.is_err());
    }

    #[test]
    fn test_parse_string_to_vector_type_value_not_properly_enclosed_in_brackets() {
        let dim = 3;
        let s = "1.0,2.0,3.0";
        let res = parse_string_to_vector_type_value(s, Some(dim));
        assert!(res.is_err());

        let s = "[1.0,2.0,3.0";
        let res = parse_string_to_vector_type_value(s, Some(dim));
        assert!(res.is_err());

        let s = "1.0,2.0,3.0]";
        let res = parse_string_to_vector_type_value(s, Some(dim));
        assert!(res.is_err());
    }

    #[test]
    fn test_parse_string_to_vector_type_value_wrong_dimension() {
        let dim = 3;
        let s = "[1.0,2.0]";
        let res = parse_string_to_vector_type_value(s, Some(dim));
        assert!(res.is_err());
    }

    #[test]
    fn test_parse_string_to_vector_type_value_elements_are_not_all_float32() {
        let dim = 3;
        let s = "[1.0,2.0,ah]";
        let res = parse_string_to_vector_type_value(s, Some(dim));
        assert!(res.is_err());
    }
}
