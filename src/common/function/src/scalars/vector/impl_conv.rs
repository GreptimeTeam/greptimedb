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

use std::borrow::Cow;

use common_query::error::{InvalidFuncArgsSnafu, Result};
use datafusion_common::ScalarValue;

/// Convert a string or binary literal to a vector literal.
pub fn as_veclit(arg: &ScalarValue) -> Result<Option<Cow<'_, [f32]>>> {
    match arg {
        ScalarValue::Binary(b) | ScalarValue::BinaryView(b) => {
            b.as_ref().map(|x| binlit_as_veclit(x)).transpose()
        }
        ScalarValue::Utf8(s) | ScalarValue::Utf8View(s) => s
            .as_ref()
            .map(|x| parse_veclit_from_strlit(x).map(Cow::Owned))
            .transpose(),
        _ => InvalidFuncArgsSnafu {
            err_msg: format!("Unsupported data type: {:?}", arg.data_type()),
        }
        .fail(),
    }
}

/// Convert a u8 slice to a vector literal.
pub fn binlit_as_veclit(bytes: &[u8]) -> Result<Cow<'_, [f32]>> {
    if bytes.len() % std::mem::size_of::<f32>() != 0 {
        return InvalidFuncArgsSnafu {
            err_msg: format!("Invalid binary length of vector: {}", bytes.len()),
        }
        .fail();
    }

    if cfg!(target_endian = "little") {
        Ok(unsafe {
            let vec = std::slice::from_raw_parts(
                bytes.as_ptr() as *const f32,
                bytes.len() / std::mem::size_of::<f32>(),
            );
            Cow::Borrowed(vec)
        })
    } else {
        let v = bytes
            .chunks_exact(std::mem::size_of::<f32>())
            .map(|chunk| f32::from_le_bytes(chunk.try_into().unwrap()))
            .collect::<Vec<f32>>();
        Ok(Cow::Owned(v))
    }
}

/// Parse a string literal to a vector literal.
/// Valid inputs are strings like "[1.0, 2.0, 3.0]".
pub fn parse_veclit_from_strlit(s: &str) -> Result<Vec<f32>> {
    let trimmed = s.trim();
    if !trimmed.starts_with('[') || !trimmed.ends_with(']') {
        return InvalidFuncArgsSnafu {
            err_msg: format!(
                "Failed to parse {s} to Vector value: not properly enclosed in brackets"
            ),
        }
        .fail();
    }
    let content = trimmed[1..trimmed.len() - 1].trim();
    if content.is_empty() {
        return Ok(Vec::new());
    }

    content
        .split(',')
        .map(|s| s.trim().parse::<f32>())
        .collect::<std::result::Result<_, _>>()
        .map_err(|e| {
            InvalidFuncArgsSnafu {
                err_msg: format!("Failed to parse {s} to Vector value: {e}"),
            }
            .build()
        })
}

/// Convert a vector literal to a binary literal.
pub fn veclit_to_binlit(vec: &[f32]) -> Vec<u8> {
    if cfg!(target_endian = "little") {
        unsafe {
            std::slice::from_raw_parts(vec.as_ptr() as *const u8, std::mem::size_of_val(vec))
                .to_vec()
        }
    } else {
        let mut bytes = Vec::with_capacity(std::mem::size_of_val(vec));
        for e in vec {
            bytes.extend_from_slice(&e.to_le_bytes());
        }
        bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_veclit_from_strlit() {
        let result = parse_veclit_from_strlit("[1.0, 2.0, 3.0]").unwrap();
        assert_eq!(result, vec![1.0, 2.0, 3.0]);

        let result = parse_veclit_from_strlit("[]").unwrap();
        assert_eq!(result, Vec::<f32>::new());

        let result = parse_veclit_from_strlit("[1.0, a, 3.0]");
        assert!(result.is_err());
    }

    #[test]
    fn test_binlit_as_veclit() {
        let vec = &[1.0, 2.0, 3.0];
        let bytes = veclit_to_binlit(vec);
        let result = binlit_as_veclit(&bytes).unwrap();
        assert_eq!(result.as_ref(), vec);

        let invalid_bytes = [0, 0, 128];
        let result = binlit_as_veclit(&invalid_bytes);
        assert!(result.is_err());
    }
}
