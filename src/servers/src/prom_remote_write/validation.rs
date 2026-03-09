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

//! Prometheus remote write validation utilities.

use prost::DecodeError;
use serde::{Deserialize, Serialize};

const IS_VALID_LABEL_REST: [bool; 256] = {
    let mut table = [false; 256];
    let mut i = 0;
    while i < 256 {
        let b = i as u8;
        table[i] = b.is_ascii_alphanumeric() || b == b'_';
        i += 1;
    }
    table
};

/// Validates that the given bytes form a valid Prometheus label name.
#[inline]
pub fn validate_label_name(name: &[u8]) -> bool {
    if name.is_empty() {
        return false;
    }
    let first = name[0];
    if !(first.is_ascii_alphabetic() || first == b'_') {
        return false;
    }

    let mut rest = &name[1..];
    while rest.len() >= 8 {
        let res = IS_VALID_LABEL_REST[rest[0] as usize] as u8
            & IS_VALID_LABEL_REST[rest[1] as usize] as u8
            & IS_VALID_LABEL_REST[rest[2] as usize] as u8
            & IS_VALID_LABEL_REST[rest[3] as usize] as u8
            & IS_VALID_LABEL_REST[rest[4] as usize] as u8
            & IS_VALID_LABEL_REST[rest[5] as usize] as u8
            & IS_VALID_LABEL_REST[rest[6] as usize] as u8
            & IS_VALID_LABEL_REST[rest[7] as usize] as u8;

        if res == 0 {
            return false;
        }
        rest = &rest[8..];
    }

    for &b in rest {
        if !IS_VALID_LABEL_REST[b as usize] {
            return false;
        }
    }

    true
}

/// Validation mode for decoding Prometheus remote write requests.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromValidationMode {
    Strict,
    Lossy,
    Unchecked,
}

impl Default for PromValidationMode {
    fn default() -> Self {
        Self::Strict
    }
}

impl PromValidationMode {
    pub fn decode_string(&self, bytes: &[u8]) -> std::result::Result<String, DecodeError> {
        let result = match self {
            PromValidationMode::Strict => match String::from_utf8(bytes.to_vec()) {
                Ok(s) => s,
                Err(e) => {
                    common_telemetry::debug!(
                        "Invalid UTF-8 string value: {:?}, error: {:?}",
                        bytes,
                        e
                    );
                    return Err(DecodeError::new("invalid utf-8"));
                }
            },
            PromValidationMode::Lossy => String::from_utf8_lossy(bytes).to_string(),
            PromValidationMode::Unchecked => unsafe { String::from_utf8_unchecked(bytes.to_vec()) },
        };
        Ok(result)
    }

    pub fn decode_label_name<'a>(
        &self,
        bytes: &'a [u8],
    ) -> std::result::Result<&'a str, DecodeError> {
        if !validate_label_name(bytes) {
            common_telemetry::debug!(
                "Invalid Prometheus label name: {:?}, must match [a-zA-Z_][a-zA-Z0-9_]*",
                bytes
            );
            return Err(DecodeError::new(format!(
                "invalid prometheus label name: '{}', must match [a-zA-Z_][a-zA-Z0-9_]*",
                String::from_utf8_lossy(bytes)
            )));
        }
        Ok(unsafe { std::str::from_utf8_unchecked(bytes) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_label_name() {
        assert!(validate_label_name(b"__name__"));
        assert!(validate_label_name(b"job"));
        assert!(validate_label_name(b"_"));
        assert!(validate_label_name(b"A"));
        assert!(validate_label_name(b"abc123"));
        assert!(!validate_label_name(b""));
        assert!(!validate_label_name(b"0starts_with_digit"));
        assert!(!validate_label_name(b"has-dash"));
    }
}
