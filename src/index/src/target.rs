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

use std::any::Any;
use std::fmt::{self, Display};
use std::str::FromStr;

use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, Snafu, ensure};
use store_api::storage::ColumnId;

/// Describes an index target. Column ids are the only supported variant for now.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexTarget {
    ColumnId(ColumnId),
    SubField {
        column_id: ColumnId,
        path: Vec<String>,
        value_type: IndexValueType,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexValueType {
    #[serde(rename = "string")]
    String,
    #[serde(rename = "i64")]
    I64,
    #[serde(rename = "u64")]
    U64,
    #[serde(rename = "f64")]
    F64,
    #[serde(rename = "bool")]
    Bool,
    #[serde(rename = "binary")]
    Binary,
    #[serde(rename = "ts_ms")]
    TimestampMs,
    #[serde(rename = "date32")]
    Date32,
}

impl Display for IndexValueType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            IndexValueType::String => "string",
            IndexValueType::I64 => "i64",
            IndexValueType::U64 => "u64",
            IndexValueType::F64 => "f64",
            IndexValueType::Bool => "bool",
            IndexValueType::Binary => "binary",
            IndexValueType::TimestampMs => "ts_ms",
            IndexValueType::Date32 => "date32",
        };
        write!(f, "{s}")
    }
}

impl FromStr for IndexValueType {
    type Err = TargetKeyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "string" => Ok(IndexValueType::String),
            "i64" => Ok(IndexValueType::I64),
            "u64" => Ok(IndexValueType::U64),
            "f64" => Ok(IndexValueType::F64),
            "bool" => Ok(IndexValueType::Bool),
            "binary" => Ok(IndexValueType::Binary),
            "ts_ms" => Ok(IndexValueType::TimestampMs),
            "date32" => Ok(IndexValueType::Date32),
            _ => InvalidIndexValueTypeSnafu {
                value_type: s.to_string(),
            }
            .fail(),
        }
    }
}

impl Display for IndexTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IndexTarget::ColumnId(id) => write!(f, "{}", id),
            IndexTarget::SubField {
                column_id,
                path,
                value_type,
            } => {
                write!(
                    f,
                    "sub:{}:{}:{}",
                    column_id,
                    value_type,
                    encode_path(path)
                )
            }
        }
    }
}

impl IndexTarget {
    /// Parse a target key string back into an index target description.
    pub fn decode(key: &str) -> Result<Self, TargetKeyError> {
        if let Some(rest) = key.strip_prefix("sub:") {
            return decode_subfield_target(rest);
        }

        validate_column_key(key)?;
        let id = key
            .parse::<ColumnId>()
            .map_err(|_| InvalidColumnIdSnafu { value: key }.build())?;
        Ok(IndexTarget::ColumnId(id))
    }
}

/// Errors that can occur when working with index target keys.
#[derive(Snafu, Clone, PartialEq, Eq)]
#[stack_trace_debug]
pub enum TargetKeyError {
    #[snafu(display("target key cannot be empty"))]
    Empty,

    #[snafu(display("target key must contain digits only: {key}"))]
    InvalidCharacters { key: String },

    #[snafu(display("failed to parse column id from '{value}'"))]
    InvalidColumnId { value: String },

    #[snafu(display("invalid subfield target key: {key}"))]
    InvalidSubfieldTargetKey { key: String },

    #[snafu(display("invalid index value type: {value_type}"))]
    InvalidIndexValueType { value_type: String },

    #[snafu(display("invalid subfield path segment: {segment}"))]
    InvalidSubfieldPathSegment { segment: String },
}

impl ErrorExt for TargetKeyError {
    fn status_code(&self) -> StatusCode {
        StatusCode::InvalidArguments
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn validate_column_key(key: &str) -> Result<(), TargetKeyError> {
    ensure!(!key.is_empty(), EmptySnafu);
    ensure!(
        key.chars().all(|ch| ch.is_ascii_digit()),
        InvalidCharactersSnafu { key }
    );
    Ok(())
}

fn decode_subfield_target(rest: &str) -> Result<IndexTarget, TargetKeyError> {
    let mut parts = rest.splitn(3, ':');
    let col = parts
        .next()
        .context(InvalidSubfieldTargetKeySnafu { key: rest })?;
    let value_type = parts
        .next()
        .context(InvalidSubfieldTargetKeySnafu { key: rest })?;
    let encoded_path = parts
        .next()
        .context(InvalidSubfieldTargetKeySnafu { key: rest })?;

    validate_column_key(col)?;
    let column_id = col
        .parse::<ColumnId>()
        .map_err(|_| InvalidColumnIdSnafu { value: col }.build())?;
    let value_type = IndexValueType::from_str(value_type)?;
    let path = decode_path(encoded_path)?;

    Ok(IndexTarget::SubField {
        column_id,
        path,
        value_type,
    })
}

fn encode_path(path: &[String]) -> String {
    path.iter()
        .map(|seg| seg.replace('\\', "\\\\").replace('.', "\\."))
        .collect::<Vec<_>>()
        .join(".")
}

fn decode_path(encoded: &str) -> Result<Vec<String>, TargetKeyError> {
    if encoded.is_empty() {
        return Ok(Vec::new());
    }

    let mut parts = Vec::new();
    let mut current = String::new();
    let mut escaped = false;
    for ch in encoded.chars() {
        if escaped {
            current.push(ch);
            escaped = false;
            continue;
        }
        if ch == '\\' {
            escaped = true;
            continue;
        }
        if ch == '.' {
            if current.is_empty() {
                return InvalidSubfieldPathSegmentSnafu {
                    segment: encoded.to_string(),
                }
                .fail();
            }
            parts.push(std::mem::take(&mut current));
            continue;
        }
        current.push(ch);
    }
    if escaped {
        return InvalidSubfieldPathSegmentSnafu {
            segment: encoded.to_string(),
        }
        .fail();
    }
    if current.is_empty() {
        return InvalidSubfieldPathSegmentSnafu {
            segment: encoded.to_string(),
        }
        .fail();
    }
    parts.push(current);
    Ok(parts)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_column() {
        let target = IndexTarget::ColumnId(42);
        let key = format!("{}", target);
        assert_eq!(key, "42");
        let decoded = IndexTarget::decode(&key).unwrap();
        assert_eq!(decoded, target);
    }

    #[test]
    fn decode_rejects_empty() {
        let err = IndexTarget::decode("").unwrap_err();
        assert!(matches!(err, TargetKeyError::Empty));
    }

    #[test]
    fn decode_rejects_invalid_digits() {
        let err = IndexTarget::decode("1a2").unwrap_err();
        assert!(matches!(err, TargetKeyError::InvalidCharacters { .. }));
    }

    #[test]
    fn encode_decode_subfield() {
        let target = IndexTarget::SubField {
            column_id: 42,
            path: vec!["a".to_string(), "b.c".to_string()],
            value_type: IndexValueType::String,
        };
        let key = format!("{}", target);
        assert_eq!(key, "sub:42:string:a.b\\.c");
        let decoded = IndexTarget::decode(&key).unwrap();
        assert_eq!(decoded, target);
    }
}
