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

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use serde::{Deserialize, Serialize};
use snafu::{Snafu, ensure};
use store_api::storage::ColumnId;

/// Describes an index target.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexTarget {
    ColumnId(ColumnId),
    ColumnNestedPath {
        column_id: ColumnId,
        path: Vec<String>,
    },
}

impl Display for IndexTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl IndexTarget {
    pub fn encode(&self) -> String {
        match self {
            IndexTarget::ColumnId(id) => id.to_string(),
            IndexTarget::ColumnNestedPath { column_id, path } => {
                let path_json = serde_json::to_vec(path).expect("serializing should not fail");
                let encoded = URL_SAFE_NO_PAD.encode(path_json);
                format!("{}:{}", column_id, encoded)
            }
        }
    }

    /// Parse a target key string back into an index target description.
    pub fn decode(key: &str) -> Result<Self, TargetKeyError> {
        ensure!(!key.is_empty(), EmptySnafu);

        let (col_id, encoded_nested_path) = match key.split_once(':') {
            Some((col_id, encoded_path)) => (col_id, Some(encoded_path)),
            None => (key, None),
        };

        validate_column_key(col_id)?;

        let col_id = col_id
            .parse::<ColumnId>()
            .map_err(|_| InvalidColumnIdSnafu { value: col_id }.build())?;

        let Some(encoded_nested_path) = encoded_nested_path else {
            return Ok(IndexTarget::ColumnId(col_id));
        };

        ensure!(!encoded_nested_path.is_empty(), InvalidPathSnafu { key });

        let path_json = URL_SAFE_NO_PAD
            .decode(encoded_nested_path)
            .map_err(|_| InvalidPathSnafu { key }.build())?;

        let nested_path: Vec<String> =
            serde_json::from_slice(&path_json).map_err(|_| InvalidPathSnafu { key }.build())?;

        ensure!(
            !nested_path.is_empty() && nested_path.iter().all(|seg| !seg.is_empty()),
            InvalidPathSnafu { key }
        );

        Ok(IndexTarget::ColumnNestedPath {
            column_id: col_id,
            path: nested_path,
        })
    }

    pub fn column_id(&self) -> ColumnId {
        match self {
            IndexTarget::ColumnId(id) => *id,
            IndexTarget::ColumnNestedPath { column_id, .. } => *column_id,
        }
    }

    pub fn path(&self) -> Option<&[String]> {
        match self {
            IndexTarget::ColumnId(_) => None,
            IndexTarget::ColumnNestedPath { path, .. } => Some(path),
        }
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

    #[snafu(display("invalid target path in key '{key}'"))]
    InvalidPath { key: String },
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_column() {
        let target = IndexTarget::ColumnId(42);
        let key = target.encode();
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
    fn encode_decode_column_path() {
        let target = IndexTarget::ColumnNestedPath {
            column_id: 42,
            path: vec!["a".to_string(), "b".to_string()],
        };
        let key = target.encode();
        assert_eq!(key, "42:WyJhIiwiYiJd");
        let decoded = IndexTarget::decode(&key).unwrap();
        assert_eq!(decoded, target);
    }
}
