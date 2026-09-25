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
use datatypes::data_type::ConcreteDataType;
use datatypes::json::JSON2_REMAINDER_FIELD_NAME;
use serde::{Deserialize, Serialize};
use snafu::{Snafu, ensure};
use store_api::storage::ColumnId;

/// Identifies a column or a typed JSON object path within a column.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum IndexTarget {
    ColumnId(ColumnId),
    JsonPath {
        column_id: ColumnId,
        path: Vec<String>,
        data_type: ConcreteDataType,
    },
}

impl Display for IndexTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IndexTarget::ColumnId(id) => write!(f, "{}", id),
            IndexTarget::JsonPath {
                column_id,
                path,
                data_type,
            } => {
                validate_json_path(path).map_err(|_| fmt::Error)?;
                let json = serde_json::to_vec(&(path, data_type)).map_err(|_| fmt::Error)?;
                write!(f, "j1:{column_id}:{}", URL_SAFE_NO_PAD.encode(json))
            }
        }
    }
}

impl IndexTarget {
    /// Creates a target for an explicit JSON hint. Schema membership is checked by callers.
    pub fn json_path(
        column_id: ColumnId,
        path: Vec<String>,
        data_type: ConcreteDataType,
    ) -> Result<Self, TargetKeyError> {
        validate_json_path(&path)?;
        Ok(Self::JsonPath {
            column_id,
            path,
            data_type,
        })
    }

    /// Parse a target key string back into an index target description.
    pub fn decode(key: &str) -> Result<Self, TargetKeyError> {
        if let Some(json_key) = key.strip_prefix("j1:") {
            let invalid = || InvalidJsonTargetSnafu { key }.build();
            let (column, payload) = json_key.split_once(':').ok_or_else(invalid)?;
            validate_column_key(column)?;
            let column_id = column.parse::<ColumnId>().map_err(|_| invalid())?;
            let bytes = URL_SAFE_NO_PAD.decode(payload).map_err(|_| invalid())?;
            let (path, data_type) =
                serde_json::from_slice::<(Vec<String>, ConcreteDataType)>(&bytes)
                    .map_err(|_| invalid())?;
            return Self::json_path(column_id, path, data_type);
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

    #[snafu(display("invalid JSON index target: {key}"))]
    InvalidJsonTarget { key: String },

    #[snafu(display(
        "JSON index path must be nonempty and contain no empty or reserved remainder segments"
    ))]
    InvalidJsonPath,
}

fn validate_json_path(path: &[String]) -> Result<(), TargetKeyError> {
    ensure!(
        !path.is_empty()
            && path
                .iter()
                .all(|part| !part.is_empty() && part != JSON2_REMAINDER_FIELD_NAME),
        InvalidJsonPathSnafu
    );
    Ok(())
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
    fn json_target_encoding_is_stable_and_typed() {
        let path = vec!["resource".to_string(), "service.name".to_string()];
        for (data_type, key) in [
            (
                ConcreteDataType::int32_datatype(),
                "j1:7:W1sicmVzb3VyY2UiLCJzZXJ2aWNlLm5hbWUiXSx7IkludDMyIjp7fX1d",
            ),
            (
                ConcreteDataType::int64_datatype(),
                "j1:7:W1sicmVzb3VyY2UiLCJzZXJ2aWNlLm5hbWUiXSx7IkludDY0Ijp7fX1d",
            ),
            (
                ConcreteDataType::string_datatype(),
                "j1:7:W1sicmVzb3VyY2UiLCJzZXJ2aWNlLm5hbWUiXSx7IlN0cmluZyI6eyJzaXplX3R5cGUiOiJVdGY4In19XQ",
            ),
        ] {
            let target = IndexTarget::json_path(7, path.clone(), data_type).unwrap();
            assert_eq!(target.to_string(), key);
            assert_eq!(IndexTarget::decode(key).unwrap(), target);
            assert_eq!(
                serde_json::from_str::<IndexTarget>(&serde_json::to_string(&target).unwrap())
                    .unwrap(),
                target
            );
        }
        let target = IndexTarget::json_path(
            42,
            vec!["引号\".:[]".into()],
            ConcreteDataType::string_datatype(),
        )
        .unwrap();
        assert_eq!(IndexTarget::decode(&target.to_string()).unwrap(), target);
        assert_eq!(
            serde_json::to_string(&IndexTarget::ColumnId(42)).unwrap(),
            r#"{"ColumnId":42}"#
        );
    }

    #[test]
    fn json_target_rejects_invalid_paths_and_keys() {
        for path in [
            vec![],
            vec![String::new()],
            vec![JSON2_REMAINDER_FIELD_NAME.into()],
            vec!["a".into(), JSON2_REMAINDER_FIELD_NAME.into()],
            vec!["a".into(), JSON2_REMAINDER_FIELD_NAME.into(), "b".into()],
        ] {
            let data_type = ConcreteDataType::int64_datatype();
            assert!(IndexTarget::json_path(7, path.clone(), data_type.clone()).is_err());
            let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&(path, data_type)).unwrap());
            assert!(IndexTarget::decode(&format!("j1:7:{payload}")).is_err());
        }
        for key in ["j2:7:e30", "j1:7", "j1:7:!", "j1:x:e30", "j1:7:e30"] {
            assert!(IndexTarget::decode(key).is_err(), "{key}");
        }
    }
}
