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

use api::v1::WriteHint;
use serde::{Deserialize, Serialize};
use strum::Display;

/// Primary key encoding mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, Display)]
#[serde(rename_all = "snake_case")]
pub enum PrimaryKeyEncoding {
    #[default]
    /// Dense primary key encoding.
    Dense,
    /// Sparse primary key encoding.
    Sparse,
}

impl From<api::v1::PrimaryKeyEncoding> for PrimaryKeyEncoding {
    fn from(value: api::v1::PrimaryKeyEncoding) -> Self {
        match value {
            api::v1::PrimaryKeyEncoding::Dense => PrimaryKeyEncoding::Dense,
            api::v1::PrimaryKeyEncoding::Sparse => PrimaryKeyEncoding::Sparse,
        }
    }
}

/// Infer primary key encoding from hint.
pub fn infer_primary_key_encoding_from_hint(hint: Option<&WriteHint>) -> PrimaryKeyEncoding {
    hint.map(|hint| PrimaryKeyEncoding::from(hint.primary_key_encoding()))
        .unwrap_or(PrimaryKeyEncoding::Dense)
}
