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

use datatypes::schema::ColumnSchema;
use serde::{Deserialize, Serialize};

use crate::storage::ColumnId;

/// Metadata of a column.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ColumnMetadata {
    /// Schema of this column. Is the same as `column_schema` in [SchemaRef].
    pub column_schema: ColumnSchema,
    /// Semantic type of this column (e.g. tag or timestamp).
    pub semantic_type: SemanticType,
    /// Immutable and unique id of a region.
    pub column_id: ColumnId,
}

/// The semantic type of one column
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SemanticType {
    /// Tag column, also is a part of primary key.
    Tag = 0,
    /// A column that isn't a time index or part of primary key.
    Field = 1,
    /// Time index column.
    Timestamp = 2,
}
