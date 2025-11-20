// Copyright 2024 Greptime Team
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

use datatypes::data_type::ConcreteDataType;
use serde::{Deserialize, Serialize};

use crate::metadata::RegionMetadata;
use crate::storage::ColumnId;

/// Metadata required to decode the encoded `__primary_key` column.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PrimaryKeyColumnMetadata {
    /// Column name.
    pub name: String,
    /// Column id in storage.
    pub column_id: ColumnId,
    /// Data type of the column.
    pub data_type: ConcreteDataType,
}

/// Collection of primary key column metadata.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PrimaryKeyMetadata {
    /// Primary key columns in the order defined by the table metadata.
    pub columns: Vec<PrimaryKeyColumnMetadata>,
}

impl PrimaryKeyMetadata {
    /// Builds metadata from region metadata.
    pub fn from_region_metadata(metadata: &RegionMetadata) -> Self {
        let columns = metadata
            .primary_key_columns()
            .map(|column| PrimaryKeyColumnMetadata {
                name: column.column_schema.name.clone(),
                column_id: column.column_id,
                data_type: column.column_schema.data_type.clone(),
            })
            .collect();

        Self { columns }
    }
}
