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

//! Metadata of mito regions.

use std::sync::Arc;

use datatypes::schema::{ColumnSchema, SchemaRef};
use store_api::storage::{ColumnId, RegionId};

use crate::region::VersionNumber;

/// Static metadata of a region.
#[derive(Debug)]
pub(crate) struct RegionMetadata {
    /// Latest schema of this region
    schema: SchemaRef,
    column_metadatas: Vec<ColumnMetadata>,
    version: VersionNumber,

    // Immutable properties
    id: RegionId,
    name: String,
}

pub(crate) type RegionMetadataRef = Arc<RegionMetadata>;

/// Metadata of a column.
#[derive(Debug)]
pub(crate) struct ColumnMetadata {
    /// Schema of this column. Is the same as `column_schema` in [SchemaRef].
    column_schema: ColumnSchema,
    semantic_type: SemanticType,
    column_id: ColumnId,
}

/// The semantic type of one column
#[derive(Debug)]
pub(crate) enum SemanticType {
    Tag,
    Field,
    Timestamp,
}
