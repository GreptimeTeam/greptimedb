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

use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use serde::{Deserialize, Deserializer, Serialize};
use store_api::storage::{ColumnId, RegionId};

use crate::region::VersionNumber;

#[cfg_attr(doc, aquamarine::aquamarine)]
/// Static metadata of a region.
///
/// This struct implements [Serialize] and [Deserialize] traits.
///
/// ```mermaid
/// class RegionMetadata {
///     +RegionId region_id
///     +VersionNumber version
///     +SchemaRef schema
///     +Vec&lt;ColumnMetadata&gt; column_metadatas
///     +Vec&lt;ColumnId&gt; primary_keys
/// }
/// class Schema
/// class ColumnMetadata {
///     +ColumnSchema column_schema
///     +SemanticTyle semantic_type
///     +ColumnId column_id
/// }
/// class SemanticType
/// RegionMetadata o-- Schema
/// RegionMetadata o-- ColumnMetadata
/// ColumnMetadata o-- SemanticType
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RegionMetadata {
    /// Latest schema of this region
    #[serde(skip)]
    schema: SchemaRef,
    column_metadatas: Vec<ColumnMetadata>,
    /// Version of metadata.
    version: VersionNumber,
    /// Maintains an ordered list of primary keys
    primary_keys: Vec<ColumnId>,

    /// Immutable and unique id
    id: RegionId,
}

pub type RegionMetadataRef = Arc<RegionMetadata>;

impl<'de> Deserialize<'de> for RegionMetadata {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // helper internal struct for deserialization
        #[derive(Deserialize)]
        struct RegionMetadataWithoutSchema {
            column_metadatas: Vec<ColumnMetadata>,
            version: VersionNumber,
            primary_keys: Vec<ColumnId>,
            id: RegionId,
        }

        let region_metadata_without_schema =
            RegionMetadataWithoutSchema::deserialize(deserializer)?;

        let column_schemas = region_metadata_without_schema
            .column_metadatas
            .iter()
            .map(|column_metadata| column_metadata.column_schema.clone())
            .collect();
        let schema = Arc::new(Schema::new(column_schemas));

        Ok(Self {
            schema,
            column_metadatas: region_metadata_without_schema.column_metadatas,
            version: region_metadata_without_schema.version,
            primary_keys: region_metadata_without_schema.primary_keys,
            id: region_metadata_without_schema.id,
        })
    }
}

pub struct RegionMetadataBuilder {
    schema: SchemaRef,
    column_metadatas: Vec<ColumnMetadata>,
    version: VersionNumber,
    primary_keys: Vec<ColumnId>,
    id: RegionId,
}

impl RegionMetadataBuilder {
    pub fn new(id: RegionId, version: VersionNumber) -> Self {
        Self {
            schema: Arc::new(Schema::new(vec![])),
            column_metadatas: vec![],
            version,
            primary_keys: vec![],
            id,
        }
    }

    /// Add a column metadata to this region metadata.
    /// This method will check the semantic type and add it to primary keys automatically.
    pub fn add_column_metadata(mut self, column_metadata: ColumnMetadata) -> Self {
        if column_metadata.semantic_type == SemanticType::Tag {
            self.primary_keys.push(column_metadata.column_id);
        }
        self.column_metadatas.push(column_metadata);
        self
    }

    pub fn build(self) -> RegionMetadata {
        let schema = Arc::new(Schema::new(
            self.column_metadatas
                .iter()
                .map(|column_metadata| column_metadata.column_schema.clone())
                .collect(),
        ));
        RegionMetadata {
            schema,
            column_metadatas: self.column_metadatas,
            version: self.version,
            primary_keys: self.primary_keys,
            id: self.id,
        }
    }
}

/// Metadata of a column.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ColumnMetadata {
    /// Schema of this column. Is the same as `column_schema` in [SchemaRef].
    column_schema: ColumnSchema,
    semantic_type: SemanticType,
    column_id: ColumnId,
}

/// The semantic type of one column
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SemanticType {
    Tag,
    Field,
    Timestamp,
}

#[cfg(test)]
mod test {
    use datatypes::prelude::ConcreteDataType;

    use super::*;

    fn build_test_region_metadata() -> RegionMetadata {
        let builder = RegionMetadataBuilder::new(RegionId::new(1234, 5678), 9);
        let builder = builder.add_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new("a", ConcreteDataType::int64_datatype(), false),
            semantic_type: SemanticType::Tag,
            column_id: 1,
        });
        let builder = builder.add_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new("b", ConcreteDataType::float64_datatype(), false),
            semantic_type: SemanticType::Field,
            column_id: 2,
        });
        let builder = builder.add_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "c",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            semantic_type: SemanticType::Timestamp,
            column_id: 3,
        });
        builder.build()
    }

    #[test]
    fn test_region_metadata_serde() {
        let region_metadata = build_test_region_metadata();
        let serialized = serde_json::to_string(&region_metadata).unwrap();
        let deserialized: RegionMetadata = serde_json::from_str(&serialized).unwrap();
        assert_eq!(region_metadata, deserialized);
    }
}
