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

use api::v1::SemanticType;
use store_api::metadata::{ColumnMetadata, RegionMetadata, RegionMetadataBuilder};
use store_api::storage::RegionId;

/// Builds a region metadata with the given column metadatas.
pub fn build_region_metadata(
    region_id: RegionId,
    column_metadatas: &[ColumnMetadata],
) -> RegionMetadata {
    let mut builder = RegionMetadataBuilder::new(region_id);
    let mut primary_key = vec![];
    for column_metadata in column_metadatas {
        builder.push_column_metadata(column_metadata.clone());
        if column_metadata.semantic_type == SemanticType::Tag {
            primary_key.push(column_metadata.column_id);
        }
    }
    builder.primary_key(primary_key);
    builder.build().unwrap()
}
