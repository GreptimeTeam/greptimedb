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

//! Utilities for testing SSTs.

use api::v1::SemanticType;
use common_time::Timestamp;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use datatypes::value::ValueRef;
use store_api::metadata::{ColumnMetadata, RegionMetadata, RegionMetadataBuilder};
use store_api::storage::RegionId;

use crate::read::{Batch, Source};
use crate::row_converter::{McmpRowCodec, RowCodec, SortField};
use crate::sst::file::{FileHandle, FileId, FileMeta};
use crate::test_util::{new_noop_file_purger, VecBatchReader};

/// Test region id.
const REGION_ID: RegionId = RegionId::new(0, 0);

/// Creates a new region metadata for testing SSTs.
///
/// Schema: tag_0, tag_1, field_0, ts
pub fn sst_region_metadata() -> RegionMetadata {
    let mut builder = RegionMetadataBuilder::new(REGION_ID);
    builder
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "tag_0".to_string(),
                ConcreteDataType::string_datatype(),
                true,
            ),
            semantic_type: SemanticType::Tag,
            column_id: 0,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "tag_1".to_string(),
                ConcreteDataType::string_datatype(),
                true,
            ),
            semantic_type: SemanticType::Tag,
            column_id: 1,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "field_0".to_string(),
                ConcreteDataType::uint64_datatype(),
                true,
            ),
            semantic_type: SemanticType::Field,
            column_id: 2,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "ts".to_string(),
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            semantic_type: SemanticType::Timestamp,
            column_id: 3,
        })
        .primary_key(vec![0, 1]);
    builder.build().unwrap()
}

/// Encodes a primary key for specific tags.
pub fn new_primary_key(tags: &[&str]) -> Vec<u8> {
    let fields = (0..tags.len())
        .map(|_| SortField::new(ConcreteDataType::string_datatype()))
        .collect();
    let converter = McmpRowCodec::new(fields);
    converter
        .encode(tags.iter().map(|tag| ValueRef::String(tag)))
        .unwrap()
}

/// Creates a [Source] from `batches`.
pub fn new_source(batches: &[Batch]) -> Source {
    let reader = VecBatchReader::new(batches);
    Source::Reader(Box::new(reader))
}

/// Creates a new [FileHandle] for a SST.
pub fn sst_file_handle(start_ms: i64, end_ms: i64) -> FileHandle {
    let file_purger = new_noop_file_purger();
    FileHandle::new(
        FileMeta {
            region_id: REGION_ID,
            file_id: FileId::random(),
            time_range: (
                Timestamp::new_millisecond(start_ms),
                Timestamp::new_millisecond(end_ms),
            ),
            level: 0,
            file_size: 0,
        },
        file_purger,
    )
}
