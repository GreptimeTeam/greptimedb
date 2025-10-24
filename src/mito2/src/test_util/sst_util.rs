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

use std::sync::Arc;

use api::v1::{OpType, SemanticType};
use common_time::Timestamp;
use datatypes::arrow::array::{BinaryArray, TimestampMillisecondArray, UInt8Array, UInt64Array};
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, SkippingIndexOptions};
use datatypes::value::ValueRef;
use mito_codec::row_converter::{DensePrimaryKeyCodec, PrimaryKeyCodecExt, SortField};
use parquet::file::metadata::ParquetMetaData;
use store_api::metadata::{
    ColumnMetadata, RegionMetadata, RegionMetadataBuilder, RegionMetadataRef,
};
use store_api::storage::{FileId, RegionId};

use crate::read::{Batch, BatchBuilder, Source};
use crate::sst::file::{FileHandle, FileMeta};
use crate::test_util::{VecBatchReader, new_batch_builder, new_noop_file_purger};

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
            )
            .with_inverted_index(true),
            semantic_type: SemanticType::Tag,
            column_id: 0,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "tag_1".to_string(),
                ConcreteDataType::string_datatype(),
                true,
            )
            .with_skipping_options(SkippingIndexOptions {
                granularity: 1,
                ..Default::default()
            })
            .unwrap(),
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
        .map(|idx| {
            (
                idx as u32,
                SortField::new(ConcreteDataType::string_datatype()),
            )
        })
        .collect();
    let converter = DensePrimaryKeyCodec::with_fields(fields);
    converter
        .encode(tags.iter().map(|tag| ValueRef::String(tag)))
        .unwrap()
}

/// Creates a [Source] from `batches`.
pub fn new_source(batches: &[Batch]) -> Source {
    let reader = VecBatchReader::new(batches);
    Source::Reader(Box::new(reader))
}

/// Creates a SST file handle with provided file id
pub fn sst_file_handle_with_file_id(file_id: FileId, start_ms: i64, end_ms: i64) -> FileHandle {
    let file_purger = new_noop_file_purger();
    FileHandle::new(
        FileMeta {
            region_id: REGION_ID,
            file_id,
            time_range: (
                Timestamp::new_millisecond(start_ms),
                Timestamp::new_millisecond(end_ms),
            ),
            level: 0,
            file_size: 0,
            available_indexes: Default::default(),
            index_file_size: 0,
            index_file_id: None,
            num_rows: 0,
            num_row_groups: 0,
            sequence: None,
            partition_expr: None,
        },
        file_purger,
    )
}

/// Creates a new [FileHandle] for a SST.
pub fn sst_file_handle(start_ms: i64, end_ms: i64) -> FileHandle {
    sst_file_handle_with_file_id(FileId::random(), start_ms, end_ms)
}

/// Creates a new batch with custom sequence for testing.
pub fn new_batch_with_custom_sequence(
    tags: &[&str],
    start: usize,
    end: usize,
    sequence: u64,
) -> Batch {
    assert!(end >= start);
    let pk = new_primary_key(tags);
    let timestamps: Vec<_> = (start..end).map(|v| v as i64).collect();
    let sequences = vec![sequence; end - start];
    let op_types = vec![OpType::Put; end - start];
    let field: Vec<_> = (start..end).map(|v| v as u64).collect();
    new_batch_builder(&pk, &timestamps, &sequences, &op_types, 2, &field)
        .build()
        .unwrap()
}

pub fn new_batch_by_range(tags: &[&str], start: usize, end: usize) -> Batch {
    new_batch_with_custom_sequence(tags, start, end, 1000)
}

pub fn new_batch_with_binary(tags: &[&str], start: usize, end: usize) -> Batch {
    assert!(end >= start);
    let pk = new_primary_key(tags);
    let timestamps: Vec<_> = (start..end).map(|v| v as i64).collect();
    let sequences = vec![1000; end - start];
    let op_types = vec![OpType::Put; end - start];

    let field: Vec<_> = (start..end)
        .map(|_v| "some data".as_bytes().to_vec())
        .collect();

    let mut builder = BatchBuilder::new(pk);
    builder
        .timestamps_array(Arc::new(TimestampMillisecondArray::from_iter_values(
            timestamps.iter().copied(),
        )))
        .unwrap()
        .sequences_array(Arc::new(UInt64Array::from_iter_values(
            sequences.iter().copied(),
        )))
        .unwrap()
        .op_types_array(Arc::new(UInt8Array::from_iter_values(
            op_types.iter().map(|v| *v as u8),
        )))
        .unwrap()
        .push_field_array(1, Arc::new(BinaryArray::from_iter_values(field)))
        .unwrap();
    builder.build().unwrap()
}

/// ParquetMetaData doesn't implement `PartialEq` trait, check internal fields manually
pub fn assert_parquet_metadata_eq(a: Arc<ParquetMetaData>, b: Arc<ParquetMetaData>) {
    macro_rules! assert_metadata {
            ( $a:expr, $b:expr, $($method:ident,)+ ) => {
                $(
                    assert_eq!($a.$method(), $b.$method());
                )+
            }
        }

    assert_metadata!(
        a.file_metadata(),
        b.file_metadata(),
        version,
        num_rows,
        created_by,
        key_value_metadata,
        schema_descr,
        column_orders,
    );

    assert_metadata!(a, b, row_groups, column_index, offset_index,);
}

/// Creates a new region metadata for testing SSTs with binary datatype.
///
/// Schema: tag_0(string), field_0(binary), ts
pub fn build_test_binary_test_region_metadata() -> RegionMetadataRef {
    let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
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
            column_schema: ColumnSchema::new("field_0", ConcreteDataType::binary_datatype(), true),
            semantic_type: SemanticType::Field,
            column_id: 1,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            semantic_type: SemanticType::Timestamp,
            column_id: 2,
        })
        .primary_key(vec![0]);
    Arc::new(builder.build().unwrap())
}
