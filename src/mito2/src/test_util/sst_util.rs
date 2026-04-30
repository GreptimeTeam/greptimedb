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
use datatypes::arrow::array::{
    ArrayRef, BinaryDictionaryBuilder, RecordBatch, StringDictionaryBuilder,
    TimestampMillisecondArray, UInt8Array, UInt64Array,
};
use datatypes::arrow::datatypes::UInt32Type;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, SkippingIndexOptions};
use datatypes::value::ValueRef;
use mito_codec::row_converter::{DensePrimaryKeyCodec, PrimaryKeyCodecExt, SortField};
use store_api::metadata::{
    ColumnMetadata, RegionMetadata, RegionMetadataBuilder, RegionMetadataRef,
};
use store_api::metric_engine_consts::{
    DATA_SCHEMA_TABLE_ID_COLUMN_NAME, DATA_SCHEMA_TSID_COLUMN_NAME,
};
use store_api::storage::consts::ReservedColumnId;
use store_api::storage::{FileId, RegionId};

use crate::read::{Batch, FlatSource, RecordBatchSource};
use crate::sst::file::{FileHandle, FileMeta};
use crate::sst::{FlatSchemaOptions, to_flat_sst_arrow_schema};
use crate::test_util::{new_batch_builder, new_noop_file_purger};

/// Test region id.
const REGION_ID: RegionId = RegionId::new(0, 0);

/// Creates a new region metadata for testing SSTs with specified encoding.
///
/// Dense schema: tag_0, tag_1, field_0, ts
/// Sparse schema: __table_id, __tsid, tag_0, tag_1, field_0, ts
pub fn sst_region_metadata_with_encoding(
    encoding: store_api::codec::PrimaryKeyEncoding,
) -> RegionMetadata {
    let mut builder = RegionMetadataBuilder::new(REGION_ID);

    // For sparse encoding, add internal columns first
    if encoding == store_api::codec::PrimaryKeyEncoding::Sparse {
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    DATA_SCHEMA_TABLE_ID_COLUMN_NAME.to_string(),
                    ConcreteDataType::uint32_datatype(),
                    false,
                )
                .with_skipping_options(SkippingIndexOptions {
                    granularity: 1,
                    ..Default::default()
                })
                .unwrap(),
                semantic_type: SemanticType::Tag,
                column_id: ReservedColumnId::table_id(),
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    DATA_SCHEMA_TSID_COLUMN_NAME.to_string(),
                    ConcreteDataType::uint64_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Tag,
                column_id: ReservedColumnId::tsid(),
            });
    }

    // Add user-defined columns (tag_0, tag_1, field_0, ts)
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
        });

    // Set primary key based on encoding
    if encoding == store_api::codec::PrimaryKeyEncoding::Sparse {
        builder.primary_key(vec![
            ReservedColumnId::table_id(),
            ReservedColumnId::tsid(),
            0, // tag_0
            1, // tag_1
        ]);
    } else {
        builder.primary_key(vec![0, 1]); // Dense: just user tags
    }

    builder.primary_key_encoding(encoding);
    builder.build().unwrap()
}

/// Creates a new region metadata for testing SSTs.
///
/// Schema: tag_0, tag_1, field_0, ts
pub fn sst_region_metadata() -> RegionMetadata {
    sst_region_metadata_with_encoding(store_api::codec::PrimaryKeyEncoding::Dense)
}

/// Encodes a primary key for specific tags using dense encoding.
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

/// Encodes a primary key for specific tags using sparse encoding.
/// Includes internal columns (table_id, tsid) required by sparse format.
pub fn new_sparse_primary_key(
    tags: &[&str],
    metadata: &Arc<RegionMetadata>,
    table_id: u32,
    tsid: u64,
) -> Vec<u8> {
    use mito_codec::row_converter::PrimaryKeyCodec;

    let codec = mito_codec::row_converter::SparsePrimaryKeyCodec::new(metadata);

    // Sparse encoding requires internal columns first, then user tags
    let values = vec![
        (ReservedColumnId::table_id(), ValueRef::UInt32(table_id)),
        (ReservedColumnId::tsid(), ValueRef::UInt64(tsid)),
        (0, ValueRef::String(tags[0])), // tag_0
        (1, ValueRef::String(tags[1])), // tag_1
    ];

    let mut buffer = Vec::new();
    codec.encode_value_refs(&values, &mut buffer).unwrap();
    buffer
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
            max_row_group_uncompressed_size: 0,
            available_indexes: Default::default(),
            indexes: Default::default(),
            index_file_size: 0,
            index_version: 0,
            num_rows: 0,
            num_row_groups: 0,
            num_series: 0,
            sequence: None,
            partition_expr: None,
            ..Default::default()
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

/// Creates a flat format RecordBatch for testing.
/// Similar to `new_batch_by_range` but returns a RecordBatch in flat format.
pub fn new_record_batch_by_range(tags: &[&str], start: usize, end: usize) -> RecordBatch {
    new_record_batch_with_custom_sequence(tags, start, end, 1000)
}

/// Creates a flat format RecordBatch for testing with a custom sequence.
pub fn new_record_batch_with_custom_sequence(
    tags: &[&str],
    start: usize,
    end: usize,
    sequence: u64,
) -> RecordBatch {
    assert!(end >= start);
    let metadata = Arc::new(sst_region_metadata());
    let flat_schema = to_flat_sst_arrow_schema(&metadata, &FlatSchemaOptions::default());

    let num_rows = end - start;
    let mut columns = Vec::new();

    // Add primary key columns (tag_0, tag_1) as dictionary arrays
    let mut tag_0_builder = StringDictionaryBuilder::<UInt32Type>::new();
    let mut tag_1_builder = StringDictionaryBuilder::<UInt32Type>::new();

    for _ in 0..num_rows {
        tag_0_builder.append_value(tags[0]);
        tag_1_builder.append_value(tags[1]);
    }

    columns.push(Arc::new(tag_0_builder.finish()) as ArrayRef);
    columns.push(Arc::new(tag_1_builder.finish()) as ArrayRef);

    // Add field column (field_0)
    let field_values: Vec<u64> = (start..end).map(|v| v as u64).collect();
    columns.push(Arc::new(UInt64Array::from(field_values)));

    // Add time index column (ts)
    let timestamps: Vec<i64> = (start..end).map(|v| v as i64).collect();
    columns.push(Arc::new(TimestampMillisecondArray::from(timestamps)));

    // Add encoded primary key column
    let pk = new_primary_key(tags);
    let mut pk_builder = BinaryDictionaryBuilder::<UInt32Type>::new();
    for _ in 0..num_rows {
        pk_builder.append(&pk).unwrap();
    }
    columns.push(Arc::new(pk_builder.finish()));

    // Add sequence column
    columns.push(Arc::new(UInt64Array::from_value(sequence, num_rows)));

    // Add op_type column
    columns.push(Arc::new(UInt8Array::from_value(
        OpType::Put as u8,
        num_rows,
    )));
    RecordBatch::try_new(flat_schema, columns).unwrap()
}

/// Creates a FlatSource from flat format RecordBatches.
pub(crate) fn new_flat_source_from_record_batches(batches: Vec<RecordBatch>) -> RecordBatchSource {
    RecordBatchSource::new(
        batches[0].schema(),
        FlatSource::Iter(Box::new(batches.into_iter().map(Ok))),
    )
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
