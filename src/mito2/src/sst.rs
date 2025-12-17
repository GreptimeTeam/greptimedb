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

//! Sorted strings tables.

use std::sync::Arc;

use api::v1::SemanticType;
use common_base::readable_size::ReadableSize;
use datatypes::arrow::datatypes::{
    DataType as ArrowDataType, Field, FieldRef, Fields, Schema, SchemaRef,
};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::prelude::ConcreteDataType;
use datatypes::timestamp::timestamp_array_to_primitive;
use serde::{Deserialize, Serialize};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::RegionMetadata;
use store_api::storage::consts::{
    OP_TYPE_COLUMN_NAME, PRIMARY_KEY_COLUMN_NAME, SEQUENCE_COLUMN_NAME,
};

use crate::read::Batch;
use crate::sst::parquet::flat_format::time_index_column_index;

pub mod file;
pub mod file_purger;
pub mod file_ref;
pub mod index;
pub mod location;
pub mod parquet;
pub(crate) mod version;

/// Default write buffer size, it should be greater than the default minimum upload part of S3 (5mb).
pub const DEFAULT_WRITE_BUFFER_SIZE: ReadableSize = ReadableSize::mb(8);

/// Default number of concurrent write, it only works on object store backend(e.g., S3).
pub const DEFAULT_WRITE_CONCURRENCY: usize = 8;

/// Format type of the SST file.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, strum::EnumString)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum FormatType {
    /// Parquet with primary key encoded.
    #[default]
    PrimaryKey,
    /// Flat Parquet format.
    Flat,
}

/// Gets the arrow schema to store in parquet.
pub fn to_sst_arrow_schema(metadata: &RegionMetadata) -> SchemaRef {
    let fields = Fields::from_iter(
        metadata
            .schema
            .arrow_schema()
            .fields()
            .iter()
            .zip(&metadata.column_metadatas)
            .filter_map(|(field, column_meta)| {
                if column_meta.semantic_type == SemanticType::Field {
                    Some(field.clone())
                } else {
                    // We have fixed positions for tags (primary key) and time index.
                    None
                }
            })
            .chain([metadata.time_index_field()])
            .chain(internal_fields()),
    );

    Arc::new(Schema::new(fields))
}

/// Options of flat schema.
pub struct FlatSchemaOptions {
    /// Whether to store primary key columns additionally instead of an encoded column.
    pub raw_pk_columns: bool,
    /// Whether to use dictionary encoding for string primary key columns
    /// when storing primary key columns.
    /// Only takes effect when `raw_pk_columns` is true.
    pub string_pk_use_dict: bool,
}

impl Default for FlatSchemaOptions {
    fn default() -> Self {
        Self {
            raw_pk_columns: true,
            string_pk_use_dict: true,
        }
    }
}

impl FlatSchemaOptions {
    /// Creates a options according to the primary key encoding.
    pub fn from_encoding(encoding: PrimaryKeyEncoding) -> Self {
        if encoding == PrimaryKeyEncoding::Dense {
            Self::default()
        } else {
            Self {
                raw_pk_columns: false,
                string_pk_use_dict: false,
            }
        }
    }
}

/// Gets the arrow schema to store in parquet.
///
/// The schema is:
/// ```text
/// primary key columns, field columns, time index, __primary_key, __sequence, __op_type
/// ```
///
/// # Panics
/// Panics if the metadata is invalid.
pub fn to_flat_sst_arrow_schema(
    metadata: &RegionMetadata,
    options: &FlatSchemaOptions,
) -> SchemaRef {
    let num_fields = flat_sst_arrow_schema_column_num(metadata, options);
    let mut fields = Vec::with_capacity(num_fields);
    let schema = metadata.schema.arrow_schema();
    if options.raw_pk_columns {
        for pk_id in &metadata.primary_key {
            let pk_index = metadata.column_index_by_id(*pk_id).unwrap();
            if options.string_pk_use_dict {
                let old_field = &schema.fields[pk_index];
                let new_field = tag_maybe_to_dictionary_field(
                    &metadata.column_metadatas[pk_index].column_schema.data_type,
                    old_field,
                );
                fields.push(new_field);
            }
        }
    }
    let remaining_fields = schema
        .fields()
        .iter()
        .zip(&metadata.column_metadatas)
        .filter_map(|(field, column_meta)| {
            if column_meta.semantic_type == SemanticType::Field {
                Some(field.clone())
            } else {
                None
            }
        })
        .chain([metadata.time_index_field()])
        .chain(internal_fields());
    for field in remaining_fields {
        fields.push(field);
    }

    Arc::new(Schema::new(fields))
}

/// Returns the number of columns in the flat format.
pub fn flat_sst_arrow_schema_column_num(
    metadata: &RegionMetadata,
    options: &FlatSchemaOptions,
) -> usize {
    if options.raw_pk_columns {
        metadata.column_metadatas.len() + 3
    } else {
        metadata.column_metadatas.len() + 3 - metadata.primary_key.len()
    }
}

/// Helper function to create a dictionary field from a field.
fn to_dictionary_field(field: &Field) -> Field {
    Field::new_dictionary(
        field.name(),
        datatypes::arrow::datatypes::DataType::UInt32,
        field.data_type().clone(),
        field.is_nullable(),
    )
}

/// Helper function to create a dictionary field from a field if it is a string column.
pub(crate) fn tag_maybe_to_dictionary_field(
    data_type: &ConcreteDataType,
    field: &Arc<Field>,
) -> Arc<Field> {
    if data_type.is_string() {
        Arc::new(to_dictionary_field(field))
    } else {
        field.clone()
    }
}

/// Fields for internal columns.
pub(crate) fn internal_fields() -> [FieldRef; 3] {
    // Internal columns are always not null.
    [
        Arc::new(Field::new_dictionary(
            PRIMARY_KEY_COLUMN_NAME,
            ArrowDataType::UInt32,
            ArrowDataType::Binary,
            false,
        )),
        Arc::new(Field::new(
            SEQUENCE_COLUMN_NAME,
            ArrowDataType::UInt64,
            false,
        )),
        Arc::new(Field::new(OP_TYPE_COLUMN_NAME, ArrowDataType::UInt8, false)),
    ]
}

/// Gets the arrow schema to store in parquet.
pub fn to_plain_sst_arrow_schema(metadata: &RegionMetadata) -> SchemaRef {
    let fields = Fields::from_iter(
        metadata
            .schema
            .arrow_schema()
            .fields()
            .iter()
            .cloned()
            .chain(plain_internal_fields()),
    );

    Arc::new(Schema::new(fields))
}

/// Fields for internal columns.
fn plain_internal_fields() -> [FieldRef; 2] {
    // Internal columns are always not null.
    [
        Arc::new(Field::new(
            SEQUENCE_COLUMN_NAME,
            ArrowDataType::UInt64,
            false,
        )),
        Arc::new(Field::new(OP_TYPE_COLUMN_NAME, ArrowDataType::UInt8, false)),
    ]
}

/// Gets the estimated number of series from record batches.
///
/// This struct tracks the last timestamp value to detect series boundaries
/// by observing when timestamps decrease (indicating a new series).
#[derive(Default)]
pub(crate) struct SeriesEstimator {
    /// The last timestamp value seen
    last_timestamp: Option<i64>,
    /// The estimated number of series
    series_count: u64,
}

impl SeriesEstimator {
    /// Updates the estimator with a new Batch.
    ///
    /// Since each Batch contains only one series, this increments the series count
    /// and updates the last timestamp.
    pub(crate) fn update(&mut self, batch: &Batch) {
        let Some(last_ts) = batch.last_timestamp() else {
            return;
        };

        // Checks if there's a boundary between the last batch and this batch
        if let Some(prev_last_ts) = self.last_timestamp {
            // If the first timestamp of this batch is less than the last timestamp
            // we've seen, it indicates a new series
            if let Some(first_ts) = batch.first_timestamp()
                && first_ts.value() <= prev_last_ts
            {
                self.series_count += 1;
            }
        } else {
            // First batch, counts as first series
            self.series_count = 1;
        }

        // Updates the last timestamp
        self.last_timestamp = Some(last_ts.value());
    }

    /// Updates the estimator with a new record batch in flat format.
    ///
    /// This method examines the time index column to detect series boundaries.
    pub(crate) fn update_flat(&mut self, record_batch: &RecordBatch) {
        let batch_rows = record_batch.num_rows();
        if batch_rows == 0 {
            return;
        }

        let time_index_pos = time_index_column_index(record_batch.num_columns());
        let timestamps = record_batch.column(time_index_pos);
        let Some((ts_values, _unit)) = timestamp_array_to_primitive(timestamps) else {
            return;
        };
        let values = ts_values.values();

        // Checks if there's a boundary between the last batch and this batch
        if let Some(last_ts) = self.last_timestamp {
            if values[0] <= last_ts {
                self.series_count += 1;
            }
        } else {
            // First batch, counts as first series
            self.series_count = 1;
        }

        // Counts series boundaries within this batch.
        for i in 0..batch_rows - 1 {
            // We assumes the same timestamp as a new series, which is different from
            // how we split batches.
            if values[i] >= values[i + 1] {
                self.series_count += 1;
            }
        }

        // Updates the last timestamp
        self.last_timestamp = Some(values[batch_rows - 1]);
    }

    /// Returns the estimated number of series.
    pub(crate) fn finish(&mut self) -> u64 {
        self.last_timestamp = None;
        let count = self.series_count;
        self.series_count = 0;

        count
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::OpType;
    use datatypes::arrow::array::{
        BinaryArray, DictionaryArray, TimestampMillisecondArray, UInt8Array, UInt8Builder,
        UInt32Array, UInt64Array,
    };
    use datatypes::arrow::datatypes::{DataType as ArrowDataType, Field, Schema, TimeUnit};
    use datatypes::arrow::record_batch::RecordBatch;

    use super::*;
    use crate::read::{Batch, BatchBuilder};

    fn new_batch(
        primary_key: &[u8],
        timestamps: &[i64],
        sequences: &[u64],
        op_types: &[OpType],
    ) -> Batch {
        let timestamps = Arc::new(TimestampMillisecondArray::from(timestamps.to_vec()));
        let sequences = Arc::new(UInt64Array::from(sequences.to_vec()));
        let mut op_type_builder = UInt8Builder::with_capacity(op_types.len());
        for op_type in op_types {
            op_type_builder.append_value(*op_type as u8);
        }
        let op_types = Arc::new(UInt8Array::from(
            op_types.iter().map(|op| *op as u8).collect::<Vec<_>>(),
        ));

        let mut builder = BatchBuilder::new(primary_key.to_vec());
        builder
            .timestamps_array(timestamps)
            .unwrap()
            .sequences_array(sequences)
            .unwrap()
            .op_types_array(op_types)
            .unwrap();
        builder.build().unwrap()
    }

    fn new_flat_record_batch(timestamps: &[i64]) -> RecordBatch {
        // Flat format has: [fields..., time_index, __primary_key, __sequence, __op_type]
        let num_cols = 4; // time_index + 3 internal columns
        let time_index_pos = time_index_column_index(num_cols);
        assert_eq!(time_index_pos, 0); // For 4 columns, time index should be at position 0

        let time_array = Arc::new(TimestampMillisecondArray::from(timestamps.to_vec()));
        let pk_array = Arc::new(DictionaryArray::new(
            UInt32Array::from(vec![0; timestamps.len()]),
            Arc::new(BinaryArray::from(vec![b"test".as_slice()])),
        ));
        let seq_array = Arc::new(UInt64Array::from(vec![1; timestamps.len()]));
        let op_array = Arc::new(UInt8Array::from(vec![1; timestamps.len()]));

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "time",
                ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new_dictionary(
                "__primary_key",
                ArrowDataType::UInt32,
                ArrowDataType::Binary,
                false,
            ),
            Field::new("__sequence", ArrowDataType::UInt64, false),
            Field::new("__op_type", ArrowDataType::UInt8, false),
        ]));

        RecordBatch::try_new(schema, vec![time_array, pk_array, seq_array, op_array]).unwrap()
    }

    #[test]
    fn test_series_estimator_empty_batch() {
        let mut estimator = SeriesEstimator::default();
        let batch = new_batch(b"test", &[], &[], &[]);
        estimator.update(&batch);
        assert_eq!(0, estimator.finish());
    }

    #[test]
    fn test_series_estimator_single_batch() {
        let mut estimator = SeriesEstimator::default();
        let batch = new_batch(
            b"test",
            &[1, 2, 3],
            &[1, 2, 3],
            &[OpType::Put, OpType::Put, OpType::Put],
        );
        estimator.update(&batch);
        assert_eq!(1, estimator.finish());
    }

    #[test]
    fn test_series_estimator_multiple_batches_same_series() {
        let mut estimator = SeriesEstimator::default();

        // First batch with timestamps 1, 2, 3
        let batch1 = new_batch(
            b"test",
            &[1, 2, 3],
            &[1, 2, 3],
            &[OpType::Put, OpType::Put, OpType::Put],
        );
        estimator.update(&batch1);

        // Second batch with timestamps 4, 5, 6 (continuation)
        let batch2 = new_batch(
            b"test",
            &[4, 5, 6],
            &[4, 5, 6],
            &[OpType::Put, OpType::Put, OpType::Put],
        );
        estimator.update(&batch2);

        assert_eq!(1, estimator.finish());
    }

    #[test]
    fn test_series_estimator_new_series_detected() {
        let mut estimator = SeriesEstimator::default();

        // First batch with timestamps 1, 2, 3
        let batch1 = new_batch(
            b"pk0",
            &[1, 2, 3],
            &[1, 2, 3],
            &[OpType::Put, OpType::Put, OpType::Put],
        );
        estimator.update(&batch1);

        // Second batch with timestamps 2, 3, 4 (timestamp goes back, new series)
        let batch2 = new_batch(
            b"pk1",
            &[2, 3, 4],
            &[4, 5, 6],
            &[OpType::Put, OpType::Put, OpType::Put],
        );
        estimator.update(&batch2);

        assert_eq!(2, estimator.finish());
    }

    #[test]
    fn test_series_estimator_equal_timestamp_boundary() {
        let mut estimator = SeriesEstimator::default();

        // First batch ending at timestamp 5
        let batch1 = new_batch(
            b"test",
            &[1, 2, 5],
            &[1, 2, 3],
            &[OpType::Put, OpType::Put, OpType::Put],
        );
        estimator.update(&batch1);

        // Second batch starting at timestamp 5 (equal, indicates new series)
        let batch2 = new_batch(
            b"test",
            &[5, 6, 7],
            &[4, 5, 6],
            &[OpType::Put, OpType::Put, OpType::Put],
        );
        estimator.update(&batch2);

        assert_eq!(2, estimator.finish());
    }

    #[test]
    fn test_series_estimator_finish_resets_state() {
        let mut estimator = SeriesEstimator::default();

        let batch1 = new_batch(
            b"test",
            &[1, 2, 3],
            &[1, 2, 3],
            &[OpType::Put, OpType::Put, OpType::Put],
        );
        estimator.update(&batch1);

        assert_eq!(1, estimator.finish());

        // After finish, state should be reset
        let batch2 = new_batch(
            b"test",
            &[4, 5, 6],
            &[4, 5, 6],
            &[OpType::Put, OpType::Put, OpType::Put],
        );
        estimator.update(&batch2);

        assert_eq!(1, estimator.finish());
    }

    #[test]
    fn test_series_estimator_flat_empty_batch() {
        let mut estimator = SeriesEstimator::default();
        let record_batch = new_flat_record_batch(&[]);
        estimator.update_flat(&record_batch);
        assert_eq!(0, estimator.finish());
    }

    #[test]
    fn test_series_estimator_flat_single_batch() {
        let mut estimator = SeriesEstimator::default();
        let record_batch = new_flat_record_batch(&[1, 2, 3]);
        estimator.update_flat(&record_batch);
        assert_eq!(1, estimator.finish());
    }

    #[test]
    fn test_series_estimator_flat_series_boundary_within_batch() {
        let mut estimator = SeriesEstimator::default();
        // Timestamps decrease from 3 to 2, indicating a series boundary
        let record_batch = new_flat_record_batch(&[1, 2, 3, 2, 4, 5]);
        estimator.update_flat(&record_batch);
        // Should detect boundary at position 3 (3 >= 2)
        assert_eq!(2, estimator.finish());
    }

    #[test]
    fn test_series_estimator_flat_multiple_boundaries_within_batch() {
        let mut estimator = SeriesEstimator::default();
        // Multiple series boundaries: 5>=4, 6>=3
        let record_batch = new_flat_record_batch(&[1, 2, 5, 4, 6, 3, 7]);
        estimator.update_flat(&record_batch);
        assert_eq!(3, estimator.finish());
    }

    #[test]
    fn test_series_estimator_flat_equal_timestamps() {
        let mut estimator = SeriesEstimator::default();
        // Equal timestamps are considered as new series
        let record_batch = new_flat_record_batch(&[1, 2, 2, 3, 3, 3, 4]);
        estimator.update_flat(&record_batch);
        // Boundaries at: 2>=2, 3>=3, 3>=3
        assert_eq!(4, estimator.finish());
    }

    #[test]
    fn test_series_estimator_flat_multiple_batches_continuation() {
        let mut estimator = SeriesEstimator::default();

        // First batch: timestamps 1, 2, 3
        let batch1 = new_flat_record_batch(&[1, 2, 3]);
        estimator.update_flat(&batch1);

        // Second batch: timestamps 4, 5, 6 (continuation)
        let batch2 = new_flat_record_batch(&[4, 5, 6]);
        estimator.update_flat(&batch2);

        assert_eq!(1, estimator.finish());
    }

    #[test]
    fn test_series_estimator_flat_multiple_batches_new_series() {
        let mut estimator = SeriesEstimator::default();

        // First batch: timestamps 1, 2, 3
        let batch1 = new_flat_record_batch(&[1, 2, 3]);
        estimator.update_flat(&batch1);

        // Second batch: timestamps 2, 3, 4 (goes back to 2, new series)
        let batch2 = new_flat_record_batch(&[2, 3, 4]);
        estimator.update_flat(&batch2);

        assert_eq!(2, estimator.finish());
    }

    #[test]
    fn test_series_estimator_flat_boundary_at_batch_edge_equal() {
        let mut estimator = SeriesEstimator::default();

        // First batch ending at 5
        let batch1 = new_flat_record_batch(&[1, 2, 5]);
        estimator.update_flat(&batch1);

        // Second batch starting at 5 (equal timestamp, new series)
        let batch2 = new_flat_record_batch(&[5, 6, 7]);
        estimator.update_flat(&batch2);

        assert_eq!(2, estimator.finish());
    }

    #[test]
    fn test_series_estimator_flat_mixed_batches() {
        let mut estimator = SeriesEstimator::default();

        // Batch 1: single series [10, 20, 30]
        let batch1 = new_flat_record_batch(&[10, 20, 30]);
        estimator.update_flat(&batch1);

        // Batch 2: starts new series [5, 15], boundary within batch [15, 10, 25]
        let batch2 = new_flat_record_batch(&[5, 15, 10, 25]);
        estimator.update_flat(&batch2);

        // Batch 3: continues from 25 to [30, 35]
        let batch3 = new_flat_record_batch(&[30, 35]);
        estimator.update_flat(&batch3);

        // Expected: 1 (batch1) + 1 (batch2 start) + 1 (within batch2) = 3
        assert_eq!(3, estimator.finish());
    }

    #[test]
    fn test_series_estimator_flat_descending_timestamps() {
        let mut estimator = SeriesEstimator::default();
        // Strictly descending timestamps - each pair creates a boundary
        let record_batch = new_flat_record_batch(&[10, 9, 8, 7, 6]);
        estimator.update_flat(&record_batch);
        // Boundaries: 10>=9, 9>=8, 8>=7, 7>=6 = 4 boundaries + 1 initial = 5 series
        assert_eq!(5, estimator.finish());
    }

    #[test]
    fn test_series_estimator_flat_finish_resets_state() {
        let mut estimator = SeriesEstimator::default();

        let batch1 = new_flat_record_batch(&[1, 2, 3]);
        estimator.update_flat(&batch1);

        assert_eq!(1, estimator.finish());

        // After finish, state should be reset
        let batch2 = new_flat_record_batch(&[4, 5, 6]);
        estimator.update_flat(&batch2);

        assert_eq!(1, estimator.finish());
    }
}
