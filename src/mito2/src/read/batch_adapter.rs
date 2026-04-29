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

//! Adapter to convert [`BoxedBatchIterator`] (primary key format) into an iterator
//! of flat-format Arrow [`RecordBatch`]es, allowing memtable iterators that only
//! produce [`Batch`] to feed into the flat read pipeline.

use std::borrow::Cow;
use std::collections::HashSet;
use std::sync::Arc;

use api::v1::SemanticType;
use datatypes::arrow::array::{ArrayRef, BinaryArray, DictionaryArray, UInt32Array};
use datatypes::arrow::datatypes::{Field, SchemaRef};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::prelude::{ConcreteDataType, DataType, Vector};
use mito_codec::row_converter::{CompositeValues, PrimaryKeyCodec};
use snafu::ResultExt;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;

use crate::error::{
    DataTypeMismatchSnafu, DecodeSnafu, EvalPartitionFilterSnafu, NewRecordBatchSnafu, Result,
};
use crate::memtable::BoxedBatchIterator;
use crate::read::Batch;
use crate::sst::{internal_fields, tag_maybe_to_dictionary_field};

/// Adapts a [`BoxedBatchIterator`] into an `Iterator<Item = Result<RecordBatch>>`
/// producing flat-format record batches.
pub struct BatchToRecordBatchAdapter {
    iter: BoxedBatchIterator,
    codec: Arc<dyn PrimaryKeyCodec>,
    output_schema: SchemaRef,
    projected_pk: Vec<ProjectedPkColumn>,
}

struct ProjectedPkColumn {
    column_id: ColumnId,
    pk_index: usize,
    data_type: ConcreteDataType,
}

impl BatchToRecordBatchAdapter {
    /// Creates a new adapter.
    ///
    /// - `iter`: the source batch iterator producing primary-key-format batches.
    /// - `metadata`: region metadata describing the schema.
    /// - `codec`: codec for decoding the encoded primary key bytes.
    /// - `read_column_ids`: projected column ids to read.
    pub fn new(
        iter: BoxedBatchIterator,
        metadata: RegionMetadataRef,
        codec: Arc<dyn PrimaryKeyCodec>,
        read_column_ids: &[ColumnId],
    ) -> Self {
        let read_column_id_set: HashSet<_> = read_column_ids.iter().copied().collect();
        let projected_pk = metadata
            .primary_key_columns()
            .enumerate()
            .filter(|(_, column_metadata)| read_column_id_set.contains(&column_metadata.column_id))
            .map(|(pk_index, column_metadata)| ProjectedPkColumn {
                column_id: column_metadata.column_id,
                pk_index,
                data_type: column_metadata.column_schema.data_type.clone(),
            })
            .collect();
        let output_schema = compute_output_arrow_schema(&metadata, &read_column_id_set);

        Self {
            iter,
            codec,
            output_schema,
            projected_pk,
        }
    }

    /// Converts a single [`Batch`] into a flat-format [`RecordBatch`].
    fn convert_batch(&self, batch: &Batch) -> Result<RecordBatch> {
        let num_rows = batch.num_rows();

        let pk_values = if let Some(vals) = batch.pk_values() {
            Cow::Borrowed(vals)
        } else {
            Cow::Owned(
                self.codec
                    .decode(batch.primary_key())
                    .context(DecodeSnafu)?,
            )
        };

        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.output_schema.fields().len());
        for pk_column in &self.projected_pk {
            if pk_column.data_type.is_string() {
                let value = get_pk_value(&pk_values, pk_column.column_id, pk_column.pk_index);
                columns.push(build_string_tag_dict_array(
                    value,
                    &pk_column.data_type,
                    num_rows,
                ));
            } else {
                let value = get_pk_value(&pk_values, pk_column.column_id, pk_column.pk_index);
                let array = build_repeated_value_array(value, &pk_column.data_type, num_rows)?;
                columns.push(array);
            }
        }
        for batch_col in batch.fields() {
            columns.push(batch_col.data.to_arrow_array());
        }

        columns.push(batch.timestamps().to_arrow_array());

        // __primary_key
        let pk_bytes = batch.primary_key();
        let values = Arc::new(BinaryArray::from_iter_values([pk_bytes]));
        let keys = UInt32Array::from(vec![0u32; num_rows]);
        let pk_dict: ArrayRef = Arc::new(DictionaryArray::new(keys, values));
        columns.push(pk_dict);

        // __sequence.
        columns.push(batch.sequences().to_arrow_array());

        // __op_type.
        columns.push(batch.op_types().to_arrow_array());

        RecordBatch::try_new(self.output_schema.clone(), columns).context(NewRecordBatchSnafu)
    }
}

impl Iterator for BatchToRecordBatchAdapter {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.iter.next()? {
                Ok(batch) => {
                    if batch.is_empty() {
                        continue;
                    }
                    return Some(self.convert_batch(&batch));
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

/// Extracts a value for the given primary key column from decoded [`CompositeValues`].
fn get_pk_value(
    pk_values: &CompositeValues,
    column_id: ColumnId,
    pk_index: usize,
) -> &datatypes::value::Value {
    match pk_values {
        CompositeValues::Dense(dense) => {
            if pk_index < dense.len() {
                &dense[pk_index].1
            } else {
                &datatypes::value::Value::Null
            }
        }
        CompositeValues::Sparse(sparse) => sparse.get_or_null(column_id),
    }
}

/// Builds an Arrow array of `num_rows` copies of `value`.
fn build_repeated_value_array(
    value: &datatypes::value::Value,
    data_type: &ConcreteDataType,
    num_rows: usize,
) -> Result<ArrayRef> {
    let scalar = value
        .try_to_scalar_value(data_type)
        .context(DataTypeMismatchSnafu)?;
    scalar
        .to_array_of_size(num_rows)
        .context(EvalPartitionFilterSnafu)
}

/// Builds a dictionary-encoded string tag array with one dictionary value.
fn build_string_tag_dict_array(
    value: &datatypes::value::Value,
    data_type: &ConcreteDataType,
    num_rows: usize,
) -> ArrayRef {
    let mut builder = data_type.create_mutable_vector(1);
    builder.push_value_ref(&value.as_value_ref());
    let values = builder.to_vector().to_arrow_array();

    let keys = UInt32Array::from(vec![0u32; num_rows]);
    Arc::new(DictionaryArray::new(keys, values))
}

fn compute_output_arrow_schema(
    metadata: &RegionMetadataRef,
    read_column_id_set: &HashSet<ColumnId>,
) -> SchemaRef {
    let mut fields = Vec::new();

    for column_metadata in metadata.primary_key_columns() {
        if !read_column_id_set.contains(&column_metadata.column_id) {
            continue;
        }
        let field = Arc::new(Field::new(
            &column_metadata.column_schema.name,
            column_metadata.column_schema.data_type.as_arrow_type(),
            column_metadata.column_schema.is_nullable(),
        ));
        let field = if column_metadata.semantic_type == SemanticType::Tag {
            tag_maybe_to_dictionary_field(&column_metadata.column_schema.data_type, &field)
        } else {
            field
        };
        fields.push(field);
    }

    for column_metadata in metadata.field_columns() {
        if !read_column_id_set.contains(&column_metadata.column_id) {
            continue;
        }
        let field = Arc::new(Field::new(
            &column_metadata.column_schema.name,
            column_metadata.column_schema.data_type.as_arrow_type(),
            column_metadata.column_schema.is_nullable(),
        ));
        fields.push(field);
    }

    let time_index = metadata.time_index_column();
    let time_index_field = Arc::new(Field::new(
        &time_index.column_schema.name,
        time_index.column_schema.data_type.as_arrow_type(),
        time_index.column_schema.is_nullable(),
    ));
    fields.push(time_index_field);
    fields.extend(internal_fields().iter().cloned());

    Arc::new(datatypes::arrow::datatypes::Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::{OpType, SemanticType};
    use datatypes::arrow::array::{Array, TimestampMillisecondArray, UInt8Array, UInt64Array};
    use datatypes::arrow::datatypes::UInt32Type;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use mito_codec::row_converter::{PrimaryKeyCodec, build_primary_key_codec};
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder, RegionMetadataRef};
    use store_api::storage::RegionId;

    use super::*;
    use crate::read::flat_projection::FlatProjectionMapper;
    use crate::sst::{FlatSchemaOptions, to_flat_sst_arrow_schema};
    use crate::test_util::new_batch_builder;
    use crate::test_util::sst_util::{new_primary_key, sst_region_metadata};

    /// Helper to build the adapter from batches and metadata.
    fn build_adapter(
        batches: Vec<Batch>,
        metadata: &RegionMetadataRef,
        codec: &Arc<dyn PrimaryKeyCodec>,
    ) -> BatchToRecordBatchAdapter {
        let read_column_ids = metadata
            .column_metadatas
            .iter()
            .map(|column| column.column_id)
            .collect::<Vec<_>>();
        let iter: BoxedBatchIterator = Box::new(batches.into_iter().map(Ok));
        BatchToRecordBatchAdapter::new(
            iter,
            Arc::clone(metadata),
            Arc::clone(codec),
            &read_column_ids,
        )
    }

    #[test]
    fn test_single_batch_two_tags() {
        // Schema: tag_0(string), tag_1(string), field_0(u64), ts
        let metadata = Arc::new(sst_region_metadata());
        let codec = build_primary_key_codec(&metadata);

        let pk = new_primary_key(&["host-1", "region-a"]);
        let batch = new_batch_builder(
            &pk,
            &[1, 2, 3],
            &[100, 100, 100],
            &[OpType::Put, OpType::Put, OpType::Put],
            2,
            &[10, 20, 30],
        )
        .build()
        .unwrap();

        let adapter = build_adapter(vec![batch], &metadata, &codec);
        let results: Vec<_> = adapter.collect::<Vec<_>>();
        assert_eq!(1, results.len());

        let rb = results[0].as_ref().unwrap();
        let expected_schema = to_flat_sst_arrow_schema(&metadata, &FlatSchemaOptions::default());
        assert_eq!(rb.schema(), expected_schema);
        assert_eq!(3, rb.num_rows());
        // 2 tags + 1 field + 1 time index + 3 internal = 7 columns
        assert_eq!(7, rb.num_columns());
    }

    #[test]
    fn test_multiple_batches() {
        let metadata = Arc::new(sst_region_metadata());
        let codec = build_primary_key_codec(&metadata);

        let pk1 = new_primary_key(&["a", "b"]);
        let batch1 = new_batch_builder(
            &pk1,
            &[1, 2],
            &[100, 100],
            &[OpType::Put, OpType::Put],
            2,
            &[10, 20],
        )
        .build()
        .unwrap();

        let pk2 = new_primary_key(&["c", "d"]);
        let batch2 = new_batch_builder(
            &pk2,
            &[3, 4],
            &[200, 200],
            &[OpType::Put, OpType::Put],
            2,
            &[30, 40],
        )
        .build()
        .unwrap();

        let adapter = build_adapter(vec![batch1, batch2], &metadata, &codec);
        let results: Vec<_> = adapter.map(|r| r.unwrap()).collect();
        assert_eq!(2, results.len());

        assert_eq!(2, results[0].num_rows());
        assert_eq!(2, results[1].num_rows());
    }

    #[test]
    fn test_empty_batch_skipped() {
        let metadata = Arc::new(sst_region_metadata());
        let codec = build_primary_key_codec(&metadata);

        let empty = Batch::empty();
        let pk = new_primary_key(&["x", "y"]);
        let batch = new_batch_builder(&pk, &[1], &[1], &[OpType::Put], 2, &[42])
            .build()
            .unwrap();

        let adapter = build_adapter(vec![empty, batch], &metadata, &codec);
        let results: Vec<_> = adapter.map(|r| r.unwrap()).collect();
        assert_eq!(1, results.len());
        assert_eq!(1, results[0].num_rows());
    }

    #[test]
    fn test_no_tags() {
        // Schema with no primary key columns: field_0(u64), ts
        let mut builder = RegionMetadataBuilder::new(RegionId::new(0, 0));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "field_0".to_string(),
                    ConcreteDataType::uint64_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Field,
                column_id: 0,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts".to_string(),
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 1,
            });
        builder.primary_key(vec![]);
        let metadata = Arc::new(builder.build().unwrap());
        let codec = build_primary_key_codec(&metadata);

        // Empty primary key
        let pk = vec![];
        let batch = new_batch_builder(
            &pk,
            &[1, 2],
            &[100, 100],
            &[OpType::Put, OpType::Put],
            0,
            &[10, 20],
        )
        .build()
        .unwrap();

        let adapter = build_adapter(vec![batch], &metadata, &codec);
        let results: Vec<_> = adapter.map(|r| r.unwrap()).collect();
        assert_eq!(1, results.len());

        let rb = &results[0];
        let expected_schema = to_flat_sst_arrow_schema(&metadata, &FlatSchemaOptions::default());
        assert_eq!(rb.schema(), expected_schema);
        // 0 tags + 1 field + 1 time index + 3 internal = 5 columns
        assert_eq!(5, rb.num_columns());
        assert_eq!(2, rb.num_rows());
    }

    #[test]
    fn test_primary_key_dict_column() {
        // Verify the __primary_key column is a proper dictionary array.
        let metadata = Arc::new(sst_region_metadata());
        let codec = build_primary_key_codec(&metadata);

        let pk = new_primary_key(&["host", "az"]);
        let batch = new_batch_builder(
            &pk,
            &[1, 2],
            &[1, 1],
            &[OpType::Put, OpType::Put],
            2,
            &[5, 6],
        )
        .build()
        .unwrap();

        let adapter = build_adapter(vec![batch.clone()], &metadata, &codec);
        let rb = adapter.into_iter().next().unwrap().unwrap();

        // __primary_key is at num_columns - 3
        let pk_col_idx = rb.num_columns() - 3;
        let pk_array = rb
            .column(pk_col_idx)
            .as_any()
            .downcast_ref::<DictionaryArray<UInt32Type>>()
            .expect("should be DictionaryArray<UInt32>");

        // Should have 2 rows, all pointing to key 0
        assert_eq!(2, pk_array.len());
        assert_eq!(0, pk_array.keys().value(0));
        assert_eq!(0, pk_array.keys().value(1));

        // The single dictionary value should be the encoded pk bytes.
        let values = pk_array
            .values()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(1, values.len());
        assert_eq!(batch.primary_key(), values.value(0));
    }

    #[test]
    fn test_sequence_and_op_type_columns() {
        let metadata = Arc::new(sst_region_metadata());
        let codec = build_primary_key_codec(&metadata);

        let pk = new_primary_key(&["a", "b"]);
        let batch = new_batch_builder(
            &pk,
            &[10, 20, 30],
            &[1, 2, 3],
            &[OpType::Put, OpType::Delete, OpType::Put],
            2,
            &[100, 200, 300],
        )
        .build()
        .unwrap();

        let adapter = build_adapter(vec![batch], &metadata, &codec);
        let rb = adapter.into_iter().next().unwrap().unwrap();

        // __sequence is at num_columns - 2
        let seq_idx = rb.num_columns() - 2;
        let seq_array = rb
            .column(seq_idx)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(&[1u64, 2, 3], seq_array.values().as_ref());

        // __op_type is at num_columns - 1
        let op_idx = rb.num_columns() - 1;
        let op_array = rb
            .column(op_idx)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();
        assert_eq!(
            &[OpType::Put as u8, OpType::Delete as u8, OpType::Put as u8],
            op_array.values().as_ref()
        );
    }

    #[test]
    fn test_integer_tag_column() {
        // Schema with an integer (non-string) tag: tag_0(u32), field_0(u64), ts
        let mut builder = RegionMetadataBuilder::new(RegionId::new(0, 0));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "tag_0".to_string(),
                    ConcreteDataType::uint32_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Tag,
                column_id: 0,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "field_0".to_string(),
                    ConcreteDataType::uint64_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Field,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts".to_string(),
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 2,
            });
        builder.primary_key(vec![0]);
        let metadata = Arc::new(builder.build().unwrap());
        let codec = build_primary_key_codec(&metadata);

        // Encode integer primary key
        let pk = {
            use datatypes::value::ValueRef;
            use mito_codec::row_converter::PrimaryKeyCodecExt;
            let codec_ext = mito_codec::row_converter::DensePrimaryKeyCodec::with_fields(vec![(
                0,
                mito_codec::row_converter::SortField::new(ConcreteDataType::uint32_datatype()),
            )]);
            codec_ext
                .encode([ValueRef::UInt32(42)].into_iter())
                .unwrap()
        };
        let batch = new_batch_builder(
            &pk,
            &[1, 2],
            &[1, 1],
            &[OpType::Put, OpType::Put],
            1,
            &[10, 20],
        )
        .build()
        .unwrap();

        let adapter = build_adapter(vec![batch], &metadata, &codec);
        let rb = adapter.into_iter().next().unwrap().unwrap();

        let expected_schema = to_flat_sst_arrow_schema(&metadata, &FlatSchemaOptions::default());
        assert_eq!(rb.schema(), expected_schema);

        // tag_0 column (index 0) should be a regular (non-dictionary) UInt32 array
        let tag_array = rb
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("integer tag should be a plain UInt32Array");
        assert_eq!(&[42u32, 42], tag_array.values().as_ref());
    }

    #[test]
    fn test_with_precomputed_pk_values() {
        // If pk_values are already set on the Batch, the adapter should use them
        // instead of calling codec.decode().
        let metadata = Arc::new(sst_region_metadata());
        let codec = build_primary_key_codec(&metadata);

        let pk = new_primary_key(&["pre", "computed"]);
        let mut batch = new_batch_builder(&pk, &[1], &[1], &[OpType::Put], 2, &[99])
            .build()
            .unwrap();

        // Decode and set pk_values ahead of time.
        let decoded = codec.decode(&pk).unwrap();
        batch.set_pk_values(decoded);

        let adapter = build_adapter(vec![batch], &metadata, &codec);
        let rb = adapter.into_iter().next().unwrap().unwrap();
        assert_eq!(1, rb.num_rows());

        let expected_schema = to_flat_sst_arrow_schema(&metadata, &FlatSchemaOptions::default());
        assert_eq!(rb.schema(), expected_schema);
    }

    #[test]
    fn test_partial_projection_schema_matches_mapper() {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(0, 0));
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
                    "field_1".to_string(),
                    ConcreteDataType::uint64_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Field,
                column_id: 3,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts".to_string(),
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 4,
            });
        builder.primary_key(vec![0, 1]);
        let metadata = Arc::new(builder.build().unwrap());
        let codec = build_primary_key_codec(&metadata);

        // Project tag_0 and field_1; skip tag_1 and field_0.
        let read_column_ids = vec![0, 3];

        let pk = new_primary_key(&["host-1", "region-a"]);
        let batch = new_batch_builder(
            &pk,
            &[1, 2, 3],
            &[100, 100, 100],
            &[OpType::Put, OpType::Put, OpType::Put],
            3,
            &[10, 20, 30],
        )
        .build()
        .unwrap();

        let iter: BoxedBatchIterator = Box::new(vec![Ok(batch)].into_iter());
        let adapter =
            BatchToRecordBatchAdapter::new(iter, metadata.clone(), codec, &read_column_ids);
        let rb = adapter.into_iter().next().unwrap().unwrap();

        let mapper = FlatProjectionMapper::new(&metadata, [0, 3]).unwrap();
        assert_eq!(rb.schema(), mapper.input_arrow_schema(false));
        // tag_0 + field_1 + ts + 3 internal columns.
        assert_eq!(6, rb.num_columns());
        assert_eq!(3, rb.num_rows());

        let field_1 = rb.column(1).as_any().downcast_ref::<UInt64Array>().unwrap();
        assert_eq!(&[10u64, 20, 30], field_1.values().as_ref());

        let ts = rb
            .column(2)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_eq!(&[1i64, 2, 3], ts.values().as_ref());
    }
}
