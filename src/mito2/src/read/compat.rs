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

//! Utilities to adapt readers with different schema.

use std::collections::HashMap;
use std::sync::Arc;

use api::v1::SemanticType;
use common_recordbatch::recordbatch::align_json_array;
use datatypes::arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, DictionaryArray, UInt32Array,
};
use datatypes::arrow::compute::{TakeOptions, take};
use datatypes::arrow::datatypes::{FieldRef, Schema, SchemaRef};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::data_type::ConcreteDataType;
use datatypes::prelude::DataType;
use datatypes::value::Value;
use datatypes::vectors::VectorRef;
use mito_codec::row_converter::{
    CompositeValues, PrimaryKeyCodec, SortField, build_primary_key_codec,
    build_primary_key_codec_with_fields,
};
use snafu::{OptionExt, ResultExt, ensure};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::{RegionMetadata, RegionMetadataRef};
use store_api::storage::ColumnId;

use crate::error::{
    CompatReaderSnafu, ComputeArrowSnafu, CreateDefaultSnafu, DecodeSnafu, EncodeSnafu,
    NewRecordBatchSnafu, RecordBatchSnafu, Result, UnsupportedOperationSnafu,
};
use crate::read::flat_projection::{FlatProjectionMapper, flat_projected_columns};
use crate::sst::parquet::flat_format::primary_key_column_index;
use crate::sst::parquet::format::{FormatProjection, INTERNAL_COLUMN_NUM, PrimaryKeyArray};
use crate::sst::{internal_fields, tag_maybe_to_dictionary_field};

/// Returns true if `left` and `right` have same columns and primary key encoding.
pub(crate) fn has_same_columns_and_pk_encoding(
    left: &RegionMetadata,
    right: &RegionMetadata,
) -> bool {
    if left.primary_key_encoding != right.primary_key_encoding {
        return false;
    }

    if left.column_metadatas.len() != right.column_metadatas.len() {
        return false;
    }

    for (left_col, right_col) in left.column_metadatas.iter().zip(&right.column_metadatas) {
        if left_col.column_id != right_col.column_id || !left_col.is_same_datatype(right_col) {
            return false;
        }
        debug_assert_eq!(
            left_col.column_schema.data_type,
            right_col.column_schema.data_type
        );
        debug_assert_eq!(left_col.semantic_type, right_col.semantic_type);
    }

    true
}

/// A helper struct to adapt schema of the batch to an expected schema.
pub(crate) struct FlatCompatBatch {
    /// Indices to convert actual fields to expect fields.
    index_or_defaults: Vec<IndexOrDefault>,
    /// Expected arrow schema.
    arrow_schema: SchemaRef,
    /// Primary key adapter.
    compat_pk: FlatCompatPrimaryKey,
}

impl FlatCompatBatch {
    /// Creates a [FlatCompatBatch].
    ///
    /// - `mapper` is built from the metadata users expect to see.
    /// - `actual` is the [RegionMetadata] of the input parquet.
    /// - `format_projection` is the projection of the read format for the input parquet.
    /// - `compaction` indicates whether the reader is for compaction.
    pub(crate) fn try_new(
        mapper: &FlatProjectionMapper,
        actual: &RegionMetadataRef,
        format_projection: &FormatProjection,
        compaction: bool,
    ) -> Result<Option<Self>> {
        let actual_schema = flat_projected_columns(actual, format_projection);
        let expect_schema = mapper.batch_schema();
        if expect_schema == actual_schema {
            // Although the SST has a different schema, but the schema after projection is the same
            // as expected schema.
            return Ok(None);
        }

        if actual.primary_key_encoding == PrimaryKeyEncoding::Sparse && compaction {
            // Special handling for sparse encoding in compaction.
            return FlatCompatBatch::try_new_compact_sparse(mapper, actual);
        }

        let (index_or_defaults, fields) =
            Self::compute_index_and_fields(&actual_schema, expect_schema, mapper.metadata())?;

        let compat_pk = FlatCompatPrimaryKey::new(mapper.metadata(), actual)?;

        Ok(Some(Self {
            index_or_defaults,
            arrow_schema: Arc::new(Schema::new(fields)),
            compat_pk,
        }))
    }

    fn compute_index_and_fields(
        actual_schema: &[(ColumnId, ConcreteDataType)],
        expect_schema: &[(ColumnId, ConcreteDataType)],
        expect_metadata: &RegionMetadata,
    ) -> Result<(Vec<IndexOrDefault>, Vec<FieldRef>)> {
        // Maps column id to the index and data type in the actual schema.
        let actual_schema_index: HashMap<_, _> = actual_schema
            .iter()
            .enumerate()
            .map(|(idx, (column_id, data_type))| (*column_id, (idx, data_type)))
            .collect();

        let mut index_or_defaults = Vec::with_capacity(expect_schema.len());
        let mut fields = Vec::with_capacity(expect_schema.len());
        for (column_id, expect_data_type) in expect_schema {
            // Safety: expect_schema comes from the same mapper.
            let column_index = expect_metadata.column_index_by_id(*column_id).unwrap();
            let expect_column = &expect_metadata.column_metadatas[column_index];
            let column_field = &expect_metadata.schema.arrow_schema().fields()[column_index];
            // For tag columns, we need to create a dictionary field.
            if expect_column.semantic_type == SemanticType::Tag {
                fields.push(tag_maybe_to_dictionary_field(
                    &expect_column.column_schema.data_type,
                    column_field,
                ));
            } else {
                fields.push(column_field.clone());
            };

            if let Some((index, actual_data_type)) = actual_schema_index.get(column_id) {
                let mut cast_type = None;

                // Same column different type.
                if expect_data_type != *actual_data_type {
                    cast_type = Some(expect_data_type.clone())
                }
                // Source has this column.
                index_or_defaults.push(IndexOrDefault::Index {
                    pos: *index,
                    cast_type,
                });
            } else {
                // Create a default vector with 1 element for that column.
                let default_vector = expect_column
                    .column_schema
                    .create_default_vector(1)
                    .context(CreateDefaultSnafu {
                        region_id: expect_metadata.region_id,
                        column: &expect_column.column_schema.name,
                    })?
                    .with_context(|| CompatReaderSnafu {
                        region_id: expect_metadata.region_id,
                        reason: format!(
                            "column {} does not have a default value to read",
                            expect_column.column_schema.name
                        ),
                    })?;
                index_or_defaults.push(IndexOrDefault::DefaultValue {
                    default_vector,
                    semantic_type: expect_column.semantic_type,
                });
            };
        }
        fields.extend_from_slice(&internal_fields());

        Ok((index_or_defaults, fields))
    }

    fn try_new_compact_sparse(
        mapper: &FlatProjectionMapper,
        actual: &RegionMetadataRef,
    ) -> Result<Option<Self>> {
        // Currently, we don't support converting sparse encoding back to dense encoding in
        // flat format.
        ensure!(
            mapper.metadata().primary_key_encoding == PrimaryKeyEncoding::Sparse,
            UnsupportedOperationSnafu {
                err_msg: "Flat format doesn't support converting sparse encoding back to dense encoding"
            }
        );

        // For sparse encoding, we don't need to check the primary keys.
        // Since this is for compaction, we always read all columns.
        let actual_schema: Vec<_> = actual
            .field_columns()
            .chain([actual.time_index_column()])
            .map(|col| (col.column_id, col.column_schema.data_type.clone()))
            .collect();
        let expect_schema: Vec<_> = mapper
            .metadata()
            .field_columns()
            .chain([mapper.metadata().time_index_column()])
            .map(|col| (col.column_id, col.column_schema.data_type.clone()))
            .collect();

        let (index_or_defaults, fields) =
            Self::compute_index_and_fields(&actual_schema, &expect_schema, mapper.metadata())?;

        let compat_pk = FlatCompatPrimaryKey::default();

        Ok(Some(Self {
            index_or_defaults,
            arrow_schema: Arc::new(Schema::new(fields)),
            compat_pk,
        }))
    }

    /// Make columns of the `batch` compatible.
    pub(crate) fn compat(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let len = batch.num_rows();
        let columns = self
            .index_or_defaults
            .iter()
            .map(|index_or_default| match index_or_default {
                IndexOrDefault::Index { pos, cast_type } => {
                    let old_column = batch.column(*pos);

                    if let Some(ty) = cast_type {
                        let casted = if let Some(json_type) = ty.as_json() {
                            align_json_array(old_column, &json_type.as_arrow_type())
                                .context(RecordBatchSnafu)?
                        } else {
                            datatypes::arrow::compute::cast(old_column, &ty.as_arrow_type())
                                .context(ComputeArrowSnafu)?
                        };
                        Ok(casted)
                    } else {
                        Ok(old_column.clone())
                    }
                }
                IndexOrDefault::DefaultValue {
                    default_vector,
                    semantic_type,
                } => repeat_vector(default_vector, len, *semantic_type == SemanticType::Tag),
            })
            .chain(
                // Adds internal columns.
                batch.columns()[batch.num_columns() - INTERNAL_COLUMN_NUM..]
                    .iter()
                    .map(|col| Ok(col.clone())),
            )
            .collect::<Result<Vec<_>>>()?;

        let compat_batch = RecordBatch::try_new(self.arrow_schema.clone(), columns)
            .context(NewRecordBatchSnafu)?;

        // Handles primary keys.
        self.compat_pk.compat(compat_batch)
    }
}

/// Repeats the vector value `to_len` times.
fn repeat_vector(vector: &VectorRef, to_len: usize, is_tag: bool) -> Result<ArrayRef> {
    assert_eq!(1, vector.len());
    let data_type = vector.data_type();
    if is_tag && data_type.is_string() {
        let values = vector.to_arrow_array();
        if values.is_null(0) {
            // Creates a dictionary array with `to_len` null keys.
            let keys = UInt32Array::new_null(to_len);
            Ok(Arc::new(DictionaryArray::new(keys, values.slice(0, 0))))
        } else {
            let keys = UInt32Array::from_value(0, to_len);
            Ok(Arc::new(DictionaryArray::new(keys, values)))
        }
    } else {
        let keys = UInt32Array::from_value(0, to_len);
        take(
            &vector.to_arrow_array(),
            &keys,
            Some(TakeOptions {
                check_bounds: false,
            }),
        )
        .context(ComputeArrowSnafu)
    }
}

/// Returns true if the actual primary keys is the same as expected.
fn is_primary_key_same(expect: &RegionMetadata, actual: &RegionMetadata) -> Result<bool> {
    ensure!(
        actual.primary_key.len() <= expect.primary_key.len(),
        CompatReaderSnafu {
            region_id: expect.region_id,
            reason: format!(
                "primary key has more columns {} than expect {}",
                actual.primary_key.len(),
                expect.primary_key.len()
            ),
        }
    );
    ensure!(
        actual.primary_key == expect.primary_key[..actual.primary_key.len()],
        CompatReaderSnafu {
            region_id: expect.region_id,
            reason: format!(
                "primary key has different prefix, expect: {:?}, actual: {:?}",
                expect.primary_key, actual.primary_key
            ),
        }
    );

    Ok(actual.primary_key.len() == expect.primary_key.len())
}

/// Index in source batch or a default value to fill a column.
#[derive(Debug)]
enum IndexOrDefault {
    /// Index of the column in source batch.
    Index {
        pos: usize,
        cast_type: Option<ConcreteDataType>,
    },
    /// Default value for the column.
    DefaultValue {
        /// Default value. The vector has only 1 element.
        default_vector: VectorRef,
        /// Semantic type of the column.
        semantic_type: SemanticType,
    },
}

/// Helper to rewrite primary key to another encoding for flat format.
struct FlatRewritePrimaryKey {
    /// New primary key encoder.
    codec: Arc<dyn PrimaryKeyCodec>,
    /// Metadata of the expected region.
    metadata: RegionMetadataRef,
    /// Original primary key codec.
    /// If we need to rewrite the primary key.
    old_codec: Arc<dyn PrimaryKeyCodec>,
}

impl FlatRewritePrimaryKey {
    fn new(
        expect: &RegionMetadataRef,
        actual: &RegionMetadataRef,
    ) -> Option<FlatRewritePrimaryKey> {
        if expect.primary_key_encoding == actual.primary_key_encoding {
            return None;
        }
        let codec = build_primary_key_codec(expect);
        let old_codec = build_primary_key_codec(actual);

        Some(FlatRewritePrimaryKey {
            codec,
            metadata: expect.clone(),
            old_codec,
        })
    }

    /// Rewrites the primary key of the `batch`.
    /// It also appends the values to the primary key.
    fn rewrite_key(
        &self,
        append_values: &[(ColumnId, Value)],
        batch: RecordBatch,
    ) -> Result<RecordBatch> {
        let old_pk_dict_array = batch
            .column(primary_key_column_index(batch.num_columns()))
            .as_any()
            .downcast_ref::<PrimaryKeyArray>()
            .unwrap();
        let old_pk_values_array = old_pk_dict_array
            .values()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let mut builder = BinaryBuilder::with_capacity(
            old_pk_values_array.len(),
            old_pk_values_array.value_data().len(),
        );

        // Binary buffer for the primary key.
        let mut buffer = Vec::with_capacity(
            old_pk_values_array.value_data().len() / old_pk_values_array.len().max(1),
        );
        let mut column_id_values = Vec::new();
        // Iterates the binary array and rewrites the primary key.
        for value in old_pk_values_array.iter() {
            let Some(old_pk) = value else {
                builder.append_null();
                continue;
            };
            // Decodes the old primary key.
            let mut pk_values = self.old_codec.decode(old_pk).context(DecodeSnafu)?;
            pk_values.extend(append_values);

            buffer.clear();
            column_id_values.clear();
            // Encodes the new primary key.
            match pk_values {
                CompositeValues::Dense(dense_values) => {
                    self.codec
                        .encode_values(dense_values.as_slice(), &mut buffer)
                        .context(EncodeSnafu)?;
                }
                CompositeValues::Sparse(sparse_values) => {
                    for id in &self.metadata.primary_key {
                        let value = sparse_values.get_or_null(*id);
                        column_id_values.push((*id, value.clone()));
                    }
                    self.codec
                        .encode_values(&column_id_values, &mut buffer)
                        .context(EncodeSnafu)?;
                }
            }
            builder.append_value(&buffer);
        }
        let new_pk_values_array = Arc::new(builder.finish());
        let new_pk_dict_array =
            PrimaryKeyArray::new(old_pk_dict_array.keys().clone(), new_pk_values_array);

        let mut columns = batch.columns().to_vec();
        columns[primary_key_column_index(batch.num_columns())] = Arc::new(new_pk_dict_array);

        RecordBatch::try_new(batch.schema(), columns).context(NewRecordBatchSnafu)
    }
}

/// Helper to make primary key compatible for flat format.
#[derive(Default)]
struct FlatCompatPrimaryKey {
    /// Primary key rewriter.
    rewriter: Option<FlatRewritePrimaryKey>,
    /// Converter to append values to primary keys.
    converter: Option<Arc<dyn PrimaryKeyCodec>>,
    /// Default values to append.
    values: Vec<(ColumnId, Value)>,
}

impl FlatCompatPrimaryKey {
    fn new(expect: &RegionMetadataRef, actual: &RegionMetadataRef) -> Result<Self> {
        let rewriter = FlatRewritePrimaryKey::new(expect, actual);

        if is_primary_key_same(expect, actual)? {
            return Ok(Self {
                rewriter,
                converter: None,
                values: Vec::new(),
            });
        }

        // We need to append default values to the primary key.
        let to_add = &expect.primary_key[actual.primary_key.len()..];
        let mut values = Vec::with_capacity(to_add.len());
        let mut fields = Vec::with_capacity(to_add.len());
        for column_id in to_add {
            // Safety: The id comes from expect region metadata.
            let column = expect.column_by_id(*column_id).unwrap();
            fields.push((
                *column_id,
                SortField::new(column.column_schema.data_type.clone()),
            ));
            let default_value = column
                .column_schema
                .create_default()
                .context(CreateDefaultSnafu {
                    region_id: expect.region_id,
                    column: &column.column_schema.name,
                })?
                .with_context(|| CompatReaderSnafu {
                    region_id: expect.region_id,
                    reason: format!(
                        "key column {} does not have a default value to read",
                        column.column_schema.name
                    ),
                })?;
            values.push((*column_id, default_value));
        }
        // is_primary_key_same() is false so we have different number of primary key columns.
        debug_assert!(!fields.is_empty());

        // Create converter to append values.
        let converter = Some(build_primary_key_codec_with_fields(
            expect.primary_key_encoding,
            fields.into_iter(),
        ));

        Ok(Self {
            rewriter,
            converter,
            values,
        })
    }

    /// Makes primary key of the `batch` compatible.
    ///
    /// Callers must ensure other columns except the `__primary_key` column is compatible.
    fn compat(&self, batch: RecordBatch) -> Result<RecordBatch> {
        if let Some(rewriter) = &self.rewriter {
            // If we have different encoding, rewrite the whole primary key.
            return rewriter.rewrite_key(&self.values, batch);
        }

        self.append_key(batch)
    }

    /// Appends values to the primary key of the `batch`.
    fn append_key(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let Some(converter) = &self.converter else {
            return Ok(batch);
        };

        let old_pk_dict_array = batch
            .column(primary_key_column_index(batch.num_columns()))
            .as_any()
            .downcast_ref::<PrimaryKeyArray>()
            .unwrap();
        let old_pk_values_array = old_pk_dict_array
            .values()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let mut builder = BinaryBuilder::with_capacity(
            old_pk_values_array.len(),
            old_pk_values_array.value_data().len()
                + converter.estimated_size().unwrap_or_default() * old_pk_values_array.len(),
        );

        // Binary buffer for the primary key.
        let mut buffer = Vec::with_capacity(
            old_pk_values_array.value_data().len() / old_pk_values_array.len().max(1)
                + converter.estimated_size().unwrap_or_default(),
        );

        // Iterates the binary array and appends values to the primary key.
        for value in old_pk_values_array.iter() {
            let Some(old_pk) = value else {
                builder.append_null();
                continue;
            };

            buffer.clear();
            buffer.extend_from_slice(old_pk);
            converter
                .encode_values(&self.values, &mut buffer)
                .context(EncodeSnafu)?;

            builder.append_value(&buffer);
        }

        let new_pk_values_array = Arc::new(builder.finish());
        let new_pk_dict_array =
            PrimaryKeyArray::new(old_pk_dict_array.keys().clone(), new_pk_values_array);

        // Overrides the primary key column.
        let mut columns = batch.columns().to_vec();
        columns[primary_key_column_index(batch.num_columns())] = Arc::new(new_pk_dict_array);

        RecordBatch::try_new(batch.schema(), columns).context(NewRecordBatchSnafu)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::{OpType, SemanticType};
    use datatypes::arrow::array::{
        ArrayRef, BinaryDictionaryBuilder, Int64Array, StringDictionaryBuilder,
        TimestampMillisecondArray, UInt8Array, UInt64Array,
    };
    use datatypes::arrow::datatypes::UInt32Type;
    use datatypes::arrow::record_batch::RecordBatch;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use datatypes::value::ValueRef;
    use mito_codec::row_converter::{
        DensePrimaryKeyCodec, PrimaryKeyCodecExt, SparsePrimaryKeyCodec,
    };
    use store_api::codec::PrimaryKeyEncoding;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::storage::RegionId;

    use super::*;
    use crate::read::flat_projection::FlatProjectionMapper;
    use crate::sst::parquet::flat_format::FlatReadFormat;
    use crate::sst::{FlatSchemaOptions, to_flat_sst_arrow_schema};

    /// Creates a new [RegionMetadata].
    fn new_metadata(
        semantic_types: &[(ColumnId, SemanticType, ConcreteDataType)],
        primary_key: &[ColumnId],
    ) -> RegionMetadata {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
        for (id, semantic_type, data_type) in semantic_types {
            let column_schema = match semantic_type {
                SemanticType::Tag => {
                    ColumnSchema::new(format!("tag_{id}"), data_type.clone(), true)
                }
                SemanticType::Field | SemanticType::FollowSchema => {
                    ColumnSchema::new(format!("field_{id}"), data_type.clone(), true)
                }
                SemanticType::Timestamp => ColumnSchema::new("ts", data_type.clone(), false),
            };

            builder.push_column_metadata(ColumnMetadata {
                column_schema,
                semantic_type: *semantic_type,
                column_id: *id,
            });
        }
        builder.primary_key(primary_key.to_vec());
        builder.build().unwrap()
    }

    /// Encode primary key.
    fn encode_key(keys: &[Option<&str>]) -> Vec<u8> {
        let fields = (0..keys.len())
            .map(|_| (0, SortField::new(ConcreteDataType::string_datatype())))
            .collect();
        let converter = DensePrimaryKeyCodec::with_fields(fields);
        let row = keys.iter().map(|str_opt| match str_opt {
            Some(v) => ValueRef::String(v),
            None => ValueRef::Null,
        });

        converter.encode(row).unwrap()
    }

    /// Encode sparse primary key.
    fn encode_sparse_key(keys: &[(ColumnId, Option<&str>)]) -> Vec<u8> {
        let fields = (0..keys.len())
            .map(|_| (1, SortField::new(ConcreteDataType::string_datatype())))
            .collect();
        let converter = SparsePrimaryKeyCodec::with_fields(fields);
        let row = keys
            .iter()
            .map(|(id, str_opt)| match str_opt {
                Some(v) => (*id, ValueRef::String(v)),
                None => (*id, ValueRef::Null),
            })
            .collect::<Vec<_>>();
        let mut buffer = vec![];
        converter.encode_value_refs(&row, &mut buffer).unwrap();
        buffer
    }

    /// Creates a primary key array for flat format testing.
    fn build_flat_test_pk_array(primary_keys: &[&[u8]]) -> ArrayRef {
        let mut builder = BinaryDictionaryBuilder::<UInt32Type>::new();
        for &pk in primary_keys {
            builder.append(pk).unwrap();
        }
        Arc::new(builder.finish())
    }

    #[test]
    fn test_flat_compat_batch_with_missing_columns() {
        let actual_metadata = Arc::new(new_metadata(
            &[
                (
                    0,
                    SemanticType::Timestamp,
                    ConcreteDataType::timestamp_millisecond_datatype(),
                ),
                (1, SemanticType::Tag, ConcreteDataType::string_datatype()),
                (2, SemanticType::Field, ConcreteDataType::int64_datatype()),
            ],
            &[1],
        ));

        let expected_metadata = Arc::new(new_metadata(
            &[
                (
                    0,
                    SemanticType::Timestamp,
                    ConcreteDataType::timestamp_millisecond_datatype(),
                ),
                (1, SemanticType::Tag, ConcreteDataType::string_datatype()),
                (2, SemanticType::Field, ConcreteDataType::int64_datatype()),
                // Adds a new field.
                (3, SemanticType::Field, ConcreteDataType::int64_datatype()),
            ],
            &[1],
        ));

        let mapper = FlatProjectionMapper::all(&expected_metadata).unwrap();
        let read_format = FlatReadFormat::new(
            actual_metadata.clone(),
            [0, 1, 2, 3].into_iter(),
            None,
            "test",
            false,
        )
        .unwrap();
        let format_projection = read_format.format_projection();

        let compat_batch =
            FlatCompatBatch::try_new(&mapper, &actual_metadata, format_projection, false)
                .unwrap()
                .unwrap();

        let mut tag_builder = StringDictionaryBuilder::<UInt32Type>::new();
        tag_builder.append_value("tag1");
        tag_builder.append_value("tag1");
        let tag_dict_array = Arc::new(tag_builder.finish());

        let k1 = encode_key(&[Some("tag1")]);
        let input_columns: Vec<ArrayRef> = vec![
            tag_dict_array.clone(),
            Arc::new(Int64Array::from(vec![100, 200])),
            Arc::new(TimestampMillisecondArray::from_iter_values([1000, 2000])),
            build_flat_test_pk_array(&[&k1, &k1]),
            Arc::new(UInt64Array::from_iter_values([1, 2])),
            Arc::new(UInt8Array::from_iter_values([
                OpType::Put as u8,
                OpType::Put as u8,
            ])),
        ];
        let input_schema =
            to_flat_sst_arrow_schema(&actual_metadata, &FlatSchemaOptions::default());
        let input_batch = RecordBatch::try_new(input_schema, input_columns).unwrap();

        let result = compat_batch.compat(input_batch).unwrap();

        let expected_schema =
            to_flat_sst_arrow_schema(&expected_metadata, &FlatSchemaOptions::default());

        let expected_columns: Vec<ArrayRef> = vec![
            tag_dict_array.clone(),
            Arc::new(Int64Array::from(vec![100, 200])),
            Arc::new(Int64Array::from(vec![None::<i64>, None::<i64>])),
            Arc::new(TimestampMillisecondArray::from_iter_values([1000, 2000])),
            build_flat_test_pk_array(&[&k1, &k1]),
            Arc::new(UInt64Array::from_iter_values([1, 2])),
            Arc::new(UInt8Array::from_iter_values([
                OpType::Put as u8,
                OpType::Put as u8,
            ])),
        ];
        let expected_batch = RecordBatch::try_new(expected_schema, expected_columns).unwrap();

        assert_eq!(expected_batch, result);
    }

    #[test]
    fn test_flat_compat_batch_with_read_projection_superset() {
        let actual_metadata = Arc::new(new_metadata(
            &[
                (
                    0,
                    SemanticType::Timestamp,
                    ConcreteDataType::timestamp_millisecond_datatype(),
                ),
                (1, SemanticType::Tag, ConcreteDataType::string_datatype()),
                (2, SemanticType::Field, ConcreteDataType::int64_datatype()),
            ],
            &[1],
        ));

        let expected_metadata = Arc::new(new_metadata(
            &[
                (
                    0,
                    SemanticType::Timestamp,
                    ConcreteDataType::timestamp_millisecond_datatype(),
                ),
                (1, SemanticType::Tag, ConcreteDataType::string_datatype()),
                (2, SemanticType::Field, ConcreteDataType::int64_datatype()),
                // Adds a new field.
                (3, SemanticType::Field, ConcreteDataType::int64_datatype()),
            ],
            &[1],
        ));

        // Output projection: tag_1, field_2. Read also includes field_3.
        let mapper = FlatProjectionMapper::new_with_read_columns(
            &expected_metadata,
            vec![1, 2],
            vec![1, 2, 3],
        )
        .unwrap();
        let read_format = FlatReadFormat::new(
            actual_metadata.clone(),
            [1, 2, 3].into_iter(),
            None,
            "test",
            false,
        )
        .unwrap();
        let format_projection = read_format.format_projection();

        let compat_batch =
            FlatCompatBatch::try_new(&mapper, &actual_metadata, format_projection, false)
                .unwrap()
                .unwrap();

        let mut tag_builder = StringDictionaryBuilder::<UInt32Type>::new();
        tag_builder.append_value("tag1");
        tag_builder.append_value("tag1");
        let tag_dict_array = Arc::new(tag_builder.finish());

        let k1 = encode_key(&[Some("tag1")]);
        let input_columns: Vec<ArrayRef> = vec![
            tag_dict_array.clone(),
            Arc::new(Int64Array::from(vec![100, 200])),
            Arc::new(TimestampMillisecondArray::from_iter_values([1000, 2000])),
            build_flat_test_pk_array(&[&k1, &k1]),
            Arc::new(UInt64Array::from_iter_values([1, 2])),
            Arc::new(UInt8Array::from_iter_values([
                OpType::Put as u8,
                OpType::Put as u8,
            ])),
        ];
        let input_schema =
            to_flat_sst_arrow_schema(&actual_metadata, &FlatSchemaOptions::default());
        let input_batch = RecordBatch::try_new(input_schema, input_columns).unwrap();

        let result = compat_batch.compat(input_batch).unwrap();

        let expected_schema =
            to_flat_sst_arrow_schema(&expected_metadata, &FlatSchemaOptions::default());
        let expected_columns: Vec<ArrayRef> = vec![
            tag_dict_array.clone(),
            Arc::new(Int64Array::from(vec![100, 200])),
            Arc::new(Int64Array::from(vec![None::<i64>, None::<i64>])),
            Arc::new(TimestampMillisecondArray::from_iter_values([1000, 2000])),
            build_flat_test_pk_array(&[&k1, &k1]),
            Arc::new(UInt64Array::from_iter_values([1, 2])),
            Arc::new(UInt8Array::from_iter_values([
                OpType::Put as u8,
                OpType::Put as u8,
            ])),
        ];
        let expected_batch = RecordBatch::try_new(expected_schema, expected_columns).unwrap();

        assert_eq!(expected_batch, result);
    }

    #[test]
    fn test_flat_compat_batch_with_different_pk_encoding() {
        let mut actual_metadata = new_metadata(
            &[
                (
                    0,
                    SemanticType::Timestamp,
                    ConcreteDataType::timestamp_millisecond_datatype(),
                ),
                (1, SemanticType::Tag, ConcreteDataType::string_datatype()),
                (2, SemanticType::Field, ConcreteDataType::int64_datatype()),
            ],
            &[1],
        );
        actual_metadata.primary_key_encoding = PrimaryKeyEncoding::Dense;
        let actual_metadata = Arc::new(actual_metadata);

        let mut expected_metadata = new_metadata(
            &[
                (
                    0,
                    SemanticType::Timestamp,
                    ConcreteDataType::timestamp_millisecond_datatype(),
                ),
                (1, SemanticType::Tag, ConcreteDataType::string_datatype()),
                (2, SemanticType::Field, ConcreteDataType::int64_datatype()),
                (3, SemanticType::Tag, ConcreteDataType::string_datatype()),
            ],
            &[1, 3],
        );
        expected_metadata.primary_key_encoding = PrimaryKeyEncoding::Sparse;
        let expected_metadata = Arc::new(expected_metadata);

        let mapper = FlatProjectionMapper::all(&expected_metadata).unwrap();
        let read_format = FlatReadFormat::new(
            actual_metadata.clone(),
            [0, 1, 2, 3].into_iter(),
            None,
            "test",
            false,
        )
        .unwrap();
        let format_projection = read_format.format_projection();

        let compat_batch =
            FlatCompatBatch::try_new(&mapper, &actual_metadata, format_projection, false)
                .unwrap()
                .unwrap();

        // Tag array.
        let mut tag1_builder = StringDictionaryBuilder::<UInt32Type>::new();
        tag1_builder.append_value("tag1");
        tag1_builder.append_value("tag1");
        let tag1_dict_array = Arc::new(tag1_builder.finish());

        let k1 = encode_key(&[Some("tag1")]);
        let input_columns: Vec<ArrayRef> = vec![
            tag1_dict_array.clone(),
            Arc::new(Int64Array::from(vec![100, 200])),
            Arc::new(TimestampMillisecondArray::from_iter_values([1000, 2000])),
            build_flat_test_pk_array(&[&k1, &k1]),
            Arc::new(UInt64Array::from_iter_values([1, 2])),
            Arc::new(UInt8Array::from_iter_values([
                OpType::Put as u8,
                OpType::Put as u8,
            ])),
        ];
        let input_schema =
            to_flat_sst_arrow_schema(&actual_metadata, &FlatSchemaOptions::default());
        let input_batch = RecordBatch::try_new(input_schema, input_columns).unwrap();

        let result = compat_batch.compat(input_batch).unwrap();

        let sparse_k1 = encode_sparse_key(&[(1, Some("tag1")), (3, None)]);
        let mut null_tag_builder = StringDictionaryBuilder::<UInt32Type>::new();
        null_tag_builder.append_nulls(2);
        let null_tag_dict_array = Arc::new(null_tag_builder.finish());
        let expected_columns: Vec<ArrayRef> = vec![
            tag1_dict_array.clone(),
            null_tag_dict_array,
            Arc::new(Int64Array::from(vec![100, 200])),
            Arc::new(TimestampMillisecondArray::from_iter_values([1000, 2000])),
            build_flat_test_pk_array(&[&sparse_k1, &sparse_k1]),
            Arc::new(UInt64Array::from_iter_values([1, 2])),
            Arc::new(UInt8Array::from_iter_values([
                OpType::Put as u8,
                OpType::Put as u8,
            ])),
        ];
        let output_schema =
            to_flat_sst_arrow_schema(&expected_metadata, &FlatSchemaOptions::default());
        let expected_batch = RecordBatch::try_new(output_schema, expected_columns).unwrap();

        assert_eq!(expected_batch, result);
    }

    #[test]
    fn test_flat_compat_batch_compact_sparse() {
        let mut actual_metadata = new_metadata(
            &[
                (
                    0,
                    SemanticType::Timestamp,
                    ConcreteDataType::timestamp_millisecond_datatype(),
                ),
                (2, SemanticType::Field, ConcreteDataType::int64_datatype()),
            ],
            &[],
        );
        actual_metadata.primary_key_encoding = PrimaryKeyEncoding::Sparse;
        let actual_metadata = Arc::new(actual_metadata);

        let mut expected_metadata = new_metadata(
            &[
                (
                    0,
                    SemanticType::Timestamp,
                    ConcreteDataType::timestamp_millisecond_datatype(),
                ),
                (2, SemanticType::Field, ConcreteDataType::int64_datatype()),
                (3, SemanticType::Field, ConcreteDataType::int64_datatype()),
            ],
            &[],
        );
        expected_metadata.primary_key_encoding = PrimaryKeyEncoding::Sparse;
        let expected_metadata = Arc::new(expected_metadata);

        let mapper = FlatProjectionMapper::all(&expected_metadata).unwrap();
        let read_format = FlatReadFormat::new(
            actual_metadata.clone(),
            [0, 2, 3].into_iter(),
            None,
            "test",
            true,
        )
        .unwrap();
        let format_projection = read_format.format_projection();

        let compat_batch =
            FlatCompatBatch::try_new(&mapper, &actual_metadata, format_projection, true)
                .unwrap()
                .unwrap();

        let sparse_k1 = encode_sparse_key(&[]);
        let input_columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![100, 200])),
            Arc::new(TimestampMillisecondArray::from_iter_values([1000, 2000])),
            build_flat_test_pk_array(&[&sparse_k1, &sparse_k1]),
            Arc::new(UInt64Array::from_iter_values([1, 2])),
            Arc::new(UInt8Array::from_iter_values([
                OpType::Put as u8,
                OpType::Put as u8,
            ])),
        ];
        let input_schema =
            to_flat_sst_arrow_schema(&actual_metadata, &FlatSchemaOptions::default());
        let input_batch = RecordBatch::try_new(input_schema, input_columns).unwrap();

        let result = compat_batch.compat(input_batch).unwrap();

        let expected_columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![100, 200])),
            Arc::new(Int64Array::from(vec![None::<i64>, None::<i64>])),
            Arc::new(TimestampMillisecondArray::from_iter_values([1000, 2000])),
            build_flat_test_pk_array(&[&sparse_k1, &sparse_k1]),
            Arc::new(UInt64Array::from_iter_values([1, 2])),
            Arc::new(UInt8Array::from_iter_values([
                OpType::Put as u8,
                OpType::Put as u8,
            ])),
        ];
        let output_schema =
            to_flat_sst_arrow_schema(&expected_metadata, &FlatSchemaOptions::default());
        let expected_batch = RecordBatch::try_new(output_schema, expected_columns).unwrap();

        assert_eq!(expected_batch, result);
    }
}
