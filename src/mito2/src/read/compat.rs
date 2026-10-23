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
use datatypes::arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, DictionaryArray, UInt32Array, new_null_array,
};
use datatypes::arrow::compute::{TakeOptions, take};
use datatypes::arrow::datatypes::{FieldRef, Schema, SchemaRef};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::data_type::ConcreteDataType;
use datatypes::prelude::DataType;
use datatypes::value::Value;
use datatypes::vectors::{Helper, VectorRef};
use mito_codec::row_converter::{
    CompositeValues, PrimaryKeyCodec, SortField, SparsePrimaryKeyCodec, build_primary_key_codec,
    build_primary_key_codec_with_fields,
};
use snafu::{OptionExt, ResultExt, ensure};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::{RegionMetadata, RegionMetadataRef, validate_sparse_tag_default};
use store_api::storage::ColumnId;

use crate::error::{
    CompatReaderSnafu, ComputeArrowSnafu, CreateDefaultSnafu, DecodeSnafu, EncodeSnafu,
    InvalidMetadataSnafu, NewRecordBatchSnafu, Result, UnexpectedSnafu, UnsupportedOperationSnafu,
};
use crate::read::flat_projection::{FlatProjectionMapper, flat_projected_columns};
use crate::sst::internal_fields;
use crate::sst::parquet::flat_format::{FlatReadFormat, primary_key_column_index};
use crate::sst::parquet::format::{INTERNAL_COLUMN_NUM, PrimaryKeyArray};

/// Returns true if the columns in the `projection_mapper` and `read_format` have same data types
/// and primary key encodings.
pub(crate) fn has_same_columns_and_pk_encoding(
    projection_mapper: &FlatProjectionMapper,
    read_format: &FlatReadFormat,
    compaction: bool,
) -> bool {
    let left = projection_mapper.metadata();
    let right = read_format.metadata();
    if left.primary_key_encoding != right.primary_key_encoding {
        return false;
    }

    if left.column_metadatas.len() != right.column_metadatas.len() {
        return false;
    }

    for (left_col, right_col) in left.column_metadatas.iter().zip(&right.column_metadatas) {
        if left_col.column_id != right_col.column_id {
            return false;
        }
        debug_assert_eq!(left_col.semantic_type, right_col.semantic_type);
    }

    &projection_mapper.input_arrow_schema(compaction) == read_format.arrow_schema()
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
    /// - `read_format` is the [FlatReadFormat] of the input parquet.
    /// - `compaction` indicates whether the reader is for compaction.
    pub(crate) fn try_new(
        mapper: &FlatProjectionMapper,
        read_format: &FlatReadFormat,
        compaction: bool,
    ) -> Result<Option<Self>> {
        let actual = read_format.metadata();
        if actual.is_ordinary_sparse() {
            for column in mapper.metadata().primary_key_columns() {
                if actual.column_by_id(column.column_id).is_none() {
                    validate_sparse_tag_default(
                        &column.column_schema.name,
                        column.column_schema.default_constraint(),
                    )
                    .context(InvalidMetadataSnafu)?;
                }
            }
        }
        let actual_schema = flat_projected_columns(
            actual,
            read_format.format_projection(),
            read_format.json_target_types(),
        );

        let expect_schema = mapper.batch_schema();
        if expect_schema == actual_schema
            && actual.primary_key == mapper.metadata().primary_key
            && actual.primary_key_encoding == mapper.metadata().primary_key_encoding
        {
            // Although the SST has a different schema, but the schema after projection is the same
            // as expected schema.
            return Ok(None);
        }

        if actual.primary_key_encoding == PrimaryKeyEncoding::Sparse && compaction {
            // Special handling for sparse encoding in compaction.
            return FlatCompatBatch::try_new_compact_sparse(mapper, actual, &actual_schema);
        }

        let (index_or_defaults, fields) = Self::compute_index_and_fields(
            &actual_schema,
            expect_schema,
            mapper.metadata(),
            mapper.input_arrow_schema(compaction).fields(),
        )?;

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
        target_fields: &[FieldRef],
    ) -> Result<(Vec<IndexOrDefault>, Vec<FieldRef>)> {
        // Maps column id to the index and data type in the actual schema.
        let actual_schema_index: HashMap<_, _> = actual_schema
            .iter()
            .enumerate()
            .map(|(idx, (column_id, data_type))| (*column_id, (idx, data_type)))
            .collect();

        let mut index_or_defaults = Vec::with_capacity(expect_schema.len());
        let mut fields = Vec::with_capacity(expect_schema.len());
        for (field_index, (column_id, expect_data_type)) in expect_schema.iter().enumerate() {
            // Safety: expect_schema comes from the same mapper.
            let column_index = expect_metadata.column_index_by_id(*column_id).unwrap();
            let expect_column = &expect_metadata.column_metadatas[column_index];
            // Query target fields follow mapper.batch_schema(); compaction reads all columns in
            // PK -> fields -> timestamp order (fields -> timestamp for sparse encoding), before
            // trailing internal columns. Keep this order consistent with expect_schema.
            // The mapper already applies string-tag dictionary types and JSON2 compaction layouts,
            // which can differ from the logical types in expect_schema.
            let column_field = &target_fields[field_index];
            debug_assert_eq!(column_field.name(), &expect_column.column_schema.name);
            fields.push(column_field.clone());

            if let Some((index, actual_data_type)) = actual_schema_index.get(column_id) {
                let mut cast_type = None;

                // Same column different type.
                if expect_data_type != *actual_data_type {
                    ensure!(
                        !expect_data_type.is_json2() && !actual_data_type.is_json2(),
                        CompatReaderSnafu {
                            region_id: expect_metadata.region_id,
                            reason: format!(
                                "JSON2 column '{}' must be aligned before FlatCompatBatch, actual: {}, expected: {}",
                                expect_column.column_schema.name,
                                actual_data_type,
                                expect_data_type,
                            ),
                        }
                    );
                    cast_type = Some(expect_data_type.clone());
                }
                // Source has this column.
                index_or_defaults.push(IndexOrDefault::Index {
                    pos: *index,
                    cast_type,
                });
            } else {
                // Create a default vector with 1 element for that column.
                let default_vector = if expect_data_type.is_json2() {
                    // JSON2 columns do not support defaults, so missing columns use NULL.
                    // A logical JSON2 placeholder cannot describe a compaction layout.
                    Helper::try_into_vector(new_null_array(column_field.data_type(), 1)).context(
                        CreateDefaultSnafu {
                            region_id: expect_metadata.region_id,
                            column: &expect_column.column_schema.name,
                        },
                    )?
                } else {
                    expect_column
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
                        })?
                };
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
        actual_schema: &[(ColumnId, ConcreteDataType)],
    ) -> Result<Option<Self>> {
        // Currently, we don't support converting sparse encoding back to dense encoding in
        // flat format.
        ensure!(
            mapper.metadata().primary_key_encoding == actual.primary_key_encoding,
            UnsupportedOperationSnafu {
                err_msg: "Flat format doesn't support converting sparse encoding back to dense encoding"
            }
        );

        // Encoded sparse tags are not materialized during compaction. Preserve
        // the projected JSON2 types: metadata only describes their logical shape.
        let actual_schema: Vec<_> = actual_schema
            .iter()
            .filter(|(id, _)| actual.column_by_id(*id).unwrap().semantic_type != SemanticType::Tag)
            .cloned()
            .collect();
        let expect_schema: Vec<_> = mapper
            .batch_schema()
            .iter()
            .filter(|(id, _)| {
                mapper.metadata().column_by_id(*id).unwrap().semantic_type != SemanticType::Tag
            })
            .cloned()
            .collect();

        let (index_or_defaults, fields) = Self::compute_index_and_fields(
            &actual_schema,
            &expect_schema,
            mapper.metadata(),
            mapper.input_arrow_schema(true).fields(),
        )?;

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
                        let casted =
                            datatypes::arrow::compute::cast(old_column, &ty.as_arrow_type())
                                .context(ComputeArrowSnafu)?;
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

        let mut columns = columns;
        let primary_key_index = primary_key_column_index(columns.len());
        columns[primary_key_index] = self.compat_primary_key(&columns[primary_key_index])?;

        RecordBatch::try_new(self.arrow_schema.clone(), columns).context(NewRecordBatchSnafu)
    }

    /// Makes an encoded primary-key array compatible with the expected metadata.
    pub(crate) fn compat_primary_key(&self, primary_key: &ArrayRef) -> Result<ArrayRef> {
        self.compat_pk.compat(primary_key)
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
        primary_key: &ArrayRef,
    ) -> Result<ArrayRef> {
        if let Some(old_pk_dict_array) = primary_key.as_any().downcast_ref::<PrimaryKeyArray>() {
            let old_pk_values_array = old_pk_dict_array
                .values()
                .as_any()
                .downcast_ref::<BinaryArray>()
                .context(UnexpectedSnafu {
                    reason: "Primary-key dictionary values are not binary",
                })?;
            let new_pk_values_array =
                Arc::new(self.rewrite_values(append_values, old_pk_values_array)?);
            return Ok(Arc::new(PrimaryKeyArray::new(
                old_pk_dict_array.keys().clone(),
                new_pk_values_array,
            )));
        }

        let old_pk_values_array =
            primary_key
                .as_any()
                .downcast_ref::<BinaryArray>()
                .context(UnexpectedSnafu {
                    reason: format!(
                        "Primary-key column is neither binary nor dictionary, got {:?}",
                        primary_key.data_type()
                    ),
                })?;
        Ok(Arc::new(
            self.rewrite_values(append_values, old_pk_values_array)?,
        ))
    }

    fn rewrite_values(
        &self,
        append_values: &[(ColumnId, Value)],
        old_pk_values_array: &BinaryArray,
    ) -> Result<BinaryArray> {
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
        Ok(builder.finish())
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
        let converter: Arc<dyn PrimaryKeyCodec> = match expect.primary_key_encoding {
            PrimaryKeyEncoding::Dense => {
                build_primary_key_codec_with_fields(expect.primary_key_encoding, fields.into_iter())
            }
            PrimaryKeyEncoding::Sparse => {
                // The existing key already contains the reserved prefix.
                Arc::new(SparsePrimaryKeyCodec::from_columns(to_add.iter().copied()))
            }
        };

        Ok(Self {
            rewriter,
            converter: Some(converter),
            values,
        })
    }

    /// Makes an encoded primary-key array compatible.
    fn compat(&self, primary_key: &ArrayRef) -> Result<ArrayRef> {
        if let Some(rewriter) = &self.rewriter {
            // If we have different encoding, rewrite the whole primary key.
            return rewriter.rewrite_key(&self.values, primary_key);
        }

        self.append_key(primary_key)
    }

    /// Appends values to the primary key array.
    fn append_key(&self, primary_key: &ArrayRef) -> Result<ArrayRef> {
        let Some(converter) = &self.converter else {
            return Ok(primary_key.clone());
        };

        if let Some(old_pk_dict_array) = primary_key.as_any().downcast_ref::<PrimaryKeyArray>() {
            let old_pk_values_array = old_pk_dict_array
                .values()
                .as_any()
                .downcast_ref::<BinaryArray>()
                .context(UnexpectedSnafu {
                    reason: "Primary-key dictionary values are not binary",
                })?;
            let new_pk_values_array =
                Arc::new(self.append_values(old_pk_values_array, converter.as_ref())?);
            return Ok(Arc::new(PrimaryKeyArray::new(
                old_pk_dict_array.keys().clone(),
                new_pk_values_array,
            )));
        }

        let old_pk_values_array =
            primary_key
                .as_any()
                .downcast_ref::<BinaryArray>()
                .context(UnexpectedSnafu {
                    reason: format!(
                        "Primary-key column is neither binary nor dictionary, got {:?}",
                        primary_key.data_type()
                    ),
                })?;
        Ok(Arc::new(
            self.append_values(old_pk_values_array, converter.as_ref())?,
        ))
    }

    fn append_values(
        &self,
        old_pk_values_array: &BinaryArray,
        converter: &dyn PrimaryKeyCodec,
    ) -> Result<BinaryArray> {
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

        Ok(builder.finish())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use api::v1::{OpType, SemanticType};
    use datatypes::arrow::array::{
        ArrayRef, BinaryArray, BinaryDictionaryBuilder, Int64Array, StringDictionaryBuilder,
        TimestampMicrosecondArray, TimestampMillisecondArray, UInt8Array, UInt64Array,
    };
    use datatypes::arrow::datatypes::UInt32Type;
    use datatypes::arrow::record_batch::RecordBatch;
    use datatypes::extension::json::JsonMetadata;
    use datatypes::json::JsonSettings;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnDefaultConstraint, ColumnSchema};
    use datatypes::types::json_type::JsonNativeType;
    use datatypes::types::parse_string_to_jsonb;
    use datatypes::value::ValueRef;
    use datatypes::vectors::json::array::JsonArray;
    use datatypes::vectors::json::json2_physical_data_type;
    use mito_codec::row_converter::{
        DensePrimaryKeyCodec, PrimaryKeyCodecExt, SparsePrimaryKeyCodec,
    };
    use store_api::codec::PrimaryKeyEncoding;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::storage::RegionId;
    use store_api::storage::consts::ReservedColumnId;

    use super::*;
    use crate::read::flat_projection::FlatProjectionMapper;
    use crate::read::read_columns::ReadColumns;
    use crate::sst::parquet::flat_format::FlatReadFormat;
    use crate::sst::parquet::{Json2RewriteTargets, Json2TargetLayout};
    use crate::sst::{FlatSchemaOptions, to_flat_sst_arrow_schema};
    use crate::test_util;
    use crate::test_util::sst_util::sst_region_metadata_with_encoding;

    /// Creates a new [RegionMetadata].
    fn new_metadata(
        semantic_types: &[(ColumnId, SemanticType, ConcreteDataType)],
        primary_key: &[ColumnId],
    ) -> RegionMetadata {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
        for (id, semantic_type, data_type) in semantic_types {
            let mut column_schema = match semantic_type {
                SemanticType::Tag => {
                    ColumnSchema::new(format!("tag_{id}"), data_type.clone(), true)
                }
                SemanticType::Field => {
                    ColumnSchema::new(format!("field_{id}"), data_type.clone(), true)
                }
                SemanticType::Timestamp => ColumnSchema::new("ts", data_type.clone(), false),
            };
            if data_type.is_json2() {
                column_schema.with_extension_type(
                    &datatypes::extension::json::Json2ExtensionType::default(),
                );
            }

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
            ReadColumns::new([0, 1, 2, 3]),
            None,
            "test",
            false,
        )
        .unwrap();

        let compat_batch = FlatCompatBatch::try_new(&mapper, &read_format, false)
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

    #[rstest::rstest]
    #[case(PrimaryKeyEncoding::Dense, false)]
    #[case(PrimaryKeyEncoding::Sparse, true)]
    fn test_flat_compat_batch_uses_projected_json2_type(
        #[case] encoding: PrimaryKeyEncoding,
        #[case] compaction: bool,
    ) -> Result<()> {
        let json2 = ConcreteDataType::json2(JsonNativeType::object());
        let mut actual_metadata = Arc::new(new_metadata(
            &[
                (
                    0,
                    SemanticType::Timestamp,
                    ConcreteDataType::timestamp_millisecond_datatype(),
                ),
                (1, SemanticType::Field, json2.clone()),
            ],
            &[],
        ));
        let mut expected_metadata = Arc::new(new_metadata(
            &[
                (
                    0,
                    SemanticType::Timestamp,
                    ConcreteDataType::timestamp_millisecond_datatype(),
                ),
                (1, SemanticType::Field, json2),
                (2, SemanticType::Field, ConcreteDataType::int64_datatype()),
            ],
            &[],
        ));
        Arc::make_mut(&mut actual_metadata).primary_key_encoding = encoding;
        Arc::make_mut(&mut expected_metadata).primary_key_encoding = encoding;
        let read_columns = ReadColumns::new([0, 1, 2])
            .with_json_target_types(BTreeMap::from([(1, JsonNativeType::Variant)]));
        let mapper = FlatProjectionMapper::new_with_read_columns(
            &expected_metadata,
            vec![0, 1, 2],
            read_columns.clone(),
        )?;
        let read_format = FlatReadFormat::new(actual_metadata, read_columns, None, "test", false)?;

        let compat = FlatCompatBatch::try_new(&mapper, &read_format, compaction)?.unwrap();
        let json_index = mapper
            .batch_schema()
            .iter()
            .position(|(id, _)| *id == 1)
            .unwrap();
        assert!(matches!(
            &compat.index_or_defaults[json_index],
            IndexOrDefault::Index {
                cast_type: None,
                ..
            }
        ));
        assert_eq!(
            compat.arrow_schema.field(json_index).data_type(),
            &datatypes::arrow::datatypes::DataType::Binary
        );
        Ok(())
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

        let mapper = FlatProjectionMapper::new_with_read_columns(
            &expected_metadata,
            vec![1, 2],
            ReadColumns::new([1, 2, 3]),
        )
        .unwrap();
        let read_format = FlatReadFormat::new(
            actual_metadata.clone(),
            ReadColumns::new([1, 2, 3]),
            None,
            "test",
            false,
        )
        .unwrap();

        let compat_batch = FlatCompatBatch::try_new(&mapper, &read_format, false)
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
            ReadColumns::new([0, 1, 2, 3]),
            None,
            "test",
            false,
        )
        .unwrap();

        let compat_batch = FlatCompatBatch::try_new(&mapper, &read_format, false)
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

    #[rstest::rstest]
    #[case::ordinary(false)]
    #[case::metric(true)]
    fn test_compat_sparse_primary_key_with_new_nullable_tag(#[case] metric: bool) {
        let mut actual = sst_region_metadata_with_encoding(if metric {
            PrimaryKeyEncoding::Sparse
        } else {
            PrimaryKeyEncoding::Dense
        });
        actual.primary_key_encoding = PrimaryKeyEncoding::Sparse;
        let actual = Arc::new(actual);
        let mut builder = RegionMetadataBuilder::from_existing((*actual).clone());
        let mut primary_key = actual.primary_key.clone();
        primary_key.push(4);
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "new_tag",
                    ConcreteDataType::string_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Tag,
                column_id: 4,
            })
            .primary_key(primary_key);
        let expected = Arc::new(builder.build().unwrap());
        let compat = FlatCompatPrimaryKey::new(&expected, &actual).unwrap();

        let mut values = Vec::new();
        if metric {
            values.extend([
                (ReservedColumnId::table_id(), Value::UInt32(23)),
                (ReservedColumnId::tsid(), Value::UInt64(42)),
            ]);
        }
        values.extend([(0, Value::from("api")), (1, Value::Null)]);
        let mut key = Vec::new();
        build_primary_key_codec(&actual)
            .encode_values(&values, &mut key)
            .unwrap();

        // Adding a NULL sparse tag must preserve the existing key, including metric IDs.
        let dictionary_key = build_flat_test_pk_array(&[&key, &key]);
        let binary_key: ArrayRef = Arc::new(BinaryArray::from(vec![
            Some(key.as_slice()),
            None,
            Some(key.as_slice()),
        ]));
        for input in [dictionary_key, binary_key] {
            let result = compat.compat(&input).unwrap();
            assert_eq!(result.as_ref(), input.as_ref());
        }
    }

    #[test]
    fn test_sparse_tag_default_backfill_rejected() {
        use datatypes::schema::ColumnDefaultConstraint;

        let mut actual = sst_region_metadata_with_encoding(PrimaryKeyEncoding::Dense);
        actual.primary_key_encoding = PrimaryKeyEncoding::Sparse;
        let actual = Arc::new(actual);
        for default in [
            None,
            Some(ColumnDefaultConstraint::null_value()),
            Some(ColumnDefaultConstraint::Value(Value::from("east"))),
        ] {
            let mut builder = RegionMetadataBuilder::from_existing((*actual).clone());
            let mut primary_key = actual.primary_key.clone();
            primary_key.push(4);
            builder
                .push_column_metadata(ColumnMetadata {
                    column_schema: ColumnSchema::new(
                        "new_tag",
                        ConcreteDataType::string_datatype(),
                        true,
                    )
                    .with_default_constraint(default.clone())
                    .unwrap(),
                    semantic_type: SemanticType::Tag,
                    column_id: 4,
                })
                .primary_key(primary_key);
            let expected = Arc::new(builder.build().unwrap());
            let non_null = matches!(
                default,
                Some(ColumnDefaultConstraint::Value(Value::String(_)))
            );
            for compaction in [false, true] {
                // Also reject when the query does not project the added tag.
                for projection in [vec![0, 1, 2, 3, 4], vec![2]] {
                    let mapper = FlatProjectionMapper::new(&expected, projection).unwrap();
                    let read_format = FlatReadFormat::new(
                        actual.clone(),
                        ReadColumns::new([0, 1, 2, 3]),
                        None,
                        "test",
                        compaction,
                    )
                    .unwrap();
                    let result = FlatCompatBatch::try_new(&mapper, &read_format, compaction);
                    if non_null {
                        let error = result.err().unwrap();
                        assert!(format!("{error:?}").contains("non-NULL defaults"));
                    } else {
                        assert!(result.is_ok());
                    }
                }

                // An SST already containing the tag needs no backfill, even with a non-NULL default.
                let mapper = FlatProjectionMapper::all(&expected).unwrap();
                let read_format = FlatReadFormat::new(
                    expected.clone(),
                    ReadColumns::new([0, 1, 2, 3, 4]),
                    None,
                    "test",
                    compaction,
                )
                .unwrap();
                assert!(FlatCompatBatch::try_new(&mapper, &read_format, compaction).is_ok());
            }
        }
    }

    #[test]
    fn test_compat_primary_key_with_different_encoding_only() {
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

        let mut expected_metadata = (*actual_metadata).clone();
        expected_metadata.primary_key_encoding = PrimaryKeyEncoding::Sparse;
        let expected_metadata = Arc::new(expected_metadata);

        let mapper = FlatProjectionMapper::all(&expected_metadata).unwrap();
        let read_format = FlatReadFormat::new(
            actual_metadata,
            ReadColumns::new([0, 1, 2]),
            None,
            "test",
            false,
        )
        .unwrap();
        let compat = FlatCompatBatch::try_new(&mapper, &read_format, false)
            .unwrap()
            .unwrap();

        let dense_key = encode_key(&[Some("tag1")]);
        let sparse_key = encode_sparse_key(&[(1, Some("tag1"))]);

        let dictionary_key = build_flat_test_pk_array(&[&dense_key, &dense_key]);
        let result = compat.compat_primary_key(&dictionary_key).unwrap();
        let result = result.as_any().downcast_ref::<PrimaryKeyArray>().unwrap();
        let values = result
            .values()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(values.value(result.keys().value(0) as usize), sparse_key);
        assert_eq!(values.value(result.keys().value(1) as usize), sparse_key);

        let binary_key: ArrayRef = Arc::new(BinaryArray::from(vec![
            Some(dense_key.as_slice()),
            Some(dense_key.as_slice()),
        ]));
        let result = compat.compat_primary_key(&binary_key).unwrap();
        let result = result.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(result.value(0), sparse_key);
        assert_eq!(result.value(1), sparse_key);
    }

    /// A widened time index unit must be cast on compat (values rescaled to
    /// the expected unit), and identical units must skip compat entirely.
    #[test]
    fn test_flat_compat_batch_with_time_index_unit_change() {
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

        let mut expected_metadata = (*actual_metadata).clone();
        for column in expected_metadata.column_metadatas.iter_mut() {
            if column.semantic_type == SemanticType::Timestamp {
                column.column_schema.data_type = ConcreteDataType::timestamp_microsecond_datatype();
            }
        }
        // Keep the schema consistent with the column metadata.
        let expected_metadata = Arc::new(
            RegionMetadataBuilder::from_existing(expected_metadata)
                .build()
                .unwrap(),
        );

        // Same units: nothing to compat.
        let same_unit_metadata = Arc::new((*actual_metadata).clone());
        let mapper = FlatProjectionMapper::all(&same_unit_metadata).unwrap();
        let read_format = FlatReadFormat::new(
            actual_metadata.clone(),
            ReadColumns::new([0, 1, 2]),
            None,
            "test",
            false,
        )
        .unwrap();
        assert!(
            FlatCompatBatch::try_new(&mapper, &read_format, false)
                .unwrap()
                .is_none()
        );

        // Widened unit: the time index column is rescaled, others pass through.
        let mapper = FlatProjectionMapper::all(&expected_metadata).unwrap();
        let read_format = FlatReadFormat::new(
            actual_metadata.clone(),
            ReadColumns::new([0, 1, 2]),
            None,
            "test",
            false,
        )
        .unwrap();
        let compat_batch = FlatCompatBatch::try_new(&mapper, &read_format, false)
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

        let expected_columns: Vec<ArrayRef> = vec![
            tag_dict_array.clone(),
            Arc::new(Int64Array::from(vec![100, 200])),
            // Rescaled, not reinterpreted: 1000ms -> 1_000_000us.
            Arc::new(TimestampMicrosecondArray::from_iter_values([
                1_000_000, 2_000_000,
            ])),
            build_flat_test_pk_array(&[&k1, &k1]),
            Arc::new(UInt64Array::from_iter_values([1, 2])),
            Arc::new(UInt8Array::from_iter_values([
                OpType::Put as u8,
                OpType::Put as u8,
            ])),
        ];
        let expected_schema =
            to_flat_sst_arrow_schema(&expected_metadata, &FlatSchemaOptions::default());
        let expected_batch = RecordBatch::try_new(expected_schema, expected_columns).unwrap();

        assert_eq!(expected_batch, result);
    }

    /// A widened time index unit combined with an added column (the realistic
    /// multi-alter sequence) must rescale and default-fill in one pass.
    #[test]
    fn test_flat_compat_batch_time_index_unit_change_with_added_column() {
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

        let mut expected_metadata = new_metadata(
            &[
                (
                    0,
                    SemanticType::Timestamp,
                    ConcreteDataType::timestamp_microsecond_datatype(),
                ),
                (1, SemanticType::Tag, ConcreteDataType::string_datatype()),
                (2, SemanticType::Field, ConcreteDataType::int64_datatype()),
                (3, SemanticType::Field, ConcreteDataType::int64_datatype()),
            ],
            &[1],
        );
        expected_metadata.primary_key_encoding = PrimaryKeyEncoding::Dense;
        let expected_metadata = Arc::new(expected_metadata);

        let mapper = FlatProjectionMapper::all(&expected_metadata).unwrap();
        let read_format = FlatReadFormat::new(
            actual_metadata.clone(),
            ReadColumns::new([0, 1, 2, 3]),
            None,
            "test",
            false,
        )
        .unwrap();

        let compat_batch = FlatCompatBatch::try_new(&mapper, &read_format, false)
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

        let expected_columns: Vec<ArrayRef> = vec![
            tag_dict_array.clone(),
            Arc::new(Int64Array::from(vec![100, 200])),
            Arc::new(Int64Array::from(vec![None::<i64>, None::<i64>])),
            Arc::new(TimestampMicrosecondArray::from_iter_values([
                1_000_000, 2_000_000,
            ])),
            build_flat_test_pk_array(&[&k1, &k1]),
            Arc::new(UInt64Array::from_iter_values([1, 2])),
            Arc::new(UInt8Array::from_iter_values([
                OpType::Put as u8,
                OpType::Put as u8,
            ])),
        ];
        let expected_schema =
            to_flat_sst_arrow_schema(&expected_metadata, &FlatSchemaOptions::default());
        let expected_batch = RecordBatch::try_new(expected_schema, expected_columns).unwrap();

        assert_eq!(expected_batch, result);
    }

    /// The sparse compaction compat path must also rescale a widened time
    /// index unit (metric-engine regions compact with sparse encoding).
    #[test]
    fn test_flat_compat_batch_compact_sparse_time_index_unit_change() {
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
        let reserved = crate::test_util::sst_util::sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        );
        actual_metadata.primary_key = reserved.primary_key.into_iter().take(2).collect();
        actual_metadata
            .column_metadatas
            .extend(reserved.column_metadatas.into_iter().take(2));
        actual_metadata.primary_key_encoding = PrimaryKeyEncoding::Sparse;
        let actual_metadata = Arc::new(
            RegionMetadataBuilder::from_existing(actual_metadata)
                .build()
                .unwrap(),
        );

        let mut expected_metadata = (*actual_metadata).clone();
        for column in expected_metadata.column_metadatas.iter_mut() {
            if column.semantic_type == SemanticType::Timestamp {
                column.column_schema.data_type = ConcreteDataType::timestamp_microsecond_datatype();
            }
        }
        let expected_metadata = Arc::new(
            RegionMetadataBuilder::from_existing(expected_metadata)
                .build()
                .unwrap(),
        );

        let mapper = FlatProjectionMapper::all(&expected_metadata).unwrap();
        let read_format = FlatReadFormat::new(
            actual_metadata.clone(),
            ReadColumns::new([0, 2]),
            None,
            "test",
            true,
        )
        .unwrap();

        let compat_batch = FlatCompatBatch::try_new(&mapper, &read_format, true)
            .unwrap()
            .unwrap();

        let mut sparse_k1 = Vec::new();
        SparsePrimaryKeyCodec::schemaless()
            .encode_internal(42, 7, &mut sparse_k1)
            .unwrap();
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
        let input_schema = to_flat_sst_arrow_schema(
            &actual_metadata,
            &FlatSchemaOptions::from_encoding(PrimaryKeyEncoding::Sparse),
        );
        let input_batch = RecordBatch::try_new(input_schema, input_columns).unwrap();

        let result = compat_batch.compat(input_batch).unwrap();

        let expected_columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![100, 200])),
            Arc::new(TimestampMicrosecondArray::from_iter_values([
                1_000_000, 2_000_000,
            ])),
            build_flat_test_pk_array(&[&sparse_k1, &sparse_k1]),
            Arc::new(UInt64Array::from_iter_values([1, 2])),
            Arc::new(UInt8Array::from_iter_values([
                OpType::Put as u8,
                OpType::Put as u8,
            ])),
        ];
        let output_schema = to_flat_sst_arrow_schema(
            &expected_metadata,
            &FlatSchemaOptions::from_encoding(PrimaryKeyEncoding::Sparse),
        );
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
            ReadColumns::new([0, 2, 3]),
            None,
            "test",
            true,
        )
        .unwrap();

        let compat_batch = FlatCompatBatch::try_new(&mapper, &read_format, true)
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
    #[test]
    fn test_compaction_json2_uses_target_fields()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let (metadata, batch) = test_util::sst_util::new_json2_record_batch()?;
        let logical_settings = JsonSettings::default();
        let layout = JsonSettings::try_new(vec![], Some(0))?;
        let targets: Json2RewriteTargets = Arc::new(BTreeMap::from([(
            0,
            Json2TargetLayout {
                extension_metadata: serde_json::to_string(&JsonMetadata::new(
                    logical_settings.clone(),
                ))?,
                target_layout: layout.clone(),
            },
        )]));
        // Model the existing Rewrite output; the compatibility layer only selects or fills arrays.
        let json = JsonArray::from(batch.column(0)).rewrite_to_v2(
            batch.schema_ref().field(0),
            &logical_settings,
            &layout,
        )?;
        let mut fields = batch.schema_ref().fields().to_vec();
        fields[0] = Arc::new(
            fields[0]
                .as_ref()
                .clone()
                .with_data_type(json.data_type().clone()),
        );
        let mut columns = batch.columns().to_vec();
        columns[0] = json;
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
        for encoding in [PrimaryKeyEncoding::Dense, PrimaryKeyEncoding::Sparse] {
            let batch = if encoding == PrimaryKeyEncoding::Sparse {
                let mut key = Vec::new();
                SparsePrimaryKeyCodec::schemaless().encode_internal(42, 7, &mut key)?;
                let mut columns = batch.columns().to_vec();
                columns[primary_key_column_index(batch.num_columns())] =
                    build_flat_test_pk_array(&[&key, &key]);
                RecordBatch::try_new(batch.schema(), columns)?
            } else {
                batch.clone()
            };
            for missing_json in [false, true] {
                let source = if missing_json {
                    new_metadata(
                        &[(
                            1,
                            SemanticType::Timestamp,
                            ConcreteDataType::timestamp_millisecond_datatype(),
                        )],
                        &[],
                    )
                } else {
                    metadata.as_ref().clone()
                };
                let mut actual_builder = RegionMetadataBuilder::from_existing(source);
                actual_builder.primary_key_encoding(encoding);
                let mut expected_builder =
                    RegionMetadataBuilder::from_existing(metadata.as_ref().clone());
                expected_builder.primary_key_encoding(encoding);
                expected_builder.push_column_metadata(ColumnMetadata {
                    column_schema: ColumnSchema::new(
                        "added",
                        ConcreteDataType::int64_datatype(),
                        true,
                    )
                    .with_default_constraint(Some(
                        ColumnDefaultConstraint::Value(Value::Int64(42)),
                    ))?,
                    semantic_type: SemanticType::Field,
                    column_id: 2,
                });
                if encoding == PrimaryKeyEncoding::Sparse {
                    let reserved = test_util::sst_util::sst_region_metadata_with_encoding(encoding);
                    for builder in [&mut actual_builder, &mut expected_builder] {
                        builder.primary_key(reserved.primary_key[..2].to_vec());
                        for column in reserved.column_metadatas.iter().take(2) {
                            builder.push_column_metadata(column.clone());
                        }
                    }
                }
                let actual = Arc::new(actual_builder.build()?);
                let expected = Arc::new(expected_builder.build()?);
                let read_columns = ReadColumns::new([0, 1, 2]);
                let mapper = FlatProjectionMapper::new_with_json2_rewrite_targets(
                    &expected,
                    vec![0, 1, 2],
                    read_columns.clone(),
                    &targets,
                )?;
                let read_format = FlatReadFormat::new(actual, read_columns, None, "test", true)?;
                let compat = FlatCompatBatch::try_new(&mapper, &read_format, true)?
                    .ok_or("missing columns require compatibility")?;
                let input = if missing_json {
                    batch.project(&[1, 2, 3, 4])?
                } else {
                    batch.clone()
                };
                let output = compat.compat(input)?;
                assert_eq!(
                    output.column(0).data_type(),
                    &json2_physical_data_type(&layout)
                );
                assert_eq!(
                    output.column(0).null_count(),
                    if missing_json { 2 } else { 0 }
                );
                let defaults = output
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or("expected int64 defaults")?;
                assert_eq!(defaults.values().as_ref(), &[42, 42]);
                if !missing_json {
                    let jsonb = JsonArray::from(output.column(0)).project_to_v2(
                        output.schema_ref().field(0),
                        &datatypes::arrow::datatypes::DataType::Binary,
                    )?;
                    let jsonb = jsonb
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .ok_or("expected JSONB")?;
                    assert_eq!(jsonb.value(0), parse_string_to_jsonb(r#"{"a":1}"#)?);
                    assert_eq!(jsonb.value(1), parse_string_to_jsonb(r#"{"a":2}"#)?);
                }
            }
        }
        Ok(())
    }
}
