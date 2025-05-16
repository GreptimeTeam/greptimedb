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

use datatypes::data_type::ConcreteDataType;
use datatypes::value::Value;
use datatypes::vectors::VectorRef;
use snafu::{ensure, OptionExt, ResultExt};
use store_api::metadata::{RegionMetadata, RegionMetadataRef};
use store_api::storage::ColumnId;

use crate::error::{CompatReaderSnafu, CreateDefaultSnafu, Result};
use crate::read::projection::{PrimaryKeyProjectionMapper, ProjectionMapper};
use crate::read::{Batch, BatchColumn, BatchReader};
use crate::row_converter::{
    build_primary_key_codec, build_primary_key_codec_with_fields, CompositeValues, PrimaryKeyCodec,
    SortField,
};

/// Reader to adapt schema of underlying reader to expected schema.
pub struct CompatReader<R> {
    /// Underlying reader.
    reader: R,
    /// Helper to compat batches.
    compat: CompatBatch,
}

impl<R> CompatReader<R> {
    /// Creates a new compat reader.
    /// - `mapper` is built from the metadata users expect to see.
    /// - `reader_meta` is the metadata of the input reader.
    /// - `reader` is the input reader.
    pub fn new(
        mapper: &ProjectionMapper,
        reader_meta: RegionMetadataRef,
        reader: R,
    ) -> Result<CompatReader<R>> {
        Ok(CompatReader {
            reader,
            compat: CompatBatch::new(mapper, reader_meta)?,
        })
    }
}

#[async_trait::async_trait]
impl<R: BatchReader> BatchReader for CompatReader<R> {
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        let Some(mut batch) = self.reader.next_batch().await? else {
            return Ok(None);
        };

        batch = self.compat.compat_batch(batch)?;

        Ok(Some(batch))
    }
}

/// A helper struct to adapt schema of the batch to an expected schema.
pub(crate) struct CompatBatch {
    /// Optional primary key adapter.
    rewrite_pk: Option<RewritePrimaryKey>,
    /// Optional primary key adapter.
    compat_pk: Option<CompatPrimaryKey>,
    /// Optional fields adapter.
    compat_fields: Option<CompatFields>,
}

impl CompatBatch {
    /// Creates a new [CompatBatch].
    /// - `mapper` is built from the metadata users expect to see.
    /// - `reader_meta` is the metadata of the input reader.
    pub(crate) fn new(mapper: &ProjectionMapper, reader_meta: RegionMetadataRef) -> Result<Self> {
        let rewrite_pk = may_rewrite_primary_key(mapper.metadata(), &reader_meta);
        let compat_pk = may_compat_primary_key(mapper.metadata(), &reader_meta)?;
        // Safety: Only primary key format use this now.
        let compat_fields = may_compat_fields(mapper.as_primary_key().unwrap(), &reader_meta)?;

        Ok(Self {
            rewrite_pk,
            compat_pk,
            compat_fields,
        })
    }

    /// Adapts the `batch` to the expected schema.
    pub(crate) fn compat_batch(&self, mut batch: Batch) -> Result<Batch> {
        if let Some(rewrite_pk) = &self.rewrite_pk {
            batch = rewrite_pk.compat(batch)?;
        }
        if let Some(compat_pk) = &self.compat_pk {
            batch = compat_pk.compat(batch)?;
        }
        if let Some(compat_fields) = &self.compat_fields {
            batch = compat_fields.compat(batch);
        }

        Ok(batch)
    }
}

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

/// Helper to make primary key compatible.
#[derive(Debug)]
struct CompatPrimaryKey {
    /// Row converter to append values to primary keys.
    converter: Arc<dyn PrimaryKeyCodec>,
    /// Default values to append.
    values: Vec<(ColumnId, Value)>,
}

impl CompatPrimaryKey {
    /// Make primary key of the `batch` compatible.
    fn compat(&self, mut batch: Batch) -> Result<Batch> {
        let mut buffer = Vec::with_capacity(
            batch.primary_key().len() + self.converter.estimated_size().unwrap_or_default(),
        );
        buffer.extend_from_slice(batch.primary_key());
        self.converter.encode_values(&self.values, &mut buffer)?;

        batch.set_primary_key(buffer);

        // update cache
        if let Some(pk_values) = &mut batch.pk_values {
            pk_values.extend(&self.values);
        }

        Ok(batch)
    }
}

/// Helper to make fields compatible.
#[derive(Debug)]
struct CompatFields {
    /// Column Ids and DataTypes the reader actually returns.
    actual_fields: Vec<(ColumnId, ConcreteDataType)>,
    /// Indices to convert actual fields to expect fields.
    index_or_defaults: Vec<IndexOrDefault>,
}

impl CompatFields {
    /// Make fields of the `batch` compatible.
    #[must_use]
    fn compat(&self, batch: Batch) -> Batch {
        debug_assert_eq!(self.actual_fields.len(), batch.fields().len());
        debug_assert!(self
            .actual_fields
            .iter()
            .zip(batch.fields())
            .all(|((id, _), batch_column)| *id == batch_column.column_id));

        let len = batch.num_rows();
        let fields = self
            .index_or_defaults
            .iter()
            .map(|index_or_default| match index_or_default {
                IndexOrDefault::Index { pos, cast_type } => {
                    let old_column = &batch.fields()[*pos];

                    let data = if let Some(ty) = cast_type {
                        // Safety: We ensure type can be converted and the new batch should be valid.
                        // Tips: `safe` must be true in `CastOptions`, which will replace the specific value with null when it cannot be converted.
                        old_column.data.cast(ty).unwrap()
                    } else {
                        old_column.data.clone()
                    };
                    BatchColumn {
                        column_id: old_column.column_id,
                        data,
                    }
                }
                IndexOrDefault::DefaultValue {
                    column_id,
                    default_vector,
                } => {
                    let data = default_vector.replicate(&[len]);
                    BatchColumn {
                        column_id: *column_id,
                        data,
                    }
                }
            })
            .collect();

        // Safety: We ensure all columns have the same length and the new batch should be valid.
        batch.with_fields(fields).unwrap()
    }
}

fn may_rewrite_primary_key(
    expect: &RegionMetadata,
    actual: &RegionMetadata,
) -> Option<RewritePrimaryKey> {
    if expect.primary_key_encoding == actual.primary_key_encoding {
        return None;
    }

    let fields = expect.primary_key.clone();
    let original = build_primary_key_codec(actual);
    let new = build_primary_key_codec(expect);

    Some(RewritePrimaryKey {
        original,
        new,
        fields,
    })
}

/// Creates a [CompatPrimaryKey] if needed.
fn may_compat_primary_key(
    expect: &RegionMetadata,
    actual: &RegionMetadata,
) -> Result<Option<CompatPrimaryKey>> {
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
    if actual.primary_key.len() == expect.primary_key.len() {
        return Ok(None);
    }

    // We need to append default values to the primary key.
    let to_add = &expect.primary_key[actual.primary_key.len()..];
    let mut fields = Vec::with_capacity(to_add.len());
    let mut values = Vec::with_capacity(to_add.len());
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
    // Using expect primary key encoding to build the converter
    let converter =
        build_primary_key_codec_with_fields(expect.primary_key_encoding, fields.into_iter());

    Ok(Some(CompatPrimaryKey { converter, values }))
}

/// Creates a [CompatFields] if needed.
fn may_compat_fields(
    mapper: &PrimaryKeyProjectionMapper,
    actual: &RegionMetadata,
) -> Result<Option<CompatFields>> {
    let expect_fields = mapper.batch_fields();
    let actual_fields = Batch::projected_fields(actual, mapper.column_ids());
    if expect_fields == actual_fields {
        return Ok(None);
    }

    let source_field_index: HashMap<_, _> = actual_fields
        .iter()
        .enumerate()
        .map(|(idx, (column_id, data_type))| (*column_id, (idx, data_type)))
        .collect();

    let index_or_defaults = expect_fields
        .iter()
        .map(|(column_id, expect_data_type)| {
            if let Some((index, actual_data_type)) = source_field_index.get(column_id) {
                let mut cast_type = None;

                if expect_data_type != *actual_data_type {
                    cast_type = Some(expect_data_type.clone())
                }
                // Source has this field.
                Ok(IndexOrDefault::Index {
                    pos: *index,
                    cast_type,
                })
            } else {
                // Safety: mapper must have this column.
                let column = mapper.metadata().column_by_id(*column_id).unwrap();
                // Create a default vector with 1 element for that column.
                let default_vector = column
                    .column_schema
                    .create_default_vector(1)
                    .context(CreateDefaultSnafu {
                        region_id: mapper.metadata().region_id,
                        column: &column.column_schema.name,
                    })?
                    .with_context(|| CompatReaderSnafu {
                        region_id: mapper.metadata().region_id,
                        reason: format!(
                            "column {} does not have a default value to read",
                            column.column_schema.name
                        ),
                    })?;
                Ok(IndexOrDefault::DefaultValue {
                    column_id: column.column_id,
                    default_vector,
                })
            }
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Some(CompatFields {
        actual_fields,
        index_or_defaults,
    }))
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
        /// Id of the column.
        column_id: ColumnId,
        /// Default value. The vector has only 1 element.
        default_vector: VectorRef,
    },
}

/// Adapter to rewrite primary key.
struct RewritePrimaryKey {
    /// Original primary key codec.
    original: Arc<dyn PrimaryKeyCodec>,
    /// New primary key codec.
    new: Arc<dyn PrimaryKeyCodec>,
    /// Order of the fields in the new primary key.
    fields: Vec<ColumnId>,
}

impl RewritePrimaryKey {
    /// Make primary key of the `batch` compatible.
    fn compat(&self, mut batch: Batch) -> Result<Batch> {
        let values = if let Some(pk_values) = batch.pk_values() {
            pk_values
        } else {
            let new_pk_values = self.original.decode(batch.primary_key())?;
            batch.set_pk_values(new_pk_values);
            // Safety: We ensure pk_values is not None.
            batch.pk_values().as_ref().unwrap()
        };

        let mut buffer = Vec::with_capacity(
            batch.primary_key().len() + self.new.estimated_size().unwrap_or_default(),
        );
        match values {
            CompositeValues::Dense(values) => {
                self.new.encode_values(values.as_slice(), &mut buffer)?;
            }
            CompositeValues::Sparse(values) => {
                let values = self
                    .fields
                    .iter()
                    .map(|id| {
                        let value = values.get_or_null(*id);
                        (*id, value.as_value_ref())
                    })
                    .collect::<Vec<_>>();
                self.new.encode_value_refs(&values, &mut buffer)?;
            }
        }
        batch.set_primary_key(buffer);

        Ok(batch)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::{OpType, SemanticType};
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use datatypes::value::ValueRef;
    use datatypes::vectors::{Int64Vector, TimestampMillisecondVector, UInt64Vector, UInt8Vector};
    use store_api::codec::PrimaryKeyEncoding;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::storage::RegionId;

    use super::*;
    use crate::row_converter::{DensePrimaryKeyCodec, PrimaryKeyCodecExt, SparsePrimaryKeyCodec};
    use crate::test_util::{check_reader_result, VecBatchReader};

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
                SemanticType::Field => {
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

    /// Creates a batch for specific primary `key`.
    ///
    /// `fields`: [(column_id of the field, is null)]
    fn new_batch(
        primary_key: &[u8],
        fields: &[(ColumnId, bool)],
        start_ts: i64,
        num_rows: usize,
    ) -> Batch {
        let timestamps = Arc::new(TimestampMillisecondVector::from_values(
            start_ts..start_ts + num_rows as i64,
        ));
        let sequences = Arc::new(UInt64Vector::from_values(0..num_rows as u64));
        let op_types = Arc::new(UInt8Vector::from_vec(vec![OpType::Put as u8; num_rows]));
        let field_columns = fields
            .iter()
            .map(|(id, is_null)| {
                let data = if *is_null {
                    Arc::new(Int64Vector::from(vec![None; num_rows]))
                } else {
                    Arc::new(Int64Vector::from_vec(vec![*id as i64; num_rows]))
                };
                BatchColumn {
                    column_id: *id,
                    data,
                }
            })
            .collect();
        Batch::new(
            primary_key.to_vec(),
            timestamps,
            sequences,
            op_types,
            field_columns,
        )
        .unwrap()
    }

    #[test]
    fn test_invalid_pk_len() {
        let reader_meta = new_metadata(
            &[
                (
                    0,
                    SemanticType::Timestamp,
                    ConcreteDataType::timestamp_millisecond_datatype(),
                ),
                (1, SemanticType::Tag, ConcreteDataType::string_datatype()),
                (2, SemanticType::Tag, ConcreteDataType::string_datatype()),
                (3, SemanticType::Field, ConcreteDataType::int64_datatype()),
            ],
            &[1, 2],
        );
        let expect_meta = new_metadata(
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
        may_compat_primary_key(&expect_meta, &reader_meta).unwrap_err();
    }

    #[test]
    fn test_different_pk() {
        let reader_meta = new_metadata(
            &[
                (
                    0,
                    SemanticType::Timestamp,
                    ConcreteDataType::timestamp_millisecond_datatype(),
                ),
                (1, SemanticType::Tag, ConcreteDataType::string_datatype()),
                (2, SemanticType::Tag, ConcreteDataType::string_datatype()),
                (3, SemanticType::Field, ConcreteDataType::int64_datatype()),
            ],
            &[2, 1],
        );
        let expect_meta = new_metadata(
            &[
                (
                    0,
                    SemanticType::Timestamp,
                    ConcreteDataType::timestamp_millisecond_datatype(),
                ),
                (1, SemanticType::Tag, ConcreteDataType::string_datatype()),
                (2, SemanticType::Tag, ConcreteDataType::string_datatype()),
                (3, SemanticType::Field, ConcreteDataType::int64_datatype()),
                (4, SemanticType::Tag, ConcreteDataType::string_datatype()),
            ],
            &[1, 2, 4],
        );
        may_compat_primary_key(&expect_meta, &reader_meta).unwrap_err();
    }

    #[test]
    fn test_same_pk() {
        let reader_meta = new_metadata(
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
        assert!(may_compat_primary_key(&reader_meta, &reader_meta)
            .unwrap()
            .is_none());
    }

    #[test]
    fn test_same_pk_encoding() {
        let reader_meta = Arc::new(new_metadata(
            &[
                (
                    0,
                    SemanticType::Timestamp,
                    ConcreteDataType::timestamp_millisecond_datatype(),
                ),
                (1, SemanticType::Tag, ConcreteDataType::string_datatype()),
            ],
            &[1],
        ));

        assert!(may_compat_primary_key(&reader_meta, &reader_meta)
            .unwrap()
            .is_none());
    }

    #[test]
    fn test_same_fields() {
        let reader_meta = Arc::new(new_metadata(
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
        let mapper = PrimaryKeyProjectionMapper::all(&reader_meta).unwrap();
        assert!(may_compat_fields(&mapper, &reader_meta).unwrap().is_none())
    }

    #[tokio::test]
    async fn test_compat_reader() {
        let reader_meta = Arc::new(new_metadata(
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
        let expect_meta = Arc::new(new_metadata(
            &[
                (
                    0,
                    SemanticType::Timestamp,
                    ConcreteDataType::timestamp_millisecond_datatype(),
                ),
                (1, SemanticType::Tag, ConcreteDataType::string_datatype()),
                (2, SemanticType::Field, ConcreteDataType::int64_datatype()),
                (3, SemanticType::Tag, ConcreteDataType::string_datatype()),
                (4, SemanticType::Field, ConcreteDataType::int64_datatype()),
            ],
            &[1, 3],
        ));
        let mapper = ProjectionMapper::all(&expect_meta, false).unwrap();
        let k1 = encode_key(&[Some("a")]);
        let k2 = encode_key(&[Some("b")]);
        let source_reader = VecBatchReader::new(&[
            new_batch(&k1, &[(2, false)], 1000, 3),
            new_batch(&k2, &[(2, false)], 1000, 3),
        ]);

        let mut compat_reader = CompatReader::new(&mapper, reader_meta, source_reader).unwrap();
        let k1 = encode_key(&[Some("a"), None]);
        let k2 = encode_key(&[Some("b"), None]);
        check_reader_result(
            &mut compat_reader,
            &[
                new_batch(&k1, &[(2, false), (4, true)], 1000, 3),
                new_batch(&k2, &[(2, false), (4, true)], 1000, 3),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_compat_reader_different_order() {
        let reader_meta = Arc::new(new_metadata(
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
        let expect_meta = Arc::new(new_metadata(
            &[
                (
                    0,
                    SemanticType::Timestamp,
                    ConcreteDataType::timestamp_millisecond_datatype(),
                ),
                (1, SemanticType::Tag, ConcreteDataType::string_datatype()),
                (3, SemanticType::Field, ConcreteDataType::int64_datatype()),
                (2, SemanticType::Field, ConcreteDataType::int64_datatype()),
                (4, SemanticType::Field, ConcreteDataType::int64_datatype()),
            ],
            &[1],
        ));
        let mapper = ProjectionMapper::all(&expect_meta, false).unwrap();
        let k1 = encode_key(&[Some("a")]);
        let k2 = encode_key(&[Some("b")]);
        let source_reader = VecBatchReader::new(&[
            new_batch(&k1, &[(2, false)], 1000, 3),
            new_batch(&k2, &[(2, false)], 1000, 3),
        ]);

        let mut compat_reader = CompatReader::new(&mapper, reader_meta, source_reader).unwrap();
        check_reader_result(
            &mut compat_reader,
            &[
                new_batch(&k1, &[(3, true), (2, false), (4, true)], 1000, 3),
                new_batch(&k2, &[(3, true), (2, false), (4, true)], 1000, 3),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_compat_reader_different_types() {
        let actual_meta = Arc::new(new_metadata(
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
        let expect_meta = Arc::new(new_metadata(
            &[
                (
                    0,
                    SemanticType::Timestamp,
                    ConcreteDataType::timestamp_millisecond_datatype(),
                ),
                (1, SemanticType::Tag, ConcreteDataType::string_datatype()),
                (2, SemanticType::Field, ConcreteDataType::string_datatype()),
            ],
            &[1],
        ));
        let mapper = ProjectionMapper::all(&expect_meta, false).unwrap();
        let k1 = encode_key(&[Some("a")]);
        let k2 = encode_key(&[Some("b")]);
        let source_reader = VecBatchReader::new(&[
            new_batch(&k1, &[(2, false)], 1000, 3),
            new_batch(&k2, &[(2, false)], 1000, 3),
        ]);

        let fn_batch_cast = |batch: Batch| {
            let mut new_fields = batch.fields.clone();
            new_fields[0].data = new_fields[0]
                .data
                .cast(&ConcreteDataType::string_datatype())
                .unwrap();

            batch.with_fields(new_fields).unwrap()
        };
        let mut compat_reader = CompatReader::new(&mapper, actual_meta, source_reader).unwrap();
        check_reader_result(
            &mut compat_reader,
            &[
                fn_batch_cast(new_batch(&k1, &[(2, false)], 1000, 3)),
                fn_batch_cast(new_batch(&k2, &[(2, false)], 1000, 3)),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_compat_reader_projection() {
        let reader_meta = Arc::new(new_metadata(
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
        let expect_meta = Arc::new(new_metadata(
            &[
                (
                    0,
                    SemanticType::Timestamp,
                    ConcreteDataType::timestamp_millisecond_datatype(),
                ),
                (1, SemanticType::Tag, ConcreteDataType::string_datatype()),
                (3, SemanticType::Field, ConcreteDataType::int64_datatype()),
                (2, SemanticType::Field, ConcreteDataType::int64_datatype()),
                (4, SemanticType::Field, ConcreteDataType::int64_datatype()),
            ],
            &[1],
        ));
        // tag_1, field_2, field_3
        let mapper = ProjectionMapper::new(&expect_meta, [1, 3, 2].into_iter(), false).unwrap();
        let k1 = encode_key(&[Some("a")]);
        let source_reader = VecBatchReader::new(&[new_batch(&k1, &[(2, false)], 1000, 3)]);

        let mut compat_reader =
            CompatReader::new(&mapper, reader_meta.clone(), source_reader).unwrap();
        check_reader_result(
            &mut compat_reader,
            &[new_batch(&k1, &[(3, true), (2, false)], 1000, 3)],
        )
        .await;

        // tag_1, field_4, field_3
        let mapper = ProjectionMapper::new(&expect_meta, [1, 4, 2].into_iter(), false).unwrap();
        let k1 = encode_key(&[Some("a")]);
        let source_reader = VecBatchReader::new(&[new_batch(&k1, &[], 1000, 3)]);

        let mut compat_reader = CompatReader::new(&mapper, reader_meta, source_reader).unwrap();
        check_reader_result(
            &mut compat_reader,
            &[new_batch(&k1, &[(3, true), (4, true)], 1000, 3)],
        )
        .await;
    }

    #[tokio::test]
    async fn test_compat_reader_different_pk_encoding() {
        let mut reader_meta = new_metadata(
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
        reader_meta.primary_key_encoding = PrimaryKeyEncoding::Dense;
        let reader_meta = Arc::new(reader_meta);
        let mut expect_meta = new_metadata(
            &[
                (
                    0,
                    SemanticType::Timestamp,
                    ConcreteDataType::timestamp_millisecond_datatype(),
                ),
                (1, SemanticType::Tag, ConcreteDataType::string_datatype()),
                (2, SemanticType::Field, ConcreteDataType::int64_datatype()),
                (3, SemanticType::Tag, ConcreteDataType::string_datatype()),
                (4, SemanticType::Field, ConcreteDataType::int64_datatype()),
            ],
            &[1, 3],
        );
        expect_meta.primary_key_encoding = PrimaryKeyEncoding::Sparse;
        let expect_meta = Arc::new(expect_meta);

        let mapper = ProjectionMapper::all(&expect_meta, false).unwrap();
        let k1 = encode_key(&[Some("a")]);
        let k2 = encode_key(&[Some("b")]);
        let source_reader = VecBatchReader::new(&[
            new_batch(&k1, &[(2, false)], 1000, 3),
            new_batch(&k2, &[(2, false)], 1000, 3),
        ]);

        let mut compat_reader = CompatReader::new(&mapper, reader_meta, source_reader).unwrap();
        let k1 = encode_sparse_key(&[(1, Some("a")), (3, None)]);
        let k2 = encode_sparse_key(&[(1, Some("b")), (3, None)]);
        check_reader_result(
            &mut compat_reader,
            &[
                new_batch(&k1, &[(2, false), (4, true)], 1000, 3),
                new_batch(&k2, &[(2, false), (4, true)], 1000, 3),
            ],
        )
        .await;
    }
}
