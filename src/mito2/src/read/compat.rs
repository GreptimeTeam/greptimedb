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

use datatypes::value::Value;
use datatypes::vectors::VectorRef;
use snafu::{ensure, OptionExt, ResultExt};
use store_api::metadata::{RegionMetadata, RegionMetadataRef};
use store_api::storage::ColumnId;

use crate::error::{CompatReaderSnafu, CreateDefaultSnafu, Result};
use crate::read::projection::ProjectionMapper;
use crate::read::{Batch, BatchColumn, BatchReader};
use crate::row_converter::{McmpRowCodec, RowCodec, SortField};

/// Reader to adapt schema of underlying reader to expected schema.
pub struct CompatReader<R> {
    /// Underlying reader.
    reader: R,
    /// Optional primary key adapter.
    compat_pk: Option<CompatPrimaryKey>,
    /// Optional fields adapter.
    compat_fields: Option<CompatFields>,
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
        let compat_pk = may_compat_primary_key(mapper.metadata(), &reader_meta)?;
        let compat_fields = may_compat_fields(mapper, &reader_meta)?;

        Ok(CompatReader {
            reader,
            compat_pk,
            compat_fields,
        })
    }
}

#[async_trait::async_trait]
impl<R: BatchReader> BatchReader for CompatReader<R> {
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        let Some(mut batch) = self.reader.next_batch().await? else {
            return Ok(None);
        };

        if let Some(compat_pk) = &self.compat_pk {
            batch = compat_pk.compat(batch)?;
        }
        if let Some(compat_fields) = &self.compat_fields {
            batch = compat_fields.compat(batch);
        }

        Ok(Some(batch))
    }
}

/// Returns true if `left` and `right` have same columns to read.
///
/// It only consider column ids.
pub(crate) fn has_same_columns(left: &RegionMetadata, right: &RegionMetadata) -> bool {
    if left.column_metadatas.len() != right.column_metadatas.len() {
        return false;
    }

    for (left_col, right_col) in left.column_metadatas.iter().zip(&right.column_metadatas) {
        if left_col.column_id != right_col.column_id {
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
struct CompatPrimaryKey {
    /// Row converter to append values to primary keys.
    converter: McmpRowCodec,
    /// Default values to append.
    values: Vec<Value>,
}

impl CompatPrimaryKey {
    /// Make primary key of the `batch` compatible.
    #[must_use]
    fn compat(&self, mut batch: Batch) -> Result<Batch> {
        let mut buffer =
            Vec::with_capacity(batch.primary_key().len() + self.converter.estimated_size());
        buffer.extend_from_slice(batch.primary_key());
        self.converter.encode_to_vec(
            self.values.iter().map(|value| value.as_value_ref()),
            &mut buffer,
        )?;

        batch.set_primary_key(buffer);
        Ok(batch)
    }
}

/// Helper to make fields compatible.
struct CompatFields {
    /// Column Ids the reader actually returns.
    actual_fields: Vec<ColumnId>,
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
            .all(|(id, batch_column)| *id == batch_column.column_id));

        let len = batch.num_rows();
        let fields = self
            .index_or_defaults
            .iter()
            .map(|index_or_default| match index_or_default {
                IndexOrDefault::Index(index) => batch.fields()[*index].clone(),
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
                "primary key has more columns {} than exepct {}",
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
        fields.push(SortField::new(column.column_schema.data_type.clone()));
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
        values.push(default_value);
    }
    let converter = McmpRowCodec::new(fields);

    Ok(Some(CompatPrimaryKey { converter, values }))
}

/// Creates a [CompatFields] if needed.
fn may_compat_fields(
    mapper: &ProjectionMapper,
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
        .map(|(idx, column_id)| (*column_id, idx))
        .collect();

    let index_or_defaults = expect_fields
        .iter()
        .map(|column_id| {
            if let Some(index) = source_field_index.get(column_id) {
                // Source has this field.
                Ok(IndexOrDefault::Index(*index))
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
enum IndexOrDefault {
    /// Index of the column in source batch.
    Index(usize),
    /// Default value for the column.
    DefaultValue {
        /// Id of the column.
        column_id: ColumnId,
        /// Default value. The vector has only 1 element.
        default_vector: VectorRef,
    },
}
