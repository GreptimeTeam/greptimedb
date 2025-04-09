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

//! Utilities for projection.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use api::v1::SemanticType;
use common_error::ext::BoxedError;
use common_recordbatch::error::ExternalSnafu;
use common_recordbatch::RecordBatch;
use datatypes::prelude::{ConcreteDataType, DataType};
use datatypes::schema::{Schema, SchemaRef};
use datatypes::value::Value;
use datatypes::vectors::VectorRef;
use snafu::{OptionExt, ResultExt};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;

use crate::cache::CacheStrategy;
use crate::error::{InvalidRequestSnafu, Result};
use crate::read::Batch;
use crate::row_converter::{build_primary_key_codec, CompositeValues, PrimaryKeyCodec};

/// Only cache vector when its length `<=` this value.
const MAX_VECTOR_LENGTH_TO_CACHE: usize = 16384;

/// Handles projection and converts a projected [Batch] to a projected [RecordBatch].
pub struct ProjectionMapper {
    /// Metadata of the region.
    metadata: RegionMetadataRef,
    /// Maps column in [RecordBatch] to index in [Batch].
    batch_indices: Vec<BatchIndex>,
    /// Output record batch contains tags.
    has_tags: bool,
    /// Decoder for primary key.
    codec: Arc<dyn PrimaryKeyCodec>,
    /// Schema for converted [RecordBatch].
    output_schema: SchemaRef,
    /// Ids of columns to project. It keeps ids in the same order as the `projection`
    /// indices to build the mapper.
    column_ids: Vec<ColumnId>,
    /// Ids and DataTypes of field columns in the [Batch].
    batch_fields: Vec<(ColumnId, ConcreteDataType)>,
    /// `true` If the original projection is empty.
    is_empty_projection: bool,
}

impl ProjectionMapper {
    /// Returns a new mapper with projection.
    /// If `projection` is empty, it outputs [RecordBatch] without any column but only a row count.
    /// `SELECT COUNT(*) FROM table` is an example that uses an empty projection. DataFusion accepts
    /// empty `RecordBatch` and only use its row count in this query.
    pub fn new(
        metadata: &RegionMetadataRef,
        projection: impl Iterator<Item = usize>,
    ) -> Result<ProjectionMapper> {
        let mut projection: Vec<_> = projection.collect();
        // If the original projection is empty.
        let is_empty_projection = projection.is_empty();
        if is_empty_projection {
            // If the projection is empty, we still read the time index column.
            projection.push(metadata.time_index_column_pos());
        }

        let mut column_schemas = Vec::with_capacity(projection.len());
        let mut column_ids = Vec::with_capacity(projection.len());
        for idx in &projection {
            // For each projection index, we get the column id for projection.
            let column = metadata
                .column_metadatas
                .get(*idx)
                .context(InvalidRequestSnafu {
                    region_id: metadata.region_id,
                    reason: format!("projection index {} is out of bound", idx),
                })?;

            column_ids.push(column.column_id);
            // Safety: idx is valid.
            column_schemas.push(metadata.schema.column_schemas()[*idx].clone());
        }

        let codec = build_primary_key_codec(metadata);
        if is_empty_projection {
            // If projection is empty, we don't output any column.
            return Ok(ProjectionMapper {
                metadata: metadata.clone(),
                batch_indices: vec![],
                has_tags: false,
                codec,
                output_schema: Arc::new(Schema::new(vec![])),
                column_ids,
                batch_fields: vec![],
                is_empty_projection,
            });
        }

        // Safety: Columns come from existing schema.
        let output_schema = Arc::new(Schema::new(column_schemas));
        // Get fields in each batch.
        let batch_fields = Batch::projected_fields(metadata, &column_ids);

        // Field column id to its index in batch.
        let field_id_to_index: HashMap<_, _> = batch_fields
            .iter()
            .enumerate()
            .map(|(index, (column_id, _))| (*column_id, index))
            .collect();
        // For each projected column, compute its index in batches.
        let mut batch_indices = Vec::with_capacity(projection.len());
        let mut has_tags = false;
        for idx in &projection {
            // Safety: idx is valid.
            let column = &metadata.column_metadatas[*idx];
            // Get column index in a batch by its semantic type and column id.
            let batch_index = match column.semantic_type {
                SemanticType::Tag => {
                    // Safety: It is a primary key column.
                    let index = metadata.primary_key_index(column.column_id).unwrap();
                    // We need to output a tag.
                    has_tags = true;
                    // We always read all primary key so the column always exists and the tag
                    // index is always valid.
                    BatchIndex::Tag((index, column.column_id))
                }
                SemanticType::Timestamp => BatchIndex::Timestamp,
                SemanticType::Field => {
                    // Safety: It is a field column so it should be in `field_id_to_index`.
                    let index = field_id_to_index[&column.column_id];
                    BatchIndex::Field(index)
                }
            };
            batch_indices.push(batch_index);
        }

        Ok(ProjectionMapper {
            metadata: metadata.clone(),
            batch_indices,
            has_tags,
            codec,
            output_schema,
            column_ids,
            batch_fields,
            is_empty_projection,
        })
    }

    /// Returns a new mapper without projection.
    pub fn all(metadata: &RegionMetadataRef) -> Result<ProjectionMapper> {
        ProjectionMapper::new(metadata, 0..metadata.column_metadatas.len())
    }

    /// Returns the metadata that created the mapper.
    pub(crate) fn metadata(&self) -> &RegionMetadataRef {
        &self.metadata
    }

    /// Returns ids of projected columns that we need to read
    /// from memtables and SSTs.
    pub(crate) fn column_ids(&self) -> &[ColumnId] {
        &self.column_ids
    }

    /// Returns ids of fields in [Batch]es the mapper expects to convert.
    pub(crate) fn batch_fields(&self) -> &[(ColumnId, ConcreteDataType)] {
        &self.batch_fields
    }

    /// Returns the schema of converted [RecordBatch].
    /// This is the schema that the stream will output. This schema may contain
    /// less columns than [ProjectionMapper::column_ids()].
    pub(crate) fn output_schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    /// Returns an empty [RecordBatch].
    pub(crate) fn empty_record_batch(&self) -> RecordBatch {
        RecordBatch::new_empty(self.output_schema.clone())
    }

    /// Converts a [Batch] to a [RecordBatch].
    ///
    /// The batch must match the `projection` using to build the mapper.
    pub(crate) fn convert(
        &self,
        batch: &Batch,
        cache_strategy: &CacheStrategy,
    ) -> common_recordbatch::error::Result<RecordBatch> {
        if self.is_empty_projection {
            return RecordBatch::new_with_count(self.output_schema.clone(), batch.num_rows());
        }

        debug_assert_eq!(self.batch_fields.len(), batch.fields().len());
        debug_assert!(self
            .batch_fields
            .iter()
            .zip(batch.fields())
            .all(|((id, _), batch_col)| *id == batch_col.column_id));

        // Skips decoding pk if we don't need to output it.
        let pk_values = if self.has_tags {
            match batch.pk_values() {
                Some(v) => v.clone(),
                None => self
                    .codec
                    .decode(batch.primary_key())
                    .map_err(BoxedError::new)
                    .context(ExternalSnafu)?,
            }
        } else {
            CompositeValues::Dense(vec![])
        };

        let mut columns = Vec::with_capacity(self.output_schema.num_columns());
        let num_rows = batch.num_rows();
        for (index, column_schema) in self
            .batch_indices
            .iter()
            .zip(self.output_schema.column_schemas())
        {
            match index {
                BatchIndex::Tag((idx, column_id)) => {
                    let value = match &pk_values {
                        CompositeValues::Dense(v) => &v[*idx].1,
                        CompositeValues::Sparse(v) => v.get_or_null(*column_id),
                    };
                    let vector = repeated_vector_with_cache(
                        &column_schema.data_type,
                        value,
                        num_rows,
                        cache_strategy,
                    )?;
                    columns.push(vector);
                }
                BatchIndex::Timestamp => {
                    columns.push(batch.timestamps().clone());
                }
                BatchIndex::Field(idx) => {
                    columns.push(batch.fields()[*idx].data.clone());
                }
            }
        }

        RecordBatch::new(self.output_schema.clone(), columns)
    }
}

/// Index of a vector in a [Batch].
#[derive(Debug, Clone, Copy)]
enum BatchIndex {
    /// Index in primary keys.
    Tag((usize, ColumnId)),
    /// The time index column.
    Timestamp,
    /// Index in fields.
    Field(usize),
}

/// Gets a vector with repeated values from specific cache or creates a new one.
fn repeated_vector_with_cache(
    data_type: &ConcreteDataType,
    value: &Value,
    num_rows: usize,
    cache_strategy: &CacheStrategy,
) -> common_recordbatch::error::Result<VectorRef> {
    if let Some(vector) = cache_strategy.get_repeated_vector(data_type, value) {
        // Tries to get the vector from cache manager. If the vector doesn't
        // have enough length, creates a new one.
        match vector.len().cmp(&num_rows) {
            Ordering::Less => (),
            Ordering::Equal => return Ok(vector),
            Ordering::Greater => return Ok(vector.slice(0, num_rows)),
        }
    }

    // Creates a new one.
    let vector = new_repeated_vector(data_type, value, num_rows)?;
    // Updates cache.
    if vector.len() <= MAX_VECTOR_LENGTH_TO_CACHE {
        cache_strategy.put_repeated_vector(value.clone(), vector.clone());
    }

    Ok(vector)
}

/// Returns a vector with repeated values.
fn new_repeated_vector(
    data_type: &ConcreteDataType,
    value: &Value,
    num_rows: usize,
) -> common_recordbatch::error::Result<VectorRef> {
    let mut mutable_vector = data_type.create_mutable_vector(1);
    mutable_vector
        .try_push_value_ref(value.as_value_ref())
        .map_err(BoxedError::new)
        .context(ExternalSnafu)?;
    // This requires an additional allocation.
    let base_vector = mutable_vector.to_vector();
    Ok(base_vector.replicate(&[num_rows]))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::OpType;
    use datatypes::arrow::array::{Int64Array, TimestampMillisecondArray, UInt64Array, UInt8Array};
    use datatypes::arrow::util::pretty;
    use datatypes::value::ValueRef;

    use super::*;
    use crate::cache::CacheManager;
    use crate::read::BatchBuilder;
    use crate::row_converter::{DensePrimaryKeyCodec, PrimaryKeyCodecExt, SortField};
    use crate::test_util::meta_util::TestRegionMetadataBuilder;

    fn new_batch(
        ts_start: i64,
        tags: &[i64],
        fields: &[(ColumnId, i64)],
        num_rows: usize,
    ) -> Batch {
        let converter = DensePrimaryKeyCodec::with_fields(
            (0..tags.len())
                .map(|idx| {
                    (
                        idx as u32,
                        SortField::new(ConcreteDataType::int64_datatype()),
                    )
                })
                .collect(),
        );
        let primary_key = converter
            .encode(tags.iter().map(|v| ValueRef::Int64(*v)))
            .unwrap();

        let mut builder = BatchBuilder::new(primary_key);
        builder
            .timestamps_array(Arc::new(TimestampMillisecondArray::from_iter_values(
                (0..num_rows).map(|i| ts_start + i as i64 * 1000),
            )))
            .unwrap()
            .sequences_array(Arc::new(UInt64Array::from_iter_values(0..num_rows as u64)))
            .unwrap()
            .op_types_array(Arc::new(UInt8Array::from_iter_values(
                (0..num_rows).map(|_| OpType::Put as u8),
            )))
            .unwrap();
        for (column_id, field) in fields {
            builder
                .push_field_array(
                    *column_id,
                    Arc::new(Int64Array::from_iter_values(std::iter::repeat_n(
                        *field, num_rows,
                    ))),
                )
                .unwrap();
        }
        builder.build().unwrap()
    }

    fn print_record_batch(record_batch: RecordBatch) -> String {
        pretty::pretty_format_batches(&[record_batch.into_df_record_batch()])
            .unwrap()
            .to_string()
    }

    #[test]
    fn test_projection_mapper_all() {
        let metadata = Arc::new(
            TestRegionMetadataBuilder::default()
                .num_tags(2)
                .num_fields(2)
                .build(),
        );
        let mapper = ProjectionMapper::all(&metadata).unwrap();
        assert_eq!([0, 1, 2, 3, 4], mapper.column_ids());
        assert_eq!(
            [
                (3, ConcreteDataType::int64_datatype()),
                (4, ConcreteDataType::int64_datatype())
            ],
            mapper.batch_fields()
        );

        // With vector cache.
        let cache = CacheManager::builder().vector_cache_size(1024).build();
        let cache = CacheStrategy::EnableAll(Arc::new(cache));
        let batch = new_batch(0, &[1, 2], &[(3, 3), (4, 4)], 3);
        let record_batch = mapper.convert(&batch, &cache).unwrap();
        let expect = "\
+---------------------+----+----+----+----+
| ts                  | k0 | k1 | v0 | v1 |
+---------------------+----+----+----+----+
| 1970-01-01T00:00:00 | 1  | 2  | 3  | 4  |
| 1970-01-01T00:00:01 | 1  | 2  | 3  | 4  |
| 1970-01-01T00:00:02 | 1  | 2  | 3  | 4  |
+---------------------+----+----+----+----+";
        assert_eq!(expect, print_record_batch(record_batch));

        assert!(cache
            .get_repeated_vector(&ConcreteDataType::int64_datatype(), &Value::Int64(1))
            .is_some());
        assert!(cache
            .get_repeated_vector(&ConcreteDataType::int64_datatype(), &Value::Int64(2))
            .is_some());
        assert!(cache
            .get_repeated_vector(&ConcreteDataType::int64_datatype(), &Value::Int64(3))
            .is_none());
        let record_batch = mapper.convert(&batch, &cache).unwrap();
        assert_eq!(expect, print_record_batch(record_batch));
    }

    #[test]
    fn test_projection_mapper_with_projection() {
        let metadata = Arc::new(
            TestRegionMetadataBuilder::default()
                .num_tags(2)
                .num_fields(2)
                .build(),
        );
        // Columns v1, k0
        let mapper = ProjectionMapper::new(&metadata, [4, 1].into_iter()).unwrap();
        assert_eq!([4, 1], mapper.column_ids());
        assert_eq!(
            [(4, ConcreteDataType::int64_datatype())],
            mapper.batch_fields()
        );

        let batch = new_batch(0, &[1, 2], &[(4, 4)], 3);
        let cache = CacheManager::builder().vector_cache_size(1024).build();
        let cache = CacheStrategy::EnableAll(Arc::new(cache));
        let record_batch = mapper.convert(&batch, &cache).unwrap();
        let expect = "\
+----+----+
| v1 | k0 |
+----+----+
| 4  | 1  |
| 4  | 1  |
| 4  | 1  |
+----+----+";
        assert_eq!(expect, print_record_batch(record_batch));
    }

    #[test]
    fn test_projection_mapper_empty_projection() {
        let metadata = Arc::new(
            TestRegionMetadataBuilder::default()
                .num_tags(2)
                .num_fields(2)
                .build(),
        );
        // Empty projection
        let mapper = ProjectionMapper::new(&metadata, [].into_iter()).unwrap();
        assert_eq!([0], mapper.column_ids()); // Should still read the time index column
        assert!(mapper.batch_fields().is_empty());
        assert!(!mapper.has_tags);
        assert!(mapper.batch_indices.is_empty());
        assert!(mapper.output_schema().is_empty());
        assert!(mapper.is_empty_projection);

        let batch = new_batch(0, &[1, 2], &[], 3);
        let cache = CacheManager::builder().vector_cache_size(1024).build();
        let cache = CacheStrategy::EnableAll(Arc::new(cache));
        let record_batch = mapper.convert(&batch, &cache).unwrap();
        assert_eq!(3, record_batch.num_rows());
        assert_eq!(0, record_batch.num_columns());
        assert!(record_batch.schema.is_empty());
    }
}
