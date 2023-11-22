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

use crate::cache::CacheManager;
use crate::error::{InvalidRequestSnafu, Result};
use crate::read::Batch;
use crate::row_converter::{McmpRowCodec, RowCodec, SortField};

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
    codec: McmpRowCodec,
    /// Schema for converted [RecordBatch].
    output_schema: SchemaRef,
    /// Ids of columns to project. It keeps ids in the same order as the `projection`
    /// indices to build the mapper.
    column_ids: Vec<ColumnId>,
    /// Ids of field columns in the [Batch].
    batch_fields: Vec<ColumnId>,
}

impl ProjectionMapper {
    /// Returns a new mapper with projection.
    pub fn new(
        metadata: &RegionMetadataRef,
        projection: impl Iterator<Item = usize>,
    ) -> Result<ProjectionMapper> {
        let projection: Vec<_> = projection.collect();
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
        let codec = McmpRowCodec::new(
            metadata
                .primary_key_columns()
                .map(|c| SortField::new(c.column_schema.data_type.clone()))
                .collect(),
        );
        // Safety: Columns come from existing schema.
        let output_schema = Arc::new(Schema::new(column_schemas));
        // Get fields in each batch.
        let batch_fields = Batch::projected_fields(metadata, &column_ids);

        // Field column id to its index in batch.
        let field_id_to_index: HashMap<_, _> = batch_fields
            .iter()
            .enumerate()
            .map(|(index, column_id)| (*column_id, index))
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
                    BatchIndex::Tag(index)
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

    /// Returns ids of projected columns.
    pub(crate) fn column_ids(&self) -> &[ColumnId] {
        &self.column_ids
    }

    /// Returns ids of fields in [Batch]es the mapper expects to convert.
    pub(crate) fn batch_fields(&self) -> &[ColumnId] {
        &self.batch_fields
    }

    /// Returns the schema of converted [RecordBatch].
    pub(crate) fn output_schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    /// Converts a [Batch] to a [RecordBatch].
    ///
    /// The batch must match the `projection` using to build the mapper.
    pub(crate) fn convert(
        &self,
        batch: &Batch,
        cache_manager: Option<&CacheManager>,
    ) -> common_recordbatch::error::Result<RecordBatch> {
        debug_assert_eq!(self.batch_fields.len(), batch.fields().len());
        debug_assert!(self
            .batch_fields
            .iter()
            .zip(batch.fields())
            .all(|(id, batch_col)| *id == batch_col.column_id));

        // Skips decoding pk if we don't need to output it.
        let pk_values = if self.has_tags {
            self.codec
                .decode(batch.primary_key())
                .map_err(BoxedError::new)
                .context(ExternalSnafu)?
        } else {
            Vec::new()
        };

        let mut columns = Vec::with_capacity(self.output_schema.num_columns());
        let num_rows = batch.num_rows();
        for (index, column_schema) in self
            .batch_indices
            .iter()
            .zip(self.output_schema.column_schemas())
        {
            match index {
                BatchIndex::Tag(idx) => {
                    let value = &pk_values[*idx];
                    let vector = match cache_manager {
                        Some(cache) => repeated_vector_with_cache(
                            &column_schema.data_type,
                            value,
                            num_rows,
                            cache,
                        )?,
                        None => new_repeated_vector(&column_schema.data_type, value, num_rows)?,
                    };
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
    Tag(usize),
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
    cache_manager: &CacheManager,
) -> common_recordbatch::error::Result<VectorRef> {
    if let Some(vector) = cache_manager.get_repeated_vector(value) {
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
        cache_manager.put_repeated_vector(value.clone(), vector.clone());
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
    use api::v1::OpType;
    use datatypes::arrow::array::{Int64Array, TimestampMillisecondArray, UInt64Array, UInt8Array};
    use datatypes::arrow::util::pretty;
    use datatypes::value::ValueRef;

    use super::*;
    use crate::read::BatchBuilder;
    use crate::test_util::meta_util::TestRegionMetadataBuilder;

    fn new_batch(
        ts_start: i64,
        tags: &[i64],
        fields: &[(ColumnId, i64)],
        num_rows: usize,
    ) -> Batch {
        let converter = McmpRowCodec::new(
            (0..tags.len())
                .map(|_| SortField::new(ConcreteDataType::int64_datatype()))
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
                    Arc::new(Int64Array::from_iter_values(
                        std::iter::repeat(*field).take(num_rows),
                    )),
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
        assert_eq!([3, 4], mapper.batch_fields());

        let cache = CacheManager::new(0, 1024, 0);
        let batch = new_batch(0, &[1, 2], &[(3, 3), (4, 4)], 3);
        let record_batch = mapper.convert(&batch, Some(&cache)).unwrap();
        let expect = "\
+---------------------+----+----+----+----+
| ts                  | k0 | k1 | v0 | v1 |
+---------------------+----+----+----+----+
| 1970-01-01T00:00:00 | 1  | 2  | 3  | 4  |
| 1970-01-01T00:00:01 | 1  | 2  | 3  | 4  |
| 1970-01-01T00:00:02 | 1  | 2  | 3  | 4  |
+---------------------+----+----+----+----+";
        assert_eq!(expect, print_record_batch(record_batch));

        assert!(cache.get_repeated_vector(&Value::Int64(1)).is_some());
        assert!(cache.get_repeated_vector(&Value::Int64(2)).is_some());
        assert!(cache.get_repeated_vector(&Value::Int64(3)).is_none());
        let record_batch = mapper.convert(&batch, Some(&cache)).unwrap();
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
        assert_eq!([4], mapper.batch_fields());

        let batch = new_batch(0, &[1, 2], &[(4, 4)], 3);
        let record_batch = mapper.convert(&batch, None).unwrap();
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
}
