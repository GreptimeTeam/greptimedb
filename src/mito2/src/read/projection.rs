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

/// Handles projection and converts a projected [Batch] to a projected [RecordBatch].
pub struct ProjectionMapper {
    /// Metadata of the region.
    metadata: RegionMetadataRef,
    /// Maps column in [RecordBatch] to index in [Batch].
    batch_indices: Vec<BatchIndex>,
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
        for idx in &projection {
            // Safety: idx is valid.
            let column = &metadata.column_metadatas[*idx];
            // Get column index in a batch by its semantic type and column id.
            let batch_index = match column.semantic_type {
                SemanticType::Tag => {
                    // Safety: It is a primary key column.
                    let index = metadata.primary_key_index(column.column_id).unwrap();
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

        let pk_values = self
            .codec
            .decode(batch.primary_key())
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;

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
    cache_manager.put_repeated_vector(value.clone(), vector.clone());

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

// TODO(yingwen): Add tests for mapper.
