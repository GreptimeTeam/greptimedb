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

//! Utilities for projection on flat format.

use std::sync::Arc;

use api::v1::SemanticType;
use common_error::ext::BoxedError;
use common_recordbatch::error::ExternalSnafu;
use common_recordbatch::RecordBatch;
use datatypes::arrow::datatypes::Field;
use datatypes::prelude::{ConcreteDataType, DataType};
use datatypes::schema::{Schema, SchemaRef};
use datatypes::vectors::Helper;
use snafu::{OptionExt, ResultExt};
use store_api::metadata::{RegionMetadata, RegionMetadataRef};
use store_api::storage::ColumnId;

use crate::error::{InvalidRequestSnafu, Result};
use crate::sst::internal_fields;
use crate::sst::parquet::flat_format::sst_column_id_indices;
use crate::sst::parquet::format::FormatProjection;

/// Handles projection and converts batches in flat format with correct schema.
///
/// This mapper support duplicate and unsorted projection indices.
/// The output schema is determined by the projection indices.
#[allow(dead_code)]
pub struct FlatProjectionMapper {
    /// Metadata of the region.
    metadata: RegionMetadataRef,
    /// Schema for converted [RecordBatch] to return.
    output_schema: SchemaRef,
    /// Ids of columns to project. It keeps ids in the same order as the `projection`
    /// indices to build the mapper.
    /// The mapper won't deduplicate the column ids.
    column_ids: Vec<ColumnId>,
    /// Ids and DataTypes of columns of the expected batch.
    /// We can use this to check if the batch is compatible with the expected schema.
    batch_schema: Vec<(ColumnId, ConcreteDataType)>,
    /// `true` If the original projection is empty.
    is_empty_projection: bool,
    /// The index in flat format [RecordBatch] for each column in the output [RecordBatch].
    batch_indices: Vec<usize>,
    /// Precomputed Arrow schema for input batches.
    input_arrow_schema: datatypes::arrow::datatypes::SchemaRef,
}

impl FlatProjectionMapper {
    /// Returns a new mapper with projection.
    /// If `projection` is empty, it outputs [RecordBatch] without any column but only a row count.
    /// `SELECT COUNT(*) FROM table` is an example that uses an empty projection. DataFusion accepts
    /// empty `RecordBatch` and only use its row count in this query.
    pub fn new(
        metadata: &RegionMetadataRef,
        projection: impl Iterator<Item = usize>,
    ) -> Result<Self> {
        let mut projection: Vec<_> = projection.collect();
        // If the original projection is empty.
        let is_empty_projection = projection.is_empty();
        if is_empty_projection {
            // If the projection is empty, we still read the time index column.
            projection.push(metadata.time_index_column_pos());
        }

        // Output column schemas for the projection.
        let mut column_schemas = Vec::with_capacity(projection.len());
        // Column ids of the projection without deduplication.
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

        // Creates a map to lookup index.
        let id_to_index = sst_column_id_indices(metadata);
        // TODO(yingwen): Support different flat schema options.
        let format_projection = FormatProjection::compute_format_projection(
            &id_to_index,
            // All columns with internal columns.
            metadata.column_metadatas.len() + 3,
            column_ids.iter().copied(),
        );

        let batch_schema = flat_projected_columns(metadata, &format_projection);

        // Safety: We get the column id from the metadata.
        let input_arrow_schema = compute_input_arrow_schema(metadata, &batch_schema);

        if is_empty_projection {
            // If projection is empty, we don't output any column.
            return Ok(FlatProjectionMapper {
                metadata: metadata.clone(),
                output_schema: Arc::new(Schema::new(vec![])),
                column_ids,
                batch_schema: vec![],
                is_empty_projection,
                batch_indices: vec![],
                input_arrow_schema,
            });
        }

        // Safety: Columns come from existing schema.
        let output_schema = Arc::new(Schema::new(column_schemas));

        let batch_indices: Vec<_> = column_ids
            .iter()
            .map(|id| {
                // Safety: The map is computed from `projection` itself.
                format_projection
                    .column_id_to_projected_index
                    .get(id)
                    .copied()
                    .unwrap()
            })
            .collect();

        Ok(FlatProjectionMapper {
            metadata: metadata.clone(),
            output_schema,
            column_ids,
            batch_schema,
            is_empty_projection,
            batch_indices,
            input_arrow_schema,
        })
    }

    /// Returns a new mapper without projection.
    pub fn all(metadata: &RegionMetadataRef) -> Result<Self> {
        FlatProjectionMapper::new(metadata, 0..metadata.column_metadatas.len())
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

    /// Returns the field column start index in output batch.
    pub(crate) fn field_column_start(&self) -> usize {
        for (idx, column_id) in self
            .batch_schema
            .iter()
            .map(|(column_id, _)| column_id)
            .enumerate()
        {
            // Safety: We get the column id from the metadata in new().
            if self
                .metadata
                .column_by_id(*column_id)
                .unwrap()
                .semantic_type
                == SemanticType::Field
            {
                return idx;
            }
        }

        self.batch_schema.len()
    }

    /// Returns ids of columns of the batch that the mapper expects to convert.
    #[allow(dead_code)]
    pub(crate) fn batch_schema(&self) -> &[(ColumnId, ConcreteDataType)] {
        &self.batch_schema
    }

    pub(crate) fn input_arrow_schema(&self) -> datatypes::arrow::datatypes::SchemaRef {
        self.input_arrow_schema.clone()
    }

    /// Returns the schema of converted [RecordBatch].
    /// This is the schema that the stream will output. This schema may contain
    /// less columns than [FlatProjectionMapper::column_ids()].
    pub(crate) fn output_schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    /// Returns an empty [RecordBatch].
    pub(crate) fn empty_record_batch(&self) -> RecordBatch {
        RecordBatch::new_empty(self.output_schema.clone())
    }

    /// Converts a flat format [RecordBatch] to a normal [RecordBatch].
    ///
    /// The batch must match the `projection` using to build the mapper.
    #[allow(dead_code)]
    pub(crate) fn convert(
        &self,
        batch: &datatypes::arrow::record_batch::RecordBatch,
    ) -> common_recordbatch::error::Result<RecordBatch> {
        if self.is_empty_projection {
            return RecordBatch::new_with_count(self.output_schema.clone(), batch.num_rows());
        }

        let mut columns = Vec::with_capacity(self.output_schema.num_columns());
        for index in &self.batch_indices {
            let array = batch.column(*index).clone();
            let vector = Helper::try_into_vector(array)
                .map_err(BoxedError::new)
                .context(ExternalSnafu)?;
            columns.push(vector);
        }

        RecordBatch::new(self.output_schema.clone(), columns)
    }
}

/// Returns ids and datatypes of columns of the output batch after applying the `projection`.
pub(crate) fn flat_projected_columns(
    metadata: &RegionMetadata,
    format_projection: &FormatProjection,
) -> Vec<(ColumnId, ConcreteDataType)> {
    let mut schema = vec![None; format_projection.column_id_to_projected_index.len()];
    for (column_id, index) in &format_projection.column_id_to_projected_index {
        // Safety: FormatProjection ensures the id is valid.
        schema[*index] = Some((
            *column_id,
            metadata
                .column_by_id(*column_id)
                .unwrap()
                .column_schema
                .data_type
                .clone(),
        ));
    }

    // Safety: FormatProjection ensures all indices can be unwrapped.
    schema.into_iter().map(|id_type| id_type.unwrap()).collect()
}

/// Computes the Arrow schema for input batches.
///
/// # Panics
/// Panics if it can't find the column by the column id in the batch_schema.
fn compute_input_arrow_schema(
    metadata: &RegionMetadata,
    batch_schema: &[(ColumnId, ConcreteDataType)],
) -> datatypes::arrow::datatypes::SchemaRef {
    let mut new_fields = Vec::with_capacity(batch_schema.len() + 3);
    for (column_id, _) in batch_schema {
        let column_metadata = metadata.column_by_id(*column_id).unwrap();
        let field = if column_metadata.semantic_type == SemanticType::Tag {
            Field::new_dictionary(
                &column_metadata.column_schema.name,
                datatypes::arrow::datatypes::DataType::UInt32,
                column_metadata.column_schema.data_type.as_arrow_type(),
                column_metadata.column_schema.is_nullable(),
            )
        } else {
            Field::new(
                &column_metadata.column_schema.name,
                column_metadata.column_schema.data_type.as_arrow_type(),
                column_metadata.column_schema.is_nullable(),
            )
        };
        new_fields.push(Arc::new(field));
    }
    new_fields.extend_from_slice(&internal_fields());

    Arc::new(datatypes::arrow::datatypes::Schema::new(new_fields))
}
