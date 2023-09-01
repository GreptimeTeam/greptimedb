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

use std::sync::Arc;

use api::v1::SemanticType;
use common_error::ext::BoxedError;
use common_recordbatch::error::ExternalSnafu;
use common_recordbatch::RecordBatch;
use datatypes::prelude::{ConcreteDataType, DataType};
use datatypes::schema::{Schema, SchemaRef};
use datatypes::value::ValueRef;
use datatypes::vectors::VectorRef;
use snafu::{OptionExt, ResultExt};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;

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
    /// Id of columns to project.
    column_ids: Vec<ColumnId>,
}

impl ProjectionMapper {
    /// Returns a new mapper with projection.
    pub fn new(
        metadata: &RegionMetadataRef,
        projection: impl Iterator<Item = usize>,
    ) -> Result<ProjectionMapper> {
        let projection_len = projection.size_hint().0;
        let mut batch_indices = Vec::with_capacity(projection_len);
        let mut column_schemas = Vec::with_capacity(projection_len);
        let mut column_ids = Vec::with_capacity(projection_len);
        for idx in projection {
            // For each projection index, we get the column id for projection.
            let column = metadata
                .column_metadatas
                .get(idx)
                .context(InvalidRequestSnafu {
                    region_id: metadata.region_id,
                    reason: format!("projection index {} is out of bound", idx),
                })?;

            // Get column index in a batch by its semantic type and column id.
            let batch_index = match column.semantic_type {
                SemanticType::Tag => {
                    // Safety: It is a primary key column.
                    let index = metadata.primary_key_index(column.column_id).unwrap();
                    BatchIndex::Tag(index)
                }
                SemanticType::Timestamp => BatchIndex::Timestamp,
                SemanticType::Field => {
                    // Safety: It is a field column.
                    let index = metadata.field_index(column.column_id).unwrap();
                    BatchIndex::Field(index)
                }
            };
            batch_indices.push(batch_index);
            column_ids.push(column.column_id);
            // Safety: idx is valid.
            column_schemas.push(metadata.schema.column_schemas()[idx].clone());
        }

        let codec = McmpRowCodec::new(
            metadata
                .primary_key_columns()
                .map(|c| SortField::new(c.column_schema.data_type.clone()))
                .collect(),
        );
        // Safety: Columns come from existing schema.
        let output_schema = Arc::new(Schema::new(column_schemas));

        Ok(ProjectionMapper {
            metadata: metadata.clone(),
            batch_indices,
            codec,
            output_schema,
            column_ids,
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

    /// Returns the schema of converted [RecordBatch].
    pub(crate) fn output_schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    /// Converts a [Batch] to a [RecordBatch].
    ///
    /// The batch must match the `projection` using to build the mapper.
    pub(crate) fn convert(&self, batch: &Batch) -> common_recordbatch::error::Result<RecordBatch> {
        // TODO(yingwen): validate batch schema.

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
                    let value = pk_values[*idx].as_value_ref();
                    let vector = new_repeated_vector(&column_schema.data_type, value, num_rows)?;
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

/// Returns a vector with repeated values.
fn new_repeated_vector(
    data_type: &ConcreteDataType,
    value: ValueRef,
    num_rows: usize,
) -> common_recordbatch::error::Result<VectorRef> {
    let mut mutable_vector = data_type.create_mutable_vector(1);
    mutable_vector
        .try_push_value_ref(value)
        .map_err(BoxedError::new)
        .context(ExternalSnafu)?;
    // This requires an additional allocation.
    // TODO(yingwen): Add a way to create repeated vector to data type.
    let base_vector = mutable_vector.to_vector();
    Ok(base_vector.replicate(&[num_rows]))
}

// TODO(yingwen): Add tests for mapper.
