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

//! Utilities for projection on plain format.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use common_error::ext::BoxedError;
use common_recordbatch::error::ExternalSnafu;
use common_recordbatch::RecordBatch;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{Schema, SchemaRef};
use datatypes::vectors::Helper;
use snafu::{OptionExt, ResultExt};
use store_api::metadata::{RegionMetadata, RegionMetadataRef};
use store_api::storage::ColumnId;

use crate::error::{InvalidRequestSnafu, Result};
use crate::read::plain_batch::PlainBatch;

/// Handles projection and converts a projected `PlainBatch` to a projected [RecordBatch].
///
/// This mapper support duplicate and unsorted projection indices.
/// The output schema is determined by the projection indices.
pub struct PlainProjectionMapper {
    /// Metadata of the region.
    metadata: RegionMetadataRef,
    /// Schema for converted [RecordBatch] to return.
    output_schema: SchemaRef,
    /// Ids of columns to project. It keeps ids in the same order as the `projection`
    /// indices to build the mapper.
    column_ids: Vec<ColumnId>,
    /// Ids and DataTypes of columns of the expected batch.
    /// We can use this to check if the batch is compatible with the expected schema.
    batch_schema: Vec<(ColumnId, ConcreteDataType)>,
    /// `true` If the original projection is empty.
    is_empty_projection: bool,
    /// The index in `PlainBatch` of each column in the output [RecordBatch].
    batch_indices: Vec<usize>,
}

impl PlainProjectionMapper {
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

        if is_empty_projection {
            // If projection is empty, we don't output any column.
            return Ok(PlainProjectionMapper {
                metadata: metadata.clone(),
                output_schema: Arc::new(Schema::new(vec![])),
                column_ids,
                batch_schema: vec![],
                is_empty_projection,
                batch_indices: vec![],
            });
        }

        // Safety: Columns come from existing schema.
        let output_schema = Arc::new(Schema::new(column_schemas));
        // Sorts and deduplicates the projection by index.
        let mut dedup_projection = projection.clone();
        dedup_projection.sort_unstable();
        dedup_projection.dedup();
        // Maps the projection to the index in the batch schema.
        // (projection -> index in the batch)
        let projection_to_index = dedup_projection
            .into_iter()
            .enumerate()
            .map(|(i, p)| (p, i))
            .collect::<HashMap<_, _>>();
        // Computes the index in the batch schema for each projection index.
        let batch_indices = projection
            .iter()
            .map(|p| {
                // Safety: The map is computed from `projection` itself.
                projection_to_index.get(p).copied().unwrap()
            })
            .collect();
        let batch_schema = plain_projected_columns(metadata, &column_ids);

        Ok(PlainProjectionMapper {
            metadata: metadata.clone(),
            output_schema,
            column_ids,
            batch_schema,
            is_empty_projection,
            batch_indices,
        })
    }

    /// Returns a new mapper without projection.
    pub fn all(metadata: &RegionMetadataRef) -> Result<Self> {
        PlainProjectionMapper::new(metadata, 0..metadata.column_metadatas.len())
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

    /// Returns ids of columns of the batch that the mapper expects to convert.
    pub(crate) fn batch_schema(&self) -> &[(ColumnId, ConcreteDataType)] {
        &self.batch_schema
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

    /// Converts a [PlainBatch] to a [RecordBatch].
    ///
    /// The batch must match the `projection` using to build the mapper.
    pub(crate) fn convert(
        &self,
        batch: &PlainBatch,
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
pub(crate) fn plain_projected_columns(
    metadata: &RegionMetadata,
    projection: &[ColumnId],
) -> Vec<(ColumnId, ConcreteDataType)> {
    let projected_ids: HashSet<_> = projection.iter().copied().collect();
    metadata
        .column_metadatas
        .iter()
        .filter_map(|column| {
            if projected_ids.contains(&column.column_id) {
                Some((column.column_id, column.column_schema.data_type.clone()))
            } else {
                None
            }
        })
        .collect()
}
