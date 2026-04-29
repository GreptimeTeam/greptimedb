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
use common_recordbatch::error::{ArrowComputeSnafu, ExternalSnafu, NewDfRecordBatchSnafu};
use common_recordbatch::{DfRecordBatch, RecordBatch};
use datatypes::arrow::array::Array;
use datatypes::arrow::datatypes::{DataType as ArrowDataType, Field};
use datatypes::prelude::{ConcreteDataType, DataType};
use datatypes::schema::{Schema, SchemaRef};
use datatypes::value::Value;
use datatypes::vectors::Helper;
use snafu::{OptionExt, ResultExt};
use store_api::metadata::{RegionMetadata, RegionMetadataRef};
use store_api::storage::ColumnId;

use crate::cache::CacheStrategy;
use crate::error::{InvalidRequestSnafu, RecordBatchSnafu, Result};
use crate::read::projection::{read_column_ids_from_projection, repeated_vector_with_cache};
use crate::read::read_columns::ReadColumns;
use crate::sst::parquet::flat_format::sst_column_id_indices;
use crate::sst::parquet::format::FormatProjection;
use crate::sst::{
    FlatSchemaOptions, internal_fields, tag_maybe_to_dictionary_field, to_flat_sst_arrow_schema,
};

/// Handles projection and converts batches in flat format with correct schema.
///
/// This mapper support duplicate and unsorted projection indices.
/// The output schema is determined by the projection indices.
pub struct FlatProjectionMapper {
    /// Metadata of the region.
    metadata: RegionMetadataRef,
    /// Schema for converted [RecordBatch] to return.
    output_schema: SchemaRef,
    /// The columns to read from memtables and SSTs.
    /// The mapper won't deduplicate the column ids.
    ///
    /// Note that this doesn't contain the `__table_id` and `__tsid`.
    read_cols: ReadColumns,
    /// Ids and DataTypes of columns of the expected batch.
    /// We can use this to check if the batch is compatible with the expected schema.
    ///
    /// It doesn't contain internal columns but always contains the time index column.
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
        projection: impl IntoIterator<Item = usize>,
    ) -> Result<Self> {
        let projection: Vec<_> = projection.into_iter().collect();
        let read_column_ids = read_column_ids_from_projection(metadata, &projection)?;
        Self::new_with_read_columns(metadata, projection, read_column_ids)
    }

    /// Returns a new mapper with output projection and explicit read columns.
    pub fn new_with_read_columns(
        metadata: &RegionMetadataRef,
        projection: Vec<usize>,
        read_cols: impl Into<ReadColumns>,
    ) -> Result<Self> {
        let projection: Vec<_> = projection.into_iter().collect();
        let read_cols = read_cols.into();
        // If the original projection is empty.
        let is_empty_projection = projection.is_empty();

        // Output column schemas for the projection.
        let mut col_schemas = Vec::with_capacity(projection.len());
        // Column ids of the output projection without deduplication.
        let mut output_col_ids = Vec::with_capacity(projection.len());
        for idx in &projection {
            let col = metadata
                .column_metadatas
                .get(*idx)
                .with_context(|| InvalidRequestSnafu {
                    region_id: metadata.region_id,
                    reason: format!("projection index {} is out of bound", idx),
                })?;
            output_col_ids.push(col.column_id);
            col_schemas.push(col.column_schema.clone());
        }

        // Creates a map to lookup index.
        let id_to_index = sst_column_id_indices(metadata);

        // TODO(yingwen): Support different flat schema options.
        let format_projection = FormatProjection::compute_format_projection(
            &id_to_index,
            // All columns with internal columns.
            metadata.column_metadatas.len() + 3,
            read_cols.clone(),
        );

        let batch_schema = flat_projected_columns(metadata, &format_projection);

        // Safety: We get the column id from the metadata.
        let input_arrow_schema = compute_input_arrow_schema(metadata, &batch_schema);

        // If projection is empty, we don't output any column.
        let output_schema = if is_empty_projection {
            Arc::new(Schema::new(vec![]))
        } else {
            // Safety: Columns come from existing schema.
            Arc::new(Schema::new(col_schemas))
        };

        let batch_indices = if is_empty_projection {
            vec![]
        } else {
            output_col_ids
                .iter()
                .map(|id| {
                    // Safety: The map is computed from the read projection.
                    format_projection
                        .column_id_to_projected_index
                        .get(id)
                        .copied()
                        .with_context(|| {
                            let name = metadata
                                .column_by_id(*id)
                                .map(|column| column.column_schema.name.clone())
                                .unwrap_or_else(|| id.to_string());
                            InvalidRequestSnafu {
                                region_id: metadata.region_id,
                                reason: format!(
                                    "output column {} is missing in read projection",
                                    name
                                ),
                            }
                        })
                })
                .collect::<Result<Vec<_>>>()?
        };

        Ok(FlatProjectionMapper {
            metadata: metadata.clone(),
            output_schema,
            read_cols,
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
    /// Returns projected columns that we need to read from memtables and SSTs.
    pub(crate) fn read_columns(&self) -> &ReadColumns {
        &self.read_cols
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
    pub(crate) fn batch_schema(&self) -> &[(ColumnId, ConcreteDataType)] {
        &self.batch_schema
    }

    /// Returns the input arrow schema from sources.
    ///
    /// The merge reader can use this schema.
    pub(crate) fn input_arrow_schema(
        &self,
        compaction: bool,
    ) -> datatypes::arrow::datatypes::SchemaRef {
        if !compaction {
            self.input_arrow_schema.clone()
        } else {
            // For compaction, we need to build a different schema from encoding.
            to_flat_sst_arrow_schema(
                &self.metadata,
                &FlatSchemaOptions::from_encoding(self.metadata.primary_key_encoding),
            )
        }
    }

    /// Returns the schema of converted [RecordBatch].
    /// This is the schema that the stream will output. This schema may contain
    /// less columns than [FlatProjectionMapper::column_ids()].
    pub(crate) fn output_schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    /// Converts a flat format [RecordBatch] to a normal [RecordBatch].
    ///
    /// The batch must match the `projection` using to build the mapper.
    pub(crate) fn convert(
        &self,
        batch: &datatypes::arrow::record_batch::RecordBatch,
        cache_strategy: &CacheStrategy,
    ) -> common_recordbatch::error::Result<RecordBatch> {
        if self.is_empty_projection {
            return RecordBatch::new_with_count(self.output_schema.clone(), batch.num_rows());
        }
        // Construct output record batch directly from Arrow arrays to avoid
        // Arrow -> Vector -> Arrow roundtrips in the hot path.
        let mut arrays = Vec::with_capacity(self.output_schema.num_columns());
        for (output_idx, index) in self.batch_indices.iter().enumerate() {
            let mut array = batch.column(*index).clone();
            // Cast dictionary values to the target type.
            if let ArrowDataType::Dictionary(_key_type, value_type) = array.data_type() {
                // When a string dictionary column contains only a single value, reuse a cached
                // repeated vector to avoid repeatedly expanding the dictionary.
                if let Some(dict_array) = single_value_string_dictionary(
                    &array,
                    &self.output_schema.column_schemas()[output_idx].data_type,
                    value_type.as_ref(),
                ) {
                    let dict_values = dict_array.values();
                    let value = if dict_values.is_null(0) {
                        Value::Null
                    } else {
                        Value::from(datatypes::arrow_array::string_array_value(dict_values, 0))
                    };

                    let repeated = repeated_vector_with_cache(
                        &self.output_schema.column_schemas()[output_idx].data_type,
                        &value,
                        batch.num_rows(),
                        cache_strategy,
                    )?;
                    array = repeated.to_arrow_array();
                } else {
                    let casted = datatypes::arrow::compute::cast(&array, value_type)
                        .context(ArrowComputeSnafu)?;
                    array = casted;
                }
            }
            arrays.push(array);
        }

        let df_record_batch =
            DfRecordBatch::try_new(self.output_schema.arrow_schema().clone(), arrays)
                .context(NewDfRecordBatchSnafu)?;
        Ok(RecordBatch::from_df_record_batch(
            self.output_schema.clone(),
            df_record_batch,
        ))
    }

    /// Projects columns from the input batch and converts them into vectors.
    pub(crate) fn project_vectors(
        &self,
        batch: &datatypes::arrow::record_batch::RecordBatch,
    ) -> common_recordbatch::error::Result<Vec<datatypes::vectors::VectorRef>> {
        let mut columns = Vec::with_capacity(self.output_schema.num_columns());
        for index in &self.batch_indices {
            let mut array = batch.column(*index).clone();
            // Casts dictionary values to the target type.
            if let datatypes::arrow::datatypes::DataType::Dictionary(_key_type, value_type) =
                array.data_type()
            {
                let casted = datatypes::arrow::compute::cast(&array, value_type)
                    .context(ArrowComputeSnafu)?;
                array = casted;
            }
            let vector = Helper::try_into_vector(array)
                .map_err(BoxedError::new)
                .context(ExternalSnafu)?;
            columns.push(vector);
        }
        Ok(columns)
    }
}

fn single_value_string_dictionary<'a>(
    array: &'a Arc<dyn Array>,
    output_type: &ConcreteDataType,
    value_type: &ArrowDataType,
) -> Option<&'a datatypes::arrow::array::DictionaryArray<datatypes::arrow::datatypes::UInt32Type>> {
    if !matches!(
        value_type,
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 | ArrowDataType::Utf8View
    ) || !output_type.is_string()
    {
        return None;
    }

    let dict_array = array
        .as_any()
        .downcast_ref::<datatypes::arrow::array::DictionaryArray<
            datatypes::arrow::datatypes::UInt32Type,
        >>()?;

    (dict_array.values().len() == 1 && dict_array.null_count() == 0).then_some(dict_array)
}

/// Returns ids and datatypes of columns of the output batch after applying the `projection`.
///
/// It adds the time index column if it doesn't present in the projection.
pub(crate) fn flat_projected_columns(
    metadata: &RegionMetadata,
    format_projection: &FormatProjection,
) -> Vec<(ColumnId, ConcreteDataType)> {
    let time_index = metadata.time_index_column();
    let num_columns = if format_projection
        .column_id_to_projected_index
        .contains_key(&time_index.column_id)
    {
        format_projection.column_id_to_projected_index.len()
    } else {
        format_projection.column_id_to_projected_index.len() + 1
    };
    let mut schema = vec![None; num_columns];
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
    if num_columns != format_projection.column_id_to_projected_index.len() {
        schema[num_columns - 1] = Some((
            time_index.column_id,
            time_index.column_schema.data_type.clone(),
        ));
    }

    // Safety: FormatProjection ensures all indices can be unwrapped.
    schema.into_iter().map(|id_type| id_type.unwrap()).collect()
}

/// Computes the Arrow schema for input batches.
///
/// # Panics
/// Panics if it can't find the column by the column id in the batch_schema.
pub(crate) fn compute_input_arrow_schema(
    metadata: &RegionMetadata,
    batch_schema: &[(ColumnId, ConcreteDataType)],
) -> datatypes::arrow::datatypes::SchemaRef {
    let mut new_fields = Vec::with_capacity(batch_schema.len() + 3);
    for (column_id, _) in batch_schema {
        let column_metadata = metadata.column_by_id(*column_id).unwrap();
        let field = Arc::new(Field::new(
            &column_metadata.column_schema.name,
            column_metadata.column_schema.data_type.as_arrow_type(),
            column_metadata.column_schema.is_nullable(),
        ));
        let field = if column_metadata.semantic_type == SemanticType::Tag {
            tag_maybe_to_dictionary_field(&column_metadata.column_schema.data_type, &field)
        } else {
            field
        };
        new_fields.push(field);
    }
    new_fields.extend_from_slice(&internal_fields());

    Arc::new(datatypes::arrow::datatypes::Schema::new(new_fields))
}

/// Helper to project compaction batches into flat format columns
/// (fields + time index + __primary_key + __sequence + __op_type).
pub(crate) struct CompactionProjectionMapper {
    mapper: FlatProjectionMapper,
    assembler: DfBatchAssembler,
}

impl CompactionProjectionMapper {
    pub(crate) fn try_new(metadata: &RegionMetadataRef) -> Result<Self> {
        let projection = metadata
            .column_metadatas
            .iter()
            .enumerate()
            .filter_map(|(idx, col)| {
                if matches!(col.semantic_type, SemanticType::Field) {
                    Some(idx)
                } else {
                    None
                }
            })
            .chain([metadata.time_index_column_pos()])
            .collect::<Vec<_>>();

        let read_col_ids = metadata.column_metadatas.iter().map(|col| col.column_id);
        let mapper =
            FlatProjectionMapper::new_with_read_columns(metadata, projection, read_col_ids)?;
        let assembler = DfBatchAssembler::new(mapper.output_schema());

        Ok(Self { mapper, assembler })
    }

    /// Projects columns and appends internal columns for compaction output.
    ///
    /// The input batch is expected to be in flat format with internal columns appended.
    pub(crate) fn project(&self, batch: DfRecordBatch) -> Result<DfRecordBatch> {
        let columns = self
            .mapper
            .project_vectors(&batch)
            .context(RecordBatchSnafu)?;
        self.assembler
            .build_df_record_batch_with_internal(&batch, columns)
            .context(RecordBatchSnafu)
    }
}

/// Builds [DfRecordBatch] with internal columns appended.
pub(crate) struct DfBatchAssembler {
    output_arrow_schema_with_internal: datatypes::arrow::datatypes::SchemaRef,
}

impl DfBatchAssembler {
    /// Precomputes the output schema with internal columns.
    pub(crate) fn new(output_schema: SchemaRef) -> Self {
        let fields = output_schema
            .arrow_schema()
            .fields()
            .into_iter()
            .chain(internal_fields().iter())
            .cloned()
            .collect::<Vec<_>>();
        let output_arrow_schema_with_internal =
            Arc::new(datatypes::arrow::datatypes::Schema::new(fields));
        Self {
            output_arrow_schema_with_internal,
        }
    }

    /// Builds a [DfRecordBatch] from projected vectors plus internal columns.
    ///
    /// Assumes the input batch already contains internal columns as the last three fields
    /// ("__primary_key", "__sequence", "__op_type").
    pub(crate) fn build_df_record_batch_with_internal(
        &self,
        batch: &datatypes::arrow::record_batch::RecordBatch,
        mut columns: Vec<datatypes::vectors::VectorRef>,
    ) -> common_recordbatch::error::Result<DfRecordBatch> {
        let num_columns = batch.columns().len();
        // The last 3 columns are the internal columns.
        let internal_indices = [num_columns - 3, num_columns - 2, num_columns - 1];
        for index in internal_indices.iter() {
            let array = batch.column(*index).clone();
            let vector = Helper::try_into_vector(array)
                .map_err(BoxedError::new)
                .context(ExternalSnafu)?;
            columns.push(vector);
        }
        RecordBatch::to_df_record_batch(self.output_arrow_schema_with_internal.clone(), columns)
    }
}
