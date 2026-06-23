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

use std::collections::HashMap;
use std::sync::Arc;

use datatypes::arrow::datatypes::{DataType as ArrowDataType, Schema, SchemaRef};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::data_type::DataType;
use datatypes::extension::json::is_json_extension_type;
use datatypes::schema::ext::ArrowSchemaExt;
use datatypes::types::JsonType;
use datatypes::vectors::json::array::JsonArray;
use snafu::{OptionExt, ResultExt};

use crate::error::{
    ConvertValueSnafu, DataTypeMismatchSnafu, NewRecordBatchSnafu, Result, UnexpectedSnafu,
};
use crate::memtable::BoxedRecordBatchIterator;

/// Aligns concrete JSON2 Arrow types across record batches.
///
/// JSON2 column concrete Arrow types are derived from data. Different memtable
/// parts may therefore have different concrete types for the same JSON2 column.
/// This helper merges those concrete types and aligns batches to the merged schema.
#[derive(Clone)]
pub(crate) struct Json2Aligner {
    /// Schema after merging all JSON2 column concrete types.
    schema: SchemaRef,
    /// JSON2 columns that may need per-batch alignment.
    json_columns: Vec<(usize, ArrowDataType)>,
}

impl Json2Aligner {
    /// Builds an aligner from input schemas.
    ///
    /// Note: except for JSON2 columns, all input schemas must be identical.
    pub(crate) fn try_new<I>(input_schemas: I) -> Result<Self>
    where
        I: IntoIterator<Item = SchemaRef>,
    {
        let mut input_schemas = input_schemas.into_iter();

        // Use first schema as base: it defines column order and non-JSON types.
        let base_schema = input_schemas.next().context(UnexpectedSnafu {
            reason: "Json2Aligner requires at least one input schema",
        })?;

        // No JSON2 columns, no alignment needed.
        if !base_schema.has_json_extension_field() {
            return Ok(Self {
                schema: base_schema,
                json_columns: Vec::new(),
            });
        }

        // Init merged types from base schema.
        let mut merged_types: HashMap<usize, JsonType> = base_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|&(_idx, field)| is_json_extension_type(field))
            .map(|(idx, field)| (idx, JsonType::from(field.data_type())))
            .collect();

        // Merge JSON2 types from remaining schemas.
        for schema in input_schemas {
            for (idx, merged) in &mut merged_types {
                if *idx >= schema.fields().len() {
                    continue;
                }
                merged
                    .merge(&JsonType::from(schema.field(*idx).data_type()))
                    .context(DataTypeMismatchSnafu)?;
            }
        }

        // Build output schema with merged JSON2 types.
        let mut json_columns = Vec::with_capacity(merged_types.len());
        let fields: Vec<_> = base_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, field)| {
                if let Some(merged) = merged_types.get(&idx) {
                    let data_type = merged.as_arrow_type();
                    json_columns.push((idx, data_type.clone()));
                    let mut field = (**field).clone();
                    field.set_data_type(data_type);
                    Arc::new(field)
                } else {
                    field.clone()
                }
            })
            .collect();

        let schema = Arc::new(Schema::new_with_metadata(
            fields,
            base_schema.metadata().clone(),
        ));

        Ok(Self {
            schema,
            json_columns,
        })
    }

    /// Returns the aligned output schema.
    pub(crate) fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Aligns a [`RecordBatch`] to [`Self::schema`].
    pub(crate) fn align_batch(&self, batch: RecordBatch) -> Result<RecordBatch> {
        if self.json_columns.is_empty() {
            return Ok(batch);
        }
        let mut cols = batch.columns().to_vec();
        for (idx, expected_type) in &self.json_columns {
            if batch.schema_ref().field(*idx).data_type() != expected_type {
                cols[*idx] = JsonArray::from(batch.column(*idx))
                    .try_align(expected_type)
                    .context(ConvertValueSnafu)?;
            }
        }
        RecordBatch::try_new(self.schema.clone(), cols).context(NewRecordBatchSnafu)
    }

    /// Aligns [`RecordBatch`]s to [`Self::schema`].
    pub(crate) fn align_batches<I>(&self, batches: I) -> Result<Vec<RecordBatch>>
    where
        I: IntoIterator<Item = RecordBatch>,
    {
        batches
            .into_iter()
            .map(|batch| self.align_batch(batch))
            .collect()
    }

    /// Wraps an iterator so each yielded [`RecordBatch`] is lazily aligned.
    pub(crate) fn wrap_iter(&self, iter: BoxedRecordBatchIterator) -> BoxedRecordBatchIterator {
        let aligner = self.clone();
        Box::new(iter.map(move |batch| aligner.align_batch(batch?)))
    }
}
