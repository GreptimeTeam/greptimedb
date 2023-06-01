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

use common_error::prelude::*;
use datatypes::arrow::datatypes::Schema as ArrowSchema;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::schema::{Schema, SchemaBuilder, SchemaRef};
use store_api::storage::consts;

use crate::error::NewRecordBatchSnafu;
use crate::metadata::{self, ColumnMetadata, ColumnsMetadata, Error, Result};
use crate::read::Batch;

const ROW_KEY_END_KEY: &str = "greptime:storage:row_key_end";
const USER_COLUMN_END_KEY: &str = "greptime:storage:user_column_end";

/// Schema that contains storage engine specific metadata, such as internal columns.
///
/// Used internally, contains all row key columns, internal columns and a sub set of
/// value columns in a region. The columns are organized in `key, value, internal` order.
#[derive(Debug, PartialEq, Eq)]
pub struct StoreSchema {
    columns: Vec<ColumnMetadata>,
    schema: SchemaRef,
    row_key_end: usize,
    user_column_end: usize,
}

pub type StoreSchemaRef = Arc<StoreSchema>;

impl StoreSchema {
    #[inline]
    pub fn version(&self) -> u32 {
        self.schema.version()
    }

    #[inline]
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    #[inline]
    pub fn arrow_schema(&self) -> &Arc<ArrowSchema> {
        self.schema.arrow_schema()
    }

    // TODO(yingwen): Remove this method.
    pub fn batch_to_arrow_record_batch(
        &self,
        batch: &Batch,
    ) -> std::result::Result<RecordBatch, crate::error::Error> {
        assert_eq!(self.schema.num_columns(), batch.num_columns(),);
        RecordBatch::try_new(
            self.schema.arrow_schema().clone(),
            batch.columns().iter().map(|v| v.to_arrow_array()).collect(),
        )
        .context(NewRecordBatchSnafu)
    }

    /// Returns the ending index of row key columns.
    ///
    /// The ending index has the same value as the number of the row key columns.
    #[inline]
    pub fn row_key_end(&self) -> usize {
        self.row_key_end
    }

    /// Returns the index of timestamp column.
    /// We always assume that timestamp is the last column in [StoreSchema].
    #[inline]
    pub fn timestamp_index(&self) -> usize {
        self.row_key_end - 1
    }

    pub(crate) fn contains_column(&self, name: &str) -> bool {
        self.schema.column_schema_by_name(name).is_some()
    }

    pub(crate) fn is_key_column(&self, name: &str) -> bool {
        self.schema
            .column_index_by_name(name)
            .map(|idx| idx < self.row_key_end)
            .unwrap_or(false)
    }

    pub(crate) fn is_user_column(&self, name: &str) -> bool {
        self.schema
            .column_index_by_name(name)
            .map(|idx| idx < self.user_column_end)
            .unwrap_or(false)
    }

    pub(crate) fn from_columns_metadata(
        columns: &ColumnsMetadata,
        version: u32,
    ) -> Result<StoreSchema> {
        StoreSchema::new(
            columns.columns().to_vec(),
            version,
            columns.row_key_end(),
            columns.user_column_end(),
        )
    }

    pub(crate) fn new(
        columns: Vec<ColumnMetadata>,
        version: u32,
        row_key_end: usize,
        user_column_end: usize,
    ) -> Result<StoreSchema> {
        let column_schemas = columns
            .iter()
            .map(|meta| meta.to_column_schema())
            .collect::<Result<Vec<_>>>()?;

        let schema = SchemaBuilder::try_from(column_schemas)
            .context(metadata::ConvertSchemaSnafu)?
            .version(version)
            .add_metadata(ROW_KEY_END_KEY, row_key_end.to_string())
            .add_metadata(USER_COLUMN_END_KEY, user_column_end.to_string())
            .build()
            .context(metadata::InvalidSchemaSnafu)?;

        assert_eq!(
            consts::SEQUENCE_COLUMN_NAME,
            schema.column_schemas()[user_column_end].name
        );
        assert_eq!(
            consts::OP_TYPE_COLUMN_NAME,
            schema.column_schemas()[user_column_end + 1].name
        );

        Ok(StoreSchema {
            columns,
            schema: Arc::new(schema),
            row_key_end,
            user_column_end,
        })
    }

    #[inline]
    pub(crate) fn sequence_index(&self) -> usize {
        self.user_column_end
    }

    #[inline]
    pub(crate) fn op_type_index(&self) -> usize {
        self.user_column_end + 1
    }

    #[inline]
    pub(crate) fn row_key_indices(&self) -> impl Iterator<Item = usize> {
        0..self.row_key_end
    }

    #[inline]
    pub(crate) fn value_indices(&self) -> impl Iterator<Item = usize> {
        self.row_key_end..self.user_column_end
    }

    #[inline]
    pub(crate) fn column_name(&self, idx: usize) -> &str {
        &self.schema.column_schemas()[idx].name
    }

    /// # Panic
    /// Panics if `name` is not a valid column name.
    #[inline]
    pub(crate) fn column_index(&self, name: &str) -> usize {
        self.schema.column_index_by_name(name).unwrap()
    }

    #[inline]
    pub(crate) fn num_columns(&self) -> usize {
        self.schema.num_columns()
    }

    #[inline]
    pub(crate) fn user_column_end(&self) -> usize {
        self.user_column_end
    }

    #[inline]
    pub(crate) fn field_columns(&self) -> &[ColumnMetadata] {
        &self.columns[self.row_key_end..self.user_column_end]
    }

    /// Returns the index of the value column according its `offset`.
    #[inline]
    pub(crate) fn field_column_index_by_offset(&self, offset: usize) -> usize {
        self.row_key_end + offset
    }

    #[inline]
    pub(crate) fn columns(&self) -> &[ColumnMetadata] {
        &self.columns
    }
}

impl TryFrom<Arc<ArrowSchema>> for StoreSchema {
    type Error = Error;

    fn try_from(arrow_schema: Arc<ArrowSchema>) -> std::result::Result<Self, Self::Error> {
        let schema = Schema::try_from(arrow_schema).context(metadata::ConvertArrowSchemaSnafu)?;
        // Recover other metadata from schema.
        let row_key_end = parse_index_from_metadata(schema.metadata(), ROW_KEY_END_KEY)?;
        let user_column_end = parse_index_from_metadata(schema.metadata(), USER_COLUMN_END_KEY)?;

        // There should be sequence and op_type columns.
        ensure!(
            consts::SEQUENCE_COLUMN_NAME == schema.column_schemas()[user_column_end].name,
            metadata::InvalidIndexSnafu
        );
        ensure!(
            consts::OP_TYPE_COLUMN_NAME == schema.column_schemas()[user_column_end + 1].name,
            metadata::InvalidIndexSnafu
        );

        // Recover ColumnMetadata from schema.
        let columns = schema
            .column_schemas()
            .iter()
            .map(ColumnMetadata::from_column_schema)
            .collect::<Result<_>>()?;

        Ok(StoreSchema {
            columns,
            schema: Arc::new(schema),
            row_key_end,
            user_column_end,
        })
    }
}

impl TryFrom<ArrowSchema> for StoreSchema {
    type Error = Error;

    fn try_from(arrow_schema: ArrowSchema) -> std::result::Result<StoreSchema, Self::Error> {
        StoreSchema::try_from(Arc::new(arrow_schema))
    }
}

fn parse_index_from_metadata(metadata: &HashMap<String, String>, key: &str) -> Result<usize> {
    let value = metadata
        .get(key)
        .context(metadata::MetaNotFoundSnafu { key })?;
    value.parse().with_context(|_| metadata::ParseMetaIntSnafu {
        key_value: format!("{key}={value}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::read::Batch;
    use crate::schema::tests;
    use crate::test_util::schema_util;

    fn check_chunk_batch(record_batch: &RecordBatch, batch: &Batch) {
        assert_eq!(5, record_batch.num_columns());
        assert_eq!(3, record_batch.num_rows());

        for i in 0..5 {
            assert_eq!(record_batch.column(i), &batch.column(i).to_arrow_array());
        }
    }

    #[test]
    fn test_store_schema() {
        let region_schema = Arc::new(schema_util::new_region_schema(123, 1));

        // Checks StoreSchema.
        let store_schema = region_schema.store_schema();
        assert_eq!(123, store_schema.version());
        let sst_arrow_schema = store_schema.arrow_schema();
        let converted_store_schema = StoreSchema::try_from((**sst_arrow_schema).clone()).unwrap();

        assert_eq!(**store_schema, converted_store_schema);

        let column_schemas: Vec<_> = region_schema
            .columns()
            .iter()
            .map(|meta| meta.to_column_schema().unwrap())
            .collect();
        let expect_schema = SchemaBuilder::try_from(column_schemas)
            .unwrap()
            .version(123)
            .build()
            .unwrap();
        // Only compare column schemas since SchemaRef in StoreSchema also contains other metadata that only used
        // by StoreSchema.
        assert_eq!(
            expect_schema.column_schemas(),
            store_schema.schema().column_schemas(),
        );
        assert_eq!(3, store_schema.sequence_index());
        assert_eq!(4, store_schema.op_type_index());
        let row_key_indices: Vec<_> = store_schema.row_key_indices().collect();
        assert_eq!([0, 1], &row_key_indices[..]);
        let value_indices: Vec<_> = store_schema.value_indices().collect();
        assert_eq!([2], &value_indices[..]);

        // Test batch and chunk conversion.
        let batch = tests::new_batch();
        // Convert batch to chunk.
        let chunk = store_schema.batch_to_arrow_record_batch(&batch).unwrap();
        check_chunk_batch(&chunk, &batch);
    }
}
