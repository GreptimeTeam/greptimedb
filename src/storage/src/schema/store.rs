use std::sync::Arc;

use common_error::prelude::*;
use datatypes::arrow::array::Array;
use datatypes::arrow::chunk::Chunk as ArrowChunk;
use datatypes::arrow::datatypes::Schema as ArrowSchema;
use datatypes::schema::{Metadata, Schema, SchemaBuilder, SchemaRef};
use store_api::storage::consts;

use crate::metadata::{self, ColumnMetadata, ColumnsMetadata, Error, Result};
use crate::read::Batch;

const ROW_KEY_END_KEY: &str = "greptime:storage:row_key_end";
const USER_COLUMN_END_KEY: &str = "greptime:storage:user_column_end";

/// Schema that contains storage engine specific metadata, such as internal columns.
///
/// Used internally, contains all row key columns, internal columns and a sub set of
/// value columns in a region. The columns are organized in `key, value, internal` order.
#[derive(Debug, PartialEq)]
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

    pub fn batch_to_arrow_chunk(&self, batch: &Batch) -> ArrowChunk<Arc<dyn Array>> {
        assert_eq!(self.schema.num_columns(), batch.num_columns());

        ArrowChunk::new(batch.columns().iter().map(|v| v.to_arrow_array()).collect())
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
            columns.timestamp_key_index(),
            columns.row_key_end(),
            columns.user_column_end(),
        )
    }

    pub(crate) fn new(
        columns: Vec<ColumnMetadata>,
        version: u32,
        timestamp_key_index: usize,
        row_key_end: usize,
        user_column_end: usize,
    ) -> Result<StoreSchema> {
        let column_schemas = columns
            .iter()
            .map(|meta| meta.to_column_schema())
            .collect::<Result<Vec<_>>>()?;

        let schema = SchemaBuilder::try_from(column_schemas)
            .context(metadata::ConvertSchemaSnafu)?
            .timestamp_index(timestamp_key_index)
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
    pub(crate) fn column_name(&self, idx: usize) -> &str {
        &self.schema.column_schemas()[idx].name
    }

    #[inline]
    pub(crate) fn num_columns(&self) -> usize {
        self.schema.num_columns()
    }

    #[inline]
    pub(crate) fn row_key_end(&self) -> usize {
        self.row_key_end
    }

    #[inline]
    pub(crate) fn user_column_end(&self) -> usize {
        self.user_column_end
    }

    #[inline]
    pub(crate) fn value_columns(&self) -> &[ColumnMetadata] {
        &self.columns[self.row_key_end..self.user_column_end]
    }

    /// Returns the index of the value column according its `offset`.
    #[inline]
    pub(crate) fn value_column_index_by_offset(&self, offset: usize) -> usize {
        self.row_key_end + offset
    }

    #[inline]
    pub(crate) fn columns(&self) -> &[ColumnMetadata] {
        &self.columns
    }
}

impl TryFrom<ArrowSchema> for StoreSchema {
    type Error = Error;

    fn try_from(arrow_schema: ArrowSchema) -> Result<StoreSchema> {
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

fn parse_index_from_metadata(metadata: &Metadata, key: &str) -> Result<usize> {
    let value = metadata
        .get(key)
        .context(metadata::MetaNotFoundSnafu { key })?;
    value.parse().with_context(|_| metadata::ParseMetaIntSnafu {
        key_value: format!("{}={}", key, value),
    })
}

#[cfg(test)]
mod tests {
    use datatypes::arrow::array::Array;
    use datatypes::arrow::chunk::Chunk as ArrowChunk;

    use super::*;
    use crate::read::Batch;
    use crate::schema::tests;

    fn check_chunk_batch(chunk: &ArrowChunk<Arc<dyn Array>>, batch: &Batch) {
        assert_eq!(5, chunk.columns().len());
        assert_eq!(3, chunk.len());

        for i in 0..5 {
            assert_eq!(chunk[i], batch.column(i).to_arrow_array());
        }
    }

    #[test]
    fn test_store_schema() {
        let region_schema = Arc::new(tests::new_region_schema(123, 1));

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
            .timestamp_index(1)
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

        // Test batch and chunk conversion.
        let batch = tests::new_batch();
        // Convert batch to chunk.
        let chunk = store_schema.batch_to_arrow_chunk(&batch);
        check_chunk_batch(&chunk, &batch);
    }
}
