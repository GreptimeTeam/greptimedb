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

mod tables;
use std::sync::Arc;

use arrow_schema::SchemaRef as ArrowSchemaRef;
use common_error::ext::BoxedError;
use common_query::physical_plan::TaskContext;
use common_recordbatch::adapter::RecordBatchStreamAdapter;
use common_recordbatch::{RecordBatch, SendableRecordBatchStream};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter as DfRecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream as DfPartitionStream;
use datafusion::physical_plan::SendableRecordBatchStream as DfSendableRecordBatchStream;
use datatypes::schema::SchemaRef;
use datatypes::vectors::VectorRef;
use snafu::ResultExt;
use store_api::storage::{ScanRequest, TableId};
pub use tables::get_schema_columns;

use crate::error::{CreateRecordBatchSnafu, InternalSnafu, Result};
use crate::information_schema::InformationTable;

/// A memory table with specified schema and columns.
pub(super) struct MemoryTable {
    table_id: TableId,
    table_name: &'static str,
    schema: SchemaRef,
    columns: Vec<VectorRef>,
}

impl MemoryTable {
    /// Creates a memory table with table id, name, schema and columns.
    pub(super) fn new(
        table_id: TableId,
        table_name: &'static str,
        schema: SchemaRef,
        columns: Vec<VectorRef>,
    ) -> Self {
        Self {
            table_id,
            table_name,
            schema,
            columns,
        }
    }

    fn builder(&self) -> MemoryTableBuilder {
        MemoryTableBuilder::new(self.schema.clone(), self.columns.clone())
    }
}

impl InformationTable for MemoryTable {
    fn table_id(&self) -> TableId {
        self.table_id
    }

    fn table_name(&self) -> &'static str {
        self.table_name
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn to_stream(&self, _request: ScanRequest) -> Result<SendableRecordBatchStream> {
        let schema = self.schema.arrow_schema().clone();
        let mut builder = self.builder();
        let stream = Box::pin(DfRecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move {
                builder
                    .memory_records()
                    .await
                    .map(|x| x.into_df_record_batch())
                    .map_err(Into::into)
            }),
        ));
        Ok(Box::pin(
            RecordBatchStreamAdapter::try_new(stream)
                .map_err(BoxedError::new)
                .context(InternalSnafu)?,
        ))
    }
}

struct MemoryTableBuilder {
    schema: SchemaRef,
    columns: Vec<VectorRef>,
}

impl MemoryTableBuilder {
    fn new(schema: SchemaRef, columns: Vec<VectorRef>) -> Self {
        Self { schema, columns }
    }

    /// Construct the `information_schema.{table_name}` virtual table
    async fn memory_records(&mut self) -> Result<RecordBatch> {
        if self.columns.is_empty() {
            RecordBatch::new_empty(self.schema.clone()).context(CreateRecordBatchSnafu)
        } else {
            RecordBatch::new(self.schema.clone(), std::mem::take(&mut self.columns))
                .context(CreateRecordBatchSnafu)
        }
    }
}

impl DfPartitionStream for MemoryTable {
    fn schema(&self) -> &ArrowSchemaRef {
        self.schema.arrow_schema()
    }

    fn execute(&self, _: Arc<TaskContext>) -> DfSendableRecordBatchStream {
        let schema = self.schema.arrow_schema().clone();
        let mut builder = self.builder();
        Box::pin(DfRecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move {
                builder
                    .memory_records()
                    .await
                    .map(|x| x.into_df_record_batch())
                    .map_err(Into::into)
            }),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_recordbatch::RecordBatches;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::StringVector;

    use super::*;

    #[tokio::test]
    async fn test_memory_table() {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new("a", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new("b", ConcreteDataType::string_datatype(), false),
        ]));

        let table = MemoryTable::new(
            42,
            "test",
            schema.clone(),
            vec![
                Arc::new(StringVector::from(vec!["a1", "a2"])),
                Arc::new(StringVector::from(vec!["b1", "b2"])),
            ],
        );

        assert_eq!(42, table.table_id());
        assert_eq!("test", table.table_name());
        assert_eq!(schema, InformationTable::schema(&table));

        let stream = table.to_stream(ScanRequest::default()).unwrap();

        let batches = RecordBatches::try_collect(stream).await.unwrap();

        assert_eq!(
            "\
+----+----+
| a  | b  |
+----+----+
| a1 | b1 |
| a2 | b2 |
+----+----+",
            batches.pretty_print().unwrap()
        );
    }

    #[tokio::test]
    async fn test_empty_memory_table() {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new("a", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new("b", ConcreteDataType::string_datatype(), false),
        ]));

        let table = MemoryTable::new(42, "test", schema.clone(), vec![]);

        assert_eq!(42, table.table_id());
        assert_eq!("test", table.table_name());
        assert_eq!(schema, InformationTable::schema(&table));

        let stream = table.to_stream(ScanRequest::default()).unwrap();

        let batches = RecordBatches::try_collect(stream).await.unwrap();

        assert_eq!(
            "\
+---+---+
| a | b |
+---+---+
+---+---+",
            batches.pretty_print().unwrap()
        );
    }
}
