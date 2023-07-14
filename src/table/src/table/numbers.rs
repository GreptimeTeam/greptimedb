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

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;

use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, NUMBERS_TABLE_ID};
use common_recordbatch::error::Result as RecordBatchResult;
use common_recordbatch::{RecordBatch, RecordBatchStream, SendableRecordBatchStream};
use datafusion::arrow::record_batch::RecordBatch as DfRecordBatch;
use datatypes::arrow::array::UInt32Array;
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::{ColumnSchema, SchemaBuilder, SchemaRef};
use futures::task::{Context, Poll};
use futures::Stream;
use store_api::storage::{RegionNumber, ScanRequest};

use crate::error::Result;
use crate::metadata::{TableId, TableInfoBuilder, TableInfoRef, TableMetaBuilder, TableType};
use crate::table::Table;

const NUMBER_COLUMN: &str = "number";

pub const NUMBERS_TABLE_NAME: &str = "numbers";

/// numbers table for test
#[derive(Debug, Clone)]
pub struct NumbersTable {
    table_id: TableId,
    schema: SchemaRef,
    name: String,
    engine: String,
}

impl NumbersTable {
    pub fn new(table_id: TableId) -> Self {
        NumbersTable::with_name(table_id, NUMBERS_TABLE_NAME.to_string())
    }

    pub fn with_name(table_id: TableId, name: String) -> Self {
        let column_schemas = vec![ColumnSchema::new(
            NUMBER_COLUMN,
            ConcreteDataType::uint32_datatype(),
            false,
        )];
        Self {
            table_id,
            name,
            engine: "test_engine".to_string(),
            schema: Arc::new(
                SchemaBuilder::try_from_columns(column_schemas)
                    .unwrap()
                    .build()
                    .unwrap(),
            ),
        }
    }
}

impl Default for NumbersTable {
    fn default() -> Self {
        NumbersTable::new(NUMBERS_TABLE_ID)
    }
}

#[async_trait::async_trait]
impl Table for NumbersTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_info(&self) -> TableInfoRef {
        Arc::new(
            TableInfoBuilder::default()
                .table_id(self.table_id)
                .name(&self.name)
                .catalog_name(DEFAULT_CATALOG_NAME)
                .schema_name(DEFAULT_SCHEMA_NAME)
                .table_version(0)
                .table_type(TableType::Base)
                .meta(
                    TableMetaBuilder::default()
                        .schema(self.schema.clone())
                        .region_numbers(vec![0])
                        .primary_key_indices(vec![0])
                        .next_column_id(1)
                        .engine(&self.engine)
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
    }

    async fn scan_to_stream(&self, request: ScanRequest) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(NumbersStream {
            limit: request.limit.unwrap_or(100) as u32,
            schema: self.schema.clone(),
            already_run: false,
        }))
    }

    async fn flush(&self, _region_number: Option<RegionNumber>, _wait: Option<bool>) -> Result<()> {
        Ok(())
    }
}

// Limited numbers stream
struct NumbersStream {
    limit: u32,
    schema: SchemaRef,
    already_run: bool,
}

impl RecordBatchStream for NumbersStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for NumbersStream {
    type Item = RecordBatchResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.already_run {
            return Poll::Ready(None);
        }
        self.already_run = true;
        let numbers: Vec<u32> = (0..self.limit).collect();
        let batch = DfRecordBatch::try_new(
            self.schema.arrow_schema().clone(),
            vec![Arc::new(UInt32Array::from(numbers))],
        )
        .unwrap();

        Poll::Ready(Some(RecordBatch::try_from_df_record_batch(
            self.schema.clone(),
            batch,
        )))
    }
}
