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

use std::pin::Pin;
use std::sync::Arc;

use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_error::ext::BoxedError;
use common_recordbatch::adapter::RecordBatchMetrics;
use common_recordbatch::error::Result as RecordBatchResult;
use common_recordbatch::{OrderOption, RecordBatch, RecordBatchStream, SendableRecordBatchStream};
use datafusion::arrow::record_batch::RecordBatch as DfRecordBatch;
use datatypes::arrow::array::UInt32Array;
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::{ColumnSchema, SchemaBuilder, SchemaRef};
use futures::Stream;
use futures::task::{Context, Poll};
use store_api::data_source::DataSource;
use store_api::storage::ScanRequest;

use crate::metadata::{
    FilterPushDownType, TableId, TableInfoBuilder, TableInfoRef, TableMeta, TableType,
};
use crate::{Table, TableRef};

const NUMBER_COLUMN: &str = "number";

pub const NUMBERS_TABLE_NAME: &str = "numbers";

/// numbers table for test
#[derive(Debug, Clone)]
pub struct NumbersTable;

impl NumbersTable {
    pub fn table(table_id: TableId) -> TableRef {
        Self::table_with_name(table_id, NUMBERS_TABLE_NAME.to_string())
    }

    pub fn table_with_name(table_id: TableId, name: String) -> TableRef {
        let data_source = Arc::new(NumbersDataSource::new(Self::schema()));
        let table = Table::new(
            Self::table_info(table_id, name, "test_engine".to_string()),
            FilterPushDownType::Unsupported,
            data_source,
        );
        Arc::new(table)
    }

    pub fn schema() -> SchemaRef {
        let column_schemas = vec![ColumnSchema::new(
            NUMBER_COLUMN,
            ConcreteDataType::uint32_datatype(),
            false,
        )];
        let schema = SchemaBuilder::try_from_columns(column_schemas)
            .unwrap()
            .build()
            .unwrap();
        Arc::new(schema)
    }

    pub fn table_info(table_id: TableId, name: String, engine: String) -> TableInfoRef {
        let table_meta = TableMeta {
            schema: Self::schema(),
            primary_key_indices: vec![0],
            value_indices: vec![],
            engine,
            region_numbers: vec![0],
            next_column_id: 1,
            options: Default::default(),
            created_on: Default::default(),
            updated_on: Default::default(),
            partition_key_indices: vec![],
            column_ids: vec![],
        };

        let table_info = TableInfoBuilder::default()
            .table_id(table_id)
            .name(name)
            .catalog_name(DEFAULT_CATALOG_NAME)
            .schema_name(DEFAULT_SCHEMA_NAME)
            .table_version(0)
            .table_type(TableType::Temporary)
            .meta(table_meta)
            .build()
            .unwrap();
        Arc::new(table_info)
    }
}

struct NumbersDataSource {
    schema: SchemaRef,
}

impl NumbersDataSource {
    pub fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}

impl DataSource for NumbersDataSource {
    fn get_stream(&self, request: ScanRequest) -> Result<SendableRecordBatchStream, BoxedError> {
        let projected_schema = match &request.projection {
            Some(projection) => Arc::new(self.schema.try_project(projection).unwrap()),
            None => self.schema.clone(),
        };
        Ok(Box::pin(NumbersStream {
            limit: request.limit.unwrap_or(100) as u32,
            schema: self.schema.clone(),
            already_run: false,
            projection: request.projection,
            projected_schema,
        }))
    }
}

// Limited numbers stream
struct NumbersStream {
    limit: u32,
    schema: SchemaRef,
    already_run: bool,
    projection: Option<Vec<usize>>,
    projected_schema: SchemaRef,
}

impl RecordBatchStream for NumbersStream {
    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn output_ordering(&self) -> Option<&[OrderOption]> {
        None
    }

    fn metrics(&self) -> Option<RecordBatchMetrics> {
        None
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
        let mut batch = DfRecordBatch::try_new(
            self.schema.arrow_schema().clone(),
            vec![Arc::new(UInt32Array::from(numbers))],
        )
        .unwrap();
        if let Some(projection) = &self.projection {
            batch = batch.project(projection).unwrap();
        }

        Poll::Ready(Some(Ok(RecordBatch::from_df_record_batch(
            self.projected_schema.clone(),
            batch,
        ))))
    }
}
