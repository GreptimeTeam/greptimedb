use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;

use common_query::physical_plan::PhysicalPlanRef;
use common_recordbatch::error::Result as RecordBatchResult;
use common_recordbatch::{RecordBatch, RecordBatchStream};
use datafusion_common::record_batch::RecordBatch as DfRecordBatch;
use datatypes::arrow::array::UInt32Array;
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::{ColumnSchema, SchemaBuilder, SchemaRef};
use futures::task::{Context, Poll};
use futures::Stream;

use crate::error::Result;
use crate::metadata::{TableInfoBuilder, TableInfoRef, TableMetaBuilder, TableType};
use crate::table::scan::SimpleTableScan;
use crate::table::{Expr, Table};

/// numbers table for test
#[derive(Debug, Clone)]
pub struct NumbersTable {
    schema: SchemaRef,
}

impl Default for NumbersTable {
    fn default() -> Self {
        let column_schemas = vec![ColumnSchema::new(
            "number",
            ConcreteDataType::uint32_datatype(),
            false,
        )];
        Self {
            schema: Arc::new(
                SchemaBuilder::try_from_columns(column_schemas)
                    .unwrap()
                    .build()
                    .unwrap(),
            ),
        }
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
                .table_id(1)
                .name("numbers")
                .catalog_name("greptime")
                .schema_name("public")
                .table_version(0)
                .table_type(TableType::Base)
                .meta(
                    TableMetaBuilder::default()
                        .schema(self.schema.clone())
                        .region_numbers(vec![0])
                        .primary_key_indices(vec![0])
                        .next_column_id(1)
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
    }

    async fn scan(
        &self,
        _projection: &Option<Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<PhysicalPlanRef> {
        let stream = Box::pin(NumbersStream {
            limit: limit.unwrap_or(100) as u32,
            schema: self.schema.clone(),
            already_run: false,
        });
        Ok(Arc::new(SimpleTableScan::new(stream)))
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
            vec![Arc::new(UInt32Array::from_slice(&numbers))],
        )
        .unwrap();

        Poll::Ready(Some(Ok(RecordBatch {
            schema: self.schema.clone(),
            df_recordbatch: batch,
        })))
    }
}
