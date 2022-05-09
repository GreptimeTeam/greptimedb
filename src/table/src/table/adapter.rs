use core::pin::Pin;
use core::task::{Context, Poll};
use std::any::Any;
use std::fmt;
use std::fmt::Debug;
use std::mem;
use std::sync::{Arc, Mutex};

use arrow::error::{ArrowError, Result as ArrowResult};
use common_query::logical_plan::Expr;
use common_recordbatch::error::{self as recordbatch_error, Result as RecordBatchResult};
use common_recordbatch::{RecordBatch, RecordBatchStream, SendableRecordBatchStream};
use datafusion::arrow::datatypes::SchemaRef as DfSchemaRef;
///  Datafusion table adpaters
use datafusion::datasource::{
    datasource::TableProviderFilterPushDown as DfTableProviderFilterPushDown, TableProvider,
    TableType as DfTableType,
};
use datafusion::error::Result as DfResult;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_plan::Expr as DfExpr;
use datafusion::physical_plan::{
    expressions::PhysicalSortExpr, ExecutionPlan, Partitioning,
    RecordBatchStream as DfRecordBatchStream,
    SendableRecordBatchStream as DfSendableRecordBatchStream, Statistics,
};
use datafusion_common::record_batch::RecordBatch as DfRecordBatch;
use datatypes::schema::SchemaRef as TableSchemaRef;
use datatypes::schema::{Schema, SchemaRef};
use futures::Stream;
use snafu::prelude::*;

use crate::error::{self, Result};
use crate::table::{Table, TableProviderFilterPushDown, TableRef, TableType};

/// Greptime SendableRecordBatchStream -> datafusion ExecutionPlan.
struct ExecutionPlanAdapter {
    stream: Mutex<Option<SendableRecordBatchStream>>,
    schema: SchemaRef,
}

impl Debug for ExecutionPlanAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        //TODO(dennis) better debug info
        write!(f, "ExecutionPlan(PlaceHolder)")
    }
}

#[async_trait::async_trait]
impl ExecutionPlan for ExecutionPlanAdapter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> DfSchemaRef {
        self.schema.arrow_schema().clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        // FIXME(dennis)
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        // FIXME(dennis)
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        // TODO(dennis)
        vec![]
    }

    fn with_new_children(
        &self,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        // TODO(dennis)
        todo!();
    }

    async fn execute(
        &self,
        _partition: usize,
        _runtime: Arc<RuntimeEnv>,
    ) -> DfResult<DfSendableRecordBatchStream> {
        let mut stream = self.stream.lock().unwrap();

        if stream.is_some() {
            let stream = mem::replace(&mut *stream, None);
            Ok(Box::pin(DfRecordBatchStreamAdapter::new(stream.unwrap())))
        } else {
            error::ExecuteRepeatedlySnafu.fail()?
        }
    }

    fn statistics(&self) -> Statistics {
        //TODO(dennis)
        Statistics::default()
    }
}

/// Greptime Table ->  datafusion TableProvider
pub struct DfTableProviderAdapter {
    table: TableRef,
}

impl DfTableProviderAdapter {
    pub fn new(table: TableRef) -> Self {
        Self { table }
    }
}

#[async_trait::async_trait]
impl TableProvider for DfTableProviderAdapter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> DfSchemaRef {
        self.table.schema().arrow_schema().clone()
    }

    fn table_type(&self) -> DfTableType {
        match self.table.table_type() {
            TableType::Base => DfTableType::Base,
            TableType::View => DfTableType::View,
            TableType::Temporary => DfTableType::Temporary,
        }
    }

    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        filters: &[DfExpr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let filters: Vec<Expr> = filters.iter().map(Clone::clone).map(Expr::new).collect();

        let stream = self.table.scan(projection, &filters, limit).await?;
        Ok(Arc::new(ExecutionPlanAdapter {
            schema: stream.schema(),
            stream: Mutex::new(Some(stream)),
        }))
    }

    fn supports_filter_pushdown(&self, filter: &DfExpr) -> DfResult<DfTableProviderFilterPushDown> {
        let p = self
            .table
            .supports_filter_pushdown(&Expr::new(filter.clone()))?;
        match p {
            TableProviderFilterPushDown::Unsupported => {
                Ok(DfTableProviderFilterPushDown::Unsupported)
            }
            TableProviderFilterPushDown::Inexact => Ok(DfTableProviderFilterPushDown::Inexact),
            TableProviderFilterPushDown::Exact => Ok(DfTableProviderFilterPushDown::Exact),
        }
    }
}

/// Datafusion TableProvider ->  greptime Table
pub struct TableAdapter {
    table_provider: Arc<dyn TableProvider>,
    runtime: Arc<RuntimeEnv>,
}

impl TableAdapter {
    pub fn new(table_provider: Arc<dyn TableProvider>, runtime: Arc<RuntimeEnv>) -> Self {
        Self {
            table_provider,
            runtime,
        }
    }
}

#[async_trait::async_trait]
impl Table for TableAdapter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> TableSchemaRef {
        Arc::new(self.table_provider.schema().into())
    }

    fn table_type(&self) -> TableType {
        match self.table_provider.table_type() {
            DfTableType::Base => TableType::Base,
            DfTableType::View => TableType::View,
            DfTableType::Temporary => TableType::Temporary,
        }
    }

    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<SendableRecordBatchStream> {
        let filters: Vec<DfExpr> = filters.iter().map(|e| e.df_expr().clone()).collect();

        let execution_plan = self
            .table_provider
            .scan(projection, &filters, limit)
            .await
            .context(error::DatafusionSnafu)?;

        // FIXME(dennis) Partitioning
        let df_stream = execution_plan
            .execute(0, self.runtime.clone())
            .await
            .context(error::DatafusionSnafu)?;

        Ok(Box::pin(RecordBatchStreamAdapter::new(df_stream)))
    }

    fn supports_filter_pushdown(&self, filter: &Expr) -> Result<TableProviderFilterPushDown> {
        match self
            .table_provider
            .supports_filter_pushdown(filter.df_expr())
            .context(error::DatafusionSnafu)?
        {
            DfTableProviderFilterPushDown::Unsupported => {
                Ok(TableProviderFilterPushDown::Unsupported)
            }
            DfTableProviderFilterPushDown::Inexact => Ok(TableProviderFilterPushDown::Inexact),
            DfTableProviderFilterPushDown::Exact => Ok(TableProviderFilterPushDown::Exact),
        }
    }
}

/// Greptime SendableRecordBatchStream -> datafusion RecordBatchStream
pub struct DfRecordBatchStreamAdapter {
    stream: SendableRecordBatchStream,
}

impl DfRecordBatchStreamAdapter {
    pub fn new(stream: SendableRecordBatchStream) -> Self {
        Self { stream }
    }
}

impl DfRecordBatchStream for DfRecordBatchStreamAdapter {
    fn schema(&self) -> DfSchemaRef {
        self.stream.schema().arrow_schema().clone()
    }
}

impl Stream for DfRecordBatchStreamAdapter {
    type Item = ArrowResult<DfRecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.stream).poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(recordbatch)) => match recordbatch {
                Ok(recordbatch) => Poll::Ready(Some(Ok(recordbatch.df_recordbatch))),
                Err(e) => Poll::Ready(Some(Err(ArrowError::External("".to_owned(), Box::new(e))))),
            },
            Poll::Ready(None) => Poll::Ready(None),
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

/// Datafusion SendableRecordBatchStream to greptime RecordBatchStream
pub struct RecordBatchStreamAdapter {
    stream: DfSendableRecordBatchStream,
}

impl RecordBatchStreamAdapter {
    pub fn new(stream: DfSendableRecordBatchStream) -> Self {
        Self { stream }
    }
}

impl RecordBatchStream for RecordBatchStreamAdapter {
    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(self.stream.schema()))
    }
}

impl Stream for RecordBatchStreamAdapter {
    type Item = RecordBatchResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.stream).poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(df_recordbatch)) => Poll::Ready(Some(Ok(RecordBatch {
                schema: self.schema(),
                df_recordbatch: df_recordbatch.context(recordbatch_error::ArrowSnafu)?,
            }))),
            Poll::Ready(None) => Poll::Ready(None),
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}
