use core::fmt::Formatter;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::any::Any;
use std::fmt::Debug;
use std::mem;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use common_query::logical_plan::Expr;
use common_recordbatch::error::Result as RecordBatchResult;
use common_recordbatch::{RecordBatch, RecordBatchStream, SendableRecordBatchStream};
use common_telemetry::debug;
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
use datafusion_common::field_util::SchemaExt;
use datafusion_common::record_batch::RecordBatch as DfRecordBatch;
use datatypes::arrow::error::{ArrowError, Result as ArrowResult};
use datatypes::schema::{Schema, SchemaRef as TableSchemaRef};
use datatypes::schema::{SchemaRef, TIMESTAMP_COLUMN_KEY};
use futures::Stream;
use snafu::prelude::*;

use crate::error::{self, Result};
use crate::metadata::TableInfoRef;
use crate::table::{FilterPushDownType, Table, TableRef, TableType};

/// Greptime SendableRecordBatchStream -> datafusion ExecutionPlan.
struct ExecutionPlanAdapter {
    stream: Mutex<Option<SendableRecordBatchStream>>,
    schema: SchemaRef,
}

impl Debug for ExecutionPlanAdapter {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ExecutionPlanAdapter")
            .field("schema", &self.schema)
            .finish()
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

    pub fn table(&self) -> TableRef {
        self.table.clone()
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
        let filters: Vec<Expr> = filters.iter().map(Clone::clone).map(Into::into).collect();

        let table_schema = self.table.schema();
        let df_schema = project_arrow_schema(table_schema, projection)?;

        let partitioning = self.table.partitioning_hint(&filters);

        Ok(Arc::new(TableScanPlan {
            table: self.table.clone(),
            df_schema,
            partitioning,
            projection: projection.clone(),
            filters,
            limit,
        }))
    }

    fn supports_filter_pushdown(&self, filter: &DfExpr) -> DfResult<DfTableProviderFilterPushDown> {
        let p = self
            .table
            .supports_filter_pushdown(&filter.clone().into())?;
        match p {
            FilterPushDownType::Unsupported => Ok(DfTableProviderFilterPushDown::Unsupported),
            FilterPushDownType::Inexact => Ok(DfTableProviderFilterPushDown::Inexact),
            FilterPushDownType::Exact => Ok(DfTableProviderFilterPushDown::Exact),
        }
    }
}

fn project_arrow_schema(
    table_schema: SchemaRef,
    projection: &Option<Vec<usize>>,
) -> DfResult<DfSchemaRef> {
    let df_schema = table_schema.arrow_schema();
    let df_schema = if let Some(projection) = &projection {
        let mut df_schema = df_schema.project(projection)?;
        if let Some(timestamp_index) = table_schema.timestamp_index() {
            if !projection.contains(&timestamp_index) {
                df_schema.metadata.remove(TIMESTAMP_COLUMN_KEY);
            }
        }
        Arc::new(df_schema)
    } else {
        df_schema.clone()
    };
    Ok(df_schema)
}

struct TableScanPlan {
    table: TableRef,
    df_schema: DfSchemaRef,
    partitioning: Partitioning,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    limit: Option<usize>,
}

impl Debug for TableScanPlan {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableScanPlan")
            .field("table", &self.table.table_info().name)
            .field("df_schema", &self.df_schema)
            .field("partitioning", &self.partitioning)
            .field("projection", &self.projection)
            .field("filters", &self.filters)
            .field("limit", &self.limit)
            .finish()
    }
}

#[async_trait]
impl ExecutionPlan for TableScanPlan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> DfSchemaRef {
        self.df_schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.partitioning.clone()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        &self,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        unimplemented!("Leaf table scan node cannot have new children for now.")
    }

    async fn execute(
        &self,
        partition: usize,
        _runtime: Arc<RuntimeEnv>,
    ) -> DfResult<DfSendableRecordBatchStream> {
        let stream = self
            .table
            .scan(partition, &self.projection, &self.filters, self.limit)
            .await?;
        Ok(Box::pin(DfRecordBatchStreamAdapter::new(stream)))
    }

    fn statistics(&self) -> Statistics {
        // TODO(LFC): impl statistics
        Statistics::default()
    }
}

/// Datafusion TableProvider ->  greptime Table
pub struct TableAdapter {
    schema: TableSchemaRef,
    table_provider: Arc<dyn TableProvider>,
    runtime: Arc<RuntimeEnv>,
}

impl TableAdapter {
    pub fn new(table_provider: Arc<dyn TableProvider>, runtime: Arc<RuntimeEnv>) -> Result<Self> {
        Ok(Self {
            schema: Arc::new(table_provider.schema().try_into().unwrap()),
            table_provider,
            runtime,
        })
    }
}

#[async_trait::async_trait]
impl Table for TableAdapter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> TableSchemaRef {
        self.schema.clone()
    }

    fn table_info(&self) -> TableInfoRef {
        unreachable!("Should not call table_info of TableAdaptor directly")
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
        _partition: usize,
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<SendableRecordBatchStream> {
        let filters: Vec<DfExpr> = filters.iter().map(|e| e.df_expr().clone()).collect();
        debug!("TableScan filter size: {}", filters.len());
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

        Ok(Box::pin(RecordBatchStreamAdapter::try_new(df_stream)?))
    }

    fn supports_filter_pushdown(&self, filter: &Expr) -> Result<FilterPushDownType> {
        match self
            .table_provider
            .supports_filter_pushdown(filter.df_expr())
            .context(error::DatafusionSnafu)?
        {
            DfTableProviderFilterPushDown::Unsupported => Ok(FilterPushDownType::Unsupported),
            DfTableProviderFilterPushDown::Inexact => Ok(FilterPushDownType::Inexact),
            DfTableProviderFilterPushDown::Exact => Ok(FilterPushDownType::Exact),
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
    schema: SchemaRef,
    stream: DfSendableRecordBatchStream,
}

impl RecordBatchStreamAdapter {
    pub fn try_new(stream: DfSendableRecordBatchStream) -> Result<Self> {
        let schema =
            Arc::new(Schema::try_from(stream.schema()).context(error::SchemaConversionSnafu)?);
        Ok(Self { schema, stream })
    }
}

impl RecordBatchStream for RecordBatchStreamAdapter {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for RecordBatchStreamAdapter {
    type Item = RecordBatchResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.stream).poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(df_recordbatch)) => Poll::Ready(Some(Ok(RecordBatch {
                schema: self.schema(),
                df_recordbatch: df_recordbatch.context(error::PollStreamSnafu)?,
            }))),
            Poll::Ready(None) => Poll::Ready(None),
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow;
    use datafusion::datasource::empty::EmptyTable;
    use datafusion_common::field_util::SchemaExt;

    use super::*;
    use crate::metadata::TableType::Base;

    #[test]
    #[should_panic]
    fn test_table_adaptor_info() {
        let df_table = Arc::new(EmptyTable::new(Arc::new(arrow::datatypes::Schema::empty())));
        let table_adapter = TableAdapter::new(df_table, Arc::new(RuntimeEnv::default())).unwrap();
        let _ = table_adapter.table_info();
    }

    #[test]
    fn test_table_adaptor_type() {
        let df_table = Arc::new(EmptyTable::new(Arc::new(arrow::datatypes::Schema::empty())));
        let table_adapter = TableAdapter::new(df_table, Arc::new(RuntimeEnv::default())).unwrap();
        assert_eq!(Base, table_adapter.table_type());
    }
}
