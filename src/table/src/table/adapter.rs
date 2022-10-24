use std::any::Any;
use std::sync::Arc;

use common_query::logical_plan::Expr;
use common_query::physical_plan::{DfPhysicalPlanAdapter, PhysicalPlanAdapter, PhysicalPlanRef};
use common_query::DfPhysicalPlan;
use common_telemetry::debug;
use datafusion::arrow::datatypes::SchemaRef as DfSchemaRef;
///  Datafusion table adpaters
use datafusion::datasource::{
    datasource::TableProviderFilterPushDown as DfTableProviderFilterPushDown, TableProvider,
    TableType as DfTableType,
};
use datafusion::error::Result as DfResult;
use datafusion::logical_plan::Expr as DfExpr;
use datatypes::schema::{SchemaRef as TableSchemaRef, SchemaRef};
use snafu::prelude::*;

use crate::error::{self, Result};
use crate::metadata::TableInfoRef;
use crate::table::{FilterPushDownType, Table, TableRef, TableType};

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
    ) -> DfResult<Arc<dyn DfPhysicalPlan>> {
        let filters: Vec<Expr> = filters.iter().map(Clone::clone).map(Into::into).collect();
        let inner = self.table.scan(projection, &filters, limit).await?;
        Ok(Arc::new(DfPhysicalPlanAdapter(inner)))
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

/// Datafusion TableProvider ->  greptime Table
pub struct TableAdapter {
    schema: TableSchemaRef,
    table_provider: Arc<dyn TableProvider>,
}

impl TableAdapter {
    pub fn new(table_provider: Arc<dyn TableProvider>) -> Result<Self> {
        Ok(Self {
            schema: Arc::new(
                table_provider
                    .schema()
                    .try_into()
                    .context(error::SchemaConversionSnafu)?,
            ),
            table_provider,
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
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<PhysicalPlanRef> {
        let filters: Vec<DfExpr> = filters.iter().map(|e| e.df_expr().clone()).collect();
        debug!("TableScan filter size: {}", filters.len());
        let execution_plan = self
            .table_provider
            .scan(projection, &filters, limit)
            .await
            .context(error::DatafusionSnafu)?;
        let schema: SchemaRef = Arc::new(
            execution_plan
                .schema()
                .try_into()
                .context(error::SchemaConversionSnafu)?,
        );
        Ok(Arc::new(PhysicalPlanAdapter::new(schema, execution_plan)))
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
        let table_adapter = TableAdapter::new(df_table).unwrap();
        let _ = table_adapter.table_info();
    }

    #[test]
    fn test_table_adaptor_type() {
        let df_table = Arc::new(EmptyTable::new(Arc::new(arrow::datatypes::Schema::empty())));
        let table_adapter = TableAdapter::new(df_table).unwrap();
        assert_eq!(Base, table_adapter.table_type());
    }
}
