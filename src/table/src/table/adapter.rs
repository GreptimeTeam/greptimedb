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
use std::sync::Arc;

use common_query::logical_plan::Expr;
use common_query::physical_plan::{DfPhysicalPlanAdapter, PhysicalPlanAdapter, PhysicalPlanRef};
use common_query::DfPhysicalPlan;
use common_telemetry::debug;
use datafusion::arrow::datatypes::SchemaRef as DfSchemaRef;
use datafusion::datasource::datasource::TableProviderFilterPushDown as DfTableProviderFilterPushDown;
use datafusion::datasource::{TableProvider, TableType as DfTableType};
use datafusion::error::Result as DfResult;
use datafusion::execution::context::SessionState;
use datafusion::prelude::SessionContext;
use datafusion_expr::expr::Expr as DfExpr;
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
        _ctx: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[DfExpr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn DfPhysicalPlan>> {
        let filters: Vec<Expr> = filters.iter().map(Clone::clone).map(Into::into).collect();
        let inner = self.table.scan(projection, &filters, limit).await?;
        Ok(Arc::new(DfPhysicalPlanAdapter(inner)))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&DfExpr],
    ) -> DfResult<Vec<DfTableProviderFilterPushDown>> {
        let filters = filters
            .iter()
            .map(|&x| x.clone().into())
            .collect::<Vec<_>>();
        Ok(self
            .table
            .supports_filters_pushdown(&filters.iter().collect::<Vec<_>>())
            .map(|v| v.into_iter().map(Into::into).collect::<Vec<_>>())?)
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
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<PhysicalPlanRef> {
        let ctx = SessionContext::new();
        let filters: Vec<DfExpr> = filters.iter().map(|e| e.df_expr().clone()).collect();
        debug!("TableScan filter size: {}", filters.len());
        let execution_plan = self
            .table_provider
            .scan(&ctx.state(), projection, &filters, limit)
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

    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<FilterPushDownType>> {
        self.table_provider
            .supports_filters_pushdown(&filters.iter().map(|x| x.df_expr()).collect::<Vec<_>>())
            .context(error::DatafusionSnafu)
            .map(|v| v.into_iter().map(Into::into).collect::<Vec<_>>())
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow;
    use datafusion::datasource::empty::EmptyTable;

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
