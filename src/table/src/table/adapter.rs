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
use std::sync::{Arc, Mutex};

use common_query::logical_plan::Expr;
use common_query::physical_plan::DfPhysicalPlanAdapter;
use common_query::DfPhysicalPlan;
use common_recordbatch::OrderOption;
use datafusion::arrow::datatypes::SchemaRef as DfSchemaRef;
use datafusion::datasource::{TableProvider, TableType as DfTableType};
use datafusion::error::Result as DfResult;
use datafusion::execution::context::SessionState;
use datafusion_expr::expr::Expr as DfExpr;
use datafusion_expr::TableProviderFilterPushDown as DfTableProviderFilterPushDown;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::PhysicalSortExpr;
use store_api::storage::ScanRequest;

use super::scan::StreamScanAdapter;
use crate::table::{TableRef, TableType};

/// Adapt greptime's [TableRef] to DataFusion's [TableProvider].
pub struct DfTableProviderAdapter {
    table: TableRef,
    scan_req: Arc<Mutex<ScanRequest>>,
}

impl DfTableProviderAdapter {
    pub fn new(table: TableRef) -> Self {
        Self {
            table,
            scan_req: Arc::default(),
        }
    }

    pub fn table(&self) -> TableRef {
        self.table.clone()
    }

    pub fn with_ordering_hint(&self, order_opts: &[OrderOption]) {
        self.scan_req.lock().unwrap().output_ordering = Some(order_opts.to_vec());
    }

    #[cfg(feature = "testing")]
    pub fn get_scan_req(&self) -> ScanRequest {
        self.scan_req.lock().unwrap().clone()
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
        let request = {
            let mut request = self.scan_req.lock().unwrap();
            request.filters = filters;
            request.projection = projection.cloned();
            request.limit = limit;
            request.clone()
        };
        let stream = self.table.scan_to_stream(request).await?;

        // build sort physical expr
        let schema = stream.schema();
        let sort_expr = stream.output_ordering().map(|order_opts| {
            order_opts
                .iter()
                .map(|order_opt| {
                    let col_index = schema.column_index_by_name(&order_opt.name).unwrap();
                    let col_expr = Arc::new(Column::new(&order_opt.name, col_index));
                    PhysicalSortExpr {
                        expr: col_expr,
                        options: order_opt.options,
                    }
                })
                .collect::<Vec<_>>()
        });

        let mut stream_adapter = StreamScanAdapter::new(stream);
        if let Some(sort_expr) = sort_expr {
            stream_adapter = stream_adapter.with_output_ordering(sort_expr);
        }
        Ok(Arc::new(DfPhysicalPlanAdapter(Arc::new(stream_adapter))))
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
