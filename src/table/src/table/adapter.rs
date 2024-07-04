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
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use common_recordbatch::OrderOption;
use datafusion::arrow::datatypes::SchemaRef as DfSchemaRef;
use datafusion::datasource::{TableProvider, TableType as DfTableType};
use datafusion::error::Result as DfResult;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_expr::expr::Expr;
use datafusion_expr::TableProviderFilterPushDown as DfTableProviderFilterPushDown;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::PhysicalSortExpr;
use datatypes::schema::ColumnSchema;
use store_api::region_engine::SinglePartitionScanner;
use store_api::storage::ScanRequest;

use crate::table::scan::RegionScanExec;
use crate::table::{TableRef, TableType};

/// Adapt greptime's [TableRef] to DataFusion's [TableProvider].
pub struct DfTableProviderAdapter {
    table: TableRef,
    scan_req: Arc<Mutex<ScanRequest>>,
    /// Columns default [`Expr`]
    column_defaults: HashMap<String, Expr>,
}

impl DfTableProviderAdapter {
    pub fn new(table: TableRef) -> Self {
        Self {
            column_defaults: collect_column_defaults(table.schema().column_schemas()),
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

/// Collects column default [`Expr`] from column schemas.
pub fn collect_column_defaults(column_schemas: &[ColumnSchema]) -> HashMap<String, Expr> {
    column_schemas
        .iter()
        .filter_map(|column_schema| {
            column_schema
                .create_default()
                .ok()??
                .try_to_scalar_value(&column_schema.data_type)
                .ok()
                .map(|scalar| (column_schema.name.to_string(), Expr::Literal(scalar)))
        })
        .collect()
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

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.column_defaults.get(column)
    }

    async fn scan(
        &self,
        _ctx: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
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

        let scanner = Box::new(SinglePartitionScanner::new(stream));
        let mut plan = RegionScanExec::new(scanner);
        if let Some(sort_expr) = sort_expr {
            plan = plan.with_output_ordering(sort_expr);
        }
        Ok(Arc::new(plan))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<DfTableProviderFilterPushDown>> {
        let filters = filters.iter().map(|&x| x.clone()).collect::<Vec<_>>();
        Ok(self
            .table
            .supports_filters_pushdown(&filters.iter().collect::<Vec<_>>())
            .map(|v| v.into_iter().map(Into::into).collect::<Vec<_>>())?)
    }
}

#[cfg(test)]
mod tests {
    use datafusion_common::ScalarValue;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnDefaultConstraint;

    use super::*;

    #[test]
    fn test_collect_columns_defaults() {
        let column_schemas = vec![
            ColumnSchema::new("col1", ConcreteDataType::int32_datatype(), false),
            ColumnSchema::new("col2", ConcreteDataType::string_datatype(), true)
                .with_default_constraint(Some(ColumnDefaultConstraint::Value("test".into())))
                .unwrap(),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true)
            .with_default_constraint(Some(ColumnDefaultConstraint::Function(
                "current_timestamp".to_string(),
            )))
            .unwrap(),
        ];
        let column_defaults = collect_column_defaults(&column_schemas[..]);

        assert!(!column_defaults.contains_key("col1"));
        assert!(matches!(column_defaults.get("col2").unwrap(),
                         Expr::Literal(ScalarValue::Utf8(Some(s))) if s == "test"));
        assert!(matches!(
            column_defaults.get("ts").unwrap(),
            Expr::Literal(ScalarValue::TimestampMillisecond(_, _))
        ));
    }
}
