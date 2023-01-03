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

use std::sync::Arc;

use datafusion::datasource::DefaultTableSource;
use datafusion::logical_expr::{Extension, LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::Expr as DfExpr;
use datafusion::sql::planner::ContextProvider;
use datafusion::sql::TableReference;
use datatypes::schema::SchemaRef;
use promql_parser::label::Matchers;
use promql_parser::parser::{EvalStmt, Expr as PromExpr};
use snafu::{OptionExt, ResultExt};
use table::table::adapter::DfTableProviderAdapter;

use crate::error::{
    DataFusionSnafu, NoTimeIndexSnafu, Result, UnexpectedPlanExprSnafu, UnknownTableSnafu,
};
use crate::extension_plan::SeriesNormalize;

pub struct PromPlanner<S: ContextProvider> {
    schema_provider: S,
}

impl<S: ContextProvider> PromPlanner<S> {
    pub fn stmt_to_plan(stmt: EvalStmt) -> Result<LogicalPlan> {
        todo!()
    }

    pub fn prom_expr_to_plan(&self, prom_expr: PromExpr) -> Result<LogicalPlan> {
        let res = match prom_expr {
            PromExpr::AggregateExpr {
                op,
                expr,
                param,
                grouping,
                without,
            } => todo!(),
            PromExpr::UnaryExpr { op, expr } => todo!(),
            PromExpr::BinaryExpr {
                op,
                lhs,
                rhs,
                matching,
                return_bool,
            } => {
                let left_input = self.prom_expr_to_plan(*lhs)?;
                let right_input = self.prom_expr_to_plan(*rhs)?;

                todo!()
            }
            PromExpr::ParenExpr { expr } => todo!(),
            PromExpr::SubqueryExpr {
                expr,
                range,
                offset,
                timestamp,
                start_or_end,
                step,
            } => todo!(),
            PromExpr::NumberLiteral { val, span } => todo!(),
            PromExpr::StringLiteral { val, span } => todo!(),
            PromExpr::VectorSelector {
                name,
                offset,
                start_or_end,
                label_matchers,
            } => {
                let series_normalize =
                    self.vector_selector_to_series_normalize(prom_expr.clone())?;

                todo!()
            }
            PromExpr::MatrixSelector {
                vector_selector,
                range,
            } => todo!(),
            PromExpr::Call { func, args } => todo!(),
        };
        Ok(res)
    }

    fn vector_selector_to_series_normalize(&self, selector: &PromExpr) -> Result<LogicalPlan> {
        if let PromExpr::VectorSelector {
            name,
            offset,
            start_or_end,
            label_matchers,
        } = selector
        {
            // This `name` should not be optional
            let name = name.unwrap().clone();
            // TODO(ruihang): add time range filter
            let filter = self.matchers_to_expr(label_matchers)?;
            let table_scan = self.create_relation(&name, filter)?;
            let offset = offset.unwrap_or_default();

            let schema = self.get_schema(&name)?;
            let time_index = schema.timestamp_column().context(NoTimeIndexSnafu)?.name;
            let series_normalize = SeriesNormalize::new(offset, time_index, table_scan);
            let logical_plan = LogicalPlan::Extension(Extension {
                node: Arc::new(series_normalize),
            });
            Ok(logical_plan)
        } else {
            UnexpectedPlanExprSnafu {
                desc: format!("{:?}", selector),
            }
            .fail()
        }
    }

    fn matchers_to_expr(&self, label_matchers: &Matchers) -> Result<Vec<DfExpr>> {
        todo!()
    }

    fn create_relation(&self, table_name: &str, filter: Vec<DfExpr>) -> Result<LogicalPlan> {
        let table_ref = TableReference::Bare { table: table_name };
        let provider = self
            .schema_provider
            .get_table_provider(table_ref)
            .context(DataFusionSnafu)?;
        let result = LogicalPlanBuilder::scan(table_name, provider, None)
            .context(DataFusionSnafu)?
            .build()
            .context(DataFusionSnafu)?;
        Ok(result)
    }

    /// Get [SchemaRef] of GreptimeDB (rather than DataFusion's).
    fn get_schema(&self, table_name: &str) -> Result<SchemaRef> {
        let table_ref = TableReference::Bare { table: table_name };
        let table = self
            .schema_provider
            .get_table_provider(table_ref)
            .context(DataFusionSnafu)?
            .as_any()
            .downcast_ref::<DefaultTableSource>()
            .context(UnknownTableSnafu)?
            .table_provider
            .as_any()
            .downcast_ref::<DfTableProviderAdapter>()
            .context(UnknownTableSnafu)?
            .table();

        Ok(table.schema())
    }
}
