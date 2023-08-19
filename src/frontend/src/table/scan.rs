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

use std::fmt::Formatter;
use std::sync::Arc;

use client::Database;
use common_meta::table_name::TableName;
use common_query::prelude::Expr;
use common_query::Output;
use datafusion::datasource::DefaultTableSource;
use datafusion_expr::{LogicalPlan, LogicalPlanBuilder};
use snafu::ResultExt;
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};
use table::table::adapter::DfTableProviderAdapter;
use table::TableRef;

use crate::error::{self, Result};

#[derive(Clone)]
pub struct DatanodeInstance {
    table: TableRef,
    db: Database,
}

impl std::fmt::Debug for DatanodeInstance {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("DatanodeInstance")
    }
}

impl DatanodeInstance {
    pub(crate) fn new(table: TableRef, db: Database) -> Self {
        Self { table, db }
    }

    pub(crate) async fn grpc_table_scan(&self, plan: TableScanPlan) -> Result<Output> {
        let logical_plan = self.build_logical_plan(&plan)?;

        let substrait_plan = DFLogicalSubstraitConvertor
            .encode(&logical_plan)
            .context(error::EncodeSubstraitLogicalPlanSnafu)?;

        let output = self
            .db
            .logical_plan(substrait_plan.to_vec(), None)
            .await
            .context(error::RequestDatanodeSnafu)?;

        match output {
            Output::Stream(_) => Ok(output),
            _ => unreachable!(),
        }
    }

    fn build_logical_plan(&self, table_scan: &TableScanPlan) -> Result<LogicalPlan> {
        let table_provider = Arc::new(DfTableProviderAdapter::new(self.table.clone()));

        let mut builder = LogicalPlanBuilder::scan_with_filters(
            table_scan.table_name.to_string(),
            Arc::new(DefaultTableSource::new(table_provider)),
            table_scan.projection.clone(),
            table_scan
                .filters
                .iter()
                .map(|x| x.df_expr().clone())
                .collect::<Vec<_>>(),
        )
        .context(error::BuildDfLogicalPlanSnafu)?;

        if let Some(filter) = table_scan
            .filters
            .iter()
            .map(|x| x.df_expr())
            .cloned()
            .reduce(|accum, expr| accum.and(expr))
        {
            builder = builder
                .filter(filter)
                .context(error::BuildDfLogicalPlanSnafu)?;
        }

        if table_scan.limit.is_some() {
            builder = builder
                .limit(0, table_scan.limit)
                .context(error::BuildDfLogicalPlanSnafu)?;
        }

        builder.build().context(error::BuildDfLogicalPlanSnafu)
    }
}

#[derive(Debug)]
pub(crate) struct TableScanPlan {
    pub table_name: TableName,
    pub projection: Option<Vec<usize>>,
    pub filters: Vec<Expr>,
    pub limit: Option<usize>,
}
