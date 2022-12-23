// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Formatter;
use std::sync::Arc;

use api::v1::InsertExpr;
use client::{Database, ObjectResult};
use common_grpc::flight::flight_messages_to_recordbatches;
use common_query::prelude::Expr;
use common_recordbatch::RecordBatches;
use datafusion::datasource::DefaultTableSource;
use datafusion_expr::{LogicalPlan, LogicalPlanBuilder};
use meta_client::rpc::TableName;
use snafu::ResultExt;
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};
use table::table::adapter::DfTableProviderAdapter;
use table::TableRef;

use crate::error::{self, ConvertFlightMessageSnafu, Result};

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

    pub(crate) async fn grpc_insert(&self, request: InsertExpr) -> client::Result<ObjectResult> {
        self.db.insert(request).await
    }

    pub(crate) async fn grpc_table_scan(&self, plan: TableScanPlan) -> Result<RecordBatches> {
        let logical_plan = self.build_logical_plan(&plan)?;

        let substrait_plan = DFLogicalSubstraitConvertor
            .encode(logical_plan)
            .context(error::EncodeSubstraitLogicalPlanSnafu)?;

        let result = self
            .db
            .logical_plan(substrait_plan.to_vec())
            .await
            .context(error::RequestDatanodeSnafu)?;
        let recordbatches = match result {
            ObjectResult::FlightData(flight_message) => {
                flight_messages_to_recordbatches(flight_message)
                    .context(ConvertFlightMessageSnafu)?
            }
            _ => unreachable!(),
        };
        Ok(recordbatches)
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
