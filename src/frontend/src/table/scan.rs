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
use client::{Database, ObjectResult, Select};
use common_query::prelude::Expr;
use common_query::Output;
use common_recordbatch::{util, RecordBatches};
use datafusion::logical_plan::{LogicalPlan as DfLogicPlan, LogicalPlanBuilder};
use datafusion_expr::Expr as DfExpr;
use datatypes::prelude::*;
use datatypes::schema::SchemaRef;
use meta_client::rpc::TableName;
use query::plan::LogicalPlan;
use snafu::ResultExt;
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

    pub(crate) async fn grpc_insert(&self, request: InsertExpr) -> client::Result<ObjectResult> {
        self.db.insert(request).await
    }

    pub(crate) async fn grpc_table_scan(&self, plan: TableScanPlan) -> Result<RecordBatches> {
        let logical_plan = self.build_logical_plan(&plan)?;

        // TODO(LFC): Directly pass in logical plan to GRPC interface when our substrait codec supports filter.
        let sql = to_sql(logical_plan)?;

        let output = self
            .db
            .select(Select::Sql(sql))
            .await
            .and_then(Output::try_from)
            .context(error::SelectSnafu)?;

        Ok(match output {
            Output::Stream(stream) => {
                let schema = stream.schema();
                let batches = util::collect(stream)
                    .await
                    .context(error::CollectRecordbatchStreamSnafu)?;
                RecordBatches::try_new(schema, batches).context(error::CreateRecordbatchesSnafu)?
            }
            Output::RecordBatches(x) => x,
            _ => unreachable!(),
        })
    }

    fn build_logical_plan(&self, table_scan: &TableScanPlan) -> Result<LogicalPlan> {
        let table_provider = Arc::new(DfTableProviderAdapter::new(self.table.clone()));

        let mut builder = LogicalPlanBuilder::scan_with_filters(
            &table_scan.table_name.to_string(),
            table_provider,
            table_scan.projection.clone(),
            table_scan
                .filters
                .iter()
                .map(|x| x.df_expr().clone())
                .collect::<Vec<_>>(),
        )
        .context(error::BuildDfLogicalPlanSnafu)?;

        if let Some(limit) = table_scan.limit {
            builder = builder
                .limit(limit)
                .context(error::BuildDfLogicalPlanSnafu)?;
        }

        let plan = builder.build().context(error::BuildDfLogicalPlanSnafu)?;
        Ok(LogicalPlan::DfPlan(plan))
    }
}

#[derive(Debug)]
pub(crate) struct TableScanPlan {
    pub table_name: TableName,
    pub projection: Option<Vec<usize>>,
    pub filters: Vec<Expr>,
    pub limit: Option<usize>,
}

fn to_sql(plan: LogicalPlan) -> Result<String> {
    let LogicalPlan::DfPlan(plan) = plan;
    let table_scan = match plan {
        DfLogicPlan::TableScan(table_scan) => table_scan,
        _ => unreachable!("unknown plan: {:?}", plan),
    };

    let schema: SchemaRef = Arc::new(
        table_scan
            .source
            .schema()
            .try_into()
            .context(error::ConvertArrowSchemaSnafu)?,
    );
    let projection = table_scan
        .projection
        .map(|x| {
            x.iter()
                .map(|i| schema.column_name_by_index(*i).to_string())
                .collect::<Vec<String>>()
        })
        .unwrap_or_else(|| {
            schema
                .column_schemas()
                .iter()
                .map(|x| x.name.clone())
                .collect::<Vec<String>>()
        })
        .join(", ");

    let mut sql = format!("select {} from {}", projection, &table_scan.table_name);

    let filters = table_scan
        .filters
        .iter()
        .map(expr_to_sql)
        .collect::<Result<Vec<String>>>()?
        .join(" AND ");
    if !filters.is_empty() {
        sql.push_str(" where ");
        sql.push_str(&filters);
    }

    if let Some(limit) = table_scan.limit {
        sql.push_str(" limit ");
        sql.push_str(&limit.to_string());
    }
    Ok(sql)
}

fn expr_to_sql(expr: &DfExpr) -> Result<String> {
    Ok(match expr {
        DfExpr::BinaryExpr {
            ref left,
            ref right,
            ref op,
        } => format!(
            "{} {} {}",
            expr_to_sql(left.as_ref())?,
            op,
            expr_to_sql(right.as_ref())?
        ),
        DfExpr::Column(c) => c.name.clone(),
        DfExpr::Literal(sv) => {
            let v: Value = Value::try_from(sv.clone())
                .with_context(|_| error::ConvertScalarValueSnafu { value: sv.clone() })?;
            if matches!(v.data_type(), ConcreteDataType::String(_)) {
                format!("'{}'", sv)
            } else {
                format!("{}", sv)
            }
        }
        _ => unimplemented!("not implemented for {:?}", expr),
    })
}
