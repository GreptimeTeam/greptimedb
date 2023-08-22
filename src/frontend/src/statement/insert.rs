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

use common_error::ext::BoxedError;
use common_query::Output;
use datafusion_expr::{DmlStatement, LogicalPlan as DfLogicalPlan, WriteOp};
use datanode::instance::sql::table_idents_to_full_name;
use futures_util::StreamExt;
use query::parser::QueryStatement;
use query::plan::LogicalPlan;
use session::context::QueryContextRef;
use snafu::ResultExt;
use sql::statements::insert::Insert;
use sql::statements::statement::Statement;
use table::engine::TableReference;
use table::requests::InsertRequest;

use super::StatementExecutor;
use crate::error::{
    BuildColumnVectorsSnafu, ExecLogicalPlanSnafu, ExecuteStatementSnafu, ExternalSnafu,
    PlanStatementSnafu, ReadRecordBatchSnafu, Result, UnexpectedSnafu,
};

impl StatementExecutor {
    pub async fn insert(&self, insert: Box<Insert>, query_ctx: QueryContextRef) -> Result<Output> {
        if insert.can_extract_values() {
            // Fast path: plain insert ("insert with literal values") is executed directly
            self.sql_stmt_executor
                .execute_sql(Statement::Insert(insert), query_ctx)
                .await
                .context(ExecuteStatementSnafu)
        } else {
            // Slow path: insert with subquery. Execute the subquery first, via query engine. Then
            // insert the results by sending insert requests.

            let (catalog_name, schema_name, table_name) =
                table_idents_to_full_name(insert.table_name(), query_ctx.clone())
                    .map_err(BoxedError::new)
                    .context(ExternalSnafu)?;

            // 1. Plan the whole insert statement into a logical plan, then a wrong insert statement
            //    will be caught and a plan error will be returned.
            let logical_plan = self
                .query_engine
                .planner()
                .plan(
                    QueryStatement::Sql(Statement::Insert(insert)),
                    query_ctx.clone(),
                )
                .await
                .context(PlanStatementSnafu)?;

            // 2. Execute the subquery, get the results as a record batch stream.
            let subquery_plan = extract_subquery_plan_from_dml(logical_plan)?;
            let output = self
                .query_engine
                .execute(subquery_plan, query_ctx)
                .await
                .context(ExecLogicalPlanSnafu)?;
            let Output::Stream(mut stream) = output else {
                return UnexpectedSnafu {
                    violated: "expected a stream",
                }
                .fail();
            };

            // 3. Send insert requests.
            let mut affected_rows = 0;
            let table_ref = TableReference::full(&catalog_name, &schema_name, &table_name);
            let table = self.get_table(&table_ref).await?;
            while let Some(batch) = stream.next().await {
                let record_batch = batch.context(ReadRecordBatchSnafu)?;
                let columns_values = record_batch
                    .column_vectors(&table_name, table.schema())
                    .context(BuildColumnVectorsSnafu)?;

                let insert_request = InsertRequest {
                    catalog_name: catalog_name.clone(),
                    schema_name: schema_name.clone(),
                    table_name: table_name.clone(),
                    columns_values,
                    region_number: 0,
                };
                affected_rows += self.send_insert_request(insert_request).await?;
            }

            Ok(Output::AffectedRows(affected_rows))
        }
    }
}

fn extract_subquery_plan_from_dml(logical_plan: LogicalPlan) -> Result<LogicalPlan> {
    let LogicalPlan::DfPlan(df_plan) = logical_plan;
    match df_plan {
        DfLogicalPlan::Dml(DmlStatement {
            op: WriteOp::Insert,
            input,
            ..
        }) => Ok(LogicalPlan::from(input.as_ref().clone())),
        _ => UnexpectedSnafu {
            violated: "expected a plan of insert dml",
        }
        .fail(),
    }
}
