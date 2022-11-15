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

use api::result::{build_err_result, AdminResultBuilder, ObjectResultBuilder};
use api::v1::{
    admin_expr, insert_expr, object_expr, select_expr, AdminExpr, AdminResult, CreateDatabaseExpr,
    ObjectExpr, ObjectResult, SelectExpr,
};
use async_trait::async_trait;
use common_catalog::consts::DEFAULT_CATALOG_NAME;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_insert::insertion_expr_to_request;
use common_query::Output;
use query::plan::LogicalPlan;
use servers::query_handler::{GrpcAdminHandler, GrpcQueryHandler};
use snafu::prelude::*;
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};
use table::requests::CreateDatabaseRequest;

use crate::error::{
    CatalogNotFoundSnafu, CatalogSnafu, DecodeLogicalPlanSnafu, EmptyInsertBatchSnafu,
    ExecuteSqlSnafu, InsertDataSnafu, InsertSnafu, Result, SchemaNotFoundSnafu, TableNotFoundSnafu,
    UnsupportedExprSnafu,
};
use crate::instance::Instance;
use crate::server::grpc::plan::PhysicalPlanner;
use crate::server::grpc::select::to_object_result;

impl Instance {
    pub async fn execute_grpc_insert(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        values: insert_expr::Values,
    ) -> Result<Output> {
        let schema_provider = self
            .catalog_manager
            .catalog(catalog_name)
            .context(CatalogSnafu)?
            .context(CatalogNotFoundSnafu { name: catalog_name })?
            .schema(schema_name)
            .context(CatalogSnafu)?
            .context(SchemaNotFoundSnafu { name: schema_name })?;

        let insert_batches =
            common_insert::insert_batches(&values.values).context(InsertDataSnafu)?;

        ensure!(!insert_batches.is_empty(), EmptyInsertBatchSnafu);

        let table = schema_provider
            .table(table_name)
            .context(CatalogSnafu)?
            .context(TableNotFoundSnafu { table_name })?;

        let insert = insertion_expr_to_request(
            catalog_name,
            schema_name,
            table_name,
            insert_batches,
            table.clone(),
        )
        .context(InsertDataSnafu)?;

        let affected_rows = table
            .insert(insert)
            .await
            .context(InsertSnafu { table_name })?;

        Ok(Output::AffectedRows(affected_rows))
    }

    async fn handle_insert(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        values: insert_expr::Values,
    ) -> ObjectResult {
        match self
            .execute_grpc_insert(catalog_name, schema_name, table_name, values)
            .await
        {
            Ok(Output::AffectedRows(rows)) => ObjectResultBuilder::new()
                .status_code(StatusCode::Success as u32)
                .mutate_result(rows as u32, 0)
                .build(),
            Err(err) => {
                common_telemetry::error!(err; "Failed to handle insert, catalog name: {}, schema name: {}, table name: {}", catalog_name, schema_name, table_name);
                // TODO(fys): failure count
                build_err_result(&err)
            }
            _ => unreachable!(),
        }
    }

    async fn handle_select(&self, select_expr: SelectExpr) -> ObjectResult {
        let result = self.do_handle_select(select_expr).await;
        to_object_result(result).await
    }

    async fn do_handle_select(&self, select_expr: SelectExpr) -> Result<Output> {
        let expr = select_expr.expr;
        match expr {
            Some(select_expr::Expr::Sql(sql)) => self.execute_sql(&sql).await,
            Some(select_expr::Expr::LogicalPlan(plan)) => self.execute_logical(plan).await,
            Some(select_expr::Expr::PhysicalPlan(api::v1::PhysicalPlan { original_ql, plan })) => {
                self.physical_planner
                    .execute(PhysicalPlanner::parse(plan)?, original_ql)
                    .await
            }
            _ => UnsupportedExprSnafu {
                name: format!("{:?}", expr),
            }
            .fail(),
        }
    }

    async fn execute_create_database(
        &self,
        create_database_expr: CreateDatabaseExpr,
    ) -> AdminResult {
        let req = CreateDatabaseRequest {
            db_name: create_database_expr.database_name,
        };
        let result = self.sql_handler.create_database(req).await;
        match result {
            Ok(Output::AffectedRows(rows)) => AdminResultBuilder::default()
                .status_code(StatusCode::Success as u32)
                .mutate_result(rows as u32, 0)
                .build(),
            Ok(Output::Stream(_)) | Ok(Output::RecordBatches(_)) => unreachable!(),
            Err(err) => AdminResultBuilder::default()
                .status_code(err.status_code() as u32)
                .err_msg(err.to_string())
                .build(),
        }
    }

    async fn execute_logical(&self, plan_bytes: Vec<u8>) -> Result<Output> {
        let logical_plan_converter = DFLogicalSubstraitConvertor::new(self.catalog_manager.clone());
        let logical_plan = logical_plan_converter
            .decode(plan_bytes.as_slice())
            .context(DecodeLogicalPlanSnafu)?;

        self.query_engine
            .execute(&LogicalPlan::DfPlan(logical_plan))
            .await
            .context(ExecuteSqlSnafu)
    }
}

#[async_trait]
impl GrpcQueryHandler for Instance {
    async fn do_query(&self, query: ObjectExpr) -> servers::error::Result<ObjectResult> {
        let object_resp = match query.expr {
            Some(object_expr::Expr::Insert(insert_expr)) => {
                let catalog_name = DEFAULT_CATALOG_NAME;
                let schema_name = &insert_expr.schema_name;
                let table_name = &insert_expr.table_name;
                let expr = insert_expr
                    .expr
                    .context(servers::error::InvalidQuerySnafu {
                        reason: "missing `expr` in `InsertExpr`",
                    })?;

                // TODO(fys): _region_number is for later use.
                let _region_number: u32 = insert_expr.region_number;

                match expr {
                    insert_expr::Expr::Values(values) => {
                        self.handle_insert(catalog_name, schema_name, table_name, values)
                            .await
                    }
                    insert_expr::Expr::Sql(sql) => {
                        let output = self.execute_sql(&sql).await;
                        to_object_result(output).await
                    }
                }
            }
            Some(object_expr::Expr::Select(select_expr)) => self.handle_select(select_expr).await,
            other => {
                return servers::error::NotSupportedSnafu {
                    feat: format!("{:?}", other),
                }
                .fail();
            }
        };
        Ok(object_resp)
    }
}

#[async_trait]
impl GrpcAdminHandler for Instance {
    async fn exec_admin_request(&self, expr: AdminExpr) -> servers::error::Result<AdminResult> {
        let admin_resp = match expr.expr {
            Some(admin_expr::Expr::Create(create_expr)) => self.handle_create(create_expr).await,
            Some(admin_expr::Expr::Alter(alter_expr)) => self.handle_alter(alter_expr).await,
            Some(admin_expr::Expr::CreateDatabase(create_database_expr)) => {
                self.execute_create_database(create_database_expr).await
            }
            other => {
                return servers::error::NotSupportedSnafu {
                    feat: format!("{:?}", other),
                }
                .fail();
            }
        };
        Ok(admin_resp)
    }
}
