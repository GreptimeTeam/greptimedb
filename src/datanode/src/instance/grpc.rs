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
    admin_expr, object_expr, AdminExpr, AdminResult, Column, CreateDatabaseExpr, ObjectExpr,
    ObjectResult, QueryRequest,
};
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::Ticket;
use async_trait::async_trait;
use common_catalog::consts::DEFAULT_CATALOG_NAME;
use common_error::ext::ErrorExt;
use common_error::prelude::BoxedError;
use common_error::status_code::StatusCode;
use common_grpc::flight::flight_data_to_object_result;
use common_grpc_expr::insertion_expr_to_request;
use common_query::Output;
use prost::Message;
use query::plan::LogicalPlan;
use servers::query_handler::{GrpcAdminHandler, GrpcQueryHandler};
use snafu::prelude::*;
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};
use table::requests::CreateDatabaseRequest;
use tonic::Request;

use crate::error::{
    self, CatalogNotFoundSnafu, CatalogSnafu, DecodeLogicalPlanSnafu, EmptyInsertBatchSnafu,
    ExecuteSqlSnafu, InsertDataSnafu, InsertSnafu, Result, SchemaNotFoundSnafu, TableNotFoundSnafu,
};
use crate::instance::Instance;

impl Instance {
    pub async fn execute_grpc_insert(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        insert_batches: Vec<(Vec<Column>, u32)>,
    ) -> Result<Output> {
        let schema_provider = self
            .catalog_manager
            .catalog(catalog_name)
            .context(CatalogSnafu)?
            .context(CatalogNotFoundSnafu { name: catalog_name })?
            .schema(schema_name)
            .context(CatalogSnafu)?
            .context(SchemaNotFoundSnafu { name: schema_name })?;

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
        insert_batches: Vec<(Vec<Column>, u32)>,
    ) -> ObjectResult {
        match self
            .execute_grpc_insert(catalog_name, schema_name, table_name, insert_batches)
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

    async fn handle_query_request(&self, query_request: QueryRequest) -> Result<ObjectResult> {
        let ticket = Request::new(Ticket {
            ticket: ObjectExpr {
                header: None,
                expr: Some(object_expr::Expr::Query(query_request)),
            }
            .encode_to_vec(),
        });
        // TODO(LFC): Temporarily use old GRPC interface here, will make it been replaced.
        let response = self.do_get(ticket).await.context(error::FlightGetSnafu)?;
        flight_data_to_object_result(response)
            .await
            .context(error::InvalidFlightDataSnafu)
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

    pub(crate) async fn execute_logical(&self, plan_bytes: Vec<u8>) -> Result<Output> {
        let logical_plan = DFLogicalSubstraitConvertor
            .decode(plan_bytes.as_slice(), self.catalog_manager.clone())
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

                // TODO(fys): _region_number is for later use.
                let _region_number: u32 = insert_expr.region_number;

                let insert_batches = vec![(insert_expr.columns, insert_expr.row_count)];
                self.handle_insert(catalog_name, schema_name, table_name, insert_batches)
                    .await
            }
            Some(object_expr::Expr::Query(query_request)) => self
                .handle_query_request(query_request.clone())
                .await
                .map_err(BoxedError::new)
                .context(servers::error::ExecuteQuerySnafu {
                    query: format!("{query_request:?}"),
                })?,
            other => {
                return servers::error::NotSupportedSnafu {
                    feat: format!("{other:?}"),
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
            Some(admin_expr::Expr::CreateTable(create_expr)) => {
                self.handle_create(create_expr).await
            }
            Some(admin_expr::Expr::Alter(alter_expr)) => self.handle_alter(alter_expr).await,
            Some(admin_expr::Expr::CreateDatabase(create_database_expr)) => {
                self.execute_create_database(create_database_expr).await
            }
            Some(admin_expr::Expr::DropTable(drop_table_expr)) => {
                self.handle_drop_table(drop_table_expr).await
            }
            other => {
                return servers::error::NotSupportedSnafu {
                    feat: format!("{other:?}"),
                }
                .fail();
            }
        };
        Ok(admin_resp)
    }
}
