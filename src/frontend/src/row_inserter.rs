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

use api::v1::alter_expr::Kind;
use api::v1::ddl_request::Expr;
use api::v1::greptime_request::Request;
use api::v1::{AlterExpr, ColumnSchema, DdlRequest, Row, RowInsertRequest, RowInsertRequests};
use catalog::CatalogManagerRef;
use common_grpc_expr::util::{extract_new_columns, ColumnExpr};
use common_query::Output;
use common_telemetry::info;
use servers::query_handler::grpc::GrpcQueryHandlerRef;
use session::context::QueryContextRef;
use snafu::{ensure, OptionExt, ResultExt};
use table::TableRef;

use crate::error::{CatalogSnafu, EmptyDataSnafu, Error, FindNewColumnsOnInsertionSnafu, Result};
use crate::expr_factory::CreateExprFactoryRef;

pub struct RowInserter {
    engine_name: String,
    catalog_manager: CatalogManagerRef,
    create_expr_factory: CreateExprFactoryRef,
    grpc_query_handler: GrpcQueryHandlerRef<Error>,
}

impl RowInserter {
    pub fn new(
        engine_name: String,
        catalog_manager: CatalogManagerRef,
        create_expr_factory: CreateExprFactoryRef,
        grpc_query_handler: GrpcQueryHandlerRef<Error>,
    ) -> Self {
        Self {
            engine_name,
            catalog_manager,
            create_expr_factory,
            grpc_query_handler,
        }
    }

    pub async fn handle_inserts(
        &self,
        requests: RowInsertRequests,
        ctx: QueryContextRef,
    ) -> Result<Output> {
        self.create_or_alter_tables_on_demand(&requests, ctx.clone())
            .await?;
        let query = Request::RowInserts(requests);
        self.grpc_query_handler.do_query(query, ctx).await
    }

    // check if tables already exist:
    // - if table does not exist, create table by inferred CreateExpr
    // - if table exist, check if schema matches. If any new column found, alter table by inferred `AlterExpr`
    async fn create_or_alter_tables_on_demand(
        &self,
        requests: &RowInsertRequests,
        ctx: QueryContextRef,
    ) -> Result<()> {
        let catalog_name = ctx.current_catalog();
        let schema_name = ctx.current_schema();

        // TODO(jeremy): create and alter in batch?
        for req in &requests.inserts {
            let table_name = &req.table_name;
            let table = self
                .catalog_manager
                .table(catalog_name, schema_name, table_name)
                .await
                .context(CatalogSnafu)?;
            match table {
                Some(table) => {
                    self.alter_table_on_demand(catalog_name, schema_name, table, req, ctx.clone())
                        .await?
                }
                None => {
                    self.create_table(catalog_name, schema_name, table_name, req, ctx.clone())
                        .await?
                }
            }
        }

        Ok(())
    }

    async fn create_table(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        req: &RowInsertRequest,
        ctx: QueryContextRef,
    ) -> Result<()> {
        let (column_schemas, _) = extract_schema_and_rows(req)?;
        let create_table_expr = self
            .create_expr_factory
            .create_table_expr_by_column_schemas(
                catalog_name,
                schema_name,
                table_name,
                column_schemas,
                &self.engine_name,
            )?;

        let req = Request::Ddl(DdlRequest {
            expr: Some(Expr::CreateTable(create_table_expr)),
        });
        self.grpc_query_handler.do_query(req, ctx).await?;

        Ok(())
    }

    async fn alter_table_on_demand(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table: TableRef,
        req: &RowInsertRequest,
        ctx: QueryContextRef,
    ) -> Result<()> {
        let (column_schemas, _) = extract_schema_and_rows(req)?;
        let column_exprs = ColumnExpr::from_column_schemas(column_schemas);
        let add_columns = extract_new_columns(&table.schema(), column_exprs)
            .context(FindNewColumnsOnInsertionSnafu)?;
        let Some(add_columns) = add_columns else {
            return Ok(());
        };
        let table_name = table.table_info().name.clone();

        info!(
            "Adding new columns: {:?} to table: {}.{}.{}",
            add_columns, catalog_name, schema_name, table_name
        );

        let alter_table_expr = AlterExpr {
            catalog_name: catalog_name.to_string(),
            schema_name: schema_name.to_string(),
            table_name,
            kind: Some(Kind::AddColumns(add_columns)),
            ..Default::default()
        };

        let req = Request::Ddl(DdlRequest {
            expr: Some(Expr::Alter(alter_table_expr)),
        });
        self.grpc_query_handler.do_query(req, ctx).await?;

        Ok(())
    }
}

fn extract_schema_and_rows(req: &RowInsertRequest) -> Result<(&[ColumnSchema], &[Row])> {
    let rows = req.rows.as_ref().context(EmptyDataSnafu {
        msg: format!("insert to table: {:?}", &req.table_name),
    })?;
    let schema = &rows.schema;
    let rows = &rows.rows;

    ensure!(
        !rows.is_empty(),
        EmptyDataSnafu {
            msg: format!("insert to table: {:?}", &req.table_name),
        }
    );

    Ok((schema, rows))
}
