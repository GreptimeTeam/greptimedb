use api::v1::ddl_request::Expr;
use api::v1::greptime_request::Request;
use api::v1::{DdlRequest, RowInsertRequest, RowInsertRequests};
use catalog::CatalogManagerRef;
use common_grpc_expr::ColumnExpr;
use common_query::Output;
use servers::query_handler::grpc::GrpcQueryHandlerRef;
use session::context::QueryContextRef;
use snafu::ResultExt;
use table::TableRef;

use crate::error::{CatalogSnafu, Error, Result};
use crate::expr_factory::CreateExprFactoryRef;

pub struct RowsInserter {
    engine_name: String,
    catalog_manager: CatalogManagerRef,
    create_expr_factory: CreateExprFactoryRef,
    grpc_query_handler: GrpcQueryHandlerRef<Error>,
}

impl RowsInserter {
    pub async fn handle_inserts(
        &self,
        ctx: QueryContextRef,
        requests: RowInsertRequests,
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

        let mut not_exist_tables = Vec::new();
        let mut tables = Vec::new();
        for req in &requests.inserts {
            let table_name = &req.table_name;
            // TODO(jeremy): get tables in batch?
            let table = self
                .catalog_manager
                .table(&catalog_name, &schema_name, table_name)
                .await
                .context(CatalogSnafu)?;
            match table {
                Some(table) => tables.push((table, req)),
                None => not_exist_tables.push((table_name.clone(), req)),
            }
        }

        if !not_exist_tables.is_empty() {
            self.create_tables(&catalog_name, &schema_name, not_exist_tables, ctx.clone())
                .await?;
        }

        if !tables.is_empty() {
            self.alter_tables_on_demand(&catalog_name, &schema_name, tables, ctx.clone())
                .await?;
        }

        Ok(())
    }

    async fn create_tables(
        &self,
        catalog_name: &str,
        schema_name: &str,
        tables: Vec<(String, &RowInsertRequest)>,
        ctx: QueryContextRef,
    ) -> Result<()> {
        let mut create_table_exprs = Vec::with_capacity(tables.len());
        for (table_name, req) in tables {
            // If there is no data, leave it to the data verification process to handle,
            // it is more inclined to first complete the table creation.
            let Some(rows) = &req.rows else { continue; };
            let column_exprs = ColumnExpr::from_column_schemas(&rows.schema);
            let create_table_expr = self.create_expr_factory.create_table_expr(
                catalog_name,
                schema_name,
                &table_name,
                column_exprs,
                &self.engine_name,
            )?;
            create_table_exprs.push(create_table_expr);
        }

        // TODO(jeremy): create tables in batch
        for create_table_expr in create_table_exprs {
            let req = Request::Ddl(DdlRequest {
                expr: Some(Expr::CreateTable(create_table_expr)),
            });
            self.grpc_query_handler.do_query(req, ctx.clone()).await?;
        }

        Ok(())
    }

    async fn alter_tables_on_demand(
        &self,
        _catalog_name: &str,
        _schema_name: &str,
        _tables: Vec<(TableRef, &RowInsertRequest)>,
        _ctx: QueryContextRef,
    ) -> Result<()> {
        // TODO(jeremy): create tables in batch
        Ok(())
    }
}
