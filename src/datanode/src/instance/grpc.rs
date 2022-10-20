use api::v1::{
    admin_expr, codec::InsertBatch, insert_expr, object_expr, select_expr, AdminExpr, AdminResult,
    ObjectExpr, ObjectResult, SelectExpr,
};
use async_trait::async_trait;
use catalog::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_error::status_code::StatusCode;
use common_query::Output;
use common_telemetry::logging::{debug, info};
use servers::query_handler::{GrpcAdminHandler, GrpcQueryHandler};
use snafu::prelude::*;
use table::requests::AddColumnRequest;

use crate::error::{
    self, CatalogSnafu, FindTableSnafu, InsertSnafu, Result, TableNotFoundSnafu,
    UnsupportedExprSnafu,
};
use crate::instance::Instance;
use crate::server::grpc::handler::{build_err_result, ObjectResultBuilder};
use crate::server::grpc::insert::{self, insertion_expr_to_request};
use crate::server::grpc::plan::PhysicalPlanner;
use crate::server::grpc::select::to_object_result;
use crate::sql::SqlRequest;

impl Instance {
    async fn add_new_columns_to_table(
        &self,
        table_name: &str,
        add_columns: Vec<AddColumnRequest>,
    ) -> Result<()> {
        let column_names = add_columns
            .iter()
            .map(|req| req.column_schema.name.clone())
            .collect::<Vec<_>>();

        let alter_request = insert::build_alter_table_request(table_name, add_columns);

        debug!(
            "Adding new columns: {:?} to table: {}",
            column_names, table_name
        );

        let _result = self
            .sql_handler()
            .execute(SqlRequest::Alter(alter_request))
            .await?;

        info!(
            "Added new columns: {:?} to table: {}",
            column_names, table_name
        );
        Ok(())
    }

    async fn create_table_by_insert_batches(
        &self,
        table_name: &str,
        insert_batches: &[InsertBatch],
    ) -> Result<()> {
        // Create table automatically, build schema from data.
        let table_id = self.catalog_manager.next_table_id().await;
        let create_table_request =
            insert::build_create_table_request(table_id, table_name, insert_batches)?;

        info!(
            "Try to create table: {} automatically with request: {:?}",
            table_name, create_table_request,
        );

        let _result = self
            .sql_handler()
            .execute(SqlRequest::Create(create_table_request))
            .await?;

        info!("Success to create table: {} automatically", table_name);

        Ok(())
    }

    pub async fn execute_grpc_insert(
        &self,
        table_name: &str,
        values: insert_expr::Values,
    ) -> Result<Output> {
        let schema_provider = self
            .catalog_manager
            .catalog(DEFAULT_CATALOG_NAME)
            .expect("datafusion does not accept fallible catalog access")
            .unwrap()
            .schema(DEFAULT_SCHEMA_NAME)
            .expect("datafusion does not accept fallible catalog access")
            .unwrap();

        let insert_batches = insert::insert_batches(values.values)?;
        ensure!(!insert_batches.is_empty(), error::IllegalInsertDataSnafu);

        let table = if let Some(table) = schema_provider
            .table(table_name)
            .context(FindTableSnafu { table_name })?
        {
            let schema = table.schema();
            if let Some(add_columns) = insert::find_new_columns(&schema, &insert_batches)? {
                self.add_new_columns_to_table(table_name, add_columns)
                    .await?;
            }

            table
        } else {
            self.create_table_by_insert_batches(table_name, &insert_batches)
                .await?;

            schema_provider
                .table(table_name)
                .context(CatalogSnafu)?
                .context(TableNotFoundSnafu { table_name })?
        };

        let insert = insertion_expr_to_request(table_name, insert_batches, table.clone())?;

        let affected_rows = table
            .insert(insert)
            .await
            .context(InsertSnafu { table_name })?;

        Ok(Output::AffectedRows(affected_rows))
    }

    async fn handle_insert(&self, table_name: &str, values: insert_expr::Values) -> ObjectResult {
        match self.execute_grpc_insert(table_name, values).await {
            Ok(Output::AffectedRows(rows)) => ObjectResultBuilder::new()
                .status_code(StatusCode::Success as u32)
                .mutate_result(rows as u32, 0)
                .build(),
            Err(err) => {
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
}

#[async_trait]
impl GrpcQueryHandler for Instance {
    async fn do_query(&self, query: ObjectExpr) -> servers::error::Result<ObjectResult> {
        let object_resp = match query.expr {
            Some(object_expr::Expr::Insert(insert_expr)) => {
                let table_name = &insert_expr.table_name;
                let expr = insert_expr
                    .expr
                    .context(servers::error::InvalidQuerySnafu {
                        reason: "missing `expr` in `InsertExpr`",
                    })?;
                match expr {
                    insert_expr::Expr::Values(values) => {
                        self.handle_insert(table_name, values).await
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
