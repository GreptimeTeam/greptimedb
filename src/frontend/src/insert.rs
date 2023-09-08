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
use api::v1::region::{region_request, RegionRequest, RegionRequestHeader};
use api::v1::{
    AlterExpr, ColumnSchema, CreateTableExpr, InsertRequests, RowInsertRequest, RowInsertRequests,
};
use catalog::CatalogManager;
use client::region_handler::RegionRequestHandler;
use common_catalog::consts::default_engine;
use common_grpc_expr::util::{extract_new_columns, ColumnExpr};
use common_query::Output;
use common_telemetry::{error, info};
use datatypes::schema::Schema;
use session::context::QueryContextRef;
use snafu::prelude::*;
use table::engine::TableReference;
use table::TableRef;

use crate::error::{
    CatalogSnafu, FindNewColumnsOnInsertionSnafu, InvalidInsertRequestSnafu, RequestDatanodeSnafu,
    Result,
};
use crate::expr_factory::CreateExprFactory;
use crate::req_convert::insert::{ColumnToRow, RowToRegion};
use crate::statement::StatementExecutor;

pub(crate) struct Inserter<'a> {
    catalog_manager: &'a dyn CatalogManager,
    statement_executor: &'a StatementExecutor,
    region_request_handler: &'a dyn RegionRequestHandler,
}

impl<'a> Inserter<'a> {
    pub fn new(
        catalog_manager: &'a dyn CatalogManager,
        statement_executor: &'a StatementExecutor,
        region_request_handler: &'a dyn RegionRequestHandler,
    ) -> Self {
        Self {
            catalog_manager,
            statement_executor,
            region_request_handler,
        }
    }

    pub async fn handle_column_inserts(
        &self,
        requests: InsertRequests,
        ctx: QueryContextRef,
    ) -> Result<Output> {
        let row_inserts = ColumnToRow::convert(requests)?;
        self.handle_row_inserts(row_inserts, ctx).await
    }

    pub async fn handle_row_inserts(
        &self,
        mut requests: RowInsertRequests,
        ctx: QueryContextRef,
    ) -> Result<Output> {
        // remove empty requests
        requests.inserts.retain(|req| {
            req.rows
                .as_ref()
                .map(|r| !r.rows.is_empty())
                .unwrap_or_default()
        });
        validate_column_count_match(&requests)?;

        self.create_or_alter_tables_on_demand(&requests, &ctx)
            .await?;
        let inserts = RowToRegion::new(self.catalog_manager, &ctx)
            .convert(requests)
            .await?;
        let region_request = RegionRequest {
            header: Some(RegionRequestHeader {
                trace_id: ctx.trace_id(),
                span_id: 0,
            }),
            body: Some(region_request::Body::Inserts(inserts)),
        };

        let affected_rows = self
            .region_request_handler
            .handle(region_request)
            .await
            .context(RequestDatanodeSnafu)?;
        Ok(Output::AffectedRows(affected_rows as _))
    }
}

impl<'a> Inserter<'a> {
    // check if tables already exist:
    // - if table does not exist, create table by inferred CreateExpr
    // - if table exist, check if schema matches. If any new column found, alter table by inferred `AlterExpr`
    async fn create_or_alter_tables_on_demand(
        &self,
        requests: &RowInsertRequests,
        ctx: &QueryContextRef,
    ) -> Result<()> {
        // TODO(jeremy): create and alter in batch?
        for req in &requests.inserts {
            match self.get_table(req, ctx).await? {
                Some(table) => {
                    validate_request_with_table(req, &table)?;
                    self.alter_table_on_demand(req, table, ctx).await?
                }
                None => self.create_table(req, ctx).await?,
            }
        }

        Ok(())
    }

    async fn get_table(
        &self,
        req: &RowInsertRequest,
        ctx: &QueryContextRef,
    ) -> Result<Option<TableRef>> {
        self.catalog_manager
            .table(ctx.current_catalog(), ctx.current_schema(), &req.table_name)
            .await
            .context(CatalogSnafu)
    }

    async fn alter_table_on_demand(
        &self,
        req: &RowInsertRequest,
        table: TableRef,
        ctx: &QueryContextRef,
    ) -> Result<()> {
        let catalog_name = ctx.current_catalog();
        let schema_name = ctx.current_schema();
        let table_name = table.table_info().name.clone();

        let request_schema = req.rows.as_ref().unwrap().schema.as_slice();
        let column_exprs = ColumnExpr::from_column_schemas(request_schema);
        let add_columns = extract_new_columns(&table.schema(), column_exprs)
            .context(FindNewColumnsOnInsertionSnafu)?;
        let Some(add_columns) = add_columns else {
            return Ok(());
        };

        info!(
            "Adding new columns: {:?} to table: {}.{}.{}",
            add_columns, catalog_name, schema_name, table_name
        );

        let alter_table_expr = AlterExpr {
            catalog_name: catalog_name.to_string(),
            schema_name: schema_name.to_string(),
            table_name: table_name.to_string(),
            kind: Some(Kind::AddColumns(add_columns)),
        };

        let res = self
            .statement_executor
            .alter_table_inner(alter_table_expr)
            .await;

        match res {
            Ok(_) => {
                info!(
                    "Successfully added new columns to table: {}.{}.{}",
                    catalog_name, schema_name, table_name
                );
                Ok(())
            }
            Err(err) => {
                error!(
                    "Failed to add new columns to table: {}.{}.{}: {}",
                    catalog_name, schema_name, table_name, err
                );
                Err(err)
            }
        }
    }

    async fn create_table(&self, req: &RowInsertRequest, ctx: &QueryContextRef) -> Result<()> {
        let table_ref =
            TableReference::full(ctx.current_catalog(), ctx.current_schema(), &req.table_name);

        let request_schema = req.rows.as_ref().unwrap().schema.as_slice();
        let create_table_expr = &mut build_create_table_expr(&table_ref, request_schema)?;

        info!(
            "Table {}.{}.{} does not exist, try create table",
            table_ref.catalog, table_ref.schema, table_ref.table,
        );

        // TODO(weny): multiple regions table.
        let res = self
            .statement_executor
            .create_table_inner(create_table_expr, None)
            .await;

        match res {
            Ok(_) => {
                info!(
                    "Successfully created table {}.{}.{}",
                    table_ref.catalog, table_ref.schema, table_ref.table,
                );
                Ok(())
            }
            Err(err) => {
                error!(
                    "Failed to create table {}.{}.{}: {}",
                    table_ref.catalog, table_ref.schema, table_ref.table, err
                );
                Err(err)
            }
        }
    }
}

fn validate_column_count_match(requests: &RowInsertRequests) -> Result<()> {
    for request in &requests.inserts {
        let rows = request.rows.as_ref().unwrap();
        let column_count = rows.schema.len();
        ensure!(
            rows.rows.iter().all(|r| r.values.len() == column_count),
            InvalidInsertRequestSnafu {
                reason: "column count mismatch"
            }
        )
    }
    Ok(())
}

fn validate_request_with_table(req: &RowInsertRequest, table: &TableRef) -> Result<()> {
    let request_schema = req.rows.as_ref().unwrap().schema.as_slice();
    let table_schema = table.schema();

    validate_required_columns(request_schema, &table_schema)?;

    Ok(())
}

fn validate_required_columns(request_schema: &[ColumnSchema], table_schema: &Schema) -> Result<()> {
    for column_schema in table_schema.column_schemas() {
        if column_schema.is_nullable() || column_schema.default_constraint().is_some() {
            continue;
        }
        if !request_schema
            .iter()
            .any(|c| c.column_name == column_schema.name)
        {
            return InvalidInsertRequestSnafu {
                reason: format!(
                    "Expecting insert data to be presented on a not null or no default value column '{}'.",
                    &column_schema.name
                )
            }.fail();
        }
    }
    Ok(())
}

fn build_create_table_expr(
    table: &TableReference,
    request_schema: &[ColumnSchema],
) -> Result<CreateTableExpr> {
    CreateExprFactory.create_table_expr_by_column_schemas(table, request_schema, default_engine())
}

#[cfg(test)]
mod tests {
    use datatypes::prelude::{ConcreteDataType, Value as DtValue};
    use datatypes::schema::{ColumnDefaultConstraint, ColumnSchema as DtColumnSchema};

    use super::*;

    #[test]
    fn test_validate_required_columns() {
        let schema = Schema::new(vec![
            DtColumnSchema::new("a", ConcreteDataType::int32_datatype(), true)
                .with_default_constraint(None)
                .unwrap(),
            DtColumnSchema::new("b", ConcreteDataType::int32_datatype(), true)
                .with_default_constraint(Some(ColumnDefaultConstraint::Value(DtValue::Int32(100))))
                .unwrap(),
        ]);
        let request_schema = &[ColumnSchema {
            column_name: "c".to_string(),
            ..Default::default()
        }];
        // If nullable is true, it doesn't matter whether the insert request has the column.
        validate_required_columns(request_schema, &schema).unwrap();

        let schema = Schema::new(vec![
            DtColumnSchema::new("a", ConcreteDataType::int32_datatype(), false)
                .with_default_constraint(None)
                .unwrap(),
            DtColumnSchema::new("b", ConcreteDataType::int32_datatype(), false)
                .with_default_constraint(Some(ColumnDefaultConstraint::Value(DtValue::Int32(-100))))
                .unwrap(),
        ]);
        let request_schema = &[ColumnSchema {
            column_name: "a".to_string(),
            ..Default::default()
        }];
        // If nullable is false, but the column is defined with default value,
        // it also doesn't matter whether the insert request has the column.
        validate_required_columns(request_schema, &schema).unwrap();

        let request_schema = &[ColumnSchema {
            column_name: "b".to_string(),
            ..Default::default()
        }];
        // Neither of the above cases.
        assert!(validate_required_columns(request_schema, &schema).is_err());
    }
}
