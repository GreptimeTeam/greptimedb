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

use std::collections::HashMap;
use std::sync::Arc;

use api::v1::alter_expr::Kind;
use api::v1::region::{InsertRequests as RegionInsertRequests, RegionRequestHeader};
use api::v1::{
    AlterExpr, ColumnSchema, CreateTableExpr, InsertRequests, RowInsertRequest, RowInsertRequests,
};
use catalog::CatalogManagerRef;
use common_catalog::consts::default_engine;
use common_grpc_expr::util::{extract_new_columns, ColumnExpr};
use common_meta::datanode_manager::{AffectedRows, DatanodeManagerRef};
use common_meta::peer::Peer;
use common_query::Output;
use common_telemetry::tracing_context::TracingContext;
use common_telemetry::{error, info};
use datatypes::schema::Schema;
use futures_util::future;
use meter_macros::write_meter;
use partition::manager::PartitionRuleManagerRef;
use session::context::QueryContextRef;
use snafu::prelude::*;
use sql::statements::insert::Insert;
use table::engine::TableReference;
use table::requests::InsertRequest as TableInsertRequest;
use table::TableRef;

use crate::error::{
    CatalogSnafu, FindNewColumnsOnInsertionSnafu, FindRegionLeaderSnafu, InvalidInsertRequestSnafu,
    JoinTaskSnafu, RequestInsertsSnafu, Result, TableNotFoundSnafu,
};
use crate::expr_factory::CreateExprFactory;
use crate::region_req_factory::RegionRequestFactory;
use crate::req_convert::insert::{ColumnToRow, RowToRegion, StatementToRegion, TableToRegion};
use crate::statement::StatementExecutor;

pub struct Inserter {
    catalog_manager: CatalogManagerRef,
    partition_manager: PartitionRuleManagerRef,
    datanode_manager: DatanodeManagerRef,
}

pub type InserterRef = Arc<Inserter>;

impl Inserter {
    pub fn new(
        catalog_manager: CatalogManagerRef,
        partition_manager: PartitionRuleManagerRef,
        datanode_manager: DatanodeManagerRef,
    ) -> Self {
        Self {
            catalog_manager,
            partition_manager,
            datanode_manager,
        }
    }

    pub async fn handle_column_inserts(
        &self,
        requests: InsertRequests,
        ctx: QueryContextRef,
        statement_executor: &StatementExecutor,
    ) -> Result<Output> {
        let row_inserts = ColumnToRow::convert(requests)?;
        self.handle_row_inserts(row_inserts, ctx, statement_executor)
            .await
    }

    pub async fn handle_row_inserts(
        &self,
        mut requests: RowInsertRequests,
        ctx: QueryContextRef,
        statement_executor: &StatementExecutor,
    ) -> Result<Output> {
        // remove empty requests
        requests.inserts.retain(|req| {
            req.rows
                .as_ref()
                .map(|r| !r.rows.is_empty())
                .unwrap_or_default()
        });
        validate_column_count_match(&requests)?;

        self.create_or_alter_tables_on_demand(&requests, &ctx, statement_executor)
            .await?;
        let inserts = RowToRegion::new(
            self.catalog_manager.as_ref(),
            self.partition_manager.as_ref(),
            &ctx,
        )
        .convert(requests)
        .await?;

        let affected_rows = self.do_request(inserts, &ctx).await?;
        Ok(Output::AffectedRows(affected_rows as _))
    }

    pub async fn handle_table_insert(
        &self,
        request: TableInsertRequest,
        ctx: QueryContextRef,
    ) -> Result<usize> {
        let catalog = request.catalog_name.as_str();
        let schema = request.schema_name.as_str();
        let table_name = request.table_name.as_str();
        let table = self.get_table(catalog, schema, table_name).await?;
        let table = table.with_context(|| TableNotFoundSnafu {
            table_name: common_catalog::format_full_table_name(catalog, schema, table_name),
        })?;
        let table_info = table.table_info();

        let inserts = TableToRegion::new(&table_info, &self.partition_manager)
            .convert(request)
            .await?;

        let affected_rows = self.do_request(inserts, &ctx).await?;
        Ok(affected_rows as _)
    }

    pub async fn handle_statement_insert(
        &self,
        insert: &Insert,
        ctx: &QueryContextRef,
    ) -> Result<Output> {
        let inserts =
            StatementToRegion::new(self.catalog_manager.as_ref(), &self.partition_manager, ctx)
                .convert(insert)
                .await?;

        let affected_rows = self.do_request(inserts, ctx).await?;
        Ok(Output::AffectedRows(affected_rows as _))
    }
}

impl Inserter {
    async fn do_request(
        &self,
        requests: RegionInsertRequests,
        ctx: &QueryContextRef,
    ) -> Result<AffectedRows> {
        write_meter!(ctx.current_catalog(), ctx.current_schema(), requests);
        let request_factory = RegionRequestFactory::new(RegionRequestHeader {
            tracing_context: TracingContext::from_current_span().to_w3c(),
            dbname: ctx.get_db_string(),
        });

        let tasks = self
            .group_requests_by_peer(requests)
            .await?
            .into_iter()
            .map(|(peer, inserts)| {
                let request = request_factory.build_insert(inserts);
                let datanode_manager = self.datanode_manager.clone();
                common_runtime::spawn_write(async move {
                    datanode_manager
                        .datanode(&peer)
                        .await
                        .handle(request)
                        .await
                        .context(RequestInsertsSnafu)
                })
            });
        let results = future::try_join_all(tasks).await.context(JoinTaskSnafu)?;

        let affected_rows = results.into_iter().sum::<Result<u64>>()?;
        crate::metrics::DIST_INGEST_ROW_COUNT.inc_by(affected_rows);
        Ok(affected_rows)
    }

    async fn group_requests_by_peer(
        &self,
        requests: RegionInsertRequests,
    ) -> Result<HashMap<Peer, RegionInsertRequests>> {
        let mut inserts: HashMap<Peer, RegionInsertRequests> = HashMap::new();

        for req in requests.requests {
            let peer = self
                .partition_manager
                .find_region_leader(req.region_id.into())
                .await
                .context(FindRegionLeaderSnafu)?;
            inserts.entry(peer).or_default().requests.push(req);
        }

        Ok(inserts)
    }

    // check if tables already exist:
    // - if table does not exist, create table by inferred CreateExpr
    // - if table exist, check if schema matches. If any new column found, alter table by inferred `AlterExpr`
    async fn create_or_alter_tables_on_demand(
        &self,
        requests: &RowInsertRequests,
        ctx: &QueryContextRef,
        statement_executor: &StatementExecutor,
    ) -> Result<()> {
        // TODO(jeremy): create and alter in batch?
        for req in &requests.inserts {
            let catalog = ctx.current_catalog();
            let schema = ctx.current_schema();
            let table = self.get_table(catalog, schema, &req.table_name).await?;
            match table {
                Some(table) => {
                    validate_request_with_table(req, &table)?;
                    self.alter_table_on_demand(req, table, ctx, statement_executor)
                        .await?
                }
                None => self.create_table(req, ctx, statement_executor).await?,
            }
        }

        Ok(())
    }

    async fn get_table(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<Option<TableRef>> {
        self.catalog_manager
            .table(catalog, schema, table)
            .await
            .context(CatalogSnafu)
    }

    async fn alter_table_on_demand(
        &self,
        req: &RowInsertRequest,
        table: TableRef,
        ctx: &QueryContextRef,
        statement_executor: &StatementExecutor,
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

        let res = statement_executor.alter_table_inner(alter_table_expr).await;

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

    async fn create_table(
        &self,
        req: &RowInsertRequest,
        ctx: &QueryContextRef,
        statement_executor: &StatementExecutor,
    ) -> Result<()> {
        let table_ref =
            TableReference::full(ctx.current_catalog(), ctx.current_schema(), &req.table_name);

        let request_schema = req.rows.as_ref().unwrap().schema.as_slice();
        let create_table_expr = &mut build_create_table_expr(&table_ref, request_schema)?;

        info!(
            "Table {}.{}.{} does not exist, try create table",
            table_ref.catalog, table_ref.schema, table_ref.table,
        );

        // TODO(weny): multiple regions table.
        let res = statement_executor
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
        rows.rows.iter().try_for_each(|r| {
            ensure!(
                r.values.len() == column_count,
                InvalidInsertRequestSnafu {
                    reason: format!(
                        "column count mismatch, columns: {}, values: {}",
                        column_count,
                        r.values.len()
                    )
                }
            );
            Ok(())
        })?;
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
