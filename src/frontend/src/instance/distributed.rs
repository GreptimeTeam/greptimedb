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

pub(crate) mod inserter;

use std::collections::HashMap;
use std::sync::Arc;

use api::helper::ColumnDataTypeWrapper;
use api::v1::ddl_request::Expr as DdlExpr;
use api::v1::greptime_request::Request;
use api::v1::{
    column_def, AlterExpr, CompactTableExpr, CreateDatabaseExpr, CreateTableExpr, DeleteRequest,
    FlushTableExpr, InsertRequests, TruncateTableExpr,
};
use async_trait::async_trait;
use catalog::{CatalogManager, DeregisterTableRequest, RegisterTableRequest};
use chrono::DateTime;
use client::client_manager::DatanodeClients;
use client::Database;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_catalog::format_full_table_name;
use common_error::ext::BoxedError;
use common_meta::key::schema_name::SchemaNameKey;
use common_meta::peer::Peer;
use common_meta::rpc::ddl::{DdlTask, SubmitDdlTaskRequest, SubmitDdlTaskResponse};
use common_meta::rpc::router::{Partition, Partition as MetaPartition, RouteRequest};
use common_meta::table_name::TableName;
use common_query::Output;
use common_telemetry::{debug, info};
use datanode::instance::sql::table_idents_to_full_name;
use datanode::sql::SqlHandler;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::RawSchema;
use meta_client::client::MetaClient;
use partition::manager::PartitionInfo;
use partition::partition::{PartitionBound, PartitionDef};
use query::error::QueryExecutionSnafu;
use query::query_engine::SqlStatementExecutor;
use servers::query_handler::grpc::GrpcQueryHandler;
use session::context::QueryContextRef;
use snafu::{ensure, OptionExt, ResultExt};
use sql::ast::{Ident, Value as SqlValue};
use sql::statements::create::{PartitionEntry, Partitions};
use sql::statements::statement::Statement;
use sql::statements::{self, sql_value_to_value};
use store_api::storage::RegionNumber;
use table::engine::TableReference;
use table::metadata::{RawTableInfo, RawTableMeta, TableId, TableIdent, TableInfo, TableType};
use table::requests::{AlterTableRequest, TableOptions};
use table::TableRef;

use crate::catalog::FrontendCatalogManager;
use crate::error::{
    self, AlterExprToRequestSnafu, CatalogSnafu, ColumnDataTypeSnafu, ColumnNotFoundSnafu,
    DeserializePartitionSnafu, InvokeDatanodeSnafu, NotSupportedSnafu, ParseSqlSnafu,
    RequestDatanodeSnafu, RequestMetaSnafu, Result, SchemaExistsSnafu, TableAlreadyExistSnafu,
    TableNotFoundSnafu, TableSnafu, ToTableDeleteRequestSnafu, UnrecognizedTableOptionSnafu,
};
use crate::expr_factory;
use crate::instance::distributed::inserter::DistInserter;
use crate::table::DistTable;

const MAX_VALUE: &str = "MAXVALUE";

#[derive(Clone)]
pub struct DistInstance {
    meta_client: Arc<MetaClient>,
    pub(crate) catalog_manager: Arc<FrontendCatalogManager>,
    datanode_clients: Arc<DatanodeClients>,
}

impl DistInstance {
    pub fn new(
        meta_client: Arc<MetaClient>,
        catalog_manager: Arc<FrontendCatalogManager>,
        datanode_clients: Arc<DatanodeClients>,
    ) -> Self {
        Self {
            meta_client,
            catalog_manager,
            datanode_clients,
        }
    }

    pub async fn create_table(
        &self,
        create_table: &mut CreateTableExpr,
        partitions: Option<Partitions>,
    ) -> Result<TableRef> {
        let _timer = common_telemetry::timer!(crate::metrics::DIST_CREATE_TABLE);
        let table_name = TableName::new(
            &create_table.catalog_name,
            &create_table.schema_name,
            &create_table.table_name,
        );

        let (partitions, partition_cols) = parse_partitions(create_table, partitions)?;

        let mut table_info = create_table_info(create_table, partition_cols)?;

        let resp = self
            .create_table_procedure(create_table, partitions, table_info.clone())
            .await?;

        let table_id = resp.table_id.context(error::UnexpectedSnafu {
            violated: "expected table_id",
        })?;
        info!("Successfully created distributed table '{table_name}' with table id {table_id}");

        table_info.ident.table_id = table_id;
        let table_info = Arc::new(table_info.try_into().context(error::CreateTableInfoSnafu)?);

        create_table.table_id = Some(api::v1::TableId { id: table_id });

        let table = Arc::new(DistTable::new(
            table_name.clone(),
            table_info,
            self.catalog_manager.clone(),
        ));

        let request = RegisterTableRequest {
            catalog: table_name.catalog_name.clone(),
            schema: table_name.schema_name.clone(),
            table_name: table_name.table_name.clone(),
            table_id,
            table: table.clone(),
        };
        ensure!(
            self.catalog_manager
                .register_table(request)
                .await
                .context(CatalogSnafu)?,
            TableAlreadyExistSnafu {
                table: table_name.to_string()
            }
        );

        // Since the table information created on meta does not go through KvBackend, so we
        // manually invalidate the cache here.
        //
        // TODO(fys): when the meta invalidation cache mechanism is established, remove it.
        self.catalog_manager
            .invalidate_table(
                &table_name.catalog_name,
                &table_name.schema_name,
                &table_name.table_name,
                table_id,
            )
            .await;

        Ok(table)
    }

    async fn drop_table(&self, table_name: TableName) -> Result<Output> {
        let table = self
            .catalog_manager
            .table(
                &table_name.catalog_name,
                &table_name.schema_name,
                &table_name.table_name,
            )
            .await
            .context(CatalogSnafu)?
            .with_context(|| TableNotFoundSnafu {
                table_name: table_name.to_string(),
            })?;
        let table_id = table.table_info().table_id();

        self.drop_table_procedure(&table_name, table_id).await?;

        let request = DeregisterTableRequest {
            catalog: table_name.catalog_name.clone(),
            schema: table_name.schema_name.clone(),
            table_name: table_name.table_name.clone(),
        };
        self.catalog_manager
            .deregister_table(request)
            .await
            .context(CatalogSnafu)?;

        // Since the table information dropped on meta does not go through KvBackend, so we
        // manually invalidate the cache here.
        //
        // TODO(fys): when the meta invalidation cache mechanism is established, remove it.
        self.catalog_manager()
            .invalidate_table(
                &table_name.catalog_name,
                &table_name.schema_name,
                &table_name.table_name,
                table_id,
            )
            .await;

        Ok(Output::AffectedRows(1))
    }

    async fn flush_table(
        &self,
        table_name: TableName,
        region_number: Option<RegionNumber>,
    ) -> Result<Output> {
        let candidates = self
            .find_flush_or_compaction_candidates(&table_name, region_number)
            .await?;

        let expr = FlushTableExpr {
            catalog_name: table_name.catalog_name.clone(),
            schema_name: table_name.schema_name.clone(),
            table_name: table_name.table_name.clone(),
            region_number,
            ..Default::default()
        };

        for candidate in candidates {
            debug!("Flushing table {table_name} on Datanode {candidate:?}");

            let client = self.datanode_clients.get_client(&candidate).await;
            let client = Database::new(&expr.catalog_name, &expr.schema_name, client);
            client
                .flush_table(expr.clone())
                .await
                .context(RequestDatanodeSnafu)?;
        }

        Ok(Output::AffectedRows(0))
    }

    async fn compact_table(
        &self,
        table_name: TableName,
        region_number: Option<RegionNumber>,
    ) -> Result<Output> {
        let candidates = self
            .find_flush_or_compaction_candidates(&table_name, region_number)
            .await?;

        let expr = CompactTableExpr {
            catalog_name: table_name.catalog_name.clone(),
            schema_name: table_name.schema_name.clone(),
            table_name: table_name.table_name.clone(),
            region_number,
        };

        for candidate in candidates {
            debug!("Compacting table {table_name} on Datanode {candidate:?}");

            let client = self.datanode_clients.get_client(&candidate).await;
            let client = Database::new(&expr.catalog_name, &expr.schema_name, client);
            client
                .compact_table(expr.clone())
                .await
                .context(RequestDatanodeSnafu)?;
        }
        Ok(Output::AffectedRows(0))
    }

    async fn find_flush_or_compaction_candidates(
        &self,
        table_name: &TableName,
        region_number: Option<RegionNumber>,
    ) -> Result<Vec<Peer>> {
        let table = self
            .catalog_manager
            .table(
                &table_name.catalog_name,
                &table_name.schema_name,
                &table_name.table_name,
            )
            .await
            .context(CatalogSnafu)?
            .with_context(|| TableNotFoundSnafu {
                table_name: table_name.to_string(),
            })?;
        let table_id = table.table_info().table_id();

        let route_response = self
            .meta_client
            .route(RouteRequest {
                table_ids: vec![table_id],
            })
            .await
            .context(RequestMetaSnafu)?;

        let res = route_response
            .table_routes
            .iter()
            .filter(|route| {
                route.region_routes.iter().any(|r| {
                    let Some(n) = region_number else {
                        return true;
                    };
                    n == r.region.id.region_number()
                })
            })
            .flat_map(|route| route.find_leaders().into_iter())
            .collect::<Vec<_>>();
        Ok(res)
    }

    async fn truncate_table(&self, table_name: TableName) -> Result<Output> {
        let table = self
            .catalog_manager
            .table(
                &table_name.catalog_name,
                &table_name.schema_name,
                &table_name.table_name,
            )
            .await
            .context(CatalogSnafu)?
            .with_context(|| TableNotFoundSnafu {
                table_name: table_name.to_string(),
            })?;
        let table_id = table.table_info().ident.table_id;

        let expr = TruncateTableExpr {
            catalog_name: table_name.catalog_name.clone(),
            schema_name: table_name.schema_name.clone(),
            table_name: table_name.table_name.clone(),
            table_id: Some(api::v1::TableId { id: table_id }),
        };
        self.truncate_table_procedure(&expr).await?;

        Ok(Output::AffectedRows(0))
    }

    async fn handle_statement(
        &self,
        stmt: Statement,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        match stmt {
            Statement::CreateDatabase(stmt) => {
                let expr = CreateDatabaseExpr {
                    database_name: stmt.name.to_string(),
                    create_if_not_exists: stmt.if_not_exists,
                };
                self.handle_create_database(expr, query_ctx).await
            }
            Statement::CreateTable(stmt) => {
                let create_expr = &mut expr_factory::create_to_expr(&stmt, query_ctx)?;
                let _ = self.create_table(create_expr, stmt.partitions).await?;
                Ok(Output::AffectedRows(0))
            }
            Statement::CreateExternalTable(stmt) => {
                let create_expr = &mut expr_factory::create_external_expr(stmt, query_ctx).await?;
                let _ = self.create_table(create_expr, None).await?;
                Ok(Output::AffectedRows(0))
            }
            Statement::Alter(alter_table) => {
                let expr = expr_factory::to_alter_expr(alter_table, query_ctx)?;
                self.handle_alter_table(expr).await
            }
            Statement::DropTable(stmt) => {
                let (catalog, schema, table) =
                    table_idents_to_full_name(stmt.table_name(), query_ctx)
                        .map_err(BoxedError::new)
                        .context(error::ExternalSnafu)?;
                let table_name = TableName::new(catalog, schema, table);
                self.drop_table(table_name).await
            }
            Statement::Insert(insert) => {
                let (catalog, schema, table) =
                    table_idents_to_full_name(insert.table_name(), query_ctx.clone())
                        .map_err(BoxedError::new)
                        .context(error::ExternalSnafu)?;

                let table = self
                    .catalog_manager
                    .table(&catalog, &schema, &table)
                    .await
                    .context(CatalogSnafu)?
                    .context(TableNotFoundSnafu { table_name: table })?;

                let insert_request =
                    SqlHandler::insert_to_request(self.catalog_manager.clone(), &insert, query_ctx)
                        .await
                        .context(InvokeDatanodeSnafu)?;

                Ok(Output::AffectedRows(
                    table.insert(insert_request).await.context(TableSnafu)?,
                ))
            }
            Statement::ShowCreateTable(show) => {
                let (catalog, schema, table) =
                    table_idents_to_full_name(&show.table_name, query_ctx.clone())
                        .map_err(BoxedError::new)
                        .context(error::ExternalSnafu)?;

                let table_ref = self
                    .catalog_manager
                    .table(&catalog, &schema, &table)
                    .await
                    .context(CatalogSnafu)?
                    .context(TableNotFoundSnafu { table_name: &table })?;
                let table_name = TableName::new(catalog, schema, table);

                self.show_create_table(table_name, table_ref).await
            }
            Statement::TruncateTable(stmt) => {
                let (catalog, schema, table) =
                    table_idents_to_full_name(stmt.table_name(), query_ctx)
                        .map_err(BoxedError::new)
                        .context(error::ExternalSnafu)?;
                let table_name = TableName::new(catalog, schema, table);
                self.truncate_table(table_name).await
            }
            _ => error::NotSupportedSnafu {
                feat: format!("{stmt:?}"),
            }
            .fail(),
        }
    }

    async fn show_create_table(&self, table_name: TableName, table: TableRef) -> Result<Output> {
        let partitions = self
            .catalog_manager
            .partition_manager()
            .find_table_partitions(table.table_info().table_id())
            .await
            .context(error::FindTablePartitionRuleSnafu {
                table_name: &table_name.table_name,
            })?;

        let partitions = create_partitions_stmt(partitions)?;

        query::sql::show_create_table(table, partitions).context(error::ExecuteStatementSnafu)
    }

    /// Handles distributed database creation
    async fn handle_create_database(
        &self,
        expr: CreateDatabaseExpr,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        let catalog = query_ctx.current_catalog();
        if self
            .catalog_manager
            .schema_exist(catalog, &expr.database_name)
            .await
            .context(CatalogSnafu)?
        {
            return if expr.create_if_not_exists {
                Ok(Output::AffectedRows(1))
            } else {
                SchemaExistsSnafu {
                    name: &expr.database_name,
                }
                .fail()
            };
        }

        let schema = SchemaNameKey::new(catalog, &expr.database_name);
        let exist = self
            .catalog_manager
            .table_metadata_manager_ref()
            .schema_manager()
            .exist(schema)
            .await
            .context(error::TableMetadataManagerSnafu)?;

        ensure!(
            !exist,
            SchemaExistsSnafu {
                name: schema.to_string(),
            }
        );

        self.catalog_manager
            .table_metadata_manager_ref()
            .schema_manager()
            .create(schema)
            .await
            .context(error::TableMetadataManagerSnafu)?;

        // Since the database created on meta does not go through KvBackend, so we manually
        // invalidate the cache here.
        //
        // TODO(fys): when the meta invalidation cache mechanism is established, remove it.
        self.catalog_manager()
            .invalidate_schema(catalog, &expr.database_name)
            .await;

        Ok(Output::AffectedRows(1))
    }

    fn verify_alter(
        &self,
        table_id: TableId,
        table_info: Arc<TableInfo>,
        expr: AlterExpr,
    ) -> Result<()> {
        let request: table::requests::AlterTableRequest =
            common_grpc_expr::alter_expr_to_request(table_id, expr)
                .context(AlterExprToRequestSnafu)?;

        let AlterTableRequest { table_name, .. } = &request;

        let _ = table_info
            .meta
            .builder_with_alter_kind(table_name, &request.alter_kind)
            .context(error::TableSnafu)?
            .build()
            .context(error::BuildTableMetaSnafu { table_name })?;

        Ok(())
    }

    async fn handle_alter_table(&self, mut expr: AlterExpr) -> Result<Output> {
        let catalog_name = if expr.catalog_name.is_empty() {
            DEFAULT_CATALOG_NAME
        } else {
            expr.catalog_name.as_str()
        };

        let schema_name = if expr.schema_name.is_empty() {
            DEFAULT_SCHEMA_NAME
        } else {
            expr.schema_name.as_str()
        };

        let table_name = expr.table_name.as_str();

        let table = self
            .catalog_manager
            .table(catalog_name, schema_name, table_name)
            .await
            .context(CatalogSnafu)?
            .context(TableNotFoundSnafu {
                table_name: format_full_table_name(catalog_name, schema_name, table_name),
            })?;

        let table_id = table.table_info().ident.table_id;
        expr.table_id = Some(api::v1::TableId { id: table_id });

        self.verify_alter(table_id, table.table_info(), expr.clone())?;

        let req = SubmitDdlTaskRequest {
            task: DdlTask::new_alter_table(expr.clone()),
        };

        self.meta_client
            .submit_ddl_task(req)
            .await
            .context(error::RequestMetaSnafu)?;

        Ok(Output::AffectedRows(0))
    }

    async fn create_table_procedure(
        &self,
        create_table: &CreateTableExpr,
        partitions: Vec<Partition>,
        table_info: RawTableInfo,
    ) -> Result<SubmitDdlTaskResponse> {
        let partitions = partitions.into_iter().map(Into::into).collect();

        let request = SubmitDdlTaskRequest {
            task: DdlTask::new_create_table(create_table.clone(), partitions, table_info),
        };

        self.meta_client
            .submit_ddl_task(request)
            .await
            .context(error::RequestMetaSnafu)
    }

    async fn drop_table_procedure(
        &self,
        table_name: &TableName,
        table_id: TableId,
    ) -> Result<SubmitDdlTaskResponse> {
        let request = SubmitDdlTaskRequest {
            task: DdlTask::new_drop_table(
                table_name.catalog_name.to_string(),
                table_name.schema_name.to_string(),
                table_name.table_name.to_string(),
                table_id,
            ),
        };

        self.meta_client
            .submit_ddl_task(request)
            .await
            .context(error::RequestMetaSnafu)
    }

    async fn truncate_table_procedure(
        &self,
        truncate_table: &TruncateTableExpr,
    ) -> Result<SubmitDdlTaskResponse> {
        let request = SubmitDdlTaskRequest {
            task: DdlTask::new_truncate_table(truncate_table.clone()),
        };

        self.meta_client
            .submit_ddl_task(request)
            .await
            .context(error::RequestMetaSnafu)
    }

    async fn handle_dist_insert(
        &self,
        requests: InsertRequests,
        ctx: QueryContextRef,
    ) -> Result<Output> {
        let inserter = DistInserter::new(
            ctx.current_catalog().to_owned(),
            ctx.current_schema().to_owned(),
            self.catalog_manager.clone(),
        );
        let affected_rows = inserter.grpc_insert(requests).await?;
        Ok(Output::AffectedRows(affected_rows as usize))
    }

    async fn handle_dist_delete(
        &self,
        request: DeleteRequest,
        ctx: QueryContextRef,
    ) -> Result<Output> {
        let catalog = ctx.current_catalog();
        let schema = ctx.current_schema();
        let table_name = &request.table_name;
        let table_ref = TableReference::full(catalog, schema, table_name);

        let table = self
            .catalog_manager
            .table(catalog, schema, table_name)
            .await
            .context(CatalogSnafu)?
            .with_context(|| TableNotFoundSnafu {
                table_name: table_ref.to_string(),
            })?;

        let request = common_grpc_expr::delete::to_table_delete_request(request)
            .context(ToTableDeleteRequestSnafu)?;

        let affected_rows = table.delete(request).await.context(TableSnafu)?;
        Ok(Output::AffectedRows(affected_rows))
    }

    pub fn catalog_manager(&self) -> Arc<FrontendCatalogManager> {
        self.catalog_manager.clone()
    }
}

#[async_trait]
impl SqlStatementExecutor for DistInstance {
    async fn execute_sql(
        &self,
        stmt: Statement,
        query_ctx: QueryContextRef,
    ) -> query::error::Result<Output> {
        self.handle_statement(stmt, query_ctx)
            .await
            .map_err(BoxedError::new)
            .context(QueryExecutionSnafu)
    }
}

#[async_trait]
impl GrpcQueryHandler for DistInstance {
    type Error = error::Error;

    async fn do_query(&self, request: Request, ctx: QueryContextRef) -> Result<Output> {
        match request {
            Request::Inserts(requests) => self.handle_dist_insert(requests, ctx).await,
            Request::Delete(request) => self.handle_dist_delete(request, ctx).await,
            Request::RowInserts(_) | Request::RowDelete(_) => NotSupportedSnafu {
                feat: "row insert/delete",
            }
            .fail(),
            Request::Query(_) => {
                unreachable!("Query should have been handled directly in Frontend Instance!")
            }
            Request::Ddl(request) => {
                let expr = request.expr.context(error::IncompleteGrpcResultSnafu {
                    err_msg: "Missing 'expr' in DDL request",
                })?;
                match expr {
                    DdlExpr::CreateDatabase(expr) => self.handle_create_database(expr, ctx).await,
                    DdlExpr::CreateTable(mut expr) => {
                        let _ = self.create_table(&mut expr, None).await?;
                        Ok(Output::AffectedRows(0))
                    }
                    DdlExpr::Alter(expr) => self.handle_alter_table(expr).await,
                    DdlExpr::DropTable(expr) => {
                        let table_name =
                            TableName::new(&expr.catalog_name, &expr.schema_name, &expr.table_name);
                        self.drop_table(table_name).await
                    }
                    DdlExpr::FlushTable(expr) => {
                        let table_name =
                            TableName::new(&expr.catalog_name, &expr.schema_name, &expr.table_name);
                        self.flush_table(table_name, expr.region_number).await
                    }
                    DdlExpr::CompactTable(expr) => {
                        let table_name =
                            TableName::new(&expr.catalog_name, &expr.schema_name, &expr.table_name);
                        self.compact_table(table_name, expr.region_number).await
                    }
                    DdlExpr::TruncateTable(expr) => {
                        let table_name =
                            TableName::new(&expr.catalog_name, &expr.schema_name, &expr.table_name);
                        self.truncate_table(table_name).await
                    }
                }
            }
        }
    }
}

fn create_partitions_stmt(partitions: Vec<PartitionInfo>) -> Result<Option<Partitions>> {
    if partitions.is_empty() {
        return Ok(None);
    }

    let column_list: Vec<Ident> = partitions[0]
        .partition
        .partition_columns()
        .iter()
        .map(|name| name[..].into())
        .collect();

    let entries = partitions
        .into_iter()
        .map(|info| {
            // Generated the partition name from id
            let name = &format!("r{}", info.id.as_u64());
            let bounds = info.partition.partition_bounds();
            let value_list = bounds
                .iter()
                .map(|b| match b {
                    PartitionBound::Value(v) => statements::value_to_sql_value(v)
                        .with_context(|_| error::ConvertSqlValueSnafu { value: v.clone() }),
                    PartitionBound::MaxValue => Ok(SqlValue::Number(MAX_VALUE.to_string(), false)),
                })
                .collect::<Result<Vec<_>>>()?;

            Ok(PartitionEntry {
                name: name[..].into(),
                value_list,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Some(Partitions {
        column_list,
        entries,
    }))
}

fn create_table_info(
    create_table: &CreateTableExpr,
    partition_columns: Vec<String>,
) -> Result<RawTableInfo> {
    let mut column_schemas = Vec::with_capacity(create_table.column_defs.len());
    let mut column_name_to_index_map = HashMap::new();

    for (idx, column) in create_table.column_defs.iter().enumerate() {
        let schema =
            column_def::try_as_column_schema(column).context(error::InvalidColumnDefSnafu {
                column: &column.name,
            })?;
        let schema = schema.with_time_index(column.name == create_table.time_index);

        column_schemas.push(schema);
        let _ = column_name_to_index_map.insert(column.name.clone(), idx);
    }

    let timestamp_index = column_name_to_index_map
        .get(&create_table.time_index)
        .cloned();

    let raw_schema = RawSchema {
        column_schemas: column_schemas.clone(),
        timestamp_index,
        version: 0,
    };

    let primary_key_indices = create_table
        .primary_keys
        .iter()
        .map(|name| {
            column_name_to_index_map
                .get(name)
                .cloned()
                .context(ColumnNotFoundSnafu { msg: name })
        })
        .collect::<Result<Vec<_>>>()?;

    let partition_key_indices = partition_columns
        .into_iter()
        .map(|col_name| {
            column_name_to_index_map
                .get(&col_name)
                .cloned()
                .context(ColumnNotFoundSnafu { msg: col_name })
        })
        .collect::<Result<Vec<_>>>()?;

    let meta = RawTableMeta {
        schema: raw_schema,
        primary_key_indices,
        value_indices: vec![],
        engine: create_table.engine.clone(),
        next_column_id: column_schemas.len() as u32,
        region_numbers: vec![],
        engine_options: HashMap::new(),
        options: TableOptions::try_from(&create_table.table_options)
            .context(UnrecognizedTableOptionSnafu)?,
        created_on: DateTime::default(),
        partition_key_indices,
    };

    let desc = if create_table.desc.is_empty() {
        None
    } else {
        Some(create_table.desc.clone())
    };

    let table_info = RawTableInfo {
        ident: TableIdent {
            // The table id of distributed table is assigned by Meta, set "0" here as a placeholder.
            table_id: 0,
            version: 0,
        },
        name: create_table.table_name.clone(),
        desc,
        catalog_name: create_table.catalog_name.clone(),
        schema_name: create_table.schema_name.clone(),
        meta,
        table_type: TableType::Base,
    };
    Ok(table_info)
}

fn parse_partitions(
    create_table: &CreateTableExpr,
    partitions: Option<Partitions>,
) -> Result<(Vec<MetaPartition>, Vec<String>)> {
    // If partitions are not defined by user, use the timestamp column (which has to be existed) as
    // the partition column, and create only one partition.
    let partition_columns = find_partition_columns(&partitions)?;
    let partition_entries = find_partition_entries(create_table, &partitions, &partition_columns)?;

    Ok((
        partition_entries
            .into_iter()
            .map(|x| MetaPartition::try_from(PartitionDef::new(partition_columns.clone(), x)))
            .collect::<std::result::Result<_, _>>()
            .context(DeserializePartitionSnafu)?,
        partition_columns,
    ))
}

fn find_partition_entries(
    create_table: &CreateTableExpr,
    partitions: &Option<Partitions>,
    partition_columns: &[String],
) -> Result<Vec<Vec<PartitionBound>>> {
    let entries = if let Some(partitions) = partitions {
        let column_defs = partition_columns
            .iter()
            .map(|pc| {
                create_table
                    .column_defs
                    .iter()
                    .find(|c| &c.name == pc)
                    // unwrap is safe here because we have checked that partition columns are defined
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let mut column_name_and_type = Vec::with_capacity(column_defs.len());
        for column in column_defs {
            let column_name = &column.name;
            let data_type = ConcreteDataType::from(
                ColumnDataTypeWrapper::try_new(column.datatype).context(ColumnDataTypeSnafu)?,
            );
            column_name_and_type.push((column_name, data_type));
        }

        let mut entries = Vec::with_capacity(partitions.entries.len());
        for e in partitions.entries.iter() {
            let mut values = Vec::with_capacity(e.value_list.len());
            for (i, v) in e.value_list.iter().enumerate() {
                // indexing is safe here because we have checked that "value_list" and "column_list" are matched in size
                let (column_name, data_type) = &column_name_and_type[i];
                let v = match v {
                    SqlValue::Number(n, _) if n == MAX_VALUE => PartitionBound::MaxValue,
                    _ => PartitionBound::Value(
                        sql_value_to_value(column_name, data_type, v).context(ParseSqlSnafu)?,
                    ),
                };
                values.push(v);
            }
            entries.push(values);
        }
        entries
    } else {
        vec![vec![PartitionBound::MaxValue]]
    };
    Ok(entries)
}

fn find_partition_columns(partitions: &Option<Partitions>) -> Result<Vec<String>> {
    let columns = if let Some(partitions) = partitions {
        partitions
            .column_list
            .iter()
            .map(|x| x.value.clone())
            .collect::<Vec<_>>()
    } else {
        vec![]
    };
    Ok(columns)
}

#[cfg(test)]
mod test {
    use session::context::QueryContext;
    use sql::dialect::GreptimeDbDialect;
    use sql::parser::ParserContext;
    use sql::statements::statement::Statement;

    use super::*;

    #[tokio::test]
    async fn test_parse_partitions() {
        common_telemetry::init_default_ut_logging();
        let cases = [
            (
                r"
CREATE TABLE rcx ( a INT, b STRING, c TIMESTAMP, TIME INDEX (c) )
PARTITION BY RANGE COLUMNS (b) (
  PARTITION r0 VALUES LESS THAN ('hz'),
  PARTITION r1 VALUES LESS THAN ('sh'),
  PARTITION r2 VALUES LESS THAN (MAXVALUE),
)
ENGINE=mito",
                r#"[{"column_list":"b","value_list":"{\"Value\":{\"String\":\"hz\"}}"},{"column_list":"b","value_list":"{\"Value\":{\"String\":\"sh\"}}"},{"column_list":"b","value_list":"\"MaxValue\""}]"#,
            ),
            (
                r"
CREATE TABLE rcx ( a INT, b STRING, c TIMESTAMP, TIME INDEX (c) )
PARTITION BY RANGE COLUMNS (b, a) (
  PARTITION r0 VALUES LESS THAN ('hz', 10),
  PARTITION r1 VALUES LESS THAN ('sh', 20),
  PARTITION r2 VALUES LESS THAN (MAXVALUE, MAXVALUE),
)
ENGINE=mito",
                r#"[{"column_list":"b,a","value_list":"{\"Value\":{\"String\":\"hz\"}},{\"Value\":{\"Int32\":10}}"},{"column_list":"b,a","value_list":"{\"Value\":{\"String\":\"sh\"}},{\"Value\":{\"Int32\":20}}"},{"column_list":"b,a","value_list":"\"MaxValue\",\"MaxValue\""}]"#,
            ),
        ];
        for (sql, expected) in cases {
            let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}).unwrap();
            match &result[0] {
                Statement::CreateTable(c) => {
                    let expr = expr_factory::create_to_expr(c, QueryContext::arc()).unwrap();
                    let (partitions, _) = parse_partitions(&expr, c.partitions.clone()).unwrap();
                    let json = serde_json::to_string(&partitions).unwrap();
                    assert_eq!(json, expected);
                }
                _ => unreachable!(),
            }
        }
    }
}
