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

pub mod deleter;
pub(crate) mod inserter;

use std::collections::HashMap;
use std::sync::Arc;

use api::helper::ColumnDataTypeWrapper;
use api::v1::ddl_request::Expr as DdlExpr;
use api::v1::greptime_request::Request;
use api::v1::region::{region_request, QueryRequest, RegionResponse};
use api::v1::{
    column_def, AlterExpr, CreateDatabaseExpr, CreateTableExpr, DeleteRequests, TruncateTableExpr,
};
use arrow_flight::Ticket;
use async_trait::async_trait;
use catalog::{CatalogManager, DeregisterTableRequest, RegisterTableRequest};
use chrono::DateTime;
use client::region::RegionRequester;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_catalog::format_full_table_name;
use common_error::ext::BoxedError;
use common_meta::ddl::{DdlExecutorRef, ExecutorContext};
use common_meta::key::schema_name::{SchemaNameKey, SchemaNameValue};
use common_meta::rpc::ddl::{DdlTask, SubmitDdlTaskRequest, SubmitDdlTaskResponse};
use common_meta::rpc::router::{Partition, Partition as MetaPartition};
use common_meta::table_name::TableName;
use common_query::Output;
use common_recordbatch::SendableRecordBatchStream;
use common_telemetry::info;
use datanode::instance::sql::table_idents_to_full_name;
use datanode::sql::SqlHandler;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::RawSchema;
use partition::manager::PartitionInfo;
use partition::partition::{PartitionBound, PartitionDef};
use prost::Message;
use query::error::QueryExecutionSnafu;
use query::query_engine::SqlStatementExecutor;
use servers::query_handler::grpc::GrpcQueryHandler;
use session::context::QueryContextRef;
use snafu::{ensure, OptionExt, ResultExt};
use sql::ast::{Ident, Value as SqlValue};
use sql::statements::create::{PartitionEntry, Partitions};
use sql::statements::statement::Statement;
use sql::statements::{self, sql_value_to_value};
use store_api::storage::RegionId;
use table::metadata::{RawTableInfo, RawTableMeta, TableId, TableIdent, TableInfo, TableType};
use table::requests::{AlterTableRequest, TableOptions};
use table::TableRef;

use super::region_handler::RegionRequestHandler;
use crate::catalog::FrontendCatalogManager;
use crate::error::{
    self, AlterExprToRequestSnafu, CatalogSnafu, ColumnDataTypeSnafu, ColumnNotFoundSnafu,
    DeserializePartitionSnafu, FindDatanodeSnafu, FindTableRouteSnafu, InvokeDatanodeSnafu,
    NotSupportedSnafu, ParseSqlSnafu, RequestDatanodeSnafu, Result, SchemaExistsSnafu,
    SchemaNotFoundSnafu, TableAlreadyExistSnafu, TableMetadataManagerSnafu, TableNotFoundSnafu,
    UnrecognizedTableOptionSnafu,
};
use crate::expr_factory;
use crate::instance::distributed::deleter::DistDeleter;
use crate::instance::distributed::inserter::DistInserter;
use crate::table::DistTable;

const MAX_VALUE: &str = "MAXVALUE";

#[derive(Clone)]
pub struct DistInstance {
    ddl_executor: DdlExecutorRef,
    pub(crate) catalog_manager: Arc<FrontendCatalogManager>,
}

impl DistInstance {
    pub fn new(ddl_executor: DdlExecutorRef, catalog_manager: Arc<FrontendCatalogManager>) -> Self {
        Self {
            ddl_executor,
            catalog_manager,
        }
    }

    pub async fn create_table(
        &self,
        create_table: &mut CreateTableExpr,
        partitions: Option<Partitions>,
    ) -> Result<TableRef> {
        let _timer = common_telemetry::timer!(crate::metrics::DIST_CREATE_TABLE);
        // 1. get schema info
        let schema = self
            .catalog_manager
            .table_metadata_manager_ref()
            .schema_manager()
            .get(SchemaNameKey::new(
                &create_table.catalog_name,
                &create_table.schema_name,
            ))
            .await
            .context(TableMetadataManagerSnafu)?;

        let Some(schema_opts) = schema else {
            return SchemaNotFoundSnafu {
                schema_info: &create_table.schema_name,
            }
            .fail();
        };

        let table_name = TableName::new(
            &create_table.catalog_name,
            &create_table.schema_name,
            &create_table.table_name,
        );

        let (partitions, partition_cols) = parse_partitions(create_table, partitions)?;

        let mut table_info = create_table_info(create_table, partition_cols, schema_opts)?;

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

        let table = DistTable::table(table_info);

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

        // Invalidates local cache ASAP.
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

        // Invalidates local cache ASAP.
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
                    options: Default::default(),
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
                let insert_request =
                    SqlHandler::insert_to_request(self.catalog_manager.clone(), &insert, query_ctx)
                        .await
                        .context(InvokeDatanodeSnafu)?;

                let inserter = DistInserter::new(&self.catalog_manager);
                let affected_rows = inserter.insert_table_request(insert_request).await?;
                Ok(Output::AffectedRows(affected_rows as usize))
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

                self.show_create_table(table_name, table_ref, query_ctx.clone())
                    .await
            }
            Statement::TruncateTable(stmt) => {
                let (catalog, schema, table) =
                    table_idents_to_full_name(stmt.table_name(), query_ctx)
                        .map_err(BoxedError::new)
                        .context(error::ExternalSnafu)?;
                let table_name = TableName::new(catalog, schema, table);
                self.truncate_table(table_name).await
            }
            _ => NotSupportedSnafu {
                feat: format!("{stmt:?}"),
            }
            .fail(),
        }
    }

    async fn show_create_table(
        &self,
        table_name: TableName,
        table: TableRef,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        let partitions = self
            .catalog_manager
            .partition_manager()
            .find_table_partitions(table.table_info().table_id())
            .await
            .context(error::FindTablePartitionRuleSnafu {
                table_name: &table_name.table_name,
            })?;

        let partitions = create_partitions_stmt(partitions)?;

        query::sql::show_create_table(table, partitions, query_ctx)
            .context(error::ExecuteStatementSnafu)
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

        let schema_value =
            SchemaNameValue::try_from(&expr.options).context(error::TableMetadataManagerSnafu)?;
        self.catalog_manager
            .table_metadata_manager_ref()
            .schema_manager()
            .create(schema, Some(schema_value))
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

    async fn handle_alter_table(&self, expr: AlterExpr) -> Result<Output> {
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
        self.verify_alter(table_id, table.table_info(), expr.clone())?;

        let req = SubmitDdlTaskRequest {
            task: DdlTask::new_alter_table(expr.clone()),
        };

        self.ddl_executor
            .submit_ddl_task(&ExecutorContext::default(), req)
            .await
            .context(error::ExecuteDdlSnafu)?;

        // Invalidates local cache ASAP.
        self.catalog_manager()
            .invalidate_table(catalog_name, schema_name, table_name, table_id)
            .await;

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

        self.ddl_executor
            .submit_ddl_task(&ExecutorContext::default(), request)
            .await
            .context(error::ExecuteDdlSnafu)
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

        self.ddl_executor
            .submit_ddl_task(&ExecutorContext::default(), request)
            .await
            .context(error::ExecuteDdlSnafu)
    }

    async fn truncate_table_procedure(
        &self,
        truncate_table: &TruncateTableExpr,
    ) -> Result<SubmitDdlTaskResponse> {
        let request = SubmitDdlTaskRequest {
            task: DdlTask::new_truncate_table(truncate_table.clone()),
        };

        self.ddl_executor
            .submit_ddl_task(&ExecutorContext::default(), request)
            .await
            .context(error::ExecuteDdlSnafu)
    }

    async fn handle_dist_delete(
        &self,
        request: DeleteRequests,
        ctx: QueryContextRef,
    ) -> Result<Output> {
        let deleter = DistDeleter::new(
            ctx.current_catalog().to_string(),
            ctx.current_schema().to_string(),
            self.catalog_manager(),
        );
        let affected_rows = deleter.grpc_delete(request).await?;
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
            Request::Inserts(_) => NotSupportedSnafu { feat: "inserts" }.fail(),
            Request::RowInserts(_) => NotSupportedSnafu {
                feat: "row inserts",
            }
            .fail(),
            Request::RowDeletes(_) => NotSupportedSnafu {
                feat: "row deletes",
            }
            .fail(),
            Request::Deletes(requests) => self.handle_dist_delete(requests, ctx).await,
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

pub(crate) struct DistRegionRequestHandler {
    catalog_manager: Arc<FrontendCatalogManager>,
}

impl DistRegionRequestHandler {
    pub fn arc(catalog_manager: Arc<FrontendCatalogManager>) -> Arc<Self> {
        Arc::new(Self { catalog_manager })
    }
}

#[async_trait]
impl RegionRequestHandler for DistRegionRequestHandler {
    async fn handle(
        &self,
        request: region_request::Body,
        ctx: QueryContextRef,
    ) -> Result<RegionResponse> {
        match request {
            region_request::Body::Inserts(inserts) => {
                let inserter =
                    DistInserter::new(&self.catalog_manager).with_trace_id(ctx.trace_id());
                let affected_rows = inserter.insert_region_requests(inserts).await? as _;
                Ok(RegionResponse {
                    header: Some(Default::default()),
                    affected_rows,
                })
            }
            region_request::Body::Deletes(_) => NotSupportedSnafu {
                feat: "region deletes",
            }
            .fail(),
            region_request::Body::Create(_) => NotSupportedSnafu {
                feat: "region create",
            }
            .fail(),
            region_request::Body::Drop(_) => NotSupportedSnafu {
                feat: "region drop",
            }
            .fail(),
            region_request::Body::Open(_) => NotSupportedSnafu {
                feat: "region open",
            }
            .fail(),
            region_request::Body::Close(_) => NotSupportedSnafu {
                feat: "region close",
            }
            .fail(),
            region_request::Body::Alter(_) => NotSupportedSnafu {
                feat: "region alter",
            }
            .fail(),
            region_request::Body::Flush(_) => NotSupportedSnafu {
                feat: "region flush",
            }
            .fail(),
            region_request::Body::Compact(_) => NotSupportedSnafu {
                feat: "region compact",
            }
            .fail(),
        }
    }

    async fn do_get(&self, request: QueryRequest) -> Result<SendableRecordBatchStream> {
        let region_id = RegionId::from_u64(request.region_id);

        let table_route = self
            .catalog_manager
            .partition_manager()
            .find_table_route(region_id.table_id())
            .await
            .context(FindTableRouteSnafu {
                table_id: region_id.table_id(),
            })?;
        let peer = table_route
            .find_region_leader(region_id.region_number())
            .context(FindDatanodeSnafu {
                region: region_id.region_number(),
            })?;
        let client = self
            .catalog_manager
            .datanode_clients()
            .get_client(peer)
            .await;

        let ticket = Ticket {
            ticket: request.encode_to_vec().into(),
        };
        let region_requester = RegionRequester::new(client);
        region_requester
            .do_get(ticket)
            .await
            .context(RequestDatanodeSnafu)
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
            let name = &format!("r{}", info.id.region_number());
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
    schema_opts: SchemaNameValue,
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

    let table_options = TableOptions::try_from(&create_table.table_options)
        .context(UnrecognizedTableOptionSnafu)?;
    let table_options = merge_options(table_options, schema_opts);

    let meta = RawTableMeta {
        schema: raw_schema,
        primary_key_indices,
        value_indices: vec![],
        engine: create_table.engine.clone(),
        next_column_id: column_schemas.len() as u32,
        region_numbers: vec![],
        engine_options: HashMap::new(),
        options: table_options,
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

fn merge_options(mut table_opts: TableOptions, schema_opts: SchemaNameValue) -> TableOptions {
    table_opts.ttl = table_opts.ttl.or(schema_opts.ttl);
    table_opts
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
                ColumnDataTypeWrapper::try_new(column.data_type).context(ColumnDataTypeSnafu)?,
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
                r#"[{"column_list":["b"],"value_list":["{\"Value\":{\"String\":\"hz\"}}"]},{"column_list":["b"],"value_list":["{\"Value\":{\"String\":\"sh\"}}"]},{"column_list":["b"],"value_list":["\"MaxValue\""]}]"#,
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
                r#"[{"column_list":["b","a"],"value_list":["{\"Value\":{\"String\":\"hz\"}}","{\"Value\":{\"Int32\":10}}"]},{"column_list":["b","a"],"value_list":["{\"Value\":{\"String\":\"sh\"}}","{\"Value\":{\"Int32\":20}}"]},{"column_list":["b","a"],"value_list":["\"MaxValue\"","\"MaxValue\""]}]"#,
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
