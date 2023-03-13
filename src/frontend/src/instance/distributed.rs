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

mod grpc;

use std::collections::HashMap;
use std::sync::Arc;

use api::helper::ColumnDataTypeWrapper;
use api::v1::{
    column_def, AlterExpr, CreateDatabaseExpr, CreateTableExpr, DropTableExpr, FlushTableExpr,
    InsertRequest, TableId,
};
use async_trait::async_trait;
use catalog::helper::{SchemaKey, SchemaValue};
use catalog::{CatalogManager, DeregisterTableRequest, RegisterTableRequest};
use chrono::DateTime;
use client::Database;
use common_base::Plugins;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_catalog::format_full_table_name;
use common_error::prelude::BoxedError;
use common_query::Output;
use common_telemetry::{debug, info};
use datanode::instance::sql::table_idents_to_full_name;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{RawSchema, Schema};
use meta_client::client::MetaClient;
use meta_client::rpc::router::DeleteRequest as MetaDeleteRequest;
use meta_client::rpc::{
    CompareAndPutRequest, CreateRequest as MetaCreateRequest, Partition as MetaPartition,
    RouteRequest, RouteResponse, TableName,
};
use partition::partition::{PartitionBound, PartitionDef};
use query::parser::{PromQuery, QueryStatement};
use query::sql::{describe_table, explain, show_databases, show_tables};
use query::{QueryEngineFactory, QueryEngineRef};
use servers::query_handler::sql::SqlQueryHandler;
use session::context::QueryContextRef;
use snafu::{ensure, OptionExt, ResultExt};
use sql::ast::Value as SqlValue;
use sql::statements::create::Partitions;
use sql::statements::sql_value_to_value;
use sql::statements::statement::Statement;
use table::metadata::{RawTableInfo, RawTableMeta, TableIdent, TableType};
use table::requests::TableOptions;
use table::table::AlterContext;

use crate::catalog::FrontendCatalogManager;
use crate::datanode::DatanodeClients;
use crate::error::{
    self, AlterExprToRequestSnafu, CatalogEntrySerdeSnafu, CatalogSnafu, ColumnDataTypeSnafu,
    DeserializePartitionSnafu, ParseSqlSnafu, PrimaryKeyNotFoundSnafu, RequestDatanodeSnafu,
    RequestMetaSnafu, Result, SchemaExistsSnafu, StartMetaClientSnafu, TableAlreadyExistSnafu,
    TableNotFoundSnafu, TableSnafu, ToTableInsertRequestSnafu, UnrecognizedTableOptionSnafu,
};
use crate::expr_factory;
use crate::instance::parse_stmt;
use crate::sql::insert_to_request;
use crate::table::DistTable;

#[derive(Clone)]
pub(crate) struct DistInstance {
    meta_client: Arc<MetaClient>,
    catalog_manager: Arc<FrontendCatalogManager>,
    datanode_clients: Arc<DatanodeClients>,
    query_engine: QueryEngineRef,
}

impl DistInstance {
    pub(crate) fn new(
        meta_client: Arc<MetaClient>,
        catalog_manager: Arc<FrontendCatalogManager>,
        datanode_clients: Arc<DatanodeClients>,
        plugins: Arc<Plugins>,
    ) -> Self {
        let query_engine =
            QueryEngineFactory::new_with_plugins(catalog_manager.clone(), plugins.clone())
                .query_engine();
        Self {
            meta_client,
            catalog_manager,
            datanode_clients,
            query_engine,
        }
    }

    pub(crate) async fn create_table(
        &self,
        create_table: &mut CreateTableExpr,
        partitions: Option<Partitions>,
    ) -> Result<Output> {
        let table_name = TableName::new(
            &create_table.catalog_name,
            &create_table.schema_name,
            &create_table.table_name,
        );

        if self
            .catalog_manager
            .table(
                &table_name.catalog_name,
                &table_name.schema_name,
                &table_name.table_name,
            )
            .await
            .context(CatalogSnafu)?
            .is_some()
        {
            return if create_table.create_if_not_exists {
                Ok(Output::AffectedRows(0))
            } else {
                TableAlreadyExistSnafu {
                    table: table_name.to_string(),
                }
                .fail()
            };
        }

        let mut table_info = create_table_info(create_table)?;

        let response = self
            .create_table_in_meta(create_table, partitions, &table_info)
            .await?;
        let table_routes = response.table_routes;
        ensure!(
            table_routes.len() == 1,
            error::CreateTableRouteSnafu {
                table_name: create_table.table_name.to_string()
            }
        );
        let table_route = table_routes.first().unwrap();
        info!(
            "Creating distributed table {table_name} with table routes: {}",
            serde_json::to_string_pretty(table_route)
                .unwrap_or_else(|_| format!("{table_route:#?}"))
        );
        let region_routes = &table_route.region_routes;
        ensure!(
            !region_routes.is_empty(),
            error::FindRegionRouteSnafu {
                table_name: create_table.table_name.to_string()
            }
        );

        let table_id = table_route.table.id as u32;
        table_info.ident.table_id = table_id;
        let table_info = Arc::new(table_info.try_into().context(error::CreateTableInfoSnafu)?);

        create_table.table_id = Some(TableId { id: table_id });

        let table = DistTable::new(
            table_name.clone(),
            table_info,
            self.catalog_manager.partition_manager(),
            self.catalog_manager.datanode_clients(),
            self.catalog_manager.backend(),
        );

        let request = RegisterTableRequest {
            catalog: table_name.catalog_name.clone(),
            schema: table_name.schema_name.clone(),
            table_name: table_name.table_name.clone(),
            table_id,
            table: Arc::new(table),
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

        for datanode in table_route.find_leaders() {
            let client = self.datanode_clients.get_client(&datanode).await;
            let client = Database::new(&table_name.catalog_name, &table_name.schema_name, client);

            let regions = table_route.find_leader_regions(&datanode);
            let mut create_expr_for_region = create_table.clone();
            create_expr_for_region.region_ids = regions;

            debug!(
                "Creating table {:?} on Datanode {:?} with regions {:?}",
                create_table, datanode, create_expr_for_region.region_ids,
            );

            client
                .create(create_expr_for_region)
                .await
                .context(RequestDatanodeSnafu)?;
        }

        // Checked in real MySQL, it truly returns "0 rows affected".
        Ok(Output::AffectedRows(0))
    }

    async fn drop_table(&self, table_name: TableName) -> Result<Output> {
        let _ = self
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

        let route_response = self
            .meta_client
            .delete_route(MetaDeleteRequest {
                table_name: table_name.clone(),
            })
            .await
            .context(RequestMetaSnafu)?;

        let request = DeregisterTableRequest {
            catalog: table_name.catalog_name.clone(),
            schema: table_name.schema_name.clone(),
            table_name: table_name.table_name.clone(),
        };
        ensure!(
            self.catalog_manager
                .deregister_table(request)
                .await
                .context(CatalogSnafu)?,
            error::TableNotFoundSnafu {
                table_name: table_name.to_string()
            }
        );

        let expr = DropTableExpr {
            catalog_name: table_name.catalog_name.clone(),
            schema_name: table_name.schema_name.clone(),
            table_name: table_name.table_name.clone(),
        };
        for table_route in route_response.table_routes.iter() {
            for datanode in table_route.find_leaders() {
                debug!("Dropping table {table_name} on Datanode {datanode:?}");

                let client = self.datanode_clients.get_client(&datanode).await;
                let client = Database::new(&expr.catalog_name, &expr.schema_name, client);
                client
                    .drop_table(expr.clone())
                    .await
                    .context(RequestDatanodeSnafu)?;
            }
        }

        Ok(Output::AffectedRows(1))
    }

    async fn flush_table(&self, table_name: TableName, region_id: Option<u32>) -> Result<Output> {
        let _ = self
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

        let route_response = self
            .meta_client
            .route(RouteRequest {
                table_names: vec![table_name.clone()],
            })
            .await
            .context(RequestMetaSnafu)?;

        let expr = FlushTableExpr {
            catalog_name: table_name.catalog_name.clone(),
            schema_name: table_name.schema_name.clone(),
            table_name: table_name.table_name.clone(),
            region_id,
        };

        for table_route in &route_response.table_routes {
            let should_send_rpc = table_route.region_routes.iter().any(|route| {
                if let Some(region_id) = region_id {
                    region_id == route.region.id as u32
                } else {
                    true
                }
            });

            if !should_send_rpc {
                continue;
            }
            for datanode in table_route.find_leaders() {
                debug!("Flushing table {table_name} on Datanode {datanode:?}");

                let client = self.datanode_clients.get_client(&datanode).await;
                let client = Database::new(&expr.catalog_name, &expr.schema_name, client);
                client
                    .flush_table(expr.clone())
                    .await
                    .context(RequestDatanodeSnafu)?;
            }
        }
        Ok(Output::AffectedRows(0))
    }

    async fn handle_statement(
        &self,
        stmt: Statement,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        match stmt {
            Statement::Query(_) => {
                let plan = self
                    .query_engine
                    .statement_to_plan(QueryStatement::Sql(stmt), query_ctx)
                    .await
                    .context(error::ExecuteStatementSnafu {})?;
                self.query_engine.execute(&plan).await
            }
            Statement::CreateDatabase(stmt) => {
                let expr = CreateDatabaseExpr {
                    database_name: stmt.name.to_string(),
                    create_if_not_exists: stmt.if_not_exists,
                };
                return self.handle_create_database(expr, query_ctx).await;
            }
            Statement::CreateTable(stmt) => {
                let create_expr = &mut expr_factory::create_to_expr(&stmt, query_ctx)?;
                Ok(self.create_table(create_expr, stmt.partitions).await?)
            }
            Statement::Alter(alter_table) => {
                let expr = grpc::to_alter_expr(alter_table, query_ctx)?;
                return self.handle_alter_table(expr).await;
            }
            Statement::DropTable(stmt) => {
                let (catalog, schema, table) =
                    table_idents_to_full_name(stmt.table_name(), query_ctx)
                        .map_err(BoxedError::new)
                        .context(error::ExternalSnafu)?;
                let table_name = TableName::new(catalog, schema, table);
                return self.drop_table(table_name).await;
            }
            Statement::ShowDatabases(stmt) => show_databases(stmt, self.catalog_manager.clone()),
            Statement::ShowTables(stmt) => {
                show_tables(stmt, self.catalog_manager.clone(), query_ctx)
            }
            Statement::DescribeTable(stmt) => {
                let (catalog, schema, table) = table_idents_to_full_name(stmt.name(), query_ctx)
                    .map_err(BoxedError::new)
                    .context(error::ExternalSnafu)?;
                let table = self
                    .catalog_manager
                    .table(&catalog, &schema, &table)
                    .await
                    .context(CatalogSnafu)?
                    .with_context(|| TableNotFoundSnafu {
                        table_name: stmt.name().to_string(),
                    })?;
                describe_table(table)
            }
            Statement::Explain(stmt) => {
                explain(Box::new(stmt), self.query_engine.clone(), query_ctx).await
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

                let insert_request = insert_to_request(&table, *insert, query_ctx)?;

                return Ok(Output::AffectedRows(
                    table.insert(insert_request).await.context(TableSnafu)?,
                ));
            }
            _ => {
                return error::NotSupportedSnafu {
                    feat: format!("{stmt:?}"),
                }
                .fail()
            }
        }
        .context(error::ExecuteStatementSnafu)
    }

    async fn handle_sql(&self, sql: &str, query_ctx: QueryContextRef) -> Vec<Result<Output>> {
        let stmts = parse_stmt(sql);
        match stmts {
            Ok(stmts) => {
                let mut results = Vec::with_capacity(stmts.len());

                for stmt in stmts {
                    let result = self.handle_statement(stmt, query_ctx.clone()).await;
                    let is_err = result.is_err();

                    results.push(result);

                    if is_err {
                        break;
                    }
                }

                results
            }
            Err(e) => vec![Err(e)],
        }
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
            .schema(&catalog, &expr.database_name)
            .context(CatalogSnafu)?
            .is_some()
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

        let key = SchemaKey {
            catalog_name: catalog,
            schema_name: expr.database_name,
        };
        let value = SchemaValue {};
        let client = self
            .meta_client
            .store_client()
            .context(StartMetaClientSnafu)?;

        let request = CompareAndPutRequest::new()
            .with_key(key.to_string())
            .with_value(value.as_bytes().context(CatalogEntrySerdeSnafu)?);
        let response = client
            .compare_and_put(request.into())
            .await
            .context(RequestMetaSnafu)?;
        ensure!(
            response.success,
            SchemaExistsSnafu {
                name: key.schema_name
            }
        );

        Ok(Output::AffectedRows(1))
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

        let request = common_grpc_expr::alter_expr_to_request(expr.clone())
            .context(AlterExprToRequestSnafu)?;

        let mut context = AlterContext::with_capacity(1);
        context.insert(expr);

        table.alter(context, &request).await.context(TableSnafu)?;

        Ok(Output::AffectedRows(0))
    }

    async fn create_table_in_meta(
        &self,
        create_table: &CreateTableExpr,
        partitions: Option<Partitions>,
        table_info: &RawTableInfo,
    ) -> Result<RouteResponse> {
        let mut catalog_name = create_table.catalog_name.clone();
        if catalog_name.is_empty() {
            catalog_name = DEFAULT_CATALOG_NAME.to_string();
        }
        let mut schema_name = create_table.schema_name.clone();
        if schema_name.is_empty() {
            schema_name = DEFAULT_SCHEMA_NAME.to_string();
        }
        let table_name = TableName::new(catalog_name, schema_name, create_table.table_name.clone());

        let partitions = parse_partitions(create_table, partitions)?;
        let request = MetaCreateRequest {
            table_name,
            partitions,
            table_info,
        };
        self.meta_client
            .create_route(request)
            .await
            .context(RequestMetaSnafu)
    }

    // TODO(LFC): Refactor insertion implementation for DistTable,
    // GRPC InsertRequest to Table InsertRequest, than split Table InsertRequest, than assemble each GRPC InsertRequest, is rather inefficient,
    // should operate on GRPC InsertRequest directly.
    // Also remember to check the "region_number" carried in InsertRequest, too.
    async fn handle_dist_insert(
        &self,
        request: InsertRequest,
        ctx: QueryContextRef,
    ) -> Result<Output> {
        let catalog = &ctx.current_catalog();
        let schema = &ctx.current_schema();
        let table_name = &request.table_name;
        let table = self
            .catalog_manager
            .table(catalog, schema, table_name)
            .await
            .context(CatalogSnafu)?
            .context(TableNotFoundSnafu { table_name })?;

        let request = common_grpc_expr::insert::to_table_insert_request(catalog, schema, request)
            .context(ToTableInsertRequestSnafu)?;

        let affected_rows = table.insert(request).await.context(TableSnafu)?;
        Ok(Output::AffectedRows(affected_rows))
    }

    #[cfg(test)]
    pub(crate) fn catalog_manager(&self) -> Arc<FrontendCatalogManager> {
        self.catalog_manager.clone()
    }
}

#[async_trait]
impl SqlQueryHandler for DistInstance {
    type Error = error::Error;

    async fn do_query(&self, query: &str, query_ctx: QueryContextRef) -> Vec<Result<Output>> {
        self.handle_sql(query, query_ctx).await
    }

    async fn do_promql_query(
        &self,
        _: &PromQuery,
        _: QueryContextRef,
    ) -> Vec<std::result::Result<Output, Self::Error>> {
        unimplemented!()
    }

    async fn do_statement_query(
        &self,
        stmt: Statement,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        self.handle_statement(stmt, query_ctx).await
    }

    async fn do_describe(
        &self,
        stmt: Statement,
        query_ctx: QueryContextRef,
    ) -> Result<Option<Schema>> {
        if let Statement::Query(_) = stmt {
            self.query_engine
                .describe(QueryStatement::Sql(stmt), query_ctx)
                .await
                .map(Some)
                .context(error::DescribeStatementSnafu)
        } else {
            Ok(None)
        }
    }

    fn is_valid_schema(&self, catalog: &str, schema: &str) -> Result<bool> {
        self.catalog_manager
            .schema(catalog, schema)
            .map(|s| s.is_some())
            .context(CatalogSnafu)
    }
}

fn create_table_info(create_table: &CreateTableExpr) -> Result<RawTableInfo> {
    let mut column_schemas = Vec::with_capacity(create_table.column_defs.len());
    let mut column_name_to_index_map = HashMap::new();

    for (idx, column) in create_table.column_defs.iter().enumerate() {
        let schema =
            column_def::try_as_column_schema(column).context(error::InvalidColumnDefSnafu {
                column: &column.name,
            })?;
        let schema = schema.with_time_index(column.name == create_table.time_index);

        column_schemas.push(schema);
        column_name_to_index_map.insert(column.name.clone(), idx);
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
                .context(PrimaryKeyNotFoundSnafu { msg: name })
        })
        .collect::<Result<Vec<_>>>()?;

    let meta = RawTableMeta {
        schema: raw_schema,
        primary_key_indices,
        value_indices: vec![],
        engine: "mito".to_string(),
        next_column_id: column_schemas.len() as u32,
        region_numbers: vec![],
        engine_options: HashMap::new(),
        options: TableOptions::try_from(&create_table.table_options)
            .context(UnrecognizedTableOptionSnafu)?,
        created_on: DateTime::default(),
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
) -> Result<Vec<MetaPartition>> {
    // If partitions are not defined by user, use the timestamp column (which has to be existed) as
    // the partition column, and create only one partition.
    let partition_columns = find_partition_columns(create_table, &partitions)?;
    let partition_entries = find_partition_entries(create_table, &partitions, &partition_columns)?;

    partition_entries
        .into_iter()
        .map(|x| MetaPartition::try_from(PartitionDef::new(partition_columns.clone(), x)))
        .collect::<std::result::Result<_, _>>()
        .context(DeserializePartitionSnafu)
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
                    SqlValue::Number(n, _) if n == "MAXVALUE" => PartitionBound::MaxValue,
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

fn find_partition_columns(
    create_table: &CreateTableExpr,
    partitions: &Option<Partitions>,
) -> Result<Vec<String>> {
    let columns = if let Some(partitions) = partitions {
        partitions
            .column_list
            .iter()
            .map(|x| x.value.clone())
            .collect::<Vec<_>>()
    } else {
        vec![create_table.time_index.clone()]
    };
    Ok(columns)
}

#[cfg(test)]
mod test {
    use itertools::Itertools;
    use servers::query_handler::sql::SqlQueryHandlerRef;
    use session::context::QueryContext;
    use sql::dialect::GenericDialect;
    use sql::parser::ParserContext;
    use sql::statements::statement::Statement;

    use super::*;
    use crate::instance::standalone::StandaloneSqlQueryHandler;

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
            let result = ParserContext::create_with_dialect(sql, &GenericDialect {}).unwrap();
            match &result[0] {
                Statement::CreateTable(c) => {
                    let expr = expr_factory::create_to_expr(c, QueryContext::arc()).unwrap();
                    let partitions = parse_partitions(&expr, c.partitions.clone()).unwrap();
                    let json = serde_json::to_string(&partitions).unwrap();
                    assert_eq!(json, expected);
                }
                _ => unreachable!(),
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_show_databases() {
        let instance = crate::tests::create_distributed_instance("test_show_databases").await;
        let dist_instance = &instance.dist_instance;

        let sql = "create database test_show_databases";
        let output = dist_instance
            .handle_sql(sql, QueryContext::arc())
            .await
            .remove(0)
            .unwrap();
        match output {
            Output::AffectedRows(rows) => assert_eq!(rows, 1),
            _ => unreachable!(),
        }

        let sql = "show databases";
        let output = dist_instance
            .handle_sql(sql, QueryContext::arc())
            .await
            .remove(0)
            .unwrap();
        match output {
            Output::RecordBatches(r) => {
                let expected1 = vec![
                    "+---------------------+",
                    "| Schemas             |",
                    "+---------------------+",
                    "| public              |",
                    "| test_show_databases |",
                    "+---------------------+",
                ]
                .into_iter()
                .join("\n");
                let expected2 = vec![
                    "+---------------------+",
                    "| Schemas             |",
                    "+---------------------+",
                    "| test_show_databases |",
                    "| public              |",
                    "+---------------------+",
                ]
                .into_iter()
                .join("\n");
                let lines = r.pretty_print().unwrap();
                assert!(lines == expected1 || lines == expected2)
            }
            _ => unreachable!(),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_show_tables() {
        let instance = crate::tests::create_distributed_instance("test_show_tables").await;
        let dist_instance = &instance.dist_instance;
        let datanode_instances = instance.datanodes;

        let sql = "create database test_show_tables";
        dist_instance
            .handle_sql(sql, QueryContext::arc())
            .await
            .remove(0)
            .unwrap();

        let sql = "
            CREATE TABLE greptime.test_show_tables.dist_numbers (
                ts BIGINT,
                n INT,
                TIME INDEX (ts),
            )
            PARTITION BY RANGE COLUMNS (n) (
                PARTITION r0 VALUES LESS THAN (10),
                PARTITION r1 VALUES LESS THAN (20),
                PARTITION r2 VALUES LESS THAN (50),
                PARTITION r3 VALUES LESS THAN (MAXVALUE),
            )
            ENGINE=mito";
        dist_instance
            .handle_sql(sql, QueryContext::arc())
            .await
            .remove(0)
            .unwrap();

        async fn assert_show_tables(instance: SqlQueryHandlerRef<error::Error>) {
            let sql = "show tables in test_show_tables";
            let output = instance
                .do_query(sql, QueryContext::arc())
                .await
                .remove(0)
                .unwrap();
            match output {
                Output::RecordBatches(r) => {
                    let expected = r#"+--------------+
| Tables       |
+--------------+
| dist_numbers |
+--------------+"#;
                    assert_eq!(r.pretty_print().unwrap(), expected);
                }
                _ => unreachable!(),
            }
        }

        assert_show_tables(dist_instance.clone()).await;

        // Asserts that new table is created in Datanode as well.
        for x in datanode_instances.values() {
            assert_show_tables(StandaloneSqlQueryHandler::arc(x.clone())).await
        }
    }
}
