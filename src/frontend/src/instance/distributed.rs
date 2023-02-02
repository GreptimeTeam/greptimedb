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
use api::v1::{AlterExpr, CreateDatabaseExpr, CreateTableExpr, InsertRequest, TableId};
use async_trait::async_trait;
use catalog::helper::{SchemaKey, SchemaValue, TableGlobalKey, TableGlobalValue};
use catalog::{CatalogList, CatalogManager};
use chrono::DateTime;
use client::Database;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_query::Output;
use common_telemetry::{debug, error, info};
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::RawSchema;
use meta_client::client::MetaClient;
use meta_client::rpc::{
    CreateRequest as MetaCreateRequest, Partition as MetaPartition, PutRequest, RouteResponse,
    TableName, TableRoute,
};
use partition::partition::{PartitionBound, PartitionDef};
use query::parser::QueryStatement;
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
use table::table::AlterContext;

use crate::catalog::FrontendCatalogManager;
use crate::datanode::DatanodeClients;
use crate::error::{
    self, AlterExprToRequestSnafu, CatalogEntrySerdeSnafu, CatalogNotFoundSnafu, CatalogSnafu,
    ColumnDataTypeSnafu, DeserializePartitionSnafu, ParseSqlSnafu, PrimaryKeyNotFoundSnafu,
    RequestDatanodeSnafu, RequestMetaSnafu, Result, SchemaNotFoundSnafu, StartMetaClientSnafu,
    TableNotFoundSnafu, TableSnafu, ToTableInsertRequestSnafu,
};
use crate::expr_factory::{CreateExprFactory, DefaultCreateExprFactory};
use crate::instance::parse_stmt;
use crate::sql::insert_to_request;

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
    ) -> Self {
        let query_engine = QueryEngineFactory::new(catalog_manager.clone()).query_engine();
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
        let response = self.create_table_in_meta(create_table, partitions).await?;
        let table_routes = response.table_routes;
        ensure!(
            table_routes.len() == 1,
            error::CreateTableRouteSnafu {
                table_name: create_table.table_name.to_string()
            }
        );
        let table_route = table_routes.first().unwrap();
        info!(
            "Create table {:?}.{:?}.{}, table routes: {:?}",
            create_table.catalog_name,
            create_table.schema_name,
            create_table.table_name,
            table_route
        );
        let region_routes = &table_route.region_routes;
        ensure!(
            !region_routes.is_empty(),
            error::FindRegionRouteSnafu {
                table_name: create_table.table_name.to_string()
            }
        );
        create_table.table_id = Some(TableId {
            id: table_route.table.id as u32,
        });
        self.put_table_global_meta(create_table, table_route)
            .await?;

        for datanode in table_route.find_leaders() {
            let client = self.datanode_clients.get_client(&datanode).await;
            let client = Database::new("greptime", client);

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
                    .context(error::ExecuteStatementSnafu {})?;
                self.query_engine.execute(&plan).await
            }
            Statement::CreateDatabase(stmt) => {
                let expr = CreateDatabaseExpr {
                    database_name: stmt.name.to_string(),
                    create_if_not_exists: stmt.if_not_exists,
                };
                Ok(self.handle_create_database(expr).await?)
            }
            Statement::CreateTable(stmt) => {
                let create_expr = &mut DefaultCreateExprFactory.create_expr_by_stmt(&stmt).await?;
                Ok(self.create_table(create_expr, stmt.partitions).await?)
            }
            Statement::ShowDatabases(stmt) => show_databases(stmt, self.catalog_manager.clone()),
            Statement::ShowTables(stmt) => {
                show_tables(stmt, self.catalog_manager.clone(), query_ctx)
            }
            Statement::DescribeTable(stmt) => describe_table(stmt, self.catalog_manager.clone()),
            Statement::Explain(stmt) => {
                explain(Box::new(stmt), self.query_engine.clone(), query_ctx).await
            }
            Statement::Insert(insert) => {
                let (catalog, schema, table) = insert.full_table_name().context(ParseSqlSnafu)?;

                let table = self
                    .catalog_manager
                    .table(&catalog, &schema, &table)
                    .context(CatalogSnafu)?
                    .context(TableNotFoundSnafu { table_name: table })?;

                let insert_request = insert_to_request(&table, *insert)?;

                return Ok(Output::AffectedRows(
                    table.insert(insert_request).await.context(TableSnafu)?,
                ));
            }
            _ => unreachable!(),
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
    async fn handle_create_database(&self, expr: CreateDatabaseExpr) -> Result<Output> {
        let key = SchemaKey {
            // TODO(sunng87): custom catalog
            catalog_name: DEFAULT_CATALOG_NAME.to_string(),
            schema_name: expr.database_name,
        };
        let value = SchemaValue {};
        let client = self
            .meta_client
            .store_client()
            .context(StartMetaClientSnafu)?;

        let request = PutRequest::default()
            .with_key(key.to_string())
            .with_value(value.as_bytes().context(CatalogEntrySerdeSnafu)?);
        client.put(request.into()).await.context(RequestMetaSnafu)?;

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
            .catalog(catalog_name)
            .context(CatalogSnafu)?
            .context(CatalogNotFoundSnafu { catalog_name })?
            .schema(schema_name)
            .context(CatalogSnafu)?
            .context(SchemaNotFoundSnafu {
                schema_info: format!("{catalog_name}.{schema_name}"),
            })?
            .table(table_name)
            .context(CatalogSnafu)?
            .context(TableNotFoundSnafu {
                table_name: format!("{catalog_name}.{schema_name}.{table_name}"),
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
        };
        self.meta_client
            .create_route(request)
            .await
            .context(error::RequestMetaSnafu)
    }

    // TODO(LFC): Maybe move this to FrontendCatalogManager's "register_table" method?
    async fn put_table_global_meta(
        &self,
        create_table: &CreateTableExpr,
        table_route: &TableRoute,
    ) -> Result<()> {
        let table_name = &table_route.table.table_name;
        let key = TableGlobalKey {
            catalog_name: table_name.catalog_name.clone(),
            schema_name: table_name.schema_name.clone(),
            table_name: table_name.table_name.clone(),
        }
        .to_string();

        let value = create_table_global_value(create_table, table_route)?
            .as_bytes()
            .context(error::CatalogEntrySerdeSnafu)?;

        if let Err(existing) = self
            .catalog_manager
            .backend()
            .compare_and_set(key.as_bytes(), &[], &value)
            .await
            .context(CatalogSnafu)?
        {
            let existing_bytes = existing.unwrap(); // this unwrap is safe since we compare with empty bytes and failed
            let existing_value =
                TableGlobalValue::from_bytes(&existing_bytes).context(CatalogEntrySerdeSnafu)?;
            if existing_value.table_info.ident.table_id
                != create_table.table_id.as_ref().unwrap().id
            {
                error!(
                    "Table with name {} already exists, value in catalog: {:?}",
                    key, existing_bytes
                );
                return error::TableAlreadyExistSnafu { table: key }.fail();
            }
        }
        Ok(())
    }

    // TODO(LFC): Refactor insertion implementation for DistTable,
    // GRPC InsertRequest to Table InsertRequest, than split Table InsertRequest, than assemble each GRPC InsertRequest, is rather inefficient,
    // should operate on GRPC InsertRequest directly.
    // Also remember to check the "region_number" carried in InsertRequest, too.
    async fn handle_dist_insert(&self, request: InsertRequest) -> Result<Output> {
        let table_name = &request.table_name;
        // TODO(LFC): InsertRequest should carry catalog name, too.
        let table = self
            .catalog_manager
            .table(DEFAULT_CATALOG_NAME, &request.schema_name, table_name)
            .context(CatalogSnafu)?
            .context(TableNotFoundSnafu { table_name })?;

        let request = common_grpc_expr::insert::to_table_insert_request(request)
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

    async fn do_statement_query(
        &self,
        stmt: Statement,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        self.handle_statement(stmt, query_ctx).await
    }

    fn is_valid_schema(&self, catalog: &str, schema: &str) -> Result<bool> {
        self.catalog_manager
            .schema(catalog, schema)
            .map(|s| s.is_some())
            .context(CatalogSnafu)
    }
}

fn create_table_global_value(
    create_table: &CreateTableExpr,
    table_route: &TableRoute,
) -> Result<TableGlobalValue> {
    let table_name = &table_route.table.table_name;

    let region_routes = &table_route.region_routes;
    let node_id = region_routes[0]
        .leader_peer
        .as_ref()
        .with_context(|| error::FindLeaderPeerSnafu {
            region: region_routes[0].region.id,
            table_name: table_name.to_string(),
        })?
        .id;

    let mut regions_id_map = HashMap::new();
    for route in region_routes.iter() {
        let node_id = route
            .leader_peer
            .as_ref()
            .with_context(|| error::FindLeaderPeerSnafu {
                region: route.region.id,
                table_name: table_name.to_string(),
            })?
            .id;
        regions_id_map
            .entry(node_id)
            .or_insert_with(Vec::new)
            .push(route.region.id as u32);
    }

    let mut column_schemas = Vec::with_capacity(create_table.column_defs.len());
    let mut column_name_to_index_map = HashMap::new();

    for (idx, column) in create_table.column_defs.iter().enumerate() {
        let schema = column
            .try_as_column_schema()
            .context(error::InvalidColumnDefSnafu {
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
        options: HashMap::new(),
        created_on: DateTime::default(),
    };

    let desc = if create_table.desc.is_empty() {
        None
    } else {
        Some(create_table.desc.clone())
    };

    let table_info = RawTableInfo {
        ident: TableIdent {
            table_id: table_route.table.id as u32,
            version: 0,
        },
        name: table_name.table_name.clone(),
        desc,
        catalog_name: table_name.catalog_name.clone(),
        schema_name: table_name.schema_name.clone(),
        meta,
        table_type: TableType::Base,
    };

    Ok(TableGlobalValue {
        node_id,
        regions_id_map,
        table_info,
    })
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
                        sql_value_to_value(column_name, data_type, v)
                            .context(error::ParseSqlSnafu)?,
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
    use crate::expr_factory::{CreateExprFactory, DefaultCreateExprFactory};
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
                    let expr = DefaultCreateExprFactory
                        .create_expr_by_stmt(c)
                        .await
                        .unwrap();
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
