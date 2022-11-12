mod influxdb;
mod opentsdb;
mod prometheus;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use api::helper::ColumnDataTypeWrapper;
use api::v1::{
    insert_expr, AdminExpr, AdminResult, AlterExpr, ColumnDataType, ColumnDef as GrpcColumnDef,
    CreateDatabaseExpr, CreateExpr, InsertExpr, ObjectExpr, ObjectResult as GrpcObjectResult,
};
use async_trait::async_trait;
use catalog::remote::MetaKvBackend;
use catalog::{CatalogList, CatalogProviderRef, SchemaProviderRef};
use client::admin::{admin_result_to_output, Admin};
use client::{Client, Database, Select};
use common_error::prelude::BoxedError;
use common_grpc::channel_manager::{ChannelConfig, ChannelManager};
use common_query::Output;
use datatypes::schema::ColumnSchema;
use meta_client::client::MetaClientBuilder;
use meta_client::MetaClientOpts;
use servers::error as server_error;
use servers::query_handler::{
    GrpcAdminHandler, GrpcQueryHandler, InfluxdbLineProtocolHandler, OpentsdbProtocolHandler,
    PrometheusProtocolHandler, SqlQueryHandler,
};
use snafu::prelude::*;
use sql::ast::{ColumnDef, TableConstraint};
use sql::statements::create::{CreateTable, TIME_INDEX};
use sql::statements::insert::Insert;
use sql::statements::statement::Statement;
use sql::statements::{column_def_to_schema, table_idents_to_full_name};
use sql::{dialect::GenericDialect, parser::ParserContext};

use crate::catalog::FrontendCatalogManager;
use crate::datanode::DatanodeClients;
use crate::error::{self, ConvertColumnDefaultConstraintSnafu, Result};
use crate::frontend::{FrontendOptions, Mode};
use crate::sql::insert_to_request;
use crate::table::route::TableRoutes;

#[async_trait]
pub trait FrontendInstance:
    GrpcAdminHandler
    + GrpcQueryHandler
    + SqlQueryHandler
    + OpentsdbProtocolHandler
    + InfluxdbLineProtocolHandler
    + PrometheusProtocolHandler
    + Send
    + Sync
    + 'static
{
    async fn start(&mut self) -> Result<()>;
}

pub type FrontendInstanceRef = Arc<dyn FrontendInstance>;

#[derive(Clone)]
pub struct Instance {
    // TODO(hl): In standalone mode, there is only one client.
    // But in distribute mode, frontend should fetch datanodes' addresses from metasrv.
    client: Client,
    /// catalog manager is None in standalone mode, datanode will keep their own
    catalog_manager: Option<FrontendCatalogManager>,
    mode: Mode,
}

impl Default for Instance {
    fn default() -> Self {
        Self {
            client: Client::default(),
            catalog_manager: None,
            mode: Mode::Standalone,
        }
    }
}

impl Instance {
    pub async fn try_new(opts: &FrontendOptions) -> Result<Self> {
        let mut instance = Instance::default();
        let addr = opts.datanode_grpc_addr();
        instance.client.start(vec![addr]);

        let meta_client = match opts.mode {
            Mode::Standalone => None,
            Mode::Distributed => {
                let meta_config = MetaClientOpts::default();
                let channel_config = ChannelConfig::new()
                    .timeout(Duration::from_millis(meta_config.timeout_millis))
                    .connect_timeout(Duration::from_millis(meta_config.connect_timeout_millis))
                    .tcp_nodelay(meta_config.tcp_nodelay);

                let channel_manager = ChannelManager::with_config(channel_config);

                let meta_client = MetaClientBuilder::new(0, 0)
                    .enable_router()
                    .enable_store()
                    .channel_manager(channel_manager)
                    .build();
                Some(Arc::new(meta_client))
            }
        };

        instance.catalog_manager = if let Some(meta_client) = meta_client {
            let meta_backend = Arc::new(MetaKvBackend {
                client: meta_client.clone(),
            });
            let table_routes = Arc::new(TableRoutes::new(meta_client));
            let datanode_clients = Arc::new(DatanodeClients::new());
            let catalog_manager =
                FrontendCatalogManager::new(meta_backend, table_routes, datanode_clients);
            Some(catalog_manager)
        } else {
            None
        };
        Ok(instance)
    }

    // TODO(fys): temporarily hard code
    pub fn database(&self) -> Database {
        Database::new("greptime", self.client.clone())
    }

    // TODO(fys): temporarily hard code
    pub fn admin(&self) -> Admin {
        Admin::new("greptime", self.client.clone())
    }

    fn get_catalog(&self, catalog_name: &str) -> Result<CatalogProviderRef> {
        self.catalog_manager
            .as_ref()
            .context(error::CatalogManagerSnafu)?
            .catalog(catalog_name)
            .context(error::CatalogSnafu)?
            .context(error::CatalogNotFoundSnafu { catalog_name })
    }

    fn get_schema(provider: CatalogProviderRef, schema_name: &str) -> Result<SchemaProviderRef> {
        provider
            .schema(schema_name)
            .context(error::CatalogSnafu)?
            .context(error::SchemaNotFoundSnafu {
                schema_info: schema_name,
            })
    }

    async fn sql_dist_insert(&self, insert: Box<Insert>) -> Result<usize> {
        let (catalog, schema, table) = insert.full_table_name().context(error::ParseSqlSnafu)?;

        let catalog_provider = self.get_catalog(&catalog)?;
        let schema_provider = Self::get_schema(catalog_provider, &schema)?;

        let insert_request = insert_to_request(&schema_provider, *insert)?;

        let table = schema_provider
            .table(&table)
            .context(error::CatalogSnafu)?
            .context(error::TableNotFoundSnafu { table_name: &table })?;

        table
            .insert(insert_request)
            .await
            .context(error::TableSnafu)
    }
}

#[async_trait]
impl FrontendInstance for Instance {
    async fn start(&mut self) -> Result<()> {
        // TODO(hl): Frontend init should move to here
        Ok(())
    }
}

#[cfg(test)]
impl Instance {
    pub fn with_client(client: Client) -> Self {
        Self {
            client,
            catalog_manager: None,
            mode: Mode::Standalone,
        }
    }
}

#[async_trait]
impl SqlQueryHandler for Instance {
    async fn do_query(&self, query: &str) -> server_error::Result<Output> {
        let mut stmt = ParserContext::create_with_dialect(query, &GenericDialect {})
            .map_err(BoxedError::new)
            .context(server_error::ExecuteQuerySnafu { query })?;
        if stmt.len() != 1 {
            // TODO(LFC): Support executing multiple SQLs,
            // which seems to be a major change to our whole server framework?
            return server_error::NotSupportedSnafu {
                feat: "Only one SQL is allowed to be executed at one time.",
            }
            .fail();
        }
        let stmt = stmt.remove(0);

        match stmt {
            Statement::Query(_) => self
                .database()
                .select(Select::Sql(query.to_string()))
                .await
                .and_then(|object_result| object_result.try_into())
                .map_err(BoxedError::new)
                .context(server_error::ExecuteQuerySnafu { query }),
            Statement::Insert(insert) => {
                match self.mode {
                    Mode::Standalone => {
                        // TODO(dennis): respect schema_name when inserting data
                        let (_catalog_name, _schema_name, table_name) = insert
                            .full_table_name()
                            .context(error::ParseSqlSnafu)
                            .map_err(BoxedError::new)
                            .context(server_error::ExecuteInsertSnafu {
                                msg: "Failed to get table name",
                            })?;

                        let expr = InsertExpr {
                            table_name,
                            expr: Some(insert_expr::Expr::Sql(query.to_string())),
                            options: HashMap::default(),
                        };
                        self.database()
                            .insert(expr)
                            .await
                            .and_then(|object_result| object_result.try_into())
                            .map_err(BoxedError::new)
                            .context(server_error::ExecuteQuerySnafu { query })
                    }
                    Mode::Distributed => {
                        let affected = self
                            .sql_dist_insert(insert)
                            .await
                            .map_err(BoxedError::new)
                            .context(server_error::ExecuteInsertSnafu {
                                msg: "execute insert failed",
                            })?;
                        Ok(Output::AffectedRows(affected))
                    }
                }
            }
            Statement::CreateTable(create) => {
                let expr = create_to_expr(create)
                    .map_err(BoxedError::new)
                    .context(server_error::ExecuteQuerySnafu { query })?;
                self.admin()
                    .create(expr)
                    .await
                    .and_then(admin_result_to_output)
                    .map_err(BoxedError::new)
                    .context(server_error::ExecuteQuerySnafu { query })
            }

            Statement::ShowDatabases(_) | Statement::ShowTables(_) => self
                .database()
                .select(Select::Sql(query.to_string()))
                .await
                .and_then(|object_result| object_result.try_into())
                .map_err(BoxedError::new)
                .context(server_error::ExecuteQuerySnafu { query }),

            Statement::CreateDatabase(c) => {
                let expr = CreateDatabaseExpr {
                    database_name: c.name.to_string(),
                };
                self.admin()
                    .create_database(expr)
                    .await
                    .and_then(admin_result_to_output)
                    .map_err(BoxedError::new)
                    .context(server_error::ExecuteQuerySnafu { query })
            }
            Statement::Alter(alter_stmt) => self
                .admin()
                .alter(
                    AlterExpr::try_from(alter_stmt)
                        .map_err(BoxedError::new)
                        .context(server_error::ExecuteAlterSnafu { query })?,
                )
                .await
                .and_then(admin_result_to_output)
                .map_err(BoxedError::new)
                .context(server_error::ExecuteQuerySnafu { query }),
            Statement::ShowCreateTable(_) => {
                return server_error::NotSupportedSnafu { feat: query }.fail()
            }
        }
    }

    async fn insert_script(&self, _name: &str, _script: &str) -> server_error::Result<()> {
        server_error::NotSupportedSnafu {
            feat: "Script execution in Frontend",
        }
        .fail()
    }

    async fn execute_script(&self, _script: &str) -> server_error::Result<Output> {
        server_error::NotSupportedSnafu {
            feat: "Script execution in Frontend",
        }
        .fail()
    }
}

fn create_to_expr(create: CreateTable) -> Result<CreateExpr> {
    let (catalog_name, schema_name, table_name) =
        table_idents_to_full_name(&create.name).context(error::ParseSqlSnafu)?;

    let time_index = find_time_index(&create.constraints)?;
    let expr = CreateExpr {
        catalog_name: Some(catalog_name),
        schema_name: Some(schema_name),
        table_name,
        column_defs: columns_to_expr(&create.columns, &time_index)?,
        time_index,
        primary_keys: find_primary_keys(&create.constraints)?,
        create_if_not_exists: create.if_not_exists,
        // TODO(LFC): Fill in other table options.
        table_options: HashMap::from([
            ("engine".to_string(), create.engine),
            ("region_id".to_string(), "0".to_string()),
        ]),
        ..Default::default()
    };
    Ok(expr)
}

fn find_primary_keys(constraints: &[TableConstraint]) -> Result<Vec<String>> {
    let primary_keys = constraints
        .iter()
        .filter_map(|constraint| match constraint {
            TableConstraint::Unique {
                name: _,
                columns,
                is_primary: true,
            } => Some(columns.iter().map(|ident| ident.value.clone())),
            _ => None,
        })
        .flatten()
        .collect::<Vec<String>>();
    Ok(primary_keys)
}

fn find_time_index(constraints: &[TableConstraint]) -> Result<String> {
    let time_index = constraints
        .iter()
        .filter_map(|constraint| match constraint {
            TableConstraint::Unique {
                name: Some(name),
                columns,
                is_primary: false,
            } => {
                if name.value == TIME_INDEX {
                    Some(columns.iter().map(|ident| &ident.value))
                } else {
                    None
                }
            }
            _ => None,
        })
        .flatten()
        .collect::<Vec<&String>>();
    ensure!(
        time_index.len() == 1,
        error::InvalidSqlSnafu {
            err_msg: "must have one and only one TimeIndex columns",
        }
    );
    Ok(time_index.first().unwrap().to_string())
}

fn columns_to_expr(column_defs: &[ColumnDef], time_index: &str) -> Result<Vec<GrpcColumnDef>> {
    let column_schemas = column_defs
        .iter()
        .map(|c| {
            column_def_to_schema(c, c.name.to_string() == time_index).context(error::ParseSqlSnafu)
        })
        .collect::<Result<Vec<ColumnSchema>>>()?;

    let column_datatypes = column_schemas
        .iter()
        .map(|c| {
            ColumnDataTypeWrapper::try_from(c.data_type.clone())
                .map(|w| w.datatype())
                .context(error::ColumnDataTypeSnafu)
        })
        .collect::<Result<Vec<ColumnDataType>>>()?;

    column_schemas
        .iter()
        .zip(column_datatypes.into_iter())
        .map(|(schema, datatype)| {
            Ok(GrpcColumnDef {
                name: schema.name.clone(),
                datatype: datatype as i32,
                is_nullable: schema.is_nullable(),
                default_constraint: match schema.default_constraint() {
                    None => None,
                    Some(v) => Some(v.clone().try_into().context(
                        ConvertColumnDefaultConstraintSnafu {
                            column_name: &schema.name,
                        },
                    )?),
                },
            })
        })
        .collect()
}

#[async_trait]
impl GrpcQueryHandler for Instance {
    async fn do_query(&self, query: ObjectExpr) -> server_error::Result<GrpcObjectResult> {
        self.database()
            .object(query.clone())
            .await
            .map_err(BoxedError::new)
            .with_context(|_| server_error::ExecuteQuerySnafu {
                query: format!("{:?}", query),
            })
    }
}

#[async_trait]
impl GrpcAdminHandler for Instance {
    async fn exec_admin_request(&self, expr: AdminExpr) -> server_error::Result<AdminResult> {
        self.admin()
            .do_request(expr.clone())
            .await
            .map_err(BoxedError::new)
            .with_context(|_| server_error::ExecuteQuerySnafu {
                query: format!("{:?}", expr),
            })
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use api::v1::codec::{InsertBatch, SelectResult};
    use api::v1::{
        admin_expr, admin_result, column, column::SemanticType, object_expr, object_result,
        select_expr, Column, ExprHeader, MutateResult, SelectExpr,
    };
    use datatypes::schema::ColumnDefaultConstraint;
    use datatypes::value::Value;

    use super::*;
    use crate::tests;

    #[tokio::test]
    async fn test_execute_sql() {
        let instance = tests::create_frontend_instance().await;

        let sql = r#"CREATE TABLE demo(
                            host STRING,
                            ts TIMESTAMP,
                            cpu DOUBLE NULL,
                            memory DOUBLE NULL,
                            disk_util DOUBLE DEFAULT 9.9,
                            TIME INDEX (ts),
                            PRIMARY KEY(ts, host)
                        ) engine=mito with(regions=1);"#;
        let output = SqlQueryHandler::do_query(&*instance, sql).await.unwrap();
        match output {
            Output::AffectedRows(rows) => assert_eq!(rows, 1),
            _ => unreachable!(),
        }

        let sql = r#"insert into demo(host, cpu, memory, ts) values
                                ('frontend.host1', 1.1, 100, 1000),
                                ('frontend.host2', null, null, 2000),
                                ('frontend.host3', 3.3, 300, 3000)
                                "#;
        let output = SqlQueryHandler::do_query(&*instance, sql).await.unwrap();
        match output {
            Output::AffectedRows(rows) => assert_eq!(rows, 3),
            _ => unreachable!(),
        }

        let sql = "select * from demo";
        let output = SqlQueryHandler::do_query(&*instance, sql).await.unwrap();
        match output {
            Output::RecordBatches(recordbatches) => {
                let pretty_print = recordbatches.pretty_print();
                let pretty_print = pretty_print.lines().collect::<Vec<&str>>();
                let expected = vec![
                    "+----------------+---------------------+-----+--------+-----------+",
                    "| host           | ts                  | cpu | memory | disk_util |",
                    "+----------------+---------------------+-----+--------+-----------+",
                    "| frontend.host1 | 1970-01-01 00:00:01 | 1.1 | 100    | 9.9       |",
                    "| frontend.host2 | 1970-01-01 00:00:02 |     |        | 9.9       |",
                    "| frontend.host3 | 1970-01-01 00:00:03 | 3.3 | 300    | 9.9       |",
                    "+----------------+---------------------+-----+--------+-----------+",
                ];
                assert_eq!(pretty_print, expected);
            }
            _ => unreachable!(),
        };

        let sql = "select * from demo where ts>cast(1000000000 as timestamp)"; // use nanoseconds as where condition
        let output = SqlQueryHandler::do_query(&*instance, sql).await.unwrap();
        match output {
            Output::RecordBatches(recordbatches) => {
                let pretty_print = recordbatches.pretty_print();
                let pretty_print = pretty_print.lines().collect::<Vec<&str>>();
                let expected = vec![
                    "+----------------+---------------------+-----+--------+-----------+",
                    "| host           | ts                  | cpu | memory | disk_util |",
                    "+----------------+---------------------+-----+--------+-----------+",
                    "| frontend.host2 | 1970-01-01 00:00:02 |     |        | 9.9       |",
                    "| frontend.host3 | 1970-01-01 00:00:03 | 3.3 | 300    | 9.9       |",
                    "+----------------+---------------------+-----+--------+-----------+",
                ];
                assert_eq!(pretty_print, expected);
            }
            _ => unreachable!(),
        };
    }

    #[tokio::test]
    async fn test_execute_grpc() {
        let instance = tests::create_frontend_instance().await;

        // testing data:
        let expected_host_col = Column {
            column_name: "host".to_string(),
            values: Some(column::Values {
                string_values: vec!["fe.host.a", "fe.host.b", "fe.host.c", "fe.host.d"]
                    .into_iter()
                    .map(|s| s.to_string())
                    .collect(),
                ..Default::default()
            }),
            semantic_type: SemanticType::Field as i32,
            datatype: ColumnDataType::String as i32,
            ..Default::default()
        };
        let expected_cpu_col = Column {
            column_name: "cpu".to_string(),
            values: Some(column::Values {
                f64_values: vec![1.0, 3.0, 4.0],
                ..Default::default()
            }),
            null_mask: vec![2],
            semantic_type: SemanticType::Field as i32,
            datatype: ColumnDataType::Float64 as i32,
        };
        let expected_mem_col = Column {
            column_name: "memory".to_string(),
            values: Some(column::Values {
                f64_values: vec![100.0, 200.0, 400.0],
                ..Default::default()
            }),
            null_mask: vec![4],
            semantic_type: SemanticType::Field as i32,
            datatype: ColumnDataType::Float64 as i32,
        };
        let expected_disk_col = Column {
            column_name: "disk_util".to_string(),
            values: Some(column::Values {
                f64_values: vec![9.9, 9.9, 9.9, 9.9],
                ..Default::default()
            }),
            semantic_type: SemanticType::Field as i32,
            datatype: ColumnDataType::Float64 as i32,
            ..Default::default()
        };
        let expected_ts_col = Column {
            column_name: "ts".to_string(),
            values: Some(column::Values {
                ts_millis_values: vec![1000, 2000, 3000, 4000],
                ..Default::default()
            }),
            semantic_type: SemanticType::Timestamp as i32,
            datatype: ColumnDataType::Timestamp as i32,
            ..Default::default()
        };

        // create
        let create_expr = create_expr();
        let admin_expr = AdminExpr {
            header: Some(ExprHeader::default()),
            expr: Some(admin_expr::Expr::Create(create_expr)),
        };
        let result = GrpcAdminHandler::exec_admin_request(&*instance, admin_expr)
            .await
            .unwrap();
        assert_matches!(
            result.result,
            Some(admin_result::Result::Mutate(MutateResult {
                success: 1,
                failure: 0
            }))
        );

        // insert
        let values = vec![InsertBatch {
            columns: vec![
                expected_host_col.clone(),
                expected_cpu_col.clone(),
                expected_mem_col.clone(),
                expected_ts_col.clone(),
            ],
            row_count: 4,
        }
        .into()];
        let insert_expr = InsertExpr {
            table_name: "demo".to_string(),
            expr: Some(insert_expr::Expr::Values(insert_expr::Values { values })),
            options: HashMap::default(),
        };
        let object_expr = ObjectExpr {
            header: Some(ExprHeader::default()),
            expr: Some(object_expr::Expr::Insert(insert_expr)),
        };
        let result = GrpcQueryHandler::do_query(&*instance, object_expr)
            .await
            .unwrap();
        assert_matches!(
            result.result,
            Some(object_result::Result::Mutate(MutateResult {
                success: 4,
                failure: 0
            }))
        );

        // select
        let object_expr = ObjectExpr {
            header: Some(ExprHeader::default()),
            expr: Some(object_expr::Expr::Select(SelectExpr {
                expr: Some(select_expr::Expr::Sql("select * from demo".to_string())),
            })),
        };
        let result = GrpcQueryHandler::do_query(&*instance, object_expr)
            .await
            .unwrap();
        match result.result {
            Some(object_result::Result::Select(select_result)) => {
                let select_result: SelectResult = (*select_result.raw_data).try_into().unwrap();

                assert_eq!(4, select_result.row_count);
                let actual_columns = select_result.columns;
                assert_eq!(5, actual_columns.len());

                // Respect the order in create table schema
                let expected_columns = vec![
                    expected_host_col,
                    expected_cpu_col,
                    expected_mem_col,
                    expected_disk_col,
                    expected_ts_col,
                ];
                expected_columns
                    .iter()
                    .zip(actual_columns.iter())
                    .for_each(|(x, y)| assert_eq!(x, y));
            }
            _ => unreachable!(),
        }
    }

    fn create_expr() -> CreateExpr {
        let column_defs = vec![
            GrpcColumnDef {
                name: "host".to_string(),
                datatype: ColumnDataType::String as i32,
                is_nullable: false,
                default_constraint: None,
            },
            GrpcColumnDef {
                name: "cpu".to_string(),
                datatype: ColumnDataType::Float64 as i32,
                is_nullable: true,
                default_constraint: None,
            },
            GrpcColumnDef {
                name: "memory".to_string(),
                datatype: ColumnDataType::Float64 as i32,
                is_nullable: true,
                default_constraint: None,
            },
            GrpcColumnDef {
                name: "disk_util".to_string(),
                datatype: ColumnDataType::Float64 as i32,
                is_nullable: true,
                default_constraint: Some(
                    ColumnDefaultConstraint::Value(Value::from(9.9f64))
                        .try_into()
                        .unwrap(),
                ),
            },
            GrpcColumnDef {
                name: "ts".to_string(),
                datatype: ColumnDataType::Timestamp as i32,
                is_nullable: true,
                default_constraint: None,
            },
        ];
        let mut table_options = HashMap::with_capacity(1);
        table_options.insert("region_id".to_string(), "0".to_string());
        CreateExpr {
            table_name: "demo".to_string(),
            column_defs,
            time_index: "ts".to_string(),
            primary_keys: vec!["ts".to_string(), "host".to_string()],
            create_if_not_exists: true,
            table_options,
            ..Default::default()
        }
    }
}
