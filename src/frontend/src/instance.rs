use std::collections::HashMap;
use std::sync::Arc;

use api::helper::ColumnDataTypeWrapper;
use api::v1::{
    AdminExpr, AdminResult, ColumnDataType, ColumnDef as GrpcColumnDef, CreateExpr, ObjectExpr,
    ObjectResult as GrpcObjectResult,
};
use async_trait::async_trait;
use catalog::local::{MemoryCatalogProvider, MemorySchemaProvider};
use catalog::remote::MetaKvBackend;
use catalog::{CatalogList, CatalogListRef, CatalogProvider};
use client::admin::{admin_result_to_output, Admin};
use client::{Client, Database};
use common_error::prelude::BoxedError;
use common_query::Output;
use common_telemetry::info;
use datatypes::schema::ColumnSchema;
use datatypes::value::Value;
use meta_client::client::MetaClient;
use query::{QueryEngineFactory, QueryEngineRef};
use servers::error as server_error;
use servers::query_handler::{GrpcAdminHandler, GrpcQueryHandler, SqlQueryHandler};
use snafu::prelude::*;
use sql::ast::{ColumnDef, TableConstraint};
use sql::statements::column_def_to_schema;
use sql::statements::create_table::{CreateTable, TIME_INDEX};
use sql::statements::statement::Statement;
use sql::statements::{
    sql_data_type_to_concrete_data_type, sql_value_to_value, table_idents_to_full_name,
};
use sql::{dialect::GenericDialect, parser::ParserContext};
use tokio::sync::RwLock;

use crate::catalog::FrontendCatalogList;
use crate::error::{self, ConvertColumnDefaultConstraintSnafu, Result};
use crate::frontend::FrontendOptions;
use crate::mock::{RangePartitionRule, Region};
use crate::sql::insert_to_request;

mod influxdb;
mod opentsdb;

pub(crate) type InstanceRef = Arc<Instance>;

pub struct Instance {
    dbs: Vec<Database>,
    admins: Vec<Admin>,
    query_engine: QueryEngineRef,
    catalog_list: CatalogListRef,
    partition_rule: Arc<RwLock<HashMap<String, RangePartitionRule>>>,
}

impl Instance {
    pub(crate) async fn new() -> Self {
        let meta_client = Self::prepare_meta_client().await;
        let partition_rules = Arc::new(RwLock::new(HashMap::new()));
        let catalog_list = Arc::new(FrontendCatalogList::new(
            Arc::new(MetaKvBackend {
                client: meta_client,
            }),
            partition_rules.clone(),
        ));

        info!(
            "Starting frontend instance, default tables: {:?}",
            catalog_list
                .catalog("greptime")
                .unwrap()
                .unwrap()
                .schema("public")
                .unwrap()
                .unwrap()
                .table_names()
        );

        let factory = QueryEngineFactory::new(catalog_list.clone());
        let query_engine = factory.query_engine().clone();

        Self {
            dbs: vec![
                Database::new("greptime", Client::default()),
                Database::new("greptime", Client::default()),
                Database::new("greptime", Client::default()),
            ],
            admins: vec![
                Admin::new("greptime", Client::default()),
                Admin::new("greptime", Client::default()),
                Admin::new("greptime", Client::default()),
            ],
            query_engine,
            catalog_list,
            partition_rule: partition_rules,
        }
    }

    pub async fn prepare_meta_client() -> MetaClient {
        let config = common_grpc::channel_manager::ChannelConfig::new()
            .timeout(std::time::Duration::from_secs(3))
            .connect_timeout(std::time::Duration::from_secs(5))
            .tcp_nodelay(true);

        let channel_manager = common_grpc::channel_manager::ChannelManager::with_config(config);
        let mut meta_client = meta_client::client::MetaClientBuilder::new(0, 1)
            .enable_heartbeat()
            .enable_router()
            .enable_store()
            .channel_manager(channel_manager)
            .build();

        meta_client
            .start(&["proxy.huanglei.rocks:22009"])
            .await
            .unwrap();
        meta_client
    }

    pub(crate) async fn start(&mut self, opts: &FrontendOptions) -> Result<()> {
        self.dbs[0].start("http://127.0.0.1:4100").await.unwrap();
        self.dbs[1].start("http://127.0.0.1:4200").await.unwrap();
        self.dbs[2].start("http://127.0.0.1:4300").await.unwrap();

        self.admins[0].start("http://127.0.0.1:4100").await.unwrap();
        self.admins[1].start("http://127.0.0.1:4200").await.unwrap();
        self.admins[2].start("http://127.0.0.1:4300").await.unwrap();

        // let addr = opts.datanode_grpc_addr();
        // self.admin
        //     .start(addr.clone())
        //     .await
        //     .context(error::ConnectDatanodeSnafu { addr })?;
        Ok(())
    }

    async fn register_dist_table(&self, create_table: &CreateTable) {
        let (catalog_name, schema_name, table_name) =
            table_idents_to_full_name(&create_table.name).unwrap();

        let column_schemas = create_table
            .columns
            .iter()
            .map(|x| column_def_to_schema(x).unwrap())
            .collect::<Vec<ColumnSchema>>();
        // let table_schema = Schema::new(column_schemas);

        let partition_rule = create_partition_rule(&create_table);
        let mut this_partition_rule = self.partition_rule.write().await;
        this_partition_rule.insert(table_name, partition_rule);

        // let mut region_dist_map = HashMap::new();
        // let partitions = create_table.partitions.as_ref().unwrap().entries.len();
        // for i in 0..partitions {
        //     region_dist_map.insert(
        //         Region::new(i as u64 + 1),
        //         Datanode::new((i as u64 + 1) * 100),
        //     );
        // }
        //
        // let dist_table = Arc::new(DistTable {
        //     table_name: table_name.clone(),
        //     schema: Arc::new(table_schema),
        //     partition_rule,
        //     region_dist_map,
        //     datanode_instances: Arc::new(Mutex::new(HashMap::new())),
        // });
        //
        // let catalog_list = self.catalog_list.clone();
        // let datanode_instance1 = DatanodeInstance::new(catalog_list.clone(), self.dbs[0].clone());
        // let datanode_instance2 = DatanodeInstance::new(catalog_list.clone(), self.dbs[1].clone());
        // let datanode_instance3 = DatanodeInstance::new(catalog_list.clone(), self.dbs[2].clone());
        //
        // let mut datanode_instances = dist_table.datanode_instances.lock().await;
        // datanode_instances.insert(Datanode::new(100), datanode_instance1);
        // datanode_instances.insert(Datanode::new(200), datanode_instance2);
        // datanode_instances.insert(Datanode::new(300), datanode_instance3);
        //
        // let catalog_provider = catalog_list.catalog(&catalog_name).unwrap().unwrap();
        // let schema_provider = catalog_provider.schema(&schema_name).unwrap().unwrap();
        // schema_provider
        //     .register_table(table_name, dist_table.clone())
        //     .unwrap();
    }
}

#[cfg(test)]
impl Instance {
    pub fn with_client(client: Client) -> Self {
        let catalog_list = catalog::local::memory::new_memory_catalog_list().unwrap();
        let catalog = Arc::new(MemoryCatalogProvider::new());
        let schema = Arc::new(MemorySchemaProvider::new());

        catalog
            .register_schema("public".to_string(), schema)
            .unwrap();
        catalog_list
            .register_catalog("greptime".to_string(), catalog)
            .unwrap();

        let factory = QueryEngineFactory::new(catalog_list.clone());
        let query_engine = factory.query_engine().clone();

        Self {
            dbs: vec![Database::new("greptime", client.clone())],
            admins: vec![Admin::new("greptime", client)],
            query_engine,
            catalog_list,
            partition_rule: Arc::new(Default::default()),
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
            Statement::Query(_) => {
                let plan = self.query_engine.statement_to_plan(stmt).unwrap();
                let output = self.query_engine.execute(&plan).await.unwrap();
                Ok(output)
            }
            Statement::Insert(insert) => {
                let table_name = insert.table_name().clone();
                let catalog = self.catalog_list.catalog("greptime").unwrap().unwrap();
                let schema_provider = catalog.schema("public").unwrap().unwrap();
                // TODO(fys): remove unwrap()
                let inert_request = insert_to_request(&schema_provider, *insert).unwrap();
                let table = schema_provider.table(&table_name).unwrap().unwrap();
                let ret = table.insert(inert_request).await.unwrap();
                Ok(Output::AffectedRows(ret))
            }
            Statement::Create(create) => {
                if create.partitions.is_some() {
                    self.register_dist_table(&create).await;
                }

                let expr = create_to_expr(create)
                    .map_err(BoxedError::new)
                    .context(server_error::ExecuteQuerySnafu { query })?;
                let mut output = None;
                for admin in &self.admins {
                    output = Some(
                        admin
                            .create(expr.clone())
                            .await
                            .and_then(admin_result_to_output)
                            .map_err(BoxedError::new)
                            .context(server_error::ExecuteQuerySnafu { query })?,
                    );
                }
                Ok(output.unwrap())
            }
            // TODO(LFC): Support other SQL execution,
            // update, delete, alter, explain, etc.
            _ => return server_error::NotSupportedSnafu { feat: query }.fail(),
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

fn create_partition_rule(create: &CreateTable) -> RangePartitionRule {
    let sql_partition = create.partitions.as_ref().unwrap();
    assert_eq!(
        sql_partition.column_list.len(),
        1,
        "currently only one partition column is allowed"
    );
    let i = 0;
    let column = &sql_partition.column_list[i];

    let partition_points = sql_partition
        .entries
        .iter()
        .take(&sql_partition.entries.len() - 1)
        .map(|x| {
            let sql_value = &x.value_list[i];
            let column_def = create
                .columns
                .iter()
                .find(|x| x.name.value == column.value)
                .unwrap();
            let data_type = sql_data_type_to_concrete_data_type(&column_def.data_type).unwrap();
            sql_value_to_value(&column.value, &data_type, sql_value).unwrap()
        })
        .collect::<Vec<Value>>();

    let regions = (0..=partition_points.len())
        .map(|x| Region::new((x + 1) as u64))
        .collect::<Vec<Region>>();

    RangePartitionRule::new(&column.value, partition_points, regions)
}

fn create_to_expr(create: CreateTable) -> Result<CreateExpr> {
    let (catalog_name, schema_name, table_name) =
        table_idents_to_full_name(&create.name).context(error::ParseSqlSnafu)?;

    let expr = CreateExpr {
        catalog_name: Some(catalog_name),
        schema_name: Some(schema_name),
        table_name,
        column_defs: columns_to_expr(&create.columns)?,
        time_index: find_time_index(&create.constraints)?,
        primary_keys: find_primary_keys(&create.constraints)?,
        create_if_not_exists: create.if_not_exists,
        // TODO(LFC): Fill in other table options.
        table_options: HashMap::from([("engine".to_string(), create.engine)]),
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

fn columns_to_expr(column_defs: &[ColumnDef]) -> Result<Vec<GrpcColumnDef>> {
    let column_schemas = column_defs
        .iter()
        .map(|c| column_def_to_schema(c).context(error::ParseSqlSnafu))
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
        // self.db
        //     .object(query.clone())
        //     .await
        //     .map_err(BoxedError::new)
        //     .with_context(|_| server_error::ExecuteQuerySnafu {
        //         query: format!("{:?}", query),
        //     })
        todo!()
    }
}

#[async_trait]
impl GrpcAdminHandler for Instance {
    async fn exec_admin_request(&self, expr: AdminExpr) -> server_error::Result<AdminResult> {
        // self.admin
        //     .do_request(expr.clone())
        //     .await
        //     .map_err(BoxedError::new)
        //     .with_context(|_| server_error::ExecuteQuerySnafu {
        //         query: format!("{:?}", expr),
        //     })
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use api::v1::codec::{InsertBatch, SelectResult};
    use api::v1::{
        admin_expr, admin_result, column, column::SemanticType, insert_expr, object_expr,
        object_result, select_expr, Column, ExprHeader, InsertExpr, MutateResult, SelectExpr,
    };
    use catalog::{CatalogProvider, SchemaProvider};
    use datafusion::arrow_print;
    use datafusion_common::record_batch::RecordBatch as DfRecordBatch;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{ColumnDefaultConstraint, Schema};
    use datatypes::value::Value;

    use super::*;
    use crate::mock::{Datanode, DatanodeInstance, RangePartitionRule, Region};
    use crate::table::DistTable;
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
                let recordbatches = recordbatches
                    .take()
                    .into_iter()
                    .map(|r| r.df_recordbatch)
                    .collect::<Vec<DfRecordBatch>>();
                let pretty_print = arrow_print::write(&recordbatches);
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
                let recordbatches = recordbatches
                    .take()
                    .into_iter()
                    .map(|r| r.df_recordbatch)
                    .collect::<Vec<DfRecordBatch>>();
                let pretty_print = arrow_print::write(&recordbatches);
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
            // FIXME(dennis): looks like the read schema in table scan doesn't have timestamp index, we have to investigate it.
            semantic_type: SemanticType::Field as i32,
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
        CreateExpr {
            table_name: "demo".to_string(),
            column_defs,
            time_index: "ts".to_string(),
            primary_keys: vec!["ts".to_string(), "host".to_string()],
            create_if_not_exists: true,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn create_tables_in_datanodes() {
        let column_defs = vec![
            GrpcColumnDef {
                name: "ts".to_string(),
                datatype: ColumnDataType::Timestamp as i32,
                is_nullable: false,
                default_constraint: Some(
                    ColumnDefaultConstraint::Function("current_timestamp()".to_string())
                        .try_into()
                        .unwrap(),
                ),
            },
            GrpcColumnDef {
                name: "n".to_string(),
                datatype: ColumnDataType::Int32 as i32,
                is_nullable: true,
                default_constraint: None,
            },
            GrpcColumnDef {
                name: "row_id".to_string(),
                datatype: ColumnDataType::Uint32 as i32,
                is_nullable: true,
                default_constraint: None,
            },
        ];
        let create_expr = CreateExpr {
            catalog_name: Some("greptime".to_string()),
            schema_name: Some("public".to_string()),
            table_name: "dist_table".to_string(),
            desc: None,
            column_defs,
            time_index: "ts".to_string(),
            primary_keys: Vec::new(),
            create_if_not_exists: true,
            table_options: HashMap::new(),
        };

        let client1 = Client::default();
        let mut admin1 = Admin::new("greptime", client1.clone());
        admin1.start("http://127.0.0.1:4100").await.unwrap();
        let result = admin1.create(create_expr.clone()).await.unwrap();
        let output = admin_result_to_output(result).unwrap();
        match output {
            Output::AffectedRows(x) => println!("affected rows: {}", x),
            _ => unreachable!(),
        }

        let client2 = Client::default();
        let mut admin2 = Admin::new("greptime", client2.clone());
        admin2.start("http://127.0.0.1:4200").await.unwrap();
        admin2.create(create_expr.clone()).await.unwrap();

        let client3 = Client::default();
        let mut admin3 = Admin::new("greptime", client3.clone());
        admin3.start("http://127.0.0.1:4300").await.unwrap();
        admin3.create(create_expr.clone()).await.unwrap();
    }

    #[tokio::test]
    async fn fill_data_in_dist_table() {
        let table_name = "dist_table".to_string();

        let client1 = Client::default();
        let mut db1 = Database::new("greptime", client1.clone());
        db1.start("http://127.0.0.1:4100").await.unwrap();
        fill_data(&table_name, &db1, (0..10).collect::<Vec<i32>>()).await;

        let client2 = Client::default();
        let mut db2 = Database::new("greptime", client2.clone());
        db2.start("http://127.0.0.1:4200").await.unwrap();
        fill_data(&table_name, &db2, (20..40).collect::<Vec<i32>>()).await;

        let client3 = Client::default();
        let mut db3 = Database::new("greptime", client3.clone());
        db3.start("http://127.0.0.1:4300").await.unwrap();
        fill_data(&table_name, &db3, (100..200).collect::<Vec<i32>>()).await;
    }

    #[tokio::test]
    async fn test_dist_table_scan() {
        let instance = Arc::new(new_frontend_instance().await);

        // let sql = "select n, row_id from dist_table order by n";
        let sql = "select n, row_id from dist_table where n < 10";
        println!("{}", sql);
        exec_table_scan(instance.clone(), sql).await;

        let sql = "select n, row_id from dist_table where n < 50 and n >= 20";
        println!("{}", sql);
        exec_table_scan(instance.clone(), sql).await;

        let sql = "select n, row_id from dist_table where n < 1000 and row_id = 1";
        println!("{}", sql);
        exec_table_scan(instance.clone(), sql).await;
    }

    async fn exec_table_scan(instance: InstanceRef, sql: &str) {
        let output = SqlQueryHandler::do_query(&*instance, sql).await.unwrap();
        println!("do query: {}", sql);
        let recordbatches = match output {
            Output::RecordBatches(recordbatches) => recordbatches.take(),
            Output::Stream(stream) => common_recordbatch::util::collect(stream).await.unwrap(),
            _ => unreachable!(),
        };

        let df_recordbatch = recordbatches
            .into_iter()
            .map(|r| r.df_recordbatch)
            .collect::<Vec<DfRecordBatch>>();

        let pretty_print = arrow_print::write(&df_recordbatch);
        let pretty_print = pretty_print.lines().collect::<Vec<&str>>();
        pretty_print.iter().for_each(|x| println!("{}", x));
    }

    async fn new_frontend_instance() -> Instance {
        let mut instance = Instance::new().await;
        instance.start(&FrontendOptions::default()).await.unwrap();
        instance
    }

    async fn fill_data(table_name: &String, db: &Database, data: Vec<i32>) {
        for (i, v) in data.iter().enumerate() {
            let sql = format!(
                "insert into {}(n, row_id) values ({}, {})",
                table_name,
                v,
                i + 1
            );
            println!("sql: {}", sql);
            let insert_expr = InsertExpr {
                table_name: table_name.clone(),
                expr: Some(insert_expr::Expr::Sql(sql)),
                options: HashMap::default(),
            };
            db.insert(insert_expr).await.unwrap();
        }
    }
}
