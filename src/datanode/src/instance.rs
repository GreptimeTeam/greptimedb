use std::{fs, path, sync::Arc};

use api::v1::InsertExpr;
use catalog::{CatalogManagerRef, DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_telemetry::logging::info;
use log_store::fs::{config::LogConfig, log::LocalFileLogStore};
use object_store::{backend::fs::Backend, util, ObjectStore};
use query::query_engine::{Output, QueryEngineFactory, QueryEngineRef};
use snafu::{OptionExt, ResultExt};
use sql::statements::statement::Statement;
use storage::{config::EngineConfig as StorageEngineConfig, EngineImpl};
#[cfg(test)]
use table::engine::TableEngineRef;
use table_engine::config::EngineConfig as TableEngineConfig;
use table_engine::engine::MitoEngine;

use crate::datanode::{DatanodeOptions, ObjectStoreConfig};
use crate::error::{
    self, ExecuteSqlSnafu, InsertSnafu, NewCatalogSnafu, Result, TableNotFoundSnafu,
};
use crate::server::grpc::insert::insertion_expr_to_request;
use crate::sql::{SqlHandler, SqlRequest};

type DefaultEngine = MitoEngine<EngineImpl<LocalFileLogStore>>;

// An abstraction to read/write services.
pub struct Instance {
    // Query service
    query_engine: QueryEngineRef,
    #[cfg(test)]
    table_engine: TableEngineRef,
    sql_handler: SqlHandler<DefaultEngine>,
    // Catalog list
    catalog_manager: CatalogManagerRef,
}

pub type InstanceRef = Arc<Instance>;

impl Instance {
    pub async fn new(opts: &DatanodeOptions) -> Result<Self> {
        let object_store = new_object_store(&opts.storage).await?;
        let log_store = create_local_file_log_store(opts).await?;

        let table_engine = DefaultEngine::new(
            TableEngineConfig::default(),
            EngineImpl::new(
                StorageEngineConfig::default(),
                Arc::new(log_store),
                object_store.clone(),
            ),
            object_store,
        );
        let catalog_manager = Arc::new(
            catalog::LocalCatalogManager::try_new(Arc::new(table_engine.clone()))
                .await
                .context(NewCatalogSnafu)?,
        );
        let factory = QueryEngineFactory::new(catalog_manager.clone());
        let query_engine = factory.query_engine().clone();

        Ok(Self {
            query_engine,
            #[cfg(test)]
            table_engine: Arc::new(table_engine.clone()),
            sql_handler: SqlHandler::new(table_engine, catalog_manager.clone()),
            catalog_manager,
        })
    }

    pub async fn execute_grpc_insert(&self, insert_expr: InsertExpr) -> Result<Output> {
        let schema_provider = self
            .catalog_manager
            .catalog(DEFAULT_CATALOG_NAME)
            .unwrap()
            .schema(DEFAULT_SCHEMA_NAME)
            .unwrap();

        let table_name = &insert_expr.table_name.clone();
        let table = schema_provider
            .table(table_name)
            .context(TableNotFoundSnafu { table_name })?;

        let insert = insertion_expr_to_request(insert_expr, table.clone())?;

        let affected_rows = table
            .insert(insert)
            .await
            .context(InsertSnafu { table_name })?;

        Ok(Output::AffectedRows(affected_rows))
    }

    pub async fn execute_sql(&self, sql: &str) -> Result<Output> {
        let stmt = self
            .query_engine
            .sql_to_statement(sql)
            .context(ExecuteSqlSnafu)?;

        match stmt {
            Statement::Query(_) => {
                let logical_plan = self
                    .query_engine
                    .statement_to_plan(stmt)
                    .context(ExecuteSqlSnafu)?;

                self.query_engine
                    .execute(&logical_plan)
                    .await
                    .context(ExecuteSqlSnafu)
            }
            Statement::Insert(i) => {
                let schema_provider = self
                    .catalog_manager
                    .catalog(DEFAULT_CATALOG_NAME)
                    .unwrap()
                    .schema(DEFAULT_SCHEMA_NAME)
                    .unwrap();

                let request = self.sql_handler.insert_to_request(schema_provider, *i)?;
                self.sql_handler.execute(request).await
            }

            Statement::Create(c) => {
                let table_id = self.catalog_manager.next_table_id();
                let _engine_name = c.engine.clone();
                // TODO(hl): Select table engine by engine_name

                let request = self.sql_handler.create_to_request(table_id, c)?;
                let catalog_name = request.catalog_name.clone();
                let schema_name = request.schema_name.clone();
                let table_name = request.table_name.clone();
                let table_id = request.id;
                info!(
                    "Creating table, catalog: {:?}, schema: {:?}, table name: {:?}, table id: {}",
                    catalog_name, schema_name, table_name, table_id
                );

                self.sql_handler.execute(SqlRequest::Create(request)).await
            }

            _ => unimplemented!(),
        }
    }

    pub async fn start(&self) -> Result<()> {
        self.catalog_manager
            .start()
            .await
            .context(NewCatalogSnafu)?;
        Ok(())
    }

    #[cfg(test)]
    pub async fn create_test_table(&self) -> Result<()> {
        use datatypes::data_type::ConcreteDataType;
        use datatypes::schema::{ColumnSchema, Schema};
        use table::engine::EngineContext;
        use table::requests::CreateTableRequest;

        use crate::error::CreateTableSnafu;

        let column_schemas = vec![
            ColumnSchema::new("host", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new("cpu", ConcreteDataType::float64_datatype(), true),
            ColumnSchema::new("memory", ConcreteDataType::float64_datatype(), true),
            ColumnSchema::new("ts", ConcreteDataType::int64_datatype(), true),
        ];

        let table_name = "demo";
        let table = self
            .table_engine
            .create_table(
                &EngineContext::default(),
                CreateTableRequest {
                    id: 1,
                    catalog_name: None,
                    schema_name: None,
                    table_name: table_name.to_string(),
                    desc: Some(" a test table".to_string()),
                    schema: Arc::new(
                        Schema::with_timestamp_index(column_schemas, 3)
                            .expect("ts is expected to be timestamp column"),
                    ),
                    create_if_not_exists: true,
                    primary_key_indices: Vec::default(),
                },
            )
            .await
            .context(CreateTableSnafu { table_name })?;

        let schema_provider = self
            .catalog_manager
            .catalog(DEFAULT_CATALOG_NAME)
            .unwrap()
            .schema(DEFAULT_SCHEMA_NAME)
            .unwrap();

        schema_provider
            .register_table(table_name.to_string(), table)
            .unwrap();

        Ok(())
    }
}

async fn new_object_store(store_config: &ObjectStoreConfig) -> Result<ObjectStore> {
    // TODO(dennis): supports other backend
    let data_dir = util::normalize_dir(match store_config {
        ObjectStoreConfig::File { data_dir } => data_dir,
    });

    fs::create_dir_all(path::Path::new(&data_dir))
        .context(error::CreateDirSnafu { dir: &data_dir })?;

    info!("The storage directory is: {}", &data_dir);

    let accessor = Backend::build()
        .root(&data_dir)
        .finish()
        .await
        .context(error::InitBackendSnafu { dir: &data_dir })?;

    Ok(ObjectStore::new(accessor))
}

async fn create_local_file_log_store(opts: &DatanodeOptions) -> Result<LocalFileLogStore> {
    // create WAL directory
    fs::create_dir_all(path::Path::new(&opts.wal_dir))
        .context(error::CreateDirSnafu { dir: &opts.wal_dir })?;

    info!("The WAL directory is: {}", &opts.wal_dir);

    let log_config = LogConfig {
        log_file_dir: opts.wal_dir.clone(),
        ..Default::default()
    };

    let log_store = LocalFileLogStore::open(&log_config)
        .await
        .context(error::OpenLogStoreSnafu)?;

    Ok(log_store)
}

#[cfg(test)]
mod tests {
    use arrow::array::UInt64Array;
    use common_recordbatch::util;

    use super::*;
    use crate::test_util;

    #[tokio::test]
    async fn test_execute_insert() {
        common_telemetry::init_default_ut_logging();
        let (opts, _guard) = test_util::create_tmp_dir_and_datanode_opts();
        let instance = Instance::new(&opts).await.unwrap();
        instance.start().await.unwrap();
        instance.create_test_table().await.unwrap();

        let output = instance
            .execute_sql(
                r#"insert into demo(host, cpu, memory, ts) values
                           ('host1', 66.6, 1024, 1655276557000),
                           ('host2', 88.8,  333.3, 1655276558000)
                           "#,
            )
            .await
            .unwrap();

        assert!(matches!(output, Output::AffectedRows(2)));
    }

    #[tokio::test]
    async fn test_execute_query() {
        let (opts, _guard) = test_util::create_tmp_dir_and_datanode_opts();
        let instance = Instance::new(&opts).await.unwrap();
        instance.start().await.unwrap();

        let output = instance
            .execute_sql("select sum(number) from numbers limit 20")
            .await
            .unwrap();

        match output {
            Output::RecordBatch(recordbatch) => {
                let numbers = util::collect(recordbatch).await.unwrap();
                let columns = numbers[0].df_recordbatch.columns();
                assert_eq!(1, columns.len());
                assert_eq!(columns[0].len(), 1);

                assert_eq!(
                    *columns[0].as_any().downcast_ref::<UInt64Array>().unwrap(),
                    UInt64Array::from_slice(&[4950])
                );
            }
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    pub async fn test_execute_create() {
        common_telemetry::init_default_ut_logging();
        let (opts, _guard) = test_util::create_tmp_dir_and_datanode_opts();
        let instance = Instance::new(&opts).await.unwrap();
        instance.start().await.unwrap();
        instance.create_test_table().await.unwrap();

        let output = instance
            .execute_sql(
                r#"create table test_table( 
                            host string, 
                            ts bigint, 
                            cpu double default 0, 
                            memory double, 
                            TIME INDEX (ts), 
                            PRIMARY KEY(ts, host)
                        ) engine=mito with(regions=1);"#,
            )
            .await
            .unwrap();

        assert!(matches!(output, Output::AffectedRows(1)));
    }
}
