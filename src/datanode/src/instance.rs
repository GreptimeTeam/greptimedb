use std::{fs, path, sync::Arc};

use api::v1::InsertExpr;
use catalog::{CatalogManagerRef, DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_telemetry::logging::info;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema};
use log_store::fs::{config::LogConfig, log::LocalFileLogStore};
use object_store::{backend::fs::Backend, util, ObjectStore};
use query::query_engine::{Output, QueryEngineFactory, QueryEngineRef};
use snafu::{OptionExt, ResultExt};
use sql::statements::statement::Statement;
use storage::{config::EngineConfig as StorageEngineConfig, EngineImpl};
use table::engine::EngineContext;
use table::engine::TableEngine;
use table::requests::CreateTableRequest;
use table_engine::config::EngineConfig as TableEngineConfig;
use table_engine::engine::MitoEngine;

use crate::datanode::{DatanodeOptions, ObjectStoreConfig};
use crate::error::{
    self, CreateTableSnafu, ExecuteSqlSnafu, InsertSnafu, NewCatalogSnafu, Result,
    TableNotFoundSnafu,
};
use crate::server::grpc::insert::insertion_expr_to_request;
use crate::sql::SqlHandler;

type DefaultEngine = MitoEngine<EngineImpl<LocalFileLogStore>>;

// An abstraction to read/write services.
pub struct Instance {
    // Query service
    query_engine: QueryEngineRef,
    table_engine: DefaultEngine,
    sql_handler: SqlHandler<DefaultEngine>,
    // Catalog list
    catalog_manager: CatalogManagerRef,
}

pub type InstanceRef = Arc<Instance>;

impl Instance {
    pub async fn new(opts: &DatanodeOptions) -> Result<Self> {
        let object_store = new_object_store(&opts.store_config).await?;
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
            sql_handler: SqlHandler::new(table_engine.clone()),
            table_engine,
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
            Statement::Insert(_) => {
                let schema_provider = self
                    .catalog_manager
                    .catalog(DEFAULT_CATALOG_NAME)
                    .unwrap()
                    .schema(DEFAULT_SCHEMA_NAME)
                    .unwrap();

                let request = self
                    .sql_handler
                    .statement_to_request(schema_provider, stmt)?;
                self.sql_handler.execute(request).await
            }
            _ => unimplemented!(),
        }
    }

    pub async fn start(&self) -> Result<()> {
        self.catalog_manager
            .start()
            .await
            .context(NewCatalogSnafu)?;
        // FIXME(dennis): create a demo table for test
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
                    table_id: 1,
                    name: table_name.to_string(),
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
    let store_dir = util::normalize_dir(match store_config {
        ObjectStoreConfig::File(file) => &file.store_dir,
    });

    fs::create_dir_all(path::Path::new(&store_dir))
        .context(error::CreateDirSnafu { dir: &store_dir })?;

    info!("The storage directory is: {}", &store_dir);

    let accessor = Backend::build()
        .root(&store_dir)
        .finish()
        .await
        .context(error::InitBackendSnafu { dir: &store_dir })?;

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
        let (opts, _wal_dir, _data_dir) = test_util::create_tmp_dir_and_datanode_opts();
        let instance = Instance::new(&opts).await.unwrap();
        instance.start().await.unwrap();

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
        let (opts, _wal_dir, _data_dir) = test_util::create_tmp_dir_and_datanode_opts();
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
}
