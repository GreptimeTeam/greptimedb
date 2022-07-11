use std::{fs, path, sync::Arc};

use common_telemetry::logging::info;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema};
use log_store::fs::{config::LogConfig, log::LocalFileLogStore};
use query::catalog::{CatalogListRef, DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use query::query_engine::{Output, QueryEngineFactory, QueryEngineRef};
use snafu::ResultExt;
use sql::statements::statement::Statement;
use storage::{config::EngineConfig, EngineImpl};
use table::engine::EngineContext;
use table::engine::TableEngine;
use table::requests::CreateTableRequest;
use table_engine::engine::MitoEngine;

use crate::datanode::DatanodeOptions;
use crate::error::{self, CreateTableSnafu, ExecuteSqlSnafu, Result};
use crate::sql::SqlHandler;

type DefaultEngine = MitoEngine<EngineImpl<LocalFileLogStore>>;

// An abstraction to read/write services.
pub struct Instance {
    // Query service
    query_engine: QueryEngineRef,
    table_engine: DefaultEngine,
    sql_handler: SqlHandler<DefaultEngine>,
    // Catalog list
    catalog_list: CatalogListRef,
}

pub type InstanceRef = Arc<Instance>;

impl Instance {
    pub async fn new(opts: &DatanodeOptions, catalog_list: CatalogListRef) -> Result<Self> {
        // create wal directory
        fs::create_dir_all(path::Path::new(&opts.wal_dir)).context(error::CreateDirSnafu {
            dir: &opts.wal_dir.clone(),
        })?;

        info!("The wal directory is: {}", &opts.wal_dir);

        // TODO(jiachun): log store config
        let log_config = LogConfig {
            append_buffer_size: 128,
            max_log_file_size: 128,
            log_file_dir: opts.wal_dir.clone(),
        };
        let log_store = LocalFileLogStore::open(&log_config)
            .await
            .context(error::OpenLogStoreSnafu)?;

        let factory = QueryEngineFactory::new(catalog_list.clone());
        let query_engine = factory.query_engine().clone();
        let table_engine = DefaultEngine::new(
            EngineImpl::new(
                EngineConfig::with_store_dir("/tmp/greptimedb"),
                Arc::new(log_store),
            )
            .await
            .context(error::OpenStorageEngineSnafu)?,
        );

        Ok(Self {
            query_engine,
            sql_handler: SqlHandler::new(table_engine.clone()),
            table_engine,
            catalog_list,
        })
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
                    .catalog_list
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
                    name: table_name.to_string(),
                    desc: Some(" a test table".to_string()),
                    schema: Arc::new(Schema::new(column_schemas)),
                },
            )
            .await
            .context(CreateTableSnafu { table_name })?;

        let schema_provider = self
            .catalog_list
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

#[cfg(test)]
mod tests {
    use arrow::array::UInt64Array;
    use common_recordbatch::util;
    use query::catalog::memory;

    use super::*;
    use crate::test_util;

    #[tokio::test]
    async fn test_execute_insert() {
        let catalog_list = memory::new_memory_catalog_list().unwrap();
        let (opts, _tmp_dir) = test_util::create_tmp_dir_and_datanode_opts();
        let instance = Instance::new(&opts, catalog_list).await.unwrap();
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
        let catalog_list = memory::new_memory_catalog_list().unwrap();
        let (opts, _tmp_dir) = test_util::create_tmp_dir_and_datanode_opts();
        let instance = Instance::new(&opts, catalog_list).await.unwrap();

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
