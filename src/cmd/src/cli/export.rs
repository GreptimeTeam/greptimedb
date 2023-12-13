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

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use clap::{Parser, ValueEnum};
use client::api::v1::auth_header::AuthScheme;
use client::api::v1::Basic;
use client::{Client, Database, DEFAULT_SCHEMA_NAME};
use common_query::Output;
use common_recordbatch::util::collect;
use common_telemetry::{debug, error, info, warn};
use datatypes::scalars::ScalarVector;
use datatypes::vectors::{StringVector, Vector};
use snafu::{OptionExt, ResultExt};
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::Semaphore;

use crate::cli::{Instance, Tool};
use crate::error::{
    CollectRecordBatchesSnafu, ConnectServerSnafu, EmptyResultSnafu, Error, FileIoSnafu,
    IllegalConfigSnafu, InvalidDatabaseNameSnafu, NotDataFromOutputSnafu, RequestDatabaseSnafu,
    Result,
};

type TableReference = (String, String, String);

#[derive(Debug, Default, Clone, ValueEnum)]
enum ExportTarget {
    /// Corresponding to `SHOW CREATE TABLE`
    #[default]
    CreateTable,
    /// Corresponding to `EXPORT TABLE`
    TableData,
}

#[derive(Debug, Default, Parser)]
pub struct ExportCommand {
    /// Server address to connect
    #[clap(long)]
    addr: String,

    /// Directory to put the exported data. E.g.: /tmp/greptimedb-export
    #[clap(long)]
    output_dir: String,

    /// The name of the catalog to export. Default to "greptime-*"".
    #[clap(long, default_value = "")]
    database: String,

    /// Parallelism of the export.
    #[clap(long, short = 'j', default_value = "1")]
    export_jobs: usize,

    /// Max retry times for each job.
    #[clap(long, default_value = "3")]
    max_retry: usize,

    /// Things to export
    #[clap(long, short = 't', value_enum)]
    target: ExportTarget,

    /// basic authentication for connecting to the server
    #[clap(long)]
    auth_basic: Option<String>,
}

impl ExportCommand {
    pub async fn build(&self) -> Result<Instance> {
        let client = Client::with_urls([self.addr.clone()]);
        client
            .health_check()
            .await
            .with_context(|_| ConnectServerSnafu {
                addr: self.addr.clone(),
            })?;
        let (catalog, schema) = split_database(&self.database)?;
        let mut database_client = Database::new(
            catalog.clone(),
            schema.clone().unwrap_or(DEFAULT_SCHEMA_NAME.to_string()),
            client,
        );

        if let Some(auth_basic) = &self.auth_basic {
            let (username, password) = auth_basic.split_once(':').context(IllegalConfigSnafu {
                msg: "auth_basic cannot be split by ':'".to_string(),
            })?;
            database_client.set_auth(AuthScheme::Basic(Basic {
                username: username.to_string(),
                password: password.to_string(),
            }));
        }

        Ok(Instance::new(Box::new(Export {
            client: database_client,
            catalog,
            schema,
            output_dir: self.output_dir.clone(),
            parallelism: self.export_jobs,
            target: self.target.clone(),
        })))
    }
}

pub struct Export {
    client: Database,
    catalog: String,
    schema: Option<String>,
    output_dir: String,
    parallelism: usize,
    target: ExportTarget,
}

impl Export {
    /// Iterate over all db names.
    ///
    /// Newbie: `db_name` is catalog + schema.
    async fn iter_db_names(&self) -> Result<Vec<(String, String)>> {
        if let Some(schema) = &self.schema {
            Ok(vec![(self.catalog.clone(), schema.clone())])
        } else {
            let mut client = self.client.clone();
            client.set_catalog(self.catalog.clone());
            let result =
                client
                    .sql("show databases")
                    .await
                    .with_context(|_| RequestDatabaseSnafu {
                        sql: "show databases".to_string(),
                    })?;
            let Output::Stream(stream) = result else {
                NotDataFromOutputSnafu.fail()?
            };
            let record_batch = collect(stream)
                .await
                .context(CollectRecordBatchesSnafu)?
                .pop()
                .context(EmptyResultSnafu)?;
            let schemas = record_batch
                .column(0)
                .as_any()
                .downcast_ref::<StringVector>()
                .unwrap();
            let mut result = Vec::with_capacity(schemas.len());
            for i in 0..schemas.len() {
                let schema = schemas.get_data(i).unwrap().to_owned();
                if schema == common_catalog::consts::INFORMATION_SCHEMA_NAME {
                    continue;
                }
                result.push((self.catalog.clone(), schema));
            }
            Ok(result)
        }
    }

    /// Return a list of [`TableReference`] to be exported.
    /// Includes all tables under the given `catalog` and `schema`
    async fn get_table_list(&self, catalog: &str, schema: &str) -> Result<Vec<TableReference>> {
        // TODO: SQL injection hurts
        let sql = format!(
            "select table_catalog, table_schema, table_name from \
            information_schema.tables where table_type = \'BASE TABLE\'\
            and table_catalog = \'{catalog}\' and table_schema = \'{schema}\'",
        );
        let mut client = self.client.clone();
        client.set_catalog(catalog);
        client.set_schema(schema);
        let result = client
            .sql(&sql)
            .await
            .with_context(|_| RequestDatabaseSnafu { sql })?;
        let Output::Stream(stream) = result else {
            NotDataFromOutputSnafu.fail()?
        };
        let Some(record_batch) = collect(stream)
            .await
            .context(CollectRecordBatchesSnafu)?
            .pop()
        else {
            return Ok(vec![]);
        };

        debug!("Fetched table list: {}", record_batch.pretty_print());

        if record_batch.num_rows() == 0 {
            return Ok(vec![]);
        }

        let mut result = Vec::with_capacity(record_batch.num_rows());
        let catalog_column = record_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringVector>()
            .unwrap();
        let schema_column = record_batch
            .column(1)
            .as_any()
            .downcast_ref::<StringVector>()
            .unwrap();
        let table_column = record_batch
            .column(2)
            .as_any()
            .downcast_ref::<StringVector>()
            .unwrap();
        for i in 0..record_batch.num_rows() {
            let catalog = catalog_column.get_data(i).unwrap().to_owned();
            let schema = schema_column.get_data(i).unwrap().to_owned();
            let table = table_column.get_data(i).unwrap().to_owned();
            result.push((catalog, schema, table));
        }

        Ok(result)
    }

    async fn show_create_table(&self, catalog: &str, schema: &str, table: &str) -> Result<String> {
        let sql = format!("show create table {}.{}.{}", catalog, schema, table);
        let mut client = self.client.clone();
        client.set_catalog(catalog);
        client.set_schema(schema);
        let result = client
            .sql(&sql)
            .await
            .with_context(|_| RequestDatabaseSnafu { sql })?;
        let Output::Stream(stream) = result else {
            NotDataFromOutputSnafu.fail()?
        };
        let record_batch = collect(stream)
            .await
            .context(CollectRecordBatchesSnafu)?
            .pop()
            .context(EmptyResultSnafu)?;
        let create_table = record_batch
            .column(1)
            .as_any()
            .downcast_ref::<StringVector>()
            .unwrap()
            .get_data(0)
            .unwrap();

        Ok(format!("{create_table};\n"))
    }

    async fn export_create_table(&self) -> Result<()> {
        let semaphore = Arc::new(Semaphore::new(self.parallelism));
        let db_names = self.iter_db_names().await?;
        let db_count = db_names.len();
        let mut tasks = Vec::with_capacity(db_names.len());
        for (catalog, schema) in db_names {
            let semaphore_moved = semaphore.clone();
            tasks.push(async move {
                let _permit = semaphore_moved.acquire().await.unwrap();
                let table_list = self.get_table_list(&catalog, &schema).await?;
                let table_count = table_list.len();
                tokio::fs::create_dir_all(&self.output_dir)
                    .await
                    .context(FileIoSnafu)?;
                let output_file =
                    Path::new(&self.output_dir).join(format!("{catalog}-{schema}.sql"));
                let mut file = File::create(output_file).await.context(FileIoSnafu)?;
                for (c, s, t) in table_list {
                    match self.show_create_table(&c, &s, &t).await {
                        Err(e) => {
                            error!(e; "Failed to export table {}.{}.{}", c, s, t)
                        }
                        Ok(create_table) => {
                            file.write_all(create_table.as_bytes())
                                .await
                                .context(FileIoSnafu)?;
                        }
                    }
                }
                info!("finished exporting {catalog}.{schema} with {table_count} tables",);
                Ok::<(), Error>(())
            });
        }

        let success = futures::future::join_all(tasks)
            .await
            .into_iter()
            .filter(|r| match r {
                Ok(_) => true,
                Err(e) => {
                    error!(e; "export job failed");
                    false
                }
            })
            .count();

        info!("success {success}/{db_count} jobs");

        Ok(())
    }

    async fn export_table_data(&self) -> Result<()> {
        let semaphore = Arc::new(Semaphore::new(self.parallelism));
        let db_names = self.iter_db_names().await?;
        let db_count = db_names.len();
        let mut tasks = Vec::with_capacity(db_names.len());
        for (catalog, schema) in db_names {
            let semaphore_moved = semaphore.clone();
            tasks.push(async move {
                let _permit = semaphore_moved.acquire().await.unwrap();
                tokio::fs::create_dir_all(&self.output_dir)
                    .await
                    .context(FileIoSnafu)?;
                let output_dir = Path::new(&self.output_dir).join(format!("{catalog}-{schema}/"));

                let mut client = self.client.clone();
                client.set_catalog(catalog.clone());
                client.set_schema(schema.clone());

                // copy database to
                let sql = format!(
                    "copy database {} to '{}' with (format='parquet');",
                    schema,
                    output_dir.to_str().unwrap()
                );
                client
                    .sql(sql.clone())
                    .await
                    .context(RequestDatabaseSnafu { sql })?;
                info!("finished exporting {catalog}.{schema} data");

                // export copy from sql
                let dir_filenames = match output_dir.read_dir() {
                    Ok(dir) => dir,
                    Err(_) => {
                        warn!("empty database {catalog}.{schema}");
                        return Ok(());
                    }
                };

                let copy_from_file =
                    Path::new(&self.output_dir).join(format!("{catalog}-{schema}_copy_from.sql"));
                let mut writer =
                    BufWriter::new(File::create(copy_from_file).await.context(FileIoSnafu)?);

                for table_file in dir_filenames {
                    let table_file = table_file.unwrap();
                    let table_name = table_file
                        .file_name()
                        .into_string()
                        .unwrap()
                        .replace(".parquet", "");

                    writer
                        .write(
                            format!(
                                "copy {} from '{}' with (format='parquet');\n",
                                table_name,
                                table_file.path().to_str().unwrap()
                            )
                            .as_bytes(),
                        )
                        .await
                        .context(FileIoSnafu)?;
                }
                writer.flush().await.context(FileIoSnafu)?;

                info!("finished exporting {catalog}.{schema} copy_from.sql");

                Ok::<(), Error>(())
            });
        }

        let success = futures::future::join_all(tasks)
            .await
            .into_iter()
            .filter(|r| match r {
                Ok(_) => true,
                Err(e) => {
                    error!(e; "export job failed");
                    false
                }
            })
            .count();

        info!("success {success}/{db_count} jobs");

        Ok(())
    }
}

#[async_trait]
impl Tool for Export {
    async fn do_work(&self) -> Result<()> {
        match self.target {
            ExportTarget::CreateTable => self.export_create_table().await,
            ExportTarget::TableData => self.export_table_data().await,
        }
    }
}

/// Split at `-`.
fn split_database(database: &str) -> Result<(String, Option<String>)> {
    let (catalog, schema) = database
        .split_once('-')
        .with_context(|| InvalidDatabaseNameSnafu {
            database: database.to_string(),
        })?;
    if schema == "*" {
        Ok((catalog.to_string(), None))
    } else {
        Ok((catalog.to_string(), Some(schema.to_string())))
    }
}
