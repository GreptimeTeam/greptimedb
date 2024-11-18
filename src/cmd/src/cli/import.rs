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

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use clap::{Parser, ValueEnum};
use common_catalog::consts::DEFAULT_SCHEMA_NAME;
use common_telemetry::{error, info, warn};
use snafu::{OptionExt, ResultExt};
use tokio::sync::Semaphore;
use tokio::time::Instant;
use tracing_appender::non_blocking::WorkerGuard;

use crate::cli::database::DatabaseClient;
use crate::cli::{database, Instance, Tool};
use crate::error::{Error, FileIoSnafu, Result, SchemaNotFoundSnafu};

#[derive(Debug, Default, Clone, ValueEnum)]
enum ImportTarget {
    /// Import all table schemas into the database.
    Schema,
    /// Import all table data into the database.
    Data,
    /// Export all table schemas and data at once.
    #[default]
    All,
}

#[derive(Debug, Default, Parser)]
pub struct ImportCommand {
    /// Server address to connect
    #[clap(long)]
    addr: String,

    /// Directory of the data. E.g.: /tmp/greptimedb-backup
    #[clap(long)]
    input_dir: String,

    /// The name of the catalog to import.
    #[clap(long, default_value = "greptime-*")]
    database: String,

    /// Parallelism of the import.
    #[clap(long, short = 'j', default_value = "1")]
    import_jobs: usize,

    /// Max retry times for each job.
    #[clap(long, default_value = "3")]
    max_retry: usize,

    /// Things to export
    #[clap(long, short = 't', value_enum, default_value = "all")]
    target: ImportTarget,

    /// The basic authentication for connecting to the server
    #[clap(long)]
    auth_basic: Option<String>,

    /// The timeout of invoking the database.
    #[clap(long, value_parser = humantime::parse_duration)]
    timeout: Option<Duration>,
}

impl ImportCommand {
    pub async fn build(&self, guard: Vec<WorkerGuard>) -> Result<Instance> {
        let (catalog, schema) = database::split_database(&self.database)?;
        let database_client = DatabaseClient::new(
            self.addr.clone(),
            catalog.clone(),
            self.auth_basic.clone(),
            self.timeout,
        );

        Ok(Instance::new(
            Box::new(Import {
                catalog,
                schema,
                database_client,
                input_dir: self.input_dir.clone(),
                parallelism: self.import_jobs,
                target: self.target.clone(),
            }),
            guard,
        ))
    }
}

pub struct Import {
    catalog: String,
    schema: Option<String>,
    database_client: DatabaseClient,
    input_dir: String,
    parallelism: usize,
    target: ImportTarget,
}

impl Import {
    async fn import_create_table(&self) -> Result<()> {
        // Use default db to creates other dbs
        self.do_sql_job("create_database.sql", Some(DEFAULT_SCHEMA_NAME))
            .await?;
        self.do_sql_job("create_tables.sql", None).await
    }

    async fn import_database_data(&self) -> Result<()> {
        self.do_sql_job("copy_from.sql", None).await
    }

    async fn do_sql_job(&self, filename: &str, exec_db: Option<&str>) -> Result<()> {
        let timer = Instant::now();
        let semaphore = Arc::new(Semaphore::new(self.parallelism));
        let db_names = self.get_db_names().await?;
        let db_count = db_names.len();
        let mut tasks = Vec::with_capacity(db_count);
        for schema in db_names {
            let semaphore_moved = semaphore.clone();
            tasks.push(async move {
                let _permit = semaphore_moved.acquire().await.unwrap();
                let database_input_dir = self.catalog_path().join(&schema);
                let sql_file = database_input_dir.join(filename);
                let sql = tokio::fs::read_to_string(sql_file)
                    .await
                    .context(FileIoSnafu)?;
                if sql.is_empty() {
                    info!("Empty `{filename}` {database_input_dir:?}");
                } else {
                    let db = exec_db.unwrap_or(&schema);
                    self.database_client.sql(&sql, db).await?;
                    info!("Imported `{filename}` for database {schema}");
                }

                Ok::<(), Error>(())
            })
        }

        let success = futures::future::join_all(tasks)
            .await
            .into_iter()
            .filter(|r| match r {
                Ok(_) => true,
                Err(e) => {
                    error!(e; "import {filename} job failed");
                    false
                }
            })
            .count();
        let elapsed = timer.elapsed();
        info!("Success {success}/{db_count} `{filename}` jobs, cost: {elapsed:?}");

        Ok(())
    }

    fn catalog_path(&self) -> PathBuf {
        PathBuf::from(&self.input_dir).join(&self.catalog)
    }

    async fn get_db_names(&self) -> Result<Vec<String>> {
        let db_names = self.all_db_names().await?;
        let Some(schema) = &self.schema else {
            return Ok(db_names);
        };

        // Check if the schema exists
        db_names
            .into_iter()
            .find(|db_name| db_name.to_lowercase() == schema.to_lowercase())
            .map(|name| vec![name])
            .context(SchemaNotFoundSnafu {
                catalog: &self.catalog,
                schema,
            })
    }

    // Get all database names in the input directory.
    // The directory structure should be like:
    // /tmp/greptimedb-backup
    // ├── greptime-1
    // │   ├── db1
    // │   └── db2
    async fn all_db_names(&self) -> Result<Vec<String>> {
        let mut db_names = vec![];
        let path = self.catalog_path();
        let mut entries = tokio::fs::read_dir(path).await.context(FileIoSnafu)?;
        while let Some(entry) = entries.next_entry().await.context(FileIoSnafu)? {
            let path = entry.path();
            if path.is_dir() {
                let db_name = match path.file_name() {
                    Some(name) => name.to_string_lossy().to_string(),
                    None => {
                        warn!("Failed to get the file name of {:?}", path);
                        continue;
                    }
                };
                db_names.push(db_name);
            }
        }
        Ok(db_names)
    }
}

#[async_trait]
impl Tool for Import {
    async fn do_work(&self) -> Result<()> {
        match self.target {
            ImportTarget::Schema => self.import_create_table().await,
            ImportTarget::Data => self.import_database_data().await,
            ImportTarget::All => {
                self.import_create_table().await?;
                self.import_database_data().await
            }
        }
    }
}
