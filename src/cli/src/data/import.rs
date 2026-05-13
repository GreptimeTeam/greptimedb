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
use common_error::ext::BoxedError;
use common_telemetry::{error, info, warn};
use snafu::{OptionExt, ResultExt, ensure};
use tokio::sync::Semaphore;
use tokio::time::Instant;

use crate::data::{COPY_PATH_PLACEHOLDER, default_database};
use crate::database::{DatabaseClient, parse_proxy_opts};
use crate::error::{Error, FileIoSnafu, InvalidArgumentsSnafu, Result, SchemaNotFoundSnafu};
use crate::{Tool, database};

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

/// Command to import data from a directory into a GreptimeDB instance.
#[derive(Debug, Default, Parser)]
pub struct ImportCommand {
    /// Server address to connect
    #[clap(long)]
    addr: String,

    /// Directory of the data. E.g.: /tmp/greptimedb-backup
    #[clap(long)]
    input_dir: String,

    /// The name of the catalog to import.
    #[clap(long, default_value_t = default_database())]
    database: String,

    /// The number of databases imported in parallel.
    /// For example, if there are 20 databases and `db_parallelism` is 4,
    /// 4 databases will be imported concurrently.
    #[clap(long, short = 'j', default_value = "1", alias = "import-jobs")]
    db_parallelism: usize,

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
    ///
    /// It is used to override the server-side timeout setting.
    /// The default behavior will disable server-side default timeout(i.e. `0s`).
    #[clap(long, value_parser = humantime::parse_duration)]
    timeout: Option<Duration>,

    /// The proxy server address to connect.
    ///
    /// If set, it overrides the system proxy unless `--no-proxy` is specified.
    /// If neither `--proxy` nor `--no-proxy` is set, system proxy (env) may be used.
    #[clap(long)]
    proxy: Option<String>,

    /// Disable all proxy usage (ignores `--proxy` and system proxy).
    ///
    /// When set and `--proxy` is not provided, this explicitly disables system proxy.
    #[clap(long, default_value = "false")]
    no_proxy: bool,
}

impl ImportCommand {
    pub async fn build(&self) -> std::result::Result<Box<dyn Tool>, BoxedError> {
        let (catalog, schema) =
            database::split_database(&self.database).map_err(BoxedError::new)?;
        let proxy = parse_proxy_opts(self.proxy.clone(), self.no_proxy)?;
        let database_client = DatabaseClient::new(
            self.addr.clone(),
            catalog.clone(),
            self.auth_basic.clone(),
            // Treats `None` as `0s` to disable server-side default timeout.
            self.timeout.unwrap_or_default(),
            proxy,
            self.no_proxy,
        );

        Ok(Box::new(Import {
            catalog,
            schema,
            database_client,
            input_dir: self.input_dir.clone(),
            parallelism: self.db_parallelism,
            target: self.target.clone(),
        }))
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
                let mut sql = tokio::fs::read_to_string(sql_file)
                    .await
                    .context(FileIoSnafu)?;
                if sql.trim().is_empty() {
                    info!("Empty `{filename}` {database_input_dir:?}");
                } else {
                    if filename == "copy_from.sql" {
                        sql = self.rewrite_copy_database_sql(&schema, &sql)?;
                    }
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

    fn rewrite_copy_database_sql(&self, schema: &str, sql: &str) -> Result<String> {
        let target_location = self.build_copy_database_location(schema);
        let escaped_location = target_location.replace('\'', "''");

        let mut first_stmt_checked = false;
        for line in sql.lines() {
            let trimmed = line.trim_start();
            if trimmed.is_empty() || trimmed.starts_with("--") {
                continue;
            }

            ensure!(
                trimmed.starts_with("COPY DATABASE"),
                InvalidArgumentsSnafu {
                    msg: "Expected COPY DATABASE statement at start of copy_from.sql"
                }
            );
            first_stmt_checked = true;
            break;
        }

        ensure!(
            first_stmt_checked,
            InvalidArgumentsSnafu {
                msg: "COPY DATABASE statement not found in copy_from.sql"
            }
        );

        ensure!(
            sql.contains(COPY_PATH_PLACEHOLDER),
            InvalidArgumentsSnafu {
                msg: format!(
                    "Placeholder `{}` not found in COPY DATABASE statement",
                    COPY_PATH_PLACEHOLDER
                )
            }
        );

        Ok(sql.replacen(COPY_PATH_PLACEHOLDER, &escaped_location, 1))
    }

    fn build_copy_database_location(&self, schema: &str) -> String {
        let mut path = self.catalog_path();
        path.push(schema);
        let mut path_str = path.to_string_lossy().into_owned();
        if !path_str.ends_with('/') {
            path_str.push('/');
        }
        path_str
    }
}

#[async_trait]
impl Tool for Import {
    async fn do_work(&self) -> std::result::Result<(), BoxedError> {
        match self.target {
            ImportTarget::Schema => self.import_create_table().await.map_err(BoxedError::new),
            ImportTarget::Data => self.import_database_data().await.map_err(BoxedError::new),
            ImportTarget::All => {
                self.import_create_table().await.map_err(BoxedError::new)?;
                self.import_database_data().await.map_err(BoxedError::new)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn build_import(input_dir: &str) -> Import {
        Import {
            catalog: "catalog".to_string(),
            schema: None,
            database_client: DatabaseClient::new(
                "127.0.0.1:4000".to_string(),
                "catalog".to_string(),
                None,
                Duration::from_secs(0),
                None,
                false,
            ),
            input_dir: input_dir.to_string(),
            parallelism: 1,
            target: ImportTarget::Data,
        }
    }

    #[test]
    fn rewrite_copy_database_sql_replaces_placeholder() {
        let import = build_import("/tmp/export-path");
        let comment = "-- COPY DATABASE \"catalog\".\"schema\" FROM 's3://bucket/demo/' WITH (format = 'parquet') CONNECTION (region = 'us-west-2')";
        let sql = format!(
            "{comment}\nCOPY DATABASE \"catalog\".\"schema\" FROM '{}' WITH (format = 'parquet');",
            COPY_PATH_PLACEHOLDER
        );

        let rewritten = import.rewrite_copy_database_sql("schema", &sql).unwrap();
        let expected_location = import.build_copy_database_location("schema");
        let escaped = expected_location.replace('\'', "''");

        assert!(rewritten.starts_with(comment));
        assert!(rewritten.contains(&format!("FROM '{escaped}'")));
        assert!(!rewritten.contains(COPY_PATH_PLACEHOLDER));
    }

    #[test]
    fn rewrite_copy_database_sql_requires_placeholder() {
        let import = build_import("/tmp/export-path");
        let sql = "COPY DATABASE \"catalog\".\"schema\" FROM '/tmp/export-path/catalog/schema/' WITH (format = 'parquet');";
        assert!(import.rewrite_copy_database_sql("schema", sql).is_err());
    }
}
