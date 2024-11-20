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

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use clap::{Parser, ValueEnum};
use common_telemetry::{debug, error, info};
use serde_json::Value;
use snafu::{OptionExt, ResultExt};
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::Semaphore;
use tokio::time::Instant;
use tracing_appender::non_blocking::WorkerGuard;

use crate::cli::database::DatabaseClient;
use crate::cli::{database, Instance, Tool};
use crate::error::{EmptyResultSnafu, Error, FileIoSnafu, Result, SchemaNotFoundSnafu};

type TableReference = (String, String, String);

#[derive(Debug, Default, Clone, ValueEnum)]
enum ExportTarget {
    /// Export all table schemas, corresponding to `SHOW CREATE TABLE`.
    Schema,
    /// Export all table data, corresponding to `COPY DATABASE TO`.
    Data,
    /// Export all table schemas and data at once.
    #[default]
    All,
}

#[derive(Debug, Default, Parser)]
pub struct ExportCommand {
    /// Server address to connect
    #[clap(long)]
    addr: String,

    /// Directory to put the exported data. E.g.: /tmp/greptimedb-export
    #[clap(long)]
    output_dir: String,

    /// The name of the catalog to export.
    #[clap(long, default_value = "greptime-*")]
    database: String,

    /// Parallelism of the export.
    #[clap(long, short = 'j', default_value = "1")]
    export_jobs: usize,

    /// Max retry times for each job.
    #[clap(long, default_value = "3")]
    max_retry: usize,

    /// Things to export
    #[clap(long, short = 't', value_enum, default_value = "all")]
    target: ExportTarget,

    /// A half-open time range: [start_time, end_time).
    /// The start of the time range (time-index column) for data export.
    #[clap(long)]
    start_time: Option<String>,

    /// A half-open time range: [start_time, end_time).
    /// The end of the time range (time-index column) for data export.
    #[clap(long)]
    end_time: Option<String>,

    /// The basic authentication for connecting to the server
    #[clap(long)]
    auth_basic: Option<String>,

    /// The timeout of invoking the database.
    ///
    /// It is used to override the server-side timeout setting.
    /// Sets `0s` to disable server-side default timeout.
    #[clap(long, value_parser = humantime::parse_duration)]
    timeout: Duration,
}

impl ExportCommand {
    pub async fn build(&self, guard: Vec<WorkerGuard>) -> Result<Instance> {
        let (catalog, schema) = database::split_database(&self.database)?;

        let database_client = DatabaseClient::new(
            self.addr.clone(),
            catalog.clone(),
            self.auth_basic.clone(),
            self.timeout,
        );

        Ok(Instance::new(
            Box::new(Export {
                catalog,
                schema,
                database_client,
                output_dir: self.output_dir.clone(),
                parallelism: self.export_jobs,
                target: self.target.clone(),
                start_time: self.start_time.clone(),
                end_time: self.end_time.clone(),
            }),
            guard,
        ))
    }
}

pub struct Export {
    catalog: String,
    schema: Option<String>,
    database_client: DatabaseClient,
    output_dir: String,
    parallelism: usize,
    target: ExportTarget,
    start_time: Option<String>,
    end_time: Option<String>,
}

impl Export {
    fn catalog_path(&self) -> PathBuf {
        PathBuf::from(&self.output_dir).join(&self.catalog)
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

    /// Iterate over all db names.
    async fn all_db_names(&self) -> Result<Vec<String>> {
        let records = self
            .database_client
            .sql_in_public("SHOW DATABASES")
            .await?
            .context(EmptyResultSnafu)?;
        let mut result = Vec::with_capacity(records.len());
        for value in records {
            let Value::String(schema) = &value[0] else {
                unreachable!()
            };
            if schema == common_catalog::consts::INFORMATION_SCHEMA_NAME {
                continue;
            }
            if schema == common_catalog::consts::PG_CATALOG_NAME {
                continue;
            }
            result.push(schema.clone());
        }
        Ok(result)
    }

    /// Return a list of [`TableReference`] to be exported.
    /// Includes all tables under the given `catalog` and `schema`.
    async fn get_table_list(
        &self,
        catalog: &str,
        schema: &str,
    ) -> Result<(
        Vec<TableReference>,
        Vec<TableReference>,
        Vec<TableReference>,
    )> {
        // Puts all metric table first
        let sql = format!(
            "SELECT table_catalog, table_schema, table_name \
            FROM information_schema.columns \
            WHERE column_name = '__tsid' \
                and table_catalog = \'{catalog}\' \
                and table_schema = \'{schema}\'"
        );
        let records = self
            .database_client
            .sql_in_public(&sql)
            .await?
            .context(EmptyResultSnafu)?;
        let mut metric_physical_tables = HashSet::with_capacity(records.len());
        for value in records {
            let mut t = Vec::with_capacity(3);
            for v in &value {
                let Value::String(value) = v else {
                    unreachable!()
                };
                t.push(value);
            }
            metric_physical_tables.insert((t[0].clone(), t[1].clone(), t[2].clone()));
        }

        let sql = format!(
            "SELECT table_catalog, table_schema, table_name, table_type \
            FROM information_schema.tables \
            WHERE (table_type = \'BASE TABLE\' OR table_type = \'VIEW\') \
                and table_catalog = \'{catalog}\' \
                and table_schema = \'{schema}\'",
        );
        let records = self
            .database_client
            .sql_in_public(&sql)
            .await?
            .context(EmptyResultSnafu)?;

        debug!("Fetched table/view list: {:?}", records);

        if records.is_empty() {
            return Ok((vec![], vec![], vec![]));
        }

        let mut remaining_tables = Vec::with_capacity(records.len());
        let mut views = Vec::new();
        for value in records {
            let mut t = Vec::with_capacity(4);
            for v in &value {
                let Value::String(value) = v else {
                    unreachable!()
                };
                t.push(value);
            }
            let table = (t[0].clone(), t[1].clone(), t[2].clone());
            let table_type = t[3].as_str();
            // Ignores the physical table
            if !metric_physical_tables.contains(&table) {
                if table_type == "VIEW" {
                    views.push(table);
                } else {
                    remaining_tables.push(table);
                }
            }
        }

        Ok((
            metric_physical_tables.into_iter().collect(),
            remaining_tables,
            views,
        ))
    }

    async fn show_create(
        &self,
        show_type: &str,
        catalog: &str,
        schema: &str,
        table: Option<&str>,
    ) -> Result<String> {
        let sql = match table {
            Some(table) => format!(
                r#"SHOW CREATE {} "{}"."{}"."{}""#,
                show_type, catalog, schema, table
            ),
            None => format!(r#"SHOW CREATE {} "{}"."{}""#, show_type, catalog, schema),
        };
        let records = self
            .database_client
            .sql_in_public(&sql)
            .await?
            .context(EmptyResultSnafu)?;
        let Value::String(create) = &records[0][1] else {
            unreachable!()
        };

        Ok(format!("{};\n", create))
    }

    async fn export_create_database(&self) -> Result<()> {
        let timer = Instant::now();
        let db_names = self.get_db_names().await?;
        let db_count = db_names.len();
        for schema in db_names {
            let db_dir = self.catalog_path().join(format!("{schema}/"));
            tokio::fs::create_dir_all(&db_dir)
                .await
                .context(FileIoSnafu)?;
            let file = db_dir.join("create_database.sql");
            let mut file = File::create(file).await.context(FileIoSnafu)?;
            let create_database = self
                .show_create("DATABASE", &self.catalog, &schema, None)
                .await?;
            file.write_all(create_database.as_bytes())
                .await
                .context(FileIoSnafu)?;
        }

        let elapsed = timer.elapsed();
        info!("Success {db_count} jobs, cost: {elapsed:?}");

        Ok(())
    }

    async fn export_create_table(&self) -> Result<()> {
        let timer = Instant::now();
        let semaphore = Arc::new(Semaphore::new(self.parallelism));
        let db_names = self.get_db_names().await?;
        let db_count = db_names.len();
        let mut tasks = Vec::with_capacity(db_names.len());
        for schema in db_names {
            let semaphore_moved = semaphore.clone();
            tasks.push(async move {
                let _permit = semaphore_moved.acquire().await.unwrap();
                let (metric_physical_tables, remaining_tables, views) =
                    self.get_table_list(&self.catalog, &schema).await?;
                let table_count =
                    metric_physical_tables.len() + remaining_tables.len() + views.len();
                let db_dir = self.catalog_path().join(format!("{schema}/"));
                tokio::fs::create_dir_all(&db_dir)
                    .await
                    .context(FileIoSnafu)?;
                let file = db_dir.join("create_tables.sql");
                let mut file = File::create(file).await.context(FileIoSnafu)?;
                for (c, s, t) in metric_physical_tables.into_iter().chain(remaining_tables) {
                    let create_table = self.show_create("TABLE", &c, &s, Some(&t)).await?;
                    file.write_all(create_table.as_bytes())
                        .await
                        .context(FileIoSnafu)?;
                }
                for (c, s, v) in views {
                    let create_view = self.show_create("VIEW", &c, &s, Some(&v)).await?;
                    file.write_all(create_view.as_bytes())
                        .await
                        .context(FileIoSnafu)?;
                }

                info!(
                    "Finished exporting {}.{schema} with {table_count} table schemas to path: {}",
                    self.catalog,
                    db_dir.to_string_lossy()
                );

                Ok::<(), Error>(())
            });
        }

        let success = futures::future::join_all(tasks)
            .await
            .into_iter()
            .filter(|r| match r {
                Ok(_) => true,
                Err(e) => {
                    error!(e; "export schema job failed");
                    false
                }
            })
            .count();

        let elapsed = timer.elapsed();
        info!("Success {success}/{db_count} jobs, cost: {elapsed:?}");

        Ok(())
    }

    async fn export_database_data(&self) -> Result<()> {
        let timer = Instant::now();
        let semaphore = Arc::new(Semaphore::new(self.parallelism));
        let db_names = self.get_db_names().await?;
        let db_count = db_names.len();
        let mut tasks = Vec::with_capacity(db_count);
        for schema in db_names {
            let semaphore_moved = semaphore.clone();
            tasks.push(async move {
                let _permit = semaphore_moved.acquire().await.unwrap();
                let db_dir = self.catalog_path().join(format!("{schema}/"));
                tokio::fs::create_dir_all(&db_dir)
                    .await
                    .context(FileIoSnafu)?;

                let with_options = match (&self.start_time, &self.end_time) {
                    (Some(start_time), Some(end_time)) => {
                        format!(
                            "WITH (FORMAT='parquet', start_time='{}', end_time='{}')",
                            start_time, end_time
                        )
                    }
                    (Some(start_time), None) => {
                        format!("WITH (FORMAT='parquet', start_time='{}')", start_time)
                    }
                    (None, Some(end_time)) => {
                        format!("WITH (FORMAT='parquet', end_time='{}')", end_time)
                    }
                    (None, None) => "WITH (FORMAT='parquet')".to_string(),
                };

                let sql = format!(
                    r#"COPY DATABASE "{}"."{}" TO '{}' {};"#,
                    self.catalog,
                    schema,
                    db_dir.to_str().unwrap(),
                    with_options
                );

                info!("Executing sql: {sql}");

                self.database_client.sql_in_public(&sql).await?;

                info!(
                    "Finished exporting {}.{schema} data into path: {}",
                    self.catalog,
                    db_dir.to_string_lossy()
                );

                // The export copy from sql
                let copy_from_file = db_dir.join("copy_from.sql");
                let mut writer =
                    BufWriter::new(File::create(copy_from_file).await.context(FileIoSnafu)?);
                let copy_database_from_sql = format!(
                    r#"COPY DATABASE "{}"."{}" FROM '{}' WITH (FORMAT='parquet');"#,
                    self.catalog,
                    schema,
                    db_dir.to_str().unwrap()
                );
                writer
                    .write(copy_database_from_sql.as_bytes())
                    .await
                    .context(FileIoSnafu)?;
                writer.flush().await.context(FileIoSnafu)?;

                info!("Finished exporting {}.{schema} copy_from.sql", self.catalog);

                Ok::<(), Error>(())
            })
        }

        let success = futures::future::join_all(tasks)
            .await
            .into_iter()
            .filter(|r| match r {
                Ok(_) => true,
                Err(e) => {
                    error!(e; "export database job failed");
                    false
                }
            })
            .count();
        let elapsed = timer.elapsed();

        info!("Success {success}/{db_count} jobs, costs: {elapsed:?}");

        Ok(())
    }
}

#[async_trait]
impl Tool for Export {
    async fn do_work(&self) -> Result<()> {
        match self.target {
            ExportTarget::Schema => {
                self.export_create_database().await?;
                self.export_create_table().await
            }
            ExportTarget::Data => self.export_database_data().await,
            ExportTarget::All => {
                self.export_create_database().await?;
                self.export_create_table().await?;
                self.export_database_data().await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;
    use client::{Client, Database};
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use common_telemetry::logging::LoggingOptions;

    use crate::error::Result as CmdResult;
    use crate::options::GlobalOptions;
    use crate::{cli, standalone, App};

    #[tokio::test(flavor = "multi_thread")]
    async fn test_export_create_table_with_quoted_names() -> CmdResult<()> {
        let output_dir = tempfile::tempdir().unwrap();

        let standalone = standalone::Command::parse_from([
            "standalone",
            "start",
            "--data-home",
            &*output_dir.path().to_string_lossy(),
        ]);

        let standalone_opts = standalone.load_options(&GlobalOptions::default()).unwrap();
        let mut instance = standalone.build(standalone_opts).await?;
        instance.start().await?;

        let client = Client::with_urls(["127.0.0.1:4001"]);
        let database = Database::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, client);
        database
            .sql(r#"CREATE DATABASE "cli.export.create_table";"#)
            .await
            .unwrap();
        database
            .sql(
                r#"CREATE TABLE "cli.export.create_table"."a.b.c"(
                        ts TIMESTAMP,
                        TIME INDEX (ts)
                    ) engine=mito;
                "#,
            )
            .await
            .unwrap();

        let output_dir = tempfile::tempdir().unwrap();
        let cli = cli::Command::parse_from([
            "cli",
            "export",
            "--addr",
            "127.0.0.1:4000",
            "--output-dir",
            &*output_dir.path().to_string_lossy(),
            "--target",
            "schema",
        ]);
        let mut cli_app = cli.build(LoggingOptions::default()).await?;
        cli_app.start().await?;

        instance.stop().await?;

        let output_file = output_dir
            .path()
            .join("greptime")
            .join("cli.export.create_table")
            .join("create_tables.sql");
        let res = std::fs::read_to_string(output_file).unwrap();
        let expect = r#"CREATE TABLE IF NOT EXISTS "a.b.c" (
  "ts" TIMESTAMP(3) NOT NULL,
  TIME INDEX ("ts")
)

ENGINE=mito
;
"#;
        assert_eq!(res.trim(), expect.trim());

        Ok(())
    }
}
