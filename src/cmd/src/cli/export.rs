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
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use base64::engine::general_purpose;
use base64::Engine;
use clap::{Parser, ValueEnum};
use client::DEFAULT_SCHEMA_NAME;
use common_catalog::consts::DEFAULT_CATALOG_NAME;
use common_telemetry::{debug, error, info};
use serde_json::Value;
use servers::http::greptime_result_v1::GreptimedbV1Response;
use servers::http::GreptimeQueryOutput;
use snafu::ResultExt;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::Semaphore;
use tokio::time::Instant;
use tracing_appender::non_blocking::WorkerGuard;

use crate::cli::{Instance, Tool};
use crate::error::{
    EmptyResultSnafu, Error, FileIoSnafu, HttpQuerySqlSnafu, Result, SerdeJsonSnafu,
};

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
}

impl ExportCommand {
    pub async fn build(&self, guard: Vec<WorkerGuard>) -> Result<Instance> {
        let (catalog, schema) = split_database(&self.database)?;

        let auth_header = if let Some(basic) = &self.auth_basic {
            let encoded = general_purpose::STANDARD.encode(basic);
            Some(format!("basic {}", encoded))
        } else {
            None
        };

        Ok(Instance::new(
            Box::new(Export {
                addr: self.addr.clone(),
                catalog,
                schema,
                output_dir: self.output_dir.clone(),
                parallelism: self.export_jobs,
                target: self.target.clone(),
                start_time: self.start_time.clone(),
                end_time: self.end_time.clone(),
                auth_header,
            }),
            guard,
        ))
    }
}

pub struct Export {
    addr: String,
    catalog: String,
    schema: Option<String>,
    output_dir: String,
    parallelism: usize,
    target: ExportTarget,
    start_time: Option<String>,
    end_time: Option<String>,
    auth_header: Option<String>,
}

impl Export {
    /// Execute one single sql query.
    async fn sql(&self, sql: &str) -> Result<Option<Vec<Vec<Value>>>> {
        let url = format!(
            "http://{}/v1/sql?db={}-{}&sql={}",
            self.addr,
            self.catalog,
            self.schema.as_deref().unwrap_or(DEFAULT_SCHEMA_NAME),
            sql
        );

        let mut request = reqwest::Client::new()
            .get(&url)
            .header("Content-Type", "application/x-www-form-urlencoded");
        if let Some(ref auth) = self.auth_header {
            request = request.header("Authorization", auth);
        }

        let response = request.send().await.with_context(|_| HttpQuerySqlSnafu {
            reason: format!("bad url: {}", url),
        })?;
        let response = response
            .error_for_status()
            .with_context(|_| HttpQuerySqlSnafu {
                reason: format!("query failed: {}", sql),
            })?;

        let text = response.text().await.with_context(|_| HttpQuerySqlSnafu {
            reason: "cannot get response text".to_string(),
        })?;

        let body = serde_json::from_str::<GreptimedbV1Response>(&text).context(SerdeJsonSnafu)?;
        Ok(body.output().first().and_then(|output| match output {
            GreptimeQueryOutput::Records(records) => Some(records.rows().clone()),
            GreptimeQueryOutput::AffectedRows(_) => None,
        }))
    }

    /// Iterate over all db names.
    ///
    /// Newbie: `db_name` is catalog + schema.
    async fn iter_db_names(&self) -> Result<Vec<(String, String)>> {
        if let Some(schema) = &self.schema {
            Ok(vec![(self.catalog.clone(), schema.clone())])
        } else {
            let result = self.sql("SHOW DATABASES").await?;
            let Some(records) = result else {
                EmptyResultSnafu.fail()?
            };
            let mut result = Vec::with_capacity(records.len());
            for value in records {
                let Value::String(schema) = &value[0] else {
                    unreachable!()
                };
                if schema == common_catalog::consts::INFORMATION_SCHEMA_NAME {
                    continue;
                }
                result.push((self.catalog.clone(), schema.clone()));
            }
            Ok(result)
        }
    }

    /// Return a list of [`TableReference`] to be exported.
    /// Includes all tables under the given `catalog` and `schema`.
    async fn get_table_list(
        &self,
        catalog: &str,
        schema: &str,
    ) -> Result<(Vec<TableReference>, Vec<TableReference>)> {
        // Puts all metric table first
        let sql = format!(
            "SELECT table_catalog, table_schema, table_name \
            FROM information_schema.columns \
            WHERE column_name = '__tsid' \
                and table_catalog = \'{catalog}\' \
                and table_schema = \'{schema}\'"
        );
        let result = self.sql(&sql).await?;
        let Some(records) = result else {
            EmptyResultSnafu.fail()?
        };
        let mut metric_physical_tables = HashSet::with_capacity(records.len());
        for value in records {
            let mut t = Vec::with_capacity(3);
            for v in &value {
                let serde_json::Value::String(value) = v else {
                    unreachable!()
                };
                t.push(value);
            }
            metric_physical_tables.insert((t[0].clone(), t[1].clone(), t[2].clone()));
        }

        // TODO: SQL injection hurts
        let sql = format!(
            "SELECT table_catalog, table_schema, table_name \
            FROM information_schema.tables \
            WHERE table_type = \'BASE TABLE\' \
                and table_catalog = \'{catalog}\' \
                and table_schema = \'{schema}\'",
        );
        let result = self.sql(&sql).await?;
        let Some(records) = result else {
            EmptyResultSnafu.fail()?
        };

        debug!("Fetched table list: {:?}", records);

        if records.is_empty() {
            return Ok((vec![], vec![]));
        }

        let mut remaining_tables = Vec::with_capacity(records.len());
        for value in records {
            let mut t = Vec::with_capacity(3);
            for v in &value {
                let serde_json::Value::String(value) = v else {
                    unreachable!()
                };
                t.push(value);
            }
            let table = (t[0].clone(), t[1].clone(), t[2].clone());
            // Ignores the physical table
            if !metric_physical_tables.contains(&table) {
                remaining_tables.push(table);
            }
        }

        Ok((
            metric_physical_tables.into_iter().collect(),
            remaining_tables,
        ))
    }

    async fn show_create_table(&self, catalog: &str, schema: &str, table: &str) -> Result<String> {
        let sql = format!(
            r#"SHOW CREATE TABLE "{}"."{}"."{}""#,
            catalog, schema, table
        );
        let result = self.sql(&sql).await?;
        let Some(records) = result else {
            EmptyResultSnafu.fail()?
        };
        let Value::String(create_table) = &records[0][1] else {
            unreachable!()
        };

        Ok(format!("{};\n", create_table))
    }

    async fn export_create_table(&self) -> Result<()> {
        let timer = Instant::now();
        let semaphore = Arc::new(Semaphore::new(self.parallelism));
        let db_names = self.iter_db_names().await?;
        let db_count = db_names.len();
        let mut tasks = Vec::with_capacity(db_names.len());
        for (catalog, schema) in db_names {
            let semaphore_moved = semaphore.clone();
            tasks.push(async move {
                let _permit = semaphore_moved.acquire().await.unwrap();
                let (metric_physical_tables, remaining_tables) =
                    self.get_table_list(&catalog, &schema).await?;
                let table_count = metric_physical_tables.len() + remaining_tables.len();
                let output_dir = Path::new(&self.output_dir)
                    .join(&catalog)
                    .join(format!("{schema}/"));
                tokio::fs::create_dir_all(&output_dir)
                    .await
                    .context(FileIoSnafu)?;
                let output_file = Path::new(&output_dir).join("create_tables.sql");
                let mut file = File::create(output_file).await.context(FileIoSnafu)?;
                for (c, s, t) in metric_physical_tables.into_iter().chain(remaining_tables) {
                    match self.show_create_table(&c, &s, &t).await {
                        Err(e) => {
                            error!(e; r#"Failed to export table "{}"."{}"."{}""#, c, s, t)
                        }
                        Ok(create_table) => {
                            file.write_all(create_table.as_bytes())
                                .await
                                .context(FileIoSnafu)?;
                        }
                    }
                }

                info!(
                    "Finished exporting {catalog}.{schema} with {table_count} table schemas to path: {}",
                    output_dir.to_string_lossy()
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
                    error!(e; "export job failed");
                    false
                }
            })
            .count();

        let elapsed = timer.elapsed();
        info!("Success {success}/{db_count} jobs, cost: {:?}", elapsed);

        Ok(())
    }

    async fn export_database_data(&self) -> Result<()> {
        let timer = Instant::now();
        let semaphore = Arc::new(Semaphore::new(self.parallelism));
        let db_names = self.iter_db_names().await?;
        let db_count = db_names.len();
        let mut tasks = Vec::with_capacity(db_names.len());
        for (catalog, schema) in db_names {
            let semaphore_moved = semaphore.clone();
            tasks.push(async move {
                let _permit = semaphore_moved.acquire().await.unwrap();
                let output_dir = Path::new(&self.output_dir)
                    .join(&catalog)
                    .join(format!("{schema}/"));
                tokio::fs::create_dir_all(&output_dir)
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
                    catalog,
                    schema,
                    output_dir.to_str().unwrap(),
                    with_options
                );

                info!("Executing sql: {sql}");

                self.sql(&sql).await?;

                info!(
                    "Finished exporting {catalog}.{schema} data into path: {}",
                    output_dir.to_string_lossy()
                );

                // The export copy from sql
                let copy_from_file = output_dir.join("copy_from.sql");
                let mut writer =
                    BufWriter::new(File::create(copy_from_file).await.context(FileIoSnafu)?);
                let copy_database_from_sql = format!(
                    r#"COPY DATABASE "{}"."{}" FROM '{}' WITH (FORMAT='parquet');"#,
                    catalog,
                    schema,
                    output_dir.to_str().unwrap()
                );
                writer
                    .write(copy_database_from_sql.as_bytes())
                    .await
                    .context(FileIoSnafu)?;
                writer.flush().await.context(FileIoSnafu)?;

                info!("Finished exporting {catalog}.{schema} copy_from.sql");

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

        info!("Success {success}/{db_count} jobs, costs: {:?}", elapsed);

        Ok(())
    }
}

#[allow(deprecated)]
#[async_trait]
impl Tool for Export {
    async fn do_work(&self) -> Result<()> {
        match self.target {
            ExportTarget::Schema => self.export_create_table().await,
            ExportTarget::Data => self.export_database_data().await,
            ExportTarget::All => {
                self.export_create_table().await?;
                self.export_database_data().await
            }
        }
    }
}

/// Split at `-`.
fn split_database(database: &str) -> Result<(String, Option<String>)> {
    let (catalog, schema) = match database.split_once('-') {
        Some((catalog, schema)) => (catalog, schema),
        None => (DEFAULT_CATALOG_NAME, database),
    };

    if schema == "*" {
        Ok((catalog.to_string(), None))
    } else {
        Ok((catalog.to_string(), Some(schema.to_string())))
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;
    use client::{Client, Database};
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use common_telemetry::logging::LoggingOptions;

    use crate::cli::export::split_database;
    use crate::error::Result as CmdResult;
    use crate::options::GlobalOptions;
    use crate::{cli, standalone, App};

    #[test]
    fn test_split_database() {
        let result = split_database("catalog-schema").unwrap();
        assert_eq!(result, ("catalog".to_string(), Some("schema".to_string())));

        let result = split_database("schema").unwrap();
        assert_eq!(result, ("greptime".to_string(), Some("schema".to_string())));

        let result = split_database("catalog-*").unwrap();
        assert_eq!(result, ("catalog".to_string(), None));

        let result = split_database("*").unwrap();
        assert_eq!(result, ("greptime".to_string(), None));
    }

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
