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
use common_telemetry::{debug, error, info, warn};
use serde_json::Value;
use servers::http::greptime_result_v1::GreptimedbV1Response;
use servers::http::GreptimeQueryOutput;
use snafu::{OptionExt, ResultExt};
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::Semaphore;
use tokio::time::Instant;

use crate::cli::{Instance, Tool};
use crate::error::{
    EmptyResultSnafu, Error, FileIoSnafu, HttpQuerySqlSnafu, InvalidDatabaseNameSnafu, Result,
    SerdeJsonSnafu,
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
    #[clap(long, short = 't', value_enum)]
    target: ExportTarget,

    /// basic authentication for connecting to the server
    #[clap(long)]
    auth_basic: Option<String>,
}

impl ExportCommand {
    pub async fn build(&self) -> Result<Instance> {
        let (catalog, schema) = split_database(&self.database)?;

        let auth_header = if let Some(basic) = &self.auth_basic {
            let encoded = general_purpose::STANDARD.encode(basic);
            Some(format!("basic {}", encoded))
        } else {
            None
        };

        Ok(Instance::new(Box::new(Export {
            addr: self.addr.clone(),
            catalog,
            schema,
            output_dir: self.output_dir.clone(),
            parallelism: self.export_jobs,
            target: self.target.clone(),
            auth_header,
        })))
    }
}

pub struct Export {
    addr: String,
    catalog: String,
    schema: Option<String>,
    output_dir: String,
    parallelism: usize,
    target: ExportTarget,
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
            let result = self.sql("show databases").await?;
            let Some(records) = result else {
                EmptyResultSnafu.fail()?
            };
            let mut result = Vec::with_capacity(records.len());
            for value in records {
                let serde_json::Value::String(schema) = &value[0] else {
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
            "select table_catalog, table_schema, table_name from \
            information_schema.columns where column_name = '__tsid' \
            and table_catalog = \'{catalog}\' and table_schema = \'{schema}\'"
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
            "select table_catalog, table_schema, table_name from \
            information_schema.tables where table_type = \'BASE TABLE\' \
            and table_catalog = \'{catalog}\' and table_schema = \'{schema}\'",
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
            r#"show create table "{}"."{}"."{}""#,
            catalog, schema, table
        );
        let result = self.sql(&sql).await?;
        let Some(records) = result else {
            EmptyResultSnafu.fail()?
        };
        let serde_json::Value::String(create_table) = &records[0][1] else {
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
                tokio::fs::create_dir_all(&self.output_dir)
                    .await
                    .context(FileIoSnafu)?;
                let output_file =
                    Path::new(&self.output_dir).join(format!("{catalog}-{schema}.sql"));
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

        let elapsed = timer.elapsed();
        info!("Success {success}/{db_count} jobs, cost: {:?}", elapsed);

        Ok(())
    }

    async fn export_table_data(&self) -> Result<()> {
        let timer = Instant::now();
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
                // Ignores metric physical tables
                let (_, table_list) = self.get_table_list(&catalog, &schema).await?;
                for (_, _, table_name) in table_list {
                    // copy table to
                    let sql = format!(
                        "copy table {} to '{}{}.parquet' with (format='parquet');",
                        schema,
                        output_dir.to_str().unwrap(),
                        table_name,
                    );
                    self.sql(&sql).await?;
                    info!("finished exporting {catalog}.{schema} data");
                }

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
        let elapsed = timer.elapsed();
        info!("Success {success}/{db_count} jobs, costs: {:?}", elapsed);

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
