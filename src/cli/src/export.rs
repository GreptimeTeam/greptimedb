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
use common_error::ext::BoxedError;
use common_telemetry::{debug, error, info};
use opendal::layers::LoggingLayer;
use opendal::{services, Operator};
use serde_json::Value;
use snafu::{OptionExt, ResultExt};
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::Semaphore;
use tokio::time::Instant;

use crate::database::{parse_proxy_opts, DatabaseClient};
use crate::error::{
    EmptyResultSnafu, Error, FileIoSnafu, OpenDalSnafu, OutputDirNotSetSnafu, Result,
    S3ConfigNotSetSnafu, SchemaNotFoundSnafu,
};
use crate::{database, Tool};

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
    /// for local export.
    #[clap(long)]
    output_dir: Option<String>,

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
    /// The default behavior will disable server-side default timeout(i.e. `0s`).
    #[clap(long, value_parser = humantime::parse_duration)]
    timeout: Option<Duration>,

    /// The proxy server address to connect, if set, will override the system proxy.
    ///
    /// The default behavior will use the system proxy if neither `proxy` nor `no_proxy` is set.
    #[clap(long)]
    proxy: Option<String>,

    /// Disable proxy server, if set, will not use any proxy.
    #[clap(long)]
    no_proxy: bool,

    /// if export data to s3
    #[clap(long)]
    s3: bool,

    /// The s3 bucket name
    /// if s3 is set, this is required
    #[clap(long)]
    s3_bucket: Option<String>,

    /// The s3 endpoint
    /// if s3 is set, this is required
    #[clap(long)]
    s3_endpoint: Option<String>,

    /// The s3 access key
    /// if s3 is set, this is required
    #[clap(long)]
    s3_access_key: Option<String>,

    /// The s3 secret key
    /// if s3 is set, this is required
    #[clap(long)]
    s3_secret_key: Option<String>,

    /// The s3 region
    /// if s3 is set, this is required
    #[clap(long)]
    s3_region: Option<String>,
}

impl ExportCommand {
    pub async fn build(&self) -> std::result::Result<Box<dyn Tool>, BoxedError> {
        if self.s3
            && (self.s3_bucket.is_none()
                || self.s3_endpoint.is_none()
                || self.s3_access_key.is_none()
                || self.s3_secret_key.is_none()
                || self.s3_region.is_none())
        {
            return Err(BoxedError::new(S3ConfigNotSetSnafu {}.build()));
        }
        if !self.s3 && self.output_dir.is_none() {
            return Err(BoxedError::new(OutputDirNotSetSnafu {}.build()));
        }
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
        );

        Ok(Box::new(Export {
            catalog,
            schema,
            database_client,
            output_dir: self.output_dir.clone(),
            parallelism: self.export_jobs,
            target: self.target.clone(),
            start_time: self.start_time.clone(),
            end_time: self.end_time.clone(),
            s3: self.s3,
            s3_bucket: self.s3_bucket.clone(),
            s3_endpoint: self.s3_endpoint.clone(),
            s3_access_key: self.s3_access_key.clone(),
            s3_secret_key: self.s3_secret_key.clone(),
            s3_region: self.s3_region.clone(),
        }))
    }
}

#[derive(Clone)]
pub struct Export {
    catalog: String,
    schema: Option<String>,
    database_client: DatabaseClient,
    output_dir: Option<String>,
    parallelism: usize,
    target: ExportTarget,
    start_time: Option<String>,
    end_time: Option<String>,
    s3: bool,
    s3_bucket: Option<String>,
    s3_endpoint: Option<String>,
    s3_access_key: Option<String>,
    s3_secret_key: Option<String>,
    s3_region: Option<String>,
}

impl Export {
    fn catalog_path(&self) -> PathBuf {
        if self.s3 {
            PathBuf::from(&self.catalog)
        } else if let Some(dir) = &self.output_dir {
            PathBuf::from(dir).join(&self.catalog)
        } else {
            unreachable!("catalog_path: output_dir must be set when not using s3")
        }
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
        let s3_operator = if self.s3 {
            Some(Arc::new(self.build_s3_operator().await?))
        } else {
            None
        };
        let mut tasks = Vec::with_capacity(db_names.len());
        for schema in db_names {
            let semaphore_moved = semaphore.clone();
            let export_self = self.clone();
            let s3_operator = s3_operator.clone();
            tasks.push(async move {
                let _permit = semaphore_moved.acquire().await.unwrap();
                let (metric_physical_tables, remaining_tables, views) = export_self
                    .get_table_list(&export_self.catalog, &schema)
                    .await?;

                if export_self.s3 {
                    let op = s3_operator.unwrap().clone();
                    let create_file_key =
                        format!("{}/{}/create_tables.sql", export_self.catalog, schema);
                    let mut content = Vec::new();
                    for (c, s, t) in metric_physical_tables.iter().chain(&remaining_tables) {
                        let create_table = export_self.show_create("TABLE", c, s, Some(t)).await?;
                        content.extend_from_slice(create_table.as_bytes());
                    }
                    for (c, s, v) in &views {
                        let create_view = export_self.show_create("VIEW", c, s, Some(v)).await?;
                        content.extend_from_slice(create_view.as_bytes());
                    }
                    op.write(&create_file_key, content)
                        .await
                        .context(OpenDalSnafu)?;
                } else {
                    let db_dir = export_self.catalog_path().join(format!("{schema}/"));
                    tokio::fs::create_dir_all(&db_dir)
                        .await
                        .context(FileIoSnafu)?;
                    let file = db_dir.join("create_tables.sql");
                    let mut file = File::create(file).await.context(FileIoSnafu)?;
                    for (c, s, t) in metric_physical_tables.iter().chain(remaining_tables.iter()) {
                        let create_table = export_self.show_create("TABLE", c, s, Some(t)).await?;
                        file.write_all(create_table.as_bytes())
                            .await
                            .context(FileIoSnafu)?;
                    }
                    for (c, s, v) in views.iter() {
                        let create_view = export_self.show_create("VIEW", c, s, Some(v)).await?;
                        file.write_all(create_view.as_bytes())
                            .await
                            .context(FileIoSnafu)?;
                    }
                    info!(
                        "Finished exporting {}.{schema} with {} table schemas to path: {}",
                        export_self.catalog,
                        metric_physical_tables.len() + remaining_tables.len() + views.len(),
                        export_self
                            .catalog_path()
                            .join(format!("{schema}/"))
                            .to_string_lossy()
                    );
                }
                info!(
                    "Finished exporting {}.{schema} with {} table schemas to path: {}",
                    export_self.catalog,
                    metric_physical_tables.len() + remaining_tables.len() + views.len(),
                    if export_self.s3 {
                        format!(
                            "s3://{}/{}/create_tables.sql",
                            export_self.s3_bucket.clone().unwrap(),
                            schema
                        )
                    } else {
                        export_self
                            .catalog_path()
                            .join(format!("{schema}/"))
                            .to_string_lossy()
                            .to_string()
                    }
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

    async fn build_s3_operator(&self) -> Result<Operator> {
        let mut builder = services::S3::default().root("").bucket(
            self.s3_bucket
                .as_ref()
                .expect("s3_bucket must be provided when s3 is enabled"),
        );

        if let Some(endpoint) = self.s3_endpoint.as_ref() {
            builder = builder.endpoint(endpoint);
        }

        if let Some(region) = self.s3_region.as_ref() {
            builder = builder.region(region);
        }

        if let Some(key_id) = self.s3_access_key.as_ref() {
            builder = builder.access_key_id(key_id);
        }

        if let Some(secret_key) = self.s3_secret_key.as_ref() {
            builder = builder.secret_access_key(secret_key);
        }

        let op = Operator::new(builder)
            .context(OpenDalSnafu)?
            .layer(LoggingLayer::default())
            .finish();
        Ok(op)
    }

    async fn export_database_data(&self) -> Result<()> {
        let timer = Instant::now();
        let semaphore = Arc::new(Semaphore::new(self.parallelism));
        let db_names = self.get_db_names().await?;
        let db_count = db_names.len();
        let mut tasks = Vec::with_capacity(db_count);

        let with_options = build_with_options(&self.start_time, &self.end_time);

        for schema in db_names {
            let semaphore_moved = semaphore.clone();
            let export_self = self.clone();
            let with_options_clone = with_options.clone();
            tasks.push(async move {
                let _permit = semaphore_moved.acquire().await.unwrap();

                let db_dir = export_self.catalog_path().join(&schema);
                tokio::fs::create_dir_all(&db_dir)
                    .await
                    .context(FileIoSnafu)?;

                let (path, connection_part) = if export_self.s3 {
                    let s3_path = format!(
                        "s3://{}/{}/{}/",
                        export_self.s3_bucket.as_ref().unwrap(),
                        export_self.catalog,
                        schema
                    );
                    // endpoint is optional
                    let endpoint_option = if let Some(endpoint) = export_self.s3_endpoint.as_ref() {
                        format!(", ENDPOINT='{}'", endpoint)
                    } else {
                        String::new()
                    };
                    let connection_options = format!(
                        "ACCESS_KEY_ID='{}', SECRET_ACCESS_KEY='{}', REGION='{}'{}",
                        export_self.s3_access_key.as_ref().unwrap(),
                        export_self.s3_secret_key.as_ref().unwrap(),
                        export_self.s3_region.as_ref().unwrap(),
                        endpoint_option
                    );
                    (s3_path, format!(" CONNECTION ({})", connection_options))
                } else {
                    (
                        export_self
                            .catalog_path()
                            .join(format!("{schema}/"))
                            .to_string_lossy()
                            .to_string(),
                        String::new(),
                    )
                };

                let sql = format!(
                    r#"COPY DATABASE "{}"."{}" TO '{}' WITH ({}){};"#,
                    export_self.catalog, schema, path, with_options_clone, connection_part
                );
                info!("Executing sql: {sql}");
                export_self.database_client.sql_in_public(&sql).await?;
                info!(
                    "Finished exporting {}.{} data to {}",
                    export_self.catalog, schema, path
                );

                let copy_from_file = db_dir.join("copy_from.sql");
                let mut writer =
                    BufWriter::new(File::create(&copy_from_file).await.context(FileIoSnafu)?);
                let copy_database_from_sql = format!(
                    r#"COPY DATABASE "{}"."{}" FROM '{}' WITH ({}){};"#,
                    export_self.catalog, schema, path, with_options_clone, connection_part
                );
                writer
                    .write_all(copy_database_from_sql.as_bytes())
                    .await
                    .context(FileIoSnafu)?;
                writer.flush().await.context(FileIoSnafu)?;
                info!(
                    "Finished exporting {}.{} copy_from.sql to local path: {}",
                    export_self.catalog,
                    schema,
                    copy_from_file.to_string_lossy()
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
    async fn do_work(&self) -> std::result::Result<(), BoxedError> {
        match self.target {
            ExportTarget::Schema => {
                self.export_create_database()
                    .await
                    .map_err(BoxedError::new)?;
                self.export_create_table().await.map_err(BoxedError::new)
            }
            ExportTarget::Data => self.export_database_data().await.map_err(BoxedError::new),
            ExportTarget::All => {
                self.export_create_database()
                    .await
                    .map_err(BoxedError::new)?;
                self.export_create_table().await.map_err(BoxedError::new)?;
                self.export_database_data().await.map_err(BoxedError::new)
            }
        }
    }
}

/// Builds the WITH options string for SQL commands, assuming consistent syntax across S3 and local exports.
fn build_with_options(start_time: &Option<String>, end_time: &Option<String>) -> String {
    let mut options = vec!["format = 'parquet'".to_string()];
    if let Some(start) = start_time {
        options.push(format!("start_time = '{}'", start));
    }
    if let Some(end) = end_time {
        options.push(format!("end_time = '{}'", end));
    }
    options.join(", ")
}
