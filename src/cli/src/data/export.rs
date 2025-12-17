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
use common_base::secrets::{ExposeSecret, SecretString};
use common_error::ext::BoxedError;
use common_telemetry::{debug, error, info};
use object_store::layers::LoggingLayer;
use object_store::services::Oss;
use object_store::{ObjectStore, services};
use serde_json::Value;
use snafu::{OptionExt, ResultExt};
use tokio::sync::Semaphore;
use tokio::time::Instant;

use crate::data::{COPY_PATH_PLACEHOLDER, default_database};
use crate::database::{DatabaseClient, parse_proxy_opts};
use crate::error::{
    EmptyResultSnafu, Error, OpenDalSnafu, OutputDirNotSetSnafu, Result, S3ConfigNotSetSnafu,
    SchemaNotFoundSnafu,
};
use crate::{Tool, database};

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

/// Command for exporting data from the GreptimeDB.
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
    #[clap(long, default_value_t = default_database())]
    database: String,

    /// The number of databases exported in parallel.
    /// For example, if there are 20 databases and `db_parallelism` is 4,
    /// 4 databases will be exported concurrently.
    #[clap(long, short = 'j', default_value = "1", alias = "export-jobs")]
    db_parallelism: usize,

    /// The number of tables exported in parallel within a single database.
    /// For example, if a database has 30 tables and `parallelism` is 8,
    /// 8 tables will be exported concurrently.
    #[clap(long, default_value = "4")]
    table_parallelism: usize,

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

    /// if both `ddl_local_dir` and remote storage (s3/oss) are set, `ddl_local_dir` will be only used for
    /// exported SQL files, and the data will be exported to remote storage.
    ///
    /// Note that `ddl_local_dir` export sql files to **LOCAL** file system, this is useful if export client don't have
    /// direct access to remote storage.
    ///
    /// if remote storage is set but `ddl_local_dir` is not set, both SQL&data will be exported to remote storage.
    #[clap(long)]
    ddl_local_dir: Option<String>,

    /// The s3 bucket name
    /// if s3 is set, this is required
    #[clap(long)]
    s3_bucket: Option<String>,

    // The s3 root path
    /// if s3 is set, this is required
    #[clap(long)]
    s3_root: Option<String>,

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

    /// if export data to oss
    #[clap(long)]
    oss: bool,

    /// The oss bucket name
    /// if oss is set, this is required
    #[clap(long)]
    oss_bucket: Option<String>,

    /// The oss endpoint
    /// if oss is set, this is required
    #[clap(long)]
    oss_endpoint: Option<String>,

    /// The oss access key id
    /// if oss is set, this is required
    #[clap(long)]
    oss_access_key_id: Option<String>,

    /// The oss access key secret
    /// if oss is set, this is required
    #[clap(long)]
    oss_access_key_secret: Option<String>,
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
        if !self.s3 && !self.oss && self.output_dir.is_none() {
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
            export_jobs: self.db_parallelism,
            target: self.target.clone(),
            start_time: self.start_time.clone(),
            end_time: self.end_time.clone(),
            parallelism: self.table_parallelism,
            s3: self.s3,
            ddl_local_dir: self.ddl_local_dir.clone(),
            s3_bucket: self.s3_bucket.clone(),
            s3_root: self.s3_root.clone(),
            s3_endpoint: self.s3_endpoint.clone(),
            // Wrap sensitive values in SecretString
            s3_access_key: self
                .s3_access_key
                .as_ref()
                .map(|k| SecretString::from(k.clone())),
            s3_secret_key: self
                .s3_secret_key
                .as_ref()
                .map(|k| SecretString::from(k.clone())),
            s3_region: self.s3_region.clone(),
            oss: self.oss,
            oss_bucket: self.oss_bucket.clone(),
            oss_endpoint: self.oss_endpoint.clone(),
            // Wrap sensitive values in SecretString
            oss_access_key_id: self
                .oss_access_key_id
                .as_ref()
                .map(|k| SecretString::from(k.clone())),
            oss_access_key_secret: self
                .oss_access_key_secret
                .as_ref()
                .map(|k| SecretString::from(k.clone())),
        }))
    }
}

#[derive(Clone)]
pub struct Export {
    catalog: String,
    schema: Option<String>,
    database_client: DatabaseClient,
    output_dir: Option<String>,
    export_jobs: usize,
    target: ExportTarget,
    start_time: Option<String>,
    end_time: Option<String>,
    parallelism: usize,
    s3: bool,
    ddl_local_dir: Option<String>,
    s3_bucket: Option<String>,
    s3_root: Option<String>,
    s3_endpoint: Option<String>,
    // Changed to SecretString for sensitive data
    s3_access_key: Option<SecretString>,
    s3_secret_key: Option<SecretString>,
    s3_region: Option<String>,
    oss: bool,
    oss_bucket: Option<String>,
    oss_endpoint: Option<String>,
    // Changed to SecretString for sensitive data
    oss_access_key_id: Option<SecretString>,
    oss_access_key_secret: Option<SecretString>,
}

impl Export {
    fn catalog_path(&self) -> PathBuf {
        if self.s3 || self.oss {
            PathBuf::from(&self.catalog)
        } else if let Some(dir) = &self.output_dir {
            PathBuf::from(dir).join(&self.catalog)
        } else {
            unreachable!("catalog_path: output_dir must be set when not using remote storage")
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
        let operator = self.build_prefer_fs_operator().await?;

        for schema in db_names {
            let create_database = self
                .show_create("DATABASE", &self.catalog, &schema, None)
                .await?;

            let file_path = self.get_file_path(&schema, "create_database.sql");
            self.write_to_storage(&operator, &file_path, create_database.into_bytes())
                .await?;

            info!(
                "Exported {}.{} database creation SQL to {}",
                self.catalog,
                schema,
                self.format_output_path(&file_path)
            );
        }

        let elapsed = timer.elapsed();
        info!("Success {db_count} jobs, cost: {elapsed:?}");

        Ok(())
    }

    async fn export_create_table(&self) -> Result<()> {
        let timer = Instant::now();
        let semaphore = Arc::new(Semaphore::new(self.export_jobs));
        let db_names = self.get_db_names().await?;
        let db_count = db_names.len();
        let operator = Arc::new(self.build_prefer_fs_operator().await?);
        let mut tasks = Vec::with_capacity(db_names.len());

        for schema in db_names {
            let semaphore_moved = semaphore.clone();
            let export_self = self.clone();
            let operator = operator.clone();
            tasks.push(async move {
                let _permit = semaphore_moved.acquire().await.unwrap();
                let (metric_physical_tables, remaining_tables, views) = export_self
                    .get_table_list(&export_self.catalog, &schema)
                    .await?;

                // Create directory if needed for file system storage
                if !export_self.s3 && !export_self.oss {
                    let db_dir = format!("{}/{}/", export_self.catalog, schema);
                    operator.create_dir(&db_dir).await.context(OpenDalSnafu)?;
                }

                let file_path = export_self.get_file_path(&schema, "create_tables.sql");
                let mut content = Vec::new();

                // Add table creation SQL
                for (c, s, t) in metric_physical_tables.iter().chain(&remaining_tables) {
                    let create_table = export_self.show_create("TABLE", c, s, Some(t)).await?;
                    content.extend_from_slice(create_table.as_bytes());
                }

                // Add view creation SQL
                for (c, s, v) in &views {
                    let create_view = export_self.show_create("VIEW", c, s, Some(v)).await?;
                    content.extend_from_slice(create_view.as_bytes());
                }

                // Write to storage
                export_self
                    .write_to_storage(&operator, &file_path, content)
                    .await?;

                info!(
                    "Finished exporting {}.{schema} with {} table schemas to path: {}",
                    export_self.catalog,
                    metric_physical_tables.len() + remaining_tables.len() + views.len(),
                    export_self.format_output_path(&file_path)
                );

                Ok::<(), Error>(())
            });
        }

        let success = self.execute_tasks(tasks).await;
        let elapsed = timer.elapsed();
        info!("Success {success}/{db_count} jobs, cost: {elapsed:?}");

        Ok(())
    }

    async fn build_operator(&self) -> Result<ObjectStore> {
        if self.s3 {
            self.build_s3_operator().await
        } else if self.oss {
            self.build_oss_operator().await
        } else {
            self.build_fs_operator().await
        }
    }

    /// build operator with preference for file system
    async fn build_prefer_fs_operator(&self) -> Result<ObjectStore> {
        if (self.s3 || self.oss) && self.ddl_local_dir.is_some() {
            let root = self.ddl_local_dir.as_ref().unwrap().clone();
            let op = ObjectStore::new(services::Fs::default().root(&root))
                .context(OpenDalSnafu)?
                .layer(LoggingLayer::default())
                .finish();
            Ok(op)
        } else if self.s3 {
            self.build_s3_operator().await
        } else if self.oss {
            self.build_oss_operator().await
        } else {
            self.build_fs_operator().await
        }
    }

    async fn build_s3_operator(&self) -> Result<ObjectStore> {
        let mut builder = services::S3::default().bucket(
            self.s3_bucket
                .as_ref()
                .expect("s3_bucket must be provided when s3 is enabled"),
        );

        if let Some(root) = self.s3_root.as_ref() {
            builder = builder.root(root);
        }

        if let Some(endpoint) = self.s3_endpoint.as_ref() {
            builder = builder.endpoint(endpoint);
        }

        if let Some(region) = self.s3_region.as_ref() {
            builder = builder.region(region);
        }

        if let Some(key_id) = self.s3_access_key.as_ref() {
            builder = builder.access_key_id(key_id.expose_secret());
        }

        if let Some(secret_key) = self.s3_secret_key.as_ref() {
            builder = builder.secret_access_key(secret_key.expose_secret());
        }

        let op = ObjectStore::new(builder)
            .context(OpenDalSnafu)?
            .layer(LoggingLayer::default())
            .finish();
        Ok(op)
    }

    async fn build_oss_operator(&self) -> Result<ObjectStore> {
        let mut builder = Oss::default()
            .bucket(self.oss_bucket.as_ref().expect("oss_bucket must be set"))
            .endpoint(
                self.oss_endpoint
                    .as_ref()
                    .expect("oss_endpoint must be set"),
            );

        // Use expose_secret() to access the actual secret value
        if let Some(key_id) = self.oss_access_key_id.as_ref() {
            builder = builder.access_key_id(key_id.expose_secret());
        }
        if let Some(secret_key) = self.oss_access_key_secret.as_ref() {
            builder = builder.access_key_secret(secret_key.expose_secret());
        }

        let op = ObjectStore::new(builder)
            .context(OpenDalSnafu)?
            .layer(LoggingLayer::default())
            .finish();
        Ok(op)
    }

    async fn build_fs_operator(&self) -> Result<ObjectStore> {
        let root = self
            .output_dir
            .as_ref()
            .context(OutputDirNotSetSnafu)?
            .clone();
        let op = ObjectStore::new(services::Fs::default().root(&root))
            .context(OpenDalSnafu)?
            .layer(LoggingLayer::default())
            .finish();
        Ok(op)
    }

    async fn export_database_data(&self) -> Result<()> {
        let timer = Instant::now();
        let semaphore = Arc::new(Semaphore::new(self.export_jobs));
        let db_names = self.get_db_names().await?;
        let db_count = db_names.len();
        let mut tasks = Vec::with_capacity(db_count);
        let operator = Arc::new(self.build_operator().await?);
        let fs_first_operator = Arc::new(self.build_prefer_fs_operator().await?);
        let with_options = build_with_options(&self.start_time, &self.end_time, self.parallelism);

        for schema in db_names {
            let semaphore_moved = semaphore.clone();
            let export_self = self.clone();
            let with_options_clone = with_options.clone();
            let operator = operator.clone();
            let fs_first_operator = fs_first_operator.clone();

            tasks.push(async move {
                let _permit = semaphore_moved.acquire().await.unwrap();

                // Create directory if not using remote storage
                if !export_self.s3 && !export_self.oss {
                    let db_dir = format!("{}/{}/", export_self.catalog, schema);
                    operator.create_dir(&db_dir).await.context(OpenDalSnafu)?;
                }

                let (path, connection_part) = export_self.get_storage_params(&schema);

                // Execute COPY DATABASE TO command
                let sql = format!(
                    r#"COPY DATABASE "{}"."{}" TO '{}' WITH ({}){};"#,
                    export_self.catalog, schema, path, with_options_clone, connection_part
                );

                // Log SQL command but mask sensitive information
                let safe_sql = export_self.mask_sensitive_sql(&sql);
                info!("Executing sql: {}", safe_sql);

                export_self.database_client.sql_in_public(&sql).await?;
                info!(
                    "Finished exporting {}.{} data to {}",
                    export_self.catalog, schema, path
                );

                // Create copy_from.sql file
                let copy_database_from_sql = {
                    let command_without_connection = format!(
                        r#"COPY DATABASE "{}"."{}" FROM '{}' WITH ({});"#,
                        export_self.catalog, schema, COPY_PATH_PLACEHOLDER, with_options_clone
                    );

                    if connection_part.is_empty() {
                        command_without_connection
                    } else {
                        let command_with_connection = format!(
                            r#"COPY DATABASE "{}"."{}" FROM '{}' WITH ({}){};"#,
                            export_self.catalog, schema, path, with_options_clone, connection_part
                        );

                        format!(
                            "-- {}\n{}",
                            command_with_connection, command_without_connection
                        )
                    }
                };

                let copy_from_path = export_self.get_file_path(&schema, "copy_from.sql");
                export_self
                    .write_to_storage(
                        &fs_first_operator,
                        &copy_from_path,
                        copy_database_from_sql.into_bytes(),
                    )
                    .await?;

                info!(
                    "Finished exporting {}.{} copy_from.sql to {}",
                    export_self.catalog,
                    schema,
                    export_self.format_output_path(&copy_from_path)
                );

                Ok::<(), Error>(())
            });
        }

        let success = self.execute_tasks(tasks).await;
        let elapsed = timer.elapsed();
        info!("Success {success}/{db_count} jobs, costs: {elapsed:?}");

        Ok(())
    }

    /// Mask sensitive information in SQL commands for safe logging
    fn mask_sensitive_sql(&self, sql: &str) -> String {
        let mut masked_sql = sql.to_string();

        // Mask S3 credentials
        if let Some(access_key) = &self.s3_access_key {
            masked_sql = masked_sql.replace(access_key.expose_secret(), "[REDACTED]");
        }
        if let Some(secret_key) = &self.s3_secret_key {
            masked_sql = masked_sql.replace(secret_key.expose_secret(), "[REDACTED]");
        }

        // Mask OSS credentials
        if let Some(access_key_id) = &self.oss_access_key_id {
            masked_sql = masked_sql.replace(access_key_id.expose_secret(), "[REDACTED]");
        }
        if let Some(access_key_secret) = &self.oss_access_key_secret {
            masked_sql = masked_sql.replace(access_key_secret.expose_secret(), "[REDACTED]");
        }

        masked_sql
    }

    fn get_file_path(&self, schema: &str, file_name: &str) -> String {
        format!("{}/{}/{}", self.catalog, schema, file_name)
    }

    fn format_output_path(&self, file_path: &str) -> String {
        if self.s3 {
            format!(
                "s3://{}{}/{}",
                self.s3_bucket.as_ref().unwrap_or(&String::new()),
                if let Some(root) = &self.s3_root {
                    format!("/{}", root)
                } else {
                    String::new()
                },
                file_path
            )
        } else if self.oss {
            format!(
                "oss://{}/{}/{}",
                self.oss_bucket.as_ref().unwrap_or(&String::new()),
                self.catalog,
                file_path
            )
        } else {
            format!(
                "{}/{}",
                self.output_dir.as_ref().unwrap_or(&String::new()),
                file_path
            )
        }
    }

    async fn write_to_storage(
        &self,
        op: &ObjectStore,
        file_path: &str,
        content: Vec<u8>,
    ) -> Result<()> {
        op.write(file_path, content)
            .await
            .context(OpenDalSnafu)
            .map(|_| ())
    }

    fn get_storage_params(&self, schema: &str) -> (String, String) {
        if self.s3 {
            let s3_path = format!(
                "s3://{}{}/{}/{}/",
                // Safety: s3_bucket is required when s3 is enabled
                self.s3_bucket.as_ref().unwrap(),
                if let Some(root) = &self.s3_root {
                    format!("/{}", root)
                } else {
                    String::new()
                },
                self.catalog,
                schema
            );

            // endpoint is optional
            let endpoint_option = if let Some(endpoint) = self.s3_endpoint.as_ref() {
                format!(", ENDPOINT='{}'", endpoint)
            } else {
                String::new()
            };

            // Safety: All s3 options are required
            // Use expose_secret() to access the actual secret values
            let connection_options = format!(
                "ACCESS_KEY_ID='{}', SECRET_ACCESS_KEY='{}', REGION='{}'{}",
                self.s3_access_key.as_ref().unwrap().expose_secret(),
                self.s3_secret_key.as_ref().unwrap().expose_secret(),
                self.s3_region.as_ref().unwrap(),
                endpoint_option
            );

            (s3_path, format!(" CONNECTION ({})", connection_options))
        } else if self.oss {
            let oss_path = format!(
                "oss://{}/{}/{}/",
                self.oss_bucket.as_ref().unwrap(),
                self.catalog,
                schema
            );
            let endpoint_option = if let Some(endpoint) = self.oss_endpoint.as_ref() {
                format!(", ENDPOINT='{}'", endpoint)
            } else {
                String::new()
            };

            let connection_options = format!(
                "ACCESS_KEY_ID='{}', ACCESS_KEY_SECRET='{}'{}",
                self.oss_access_key_id.as_ref().unwrap().expose_secret(),
                self.oss_access_key_secret.as_ref().unwrap().expose_secret(),
                endpoint_option
            );
            (oss_path, format!(" CONNECTION ({})", connection_options))
        } else {
            (
                self.catalog_path()
                    .join(format!("{schema}/"))
                    .to_string_lossy()
                    .to_string(),
                String::new(),
            )
        }
    }

    async fn execute_tasks(
        &self,
        tasks: Vec<impl std::future::Future<Output = Result<()>>>,
    ) -> usize {
        futures::future::join_all(tasks)
            .await
            .into_iter()
            .filter(|r| match r {
                Ok(_) => true,
                Err(e) => {
                    error!(e; "export job failed");
                    false
                }
            })
            .count()
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
fn build_with_options(
    start_time: &Option<String>,
    end_time: &Option<String>,
    parallelism: usize,
) -> String {
    let mut options = vec!["format = 'parquet'".to_string()];
    if let Some(start) = start_time {
        options.push(format!("start_time = '{}'", start));
    }
    if let Some(end) = end_time {
        options.push(format!("end_time = '{}'", end));
    }
    options.push(format!("parallelism = {}", parallelism));
    options.join(", ")
}
