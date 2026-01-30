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

//! Export V2 CLI commands.

use std::collections::HashSet;
use std::time::Duration;

use async_trait::async_trait;
use clap::{Parser, Subcommand};
use common_error::ext::BoxedError;
use common_telemetry::info;
use serde_json::Value;
use snafu::{OptionExt, ResultExt};

use super::coordinator::export_data;
use super::error::{
    CannotResumeSchemaOnlySnafu, DatabaseSnafu, EmptyResultSnafu, Result, UnexpectedValueTypeSnafu,
};
use super::extractor::SchemaExtractor;
use super::manifest::{DataFormat, MANIFEST_VERSION, Manifest, TimeRange};
use super::schema::{DDL_DIR, SCHEMA_DIR, SchemaSnapshot};
use super::storage::{OpenDalStorage, SnapshotStorage, validate_uri};
use crate::Tool;
use crate::common::ObjectStoreConfig;
use crate::database::{DatabaseClient, parse_proxy_opts};

/// Export V2 commands.
#[derive(Debug, Subcommand)]
pub enum ExportV2Command {
    /// Create a new snapshot.
    Create(ExportCreateCommand),
}

impl ExportV2Command {
    pub async fn build(&self) -> std::result::Result<Box<dyn Tool>, BoxedError> {
        match self {
            ExportV2Command::Create(cmd) => cmd.build().await,
        }
    }
}

/// Create a new snapshot.
#[derive(Debug, Parser)]
pub struct ExportCreateCommand {
    /// Server address to connect (e.g., 127.0.0.1:4000).
    #[clap(long)]
    addr: String,

    /// Target storage location (e.g., s3://bucket/path, file:///tmp/backup).
    #[clap(long)]
    to: String,

    /// Catalog name.
    #[clap(long, default_value = "greptime")]
    catalog: String,

    /// Schema list to export (default: all non-system schemas).
    /// Can be specified multiple times or comma-separated.
    #[clap(long, value_delimiter = ',')]
    schemas: Vec<String>,

    /// Export schema only, no data.
    #[clap(long)]
    schema_only: bool,

    /// Also export DDL (SHOW CREATE) into schema/ddl.sql.
    #[clap(long)]
    include_ddl: bool,

    /// Time range start (ISO 8601 format, e.g., 2024-01-01T00:00:00Z).
    /// If only start is provided, export data from start (inclusive) to now.
    #[clap(long)]
    start_time: Option<String>,

    /// Time range end (ISO 8601 format, e.g., 2024-12-31T23:59:59Z).
    /// If only end is provided, export data from earliest available to end (exclusive).
    #[clap(long)]
    end_time: Option<String>,

    /// Chunk time window (e.g., 1h, 6h, 1d, 7d).
    /// If omitted, export uses a single chunk.
    /// Requires both --start-time and --end-time when specified.
    #[clap(long, value_parser = humantime::parse_duration)]
    chunk_time_window: Option<Duration>,

    /// Data format: parquet, csv, json.
    #[clap(long, value_enum, default_value = "parquet")]
    format: DataFormat,

    /// Delete existing snapshot and recreate.
    #[clap(long)]
    force: bool,

    /// Parallelism for COPY DATABASE execution (server-side, per schema per chunk).
    #[clap(long, default_value = "1")]
    parallelism: usize,

    /// Basic authentication (user:password).
    #[clap(long)]
    auth_basic: Option<String>,

    /// Request timeout.
    #[clap(long, value_parser = humantime::parse_duration)]
    timeout: Option<Duration>,

    /// Proxy server address.
    ///
    /// If set, it overrides the system proxy unless `--no-proxy` is specified.
    /// If neither `--proxy` nor `--no-proxy` is set, system proxy (env) may be used.
    #[clap(long)]
    proxy: Option<String>,

    /// Disable all proxy usage (ignores `--proxy` and system proxy).
    ///
    /// When set and `--proxy` is not provided, this explicitly disables system proxy.
    #[clap(long)]
    no_proxy: bool,

    /// Object store configuration for remote storage backends.
    #[clap(flatten)]
    storage: ObjectStoreConfig,
}

impl ExportCreateCommand {
    pub async fn build(&self) -> std::result::Result<Box<dyn Tool>, BoxedError> {
        // Validate URI format
        validate_uri(&self.to).map_err(BoxedError::new)?;

        // Parse and validate time range
        let time_range = TimeRange::parse(self.start_time.as_deref(), self.end_time.as_deref())
            .map_err(BoxedError::new)?;
        if self.chunk_time_window.is_some() && !time_range.is_bounded() {
            return Err(BoxedError::new(
                super::error::ChunkTimeWindowRequiresBoundsSnafu.build(),
            ));
        }

        // Parse schemas (empty vec means all schemas)
        let schemas = if self.schemas.is_empty() {
            None
        } else {
            Some(self.schemas.clone())
        };

        // Build storage
        let storage = OpenDalStorage::from_uri(&self.to, &self.storage).map_err(BoxedError::new)?;

        // Build database client
        let proxy = parse_proxy_opts(self.proxy.clone(), self.no_proxy)?;
        let database_client = DatabaseClient::new(
            self.addr.clone(),
            self.catalog.clone(),
            self.auth_basic.clone(),
            self.timeout.unwrap_or(Duration::from_secs(60)),
            proxy,
            self.no_proxy,
        );

        Ok(Box::new(ExportCreate {
            config: ExportConfig {
                catalog: self.catalog.clone(),
                schemas,
                schema_only: self.schema_only,
                include_ddl: self.include_ddl,
                format: self.format,
                force: self.force,
                time_range,
                parallelism: self.parallelism,
                chunk_time_window: self.chunk_time_window,
                snapshot_uri: self.to.clone(),
                storage_config: self.storage.clone(),
            },
            storage: Box::new(storage),
            database_client,
        }))
    }
}

/// Export tool implementation.
pub struct ExportCreate {
    config: ExportConfig,
    storage: Box<dyn SnapshotStorage>,
    database_client: DatabaseClient,
}

struct ExportConfig {
    catalog: String,
    schemas: Option<Vec<String>>,
    schema_only: bool,
    include_ddl: bool,
    format: DataFormat,
    force: bool,
    time_range: TimeRange,
    chunk_time_window: Option<Duration>,
    parallelism: usize,
    snapshot_uri: String,
    storage_config: ObjectStoreConfig,
}

#[async_trait]
impl Tool for ExportCreate {
    async fn do_work(&self) -> std::result::Result<(), BoxedError> {
        self.run().await.map_err(BoxedError::new)
    }
}

impl ExportCreate {
    async fn run(&self) -> Result<()> {
        // 1. Check if snapshot exists
        let exists = self.storage.exists().await?;

        if exists {
            if self.config.force {
                info!("Deleting existing snapshot (--force)");
                self.storage.delete_snapshot().await?;
            } else {
                // Resume mode - read existing manifest
                let manifest = self.storage.read_manifest().await?;

                // Check version compatibility
                if manifest.version != MANIFEST_VERSION {
                    info!(
                        "Warning: Manifest version mismatch (expected {}, found {})",
                        MANIFEST_VERSION, manifest.version
                    );
                }

                // Cannot resume schema-only with data export
                if manifest.schema_only && !self.config.schema_only {
                    return CannotResumeSchemaOnlySnafu.fail();
                }

                info!(
                    "Resuming existing snapshot: {} (completed: {}/{} chunks)",
                    manifest.snapshot_id,
                    manifest.completed_count(),
                    manifest.chunks.len()
                );

                // For M1, we only handle schema-only exports
                // M2 will add chunk resume logic
                if manifest.is_complete() {
                    info!("Snapshot is already complete");
                    return Ok(());
                }

                // TODO: Resume data export in M2
                info!("Data export resume not yet implemented (M2)");
                return Ok(());
            }
        }

        // 2. Get schema list
        let extractor = SchemaExtractor::new(&self.database_client, &self.config.catalog);
        let schema_snapshot = extractor.extract(self.config.schemas.as_deref()).await?;

        let schema_names: Vec<String> = schema_snapshot
            .schemas
            .iter()
            .map(|s| s.name.clone())
            .collect();
        info!("Exporting schemas: {:?}", schema_names);

        // 3. Create manifest
        let mut manifest = Manifest::new_for_export(
            self.config.catalog.clone(),
            schema_names.clone(),
            self.config.schema_only,
            self.config.time_range.clone(),
            self.config.format,
            self.config.chunk_time_window,
        );

        // 4. Write schema files
        self.storage.write_schema(&schema_snapshot).await?;
        info!(
            "Exported {} schemas, {} tables, {} views",
            schema_snapshot.schemas.len(),
            schema_snapshot.tables.len(),
            schema_snapshot.views.len()
        );

        // 5. Optional DDL export (SHOW CREATE)
        if self.config.include_ddl {
            let ddl_by_schema = self.build_ddl_by_schema(&schema_snapshot).await?;
            for (schema, ddl) in ddl_by_schema {
                let ddl_path = ddl_path_for_schema(&schema);
                self.storage.write_text(&ddl_path, &ddl).await?;
                info!("Exported DDL for schema {} to {}", schema, ddl_path);
            }
        }

        // 6. Write manifest
        self.storage.write_manifest(&manifest).await?;
        info!("Snapshot created: {}", manifest.snapshot_id);

        // 7. Export data if not schema-only
        if !self.config.schema_only {
            export_data(
                self.storage.as_ref(),
                &self.database_client,
                &self.config.snapshot_uri,
                &self.config.storage_config,
                &mut manifest,
                self.config.parallelism,
            )
            .await?;
        }

        Ok(())
    }

    async fn build_ddl_by_schema(
        &self,
        schema_snapshot: &SchemaSnapshot,
    ) -> Result<Vec<(String, String)>> {
        let mut schemas = schema_snapshot
            .schemas
            .iter()
            .map(|s| s.name.clone())
            .collect::<Vec<_>>();
        schemas.sort();

        let mut ddl_by_schema = Vec::with_capacity(schemas.len());
        for schema in schemas {
            let create_database = self.show_create("DATABASE", &schema, None).await?;

            let mut physical_tables = self.get_metric_physical_tables(&schema).await?;
            physical_tables.sort();
            let mut physical_ddls = Vec::with_capacity(physical_tables.len());
            for table in physical_tables {
                physical_ddls.push(self.show_create("TABLE", &schema, Some(&table)).await?);
            }

            let mut tables = schema_snapshot
                .tables_in_schema(&schema)
                .into_iter()
                .map(|t| t.name.clone())
                .collect::<Vec<_>>();
            tables.sort();
            let mut table_ddls = Vec::with_capacity(tables.len());
            for table in tables {
                table_ddls.push(self.show_create("TABLE", &schema, Some(&table)).await?);
            }

            let mut views = schema_snapshot
                .views_in_schema(&schema)
                .into_iter()
                .map(|v| v.name.clone())
                .collect::<Vec<_>>();
            views.sort();
            let mut view_ddls = Vec::with_capacity(views.len());
            for view in views {
                view_ddls.push(self.show_create("VIEW", &schema, Some(&view)).await?);
            }

            let ddl = build_schema_ddl(
                &schema,
                create_database,
                physical_ddls,
                table_ddls,
                view_ddls,
            );
            ddl_by_schema.push((schema, ddl));
        }

        Ok(ddl_by_schema)
    }

    async fn get_metric_physical_tables(&self, schema: &str) -> Result<Vec<String>> {
        let sql = format!(
            "SELECT DISTINCT table_name FROM information_schema.columns \
             WHERE table_catalog = '{}' AND table_schema = '{}' AND column_name = '__tsid'",
            self.config.catalog, schema
        );
        let records: Option<Vec<Vec<Value>>> = self
            .database_client
            .sql_in_public(&sql)
            .await
            .context(DatabaseSnafu)?;

        let mut tables = HashSet::new();
        if let Some(rows) = records {
            for row in rows {
                let name = match row.first() {
                    Some(Value::String(name)) => name.clone(),
                    _ => return UnexpectedValueTypeSnafu.fail(),
                };
                tables.insert(name);
            }
        }

        Ok(tables.into_iter().collect())
    }

    async fn show_create(
        &self,
        show_type: &str,
        schema: &str,
        table: Option<&str>,
    ) -> Result<String> {
        let sql = match table {
            Some(table) => format!(
                r#"SHOW CREATE {} "{}"."{}"."{}""#,
                show_type, self.config.catalog, schema, table
            ),
            None => format!(
                r#"SHOW CREATE {} "{}"."{}""#,
                show_type, self.config.catalog, schema
            ),
        };

        let records: Option<Vec<Vec<Value>>> = self
            .database_client
            .sql_in_public(&sql)
            .await
            .context(DatabaseSnafu)?;
        let rows = records.context(EmptyResultSnafu)?;
        let row = rows.first().context(EmptyResultSnafu)?;
        let Some(Value::String(create)) = row.get(1) else {
            return UnexpectedValueTypeSnafu.fail();
        };

        Ok(format!("{};\n", create))
    }
}

fn ddl_path_for_schema(schema: &str) -> String {
    format!("{}/{}/{}.sql", SCHEMA_DIR, DDL_DIR, schema)
}

fn build_schema_ddl(
    schema: &str,
    create_database: String,
    physical_tables: Vec<String>,
    tables: Vec<String>,
    views: Vec<String>,
) -> String {
    let mut ddl = String::new();
    ddl.push_str(&format!("-- Schema: {}\n", schema));
    ddl.push_str(&create_database);
    for stmt in physical_tables {
        ddl.push_str(&stmt);
    }
    for stmt in tables {
        ddl.push_str(&stmt);
    }
    for stmt in views {
        ddl.push_str(&stmt);
    }
    ddl.push('\n');
    ddl
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ddl_path_for_schema() {
        assert_eq!(ddl_path_for_schema("public"), "schema/ddl/public.sql");
    }

    #[test]
    fn test_build_schema_ddl_order() {
        let ddl = build_schema_ddl(
            "public",
            "CREATE DATABASE public;\n".to_string(),
            vec!["PHYSICAL;\n".to_string()],
            vec!["TABLE;\n".to_string()],
            vec!["VIEW;\n".to_string()],
        );

        let db_pos = ddl.find("CREATE DATABASE").unwrap();
        let physical_pos = ddl.find("PHYSICAL;").unwrap();
        let table_pos = ddl.find("TABLE;").unwrap();
        let view_pos = ddl.find("VIEW;").unwrap();
        assert!(db_pos < physical_pos);
        assert!(physical_pos < table_pos);
        assert!(table_pos < view_pos);
    }
}
