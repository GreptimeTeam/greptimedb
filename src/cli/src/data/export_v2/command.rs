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

use crate::Tool;
use crate::common::ObjectStoreConfig;
use crate::data::export_v2::error::{
    CannotResumeSchemaOnlySnafu, DataExportNotImplementedSnafu, DatabaseSnafu, EmptyResultSnafu,
    ManifestVersionMismatchSnafu, Result, UnexpectedValueTypeSnafu,
};
use crate::data::export_v2::extractor::SchemaExtractor;
use crate::data::export_v2::manifest::{DataFormat, MANIFEST_VERSION, Manifest};
use crate::data::path::ddl_path_for_schema;
use crate::data::snapshot_storage::{OpenDalStorage, SnapshotStorage, validate_uri};
use crate::data::sql::{escape_sql_identifier, escape_sql_literal};
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

    /// Time range start (ISO 8601 format, e.g., 2024-01-01T00:00:00Z).
    #[clap(long)]
    start_time: Option<String>,

    /// Time range end (ISO 8601 format, e.g., 2024-12-31T23:59:59Z).
    #[clap(long)]
    end_time: Option<String>,

    /// Data format: parquet, csv, json.
    #[clap(long, value_enum, default_value = "parquet")]
    format: DataFormat,

    /// Delete existing snapshot and recreate.
    #[clap(long)]
    force: bool,

    /// Concurrency level (for future use).
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

        if !self.schema_only {
            return DataExportNotImplementedSnafu
                .fail()
                .map_err(BoxedError::new);
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
            catalog: self.catalog.clone(),
            schemas,
            schema_only: self.schema_only,
            _format: self.format,
            force: self.force,
            _parallelism: self.parallelism,
            storage: Box::new(storage),
            database_client,
        }))
    }
}

/// Export tool implementation.
pub struct ExportCreate {
    catalog: String,
    schemas: Option<Vec<String>>,
    schema_only: bool,
    _format: DataFormat,
    force: bool,
    _parallelism: usize,
    storage: Box<dyn SnapshotStorage>,
    database_client: DatabaseClient,
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
            if self.force {
                info!("Deleting existing snapshot (--force)");
                self.storage.delete_snapshot().await?;
            } else {
                // Resume mode - read existing manifest
                let manifest = self.storage.read_manifest().await?;

                // Check version compatibility
                if manifest.version != MANIFEST_VERSION {
                    return ManifestVersionMismatchSnafu {
                        expected: MANIFEST_VERSION,
                        found: manifest.version,
                    }
                    .fail();
                }

                // Cannot resume schema-only with data export
                if manifest.schema_only && !self.schema_only {
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
        let extractor = SchemaExtractor::new(&self.database_client, &self.catalog);
        let schema_snapshot = extractor.extract(self.schemas.as_deref()).await?;

        let schema_names: Vec<String> = schema_snapshot
            .schemas
            .iter()
            .map(|s| s.name.clone())
            .collect();
        info!("Exporting schemas: {:?}", schema_names);

        // 3. Create manifest
        let manifest = Manifest::new_schema_only(self.catalog.clone(), schema_names.clone());

        // 4. Write schema files
        self.storage.write_schema(&schema_snapshot).await?;
        info!("Exported {} schemas", schema_snapshot.schemas.len());

        // 5. Export DDL files for import recovery.
        let ddl_by_schema = self.build_ddl_by_schema(&schema_names).await?;
        for (schema, ddl) in ddl_by_schema {
            let ddl_path = ddl_path_for_schema(&schema);
            self.storage.write_text(&ddl_path, &ddl).await?;
            info!("Exported DDL for schema {} to {}", schema, ddl_path);
        }

        // 6. Write manifest last.
        //
        // The manifest is the snapshot commit point: only write it after the schema
        // index and all DDL files are durable, so a crash cannot leave a "valid"
        // snapshot that is missing required schema artifacts.
        self.storage.write_manifest(&manifest).await?;
        info!("Snapshot created: {}", manifest.snapshot_id);

        Ok(())
    }

    async fn build_ddl_by_schema(&self, schema_names: &[String]) -> Result<Vec<(String, String)>> {
        let mut schemas = schema_names.to_vec();
        schemas.sort();

        let mut ddl_by_schema = Vec::with_capacity(schemas.len());
        for schema in schemas {
            let create_database = self.show_create("DATABASE", &schema, None).await?;

            let (mut physical_tables, mut tables, mut views) =
                self.get_schema_objects(&schema).await?;
            physical_tables.sort();
            let mut physical_ddls = Vec::with_capacity(physical_tables.len());
            for table in physical_tables {
                physical_ddls.push(self.show_create("TABLE", &schema, Some(&table)).await?);
            }

            tables.sort();
            let mut table_ddls = Vec::with_capacity(tables.len());
            for table in tables {
                table_ddls.push(self.show_create("TABLE", &schema, Some(&table)).await?);
            }

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

    async fn get_schema_objects(
        &self,
        schema: &str,
    ) -> Result<(Vec<String>, Vec<String>, Vec<String>)> {
        let physical_tables = self.get_metric_physical_tables(schema).await?;
        let physical_set: HashSet<&str> = physical_tables.iter().map(String::as_str).collect();
        let sql = format!(
            "SELECT table_name, table_type FROM information_schema.tables \
             WHERE table_catalog = '{}' AND table_schema = '{}' \
             AND (table_type = 'BASE TABLE' OR table_type = 'VIEW')",
            escape_sql_literal(&self.catalog),
            escape_sql_literal(schema)
        );
        let records: Option<Vec<Vec<Value>>> = self
            .database_client
            .sql_in_public(&sql)
            .await
            .context(DatabaseSnafu)?;

        let mut tables = Vec::new();
        let mut views = Vec::new();
        if let Some(rows) = records {
            for row in rows {
                let name = match row.first() {
                    Some(Value::String(name)) => name.clone(),
                    _ => return UnexpectedValueTypeSnafu.fail(),
                };
                let table_type = match row.get(1) {
                    Some(Value::String(table_type)) => table_type.as_str(),
                    _ => return UnexpectedValueTypeSnafu.fail(),
                };
                if !physical_set.contains(name.as_str()) {
                    if table_type == "VIEW" {
                        views.push(name);
                    } else {
                        tables.push(name);
                    }
                }
            }
        }

        Ok((physical_tables, tables, views))
    }

    async fn get_metric_physical_tables(&self, schema: &str) -> Result<Vec<String>> {
        let sql = format!(
            "SELECT DISTINCT table_name FROM information_schema.columns \
             WHERE table_catalog = '{}' AND table_schema = '{}' AND column_name = '__tsid'",
            escape_sql_literal(&self.catalog),
            escape_sql_literal(schema)
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
                show_type,
                escape_sql_identifier(&self.catalog),
                escape_sql_identifier(schema),
                escape_sql_identifier(table)
            ),
            None => format!(
                r#"SHOW CREATE {} "{}"."{}""#,
                show_type,
                escape_sql_identifier(&self.catalog),
                escape_sql_identifier(schema)
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
    use clap::Parser;

    use super::*;
    use crate::data::path::ddl_path_for_schema;

    #[test]
    fn test_ddl_path_for_schema() {
        assert_eq!(ddl_path_for_schema("public"), "schema/ddl/public.sql");
        assert_eq!(
            ddl_path_for_schema("../evil"),
            "schema/ddl/%2E%2E%2Fevil.sql"
        );
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

    #[tokio::test]
    async fn test_build_rejects_non_schema_only_export() {
        let cmd = ExportCreateCommand::parse_from([
            "export-v2-create",
            "--addr",
            "127.0.0.1:4000",
            "--to",
            "file:///tmp/export-v2-test",
        ]);

        let result = cmd.build().await;
        assert!(result.is_err());
        let error = result.err().unwrap().to_string();

        assert!(error.contains("Data export is not implemented yet"));
    }
}
