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
use crate::data::export_v2::coordinator::export_data;
use crate::data::export_v2::error::{
    ChunkTimeWindowRequiresBoundsSnafu, DatabaseSnafu, EmptyResultSnafu,
    ManifestVersionMismatchSnafu, Result, ResumeConfigMismatchSnafu, SchemaOnlyModeMismatchSnafu,
    UnexpectedValueTypeSnafu,
};
use crate::data::export_v2::extractor::SchemaExtractor;
use crate::data::export_v2::manifest::{
    ChunkMeta, DataFormat, MANIFEST_VERSION, Manifest, TimeRange,
};
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

    /// Chunk time window (e.g., 1h, 6h, 1d, 7d).
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

        let time_range = TimeRange::parse(self.start_time.as_deref(), self.end_time.as_deref())
            .map_err(BoxedError::new)?;
        if self.chunk_time_window.is_some() && !time_range.is_bounded() {
            return ChunkTimeWindowRequiresBoundsSnafu
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
            config: ExportConfig {
                catalog: self.catalog.clone(),
                schemas,
                schema_only: self.schema_only,
                format: self.format,
                force: self.force,
                time_range,
                chunk_time_window: self.chunk_time_window,
                parallelism: self.parallelism,
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
                let mut manifest = self.storage.read_manifest().await?;

                // Check version compatibility
                if manifest.version != MANIFEST_VERSION {
                    return ManifestVersionMismatchSnafu {
                        expected: MANIFEST_VERSION,
                        found: manifest.version,
                    }
                    .fail();
                }

                validate_resume_config(&manifest, &self.config)?;

                info!(
                    "Resuming existing snapshot: {} (completed: {}/{} chunks)",
                    manifest.snapshot_id,
                    manifest.completed_count(),
                    manifest.chunks.len()
                );

                if manifest.is_complete() {
                    info!("Snapshot is already complete");
                    return Ok(());
                }

                if manifest.schema_only {
                    return Ok(());
                }

                export_data(
                    self.storage.as_ref(),
                    &self.database_client,
                    &self.config.snapshot_uri,
                    &self.config.storage_config,
                    &mut manifest,
                    self.config.parallelism,
                )
                .await?;
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
        )?;

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

        // 6. Write manifest after schema artifacts and before any data export.
        //
        // The manifest is the snapshot commit point: only write it after the schema
        // index and all DDL files are durable, so a crash cannot leave a "valid"
        // snapshot that is missing required schema artifacts. For full exports we
        // still need the manifest before data copy starts, because chunk resume is
        // tracked by updating this manifest in place.
        self.storage.write_manifest(&manifest).await?;
        info!("Snapshot created: {}", manifest.snapshot_id);

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
            escape_sql_literal(&self.config.catalog),
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
            escape_sql_literal(&self.config.catalog),
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
                escape_sql_identifier(&self.config.catalog),
                escape_sql_identifier(schema),
                escape_sql_identifier(table)
            ),
            None => format!(
                r#"SHOW CREATE {} "{}"."{}""#,
                show_type,
                escape_sql_identifier(&self.config.catalog),
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

fn validate_resume_config(manifest: &Manifest, config: &ExportConfig) -> Result<()> {
    if manifest.schema_only != config.schema_only {
        return SchemaOnlyModeMismatchSnafu {
            existing_schema_only: manifest.schema_only,
            requested_schema_only: config.schema_only,
        }
        .fail();
    }

    if manifest.catalog != config.catalog {
        return ResumeConfigMismatchSnafu {
            field: "catalog",
            existing: manifest.catalog.clone(),
            requested: config.catalog.clone(),
        }
        .fail();
    }

    // If no schema filter is provided on resume, inherit the existing snapshot
    // selection instead of reinterpreting the request as "all schemas".
    if let Some(requested_schemas) = &config.schemas
        && !schema_selection_matches(&manifest.schemas, requested_schemas)
    {
        return ResumeConfigMismatchSnafu {
            field: "schemas",
            existing: format_schema_selection(&manifest.schemas),
            requested: format_schema_selection(requested_schemas),
        }
        .fail();
    }

    if manifest.time_range != config.time_range {
        return ResumeConfigMismatchSnafu {
            field: "time_range",
            existing: format!("{:?}", manifest.time_range),
            requested: format!("{:?}", config.time_range),
        }
        .fail();
    }

    if manifest.format != config.format {
        return ResumeConfigMismatchSnafu {
            field: "format",
            existing: manifest.format.to_string(),
            requested: config.format.to_string(),
        }
        .fail();
    }

    let expected_plan = Manifest::new_for_export(
        manifest.catalog.clone(),
        manifest.schemas.clone(),
        config.schema_only,
        config.time_range.clone(),
        config.format,
        config.chunk_time_window,
    )?;
    if !chunk_plan_matches(manifest, &expected_plan) {
        return ResumeConfigMismatchSnafu {
            field: "chunk plan",
            existing: format_chunk_plan(&manifest.chunks),
            requested: format_chunk_plan(&expected_plan.chunks),
        }
        .fail();
    }

    Ok(())
}

fn schema_selection_matches(existing: &[String], requested: &[String]) -> bool {
    canonical_schema_selection(existing) == canonical_schema_selection(requested)
}

fn canonical_schema_selection(schemas: &[String]) -> Vec<String> {
    let mut canonicalized = Vec::new();
    let mut seen = HashSet::new();

    for schema in schemas {
        let normalized = schema.to_ascii_lowercase();
        if seen.insert(normalized.clone()) {
            canonicalized.push(normalized);
        }
    }

    canonicalized.sort();
    canonicalized
}

fn format_schema_selection(schemas: &[String]) -> String {
    format!("[{}]", schemas.join(", "))
}

fn chunk_plan_matches(existing: &Manifest, expected: &Manifest) -> bool {
    existing.chunks.len() == expected.chunks.len()
        && existing
            .chunks
            .iter()
            .zip(&expected.chunks)
            .all(|(left, right)| left.id == right.id && left.time_range == right.time_range)
}

fn format_chunk_plan(chunks: &[ChunkMeta]) -> String {
    let items = chunks
        .iter()
        .map(|chunk| format!("#{}:{:?}", chunk.id, chunk.time_range))
        .collect::<Vec<_>>();
    format!("[{}]", items.join(", "))
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
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
    async fn test_build_rejects_chunk_window_without_bounds() {
        let cmd = ExportCreateCommand::parse_from([
            "export-v2-create",
            "--addr",
            "127.0.0.1:4000",
            "--to",
            "file:///tmp/export-v2-test",
            "--chunk-time-window",
            "1h",
        ]);

        let result = cmd.build().await;
        assert!(result.is_err());
        let error = result.err().unwrap().to_string();

        assert!(error.contains("chunk_time_window requires both --start-time and --end-time"));
    }

    #[test]
    fn test_schema_only_mode_mismatch_error_message() {
        let error = crate::data::export_v2::error::SchemaOnlyModeMismatchSnafu {
            existing_schema_only: false,
            requested_schema_only: true,
        }
        .build()
        .to_string();

        assert!(error.contains("existing: false"));
        assert!(error.contains("requested: true"));
    }

    #[test]
    fn test_validate_resume_config_rejects_catalog_mismatch() {
        let manifest = Manifest::new_for_export(
            "greptime".to_string(),
            vec!["public".to_string()],
            false,
            TimeRange::unbounded(),
            DataFormat::Parquet,
            None,
        )
        .unwrap();
        let config = ExportConfig {
            catalog: "other".to_string(),
            schemas: None,
            schema_only: false,
            format: DataFormat::Parquet,
            force: false,
            time_range: TimeRange::unbounded(),
            chunk_time_window: None,
            parallelism: 1,
            snapshot_uri: "file:///tmp/snapshot".to_string(),
            storage_config: ObjectStoreConfig::default(),
        };

        let error = validate_resume_config(&manifest, &config)
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("catalog"));
    }

    #[test]
    fn test_validate_resume_config_accepts_schema_selection_with_different_case_and_order() {
        let manifest = Manifest::new_for_export(
            "greptime".to_string(),
            vec!["public".to_string(), "analytics".to_string()],
            false,
            TimeRange::unbounded(),
            DataFormat::Parquet,
            None,
        )
        .unwrap();
        let config = ExportConfig {
            catalog: "greptime".to_string(),
            schemas: Some(vec![
                "ANALYTICS".to_string(),
                "PUBLIC".to_string(),
                "public".to_string(),
            ]),
            schema_only: false,
            format: DataFormat::Parquet,
            force: false,
            time_range: TimeRange::unbounded(),
            chunk_time_window: None,
            parallelism: 1,
            snapshot_uri: "file:///tmp/snapshot".to_string(),
            storage_config: ObjectStoreConfig::default(),
        };

        assert!(validate_resume_config(&manifest, &config).is_ok());
    }

    #[test]
    fn test_validate_resume_config_rejects_chunk_plan_mismatch() {
        let start = chrono::Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let end = chrono::Utc.with_ymd_and_hms(2025, 1, 1, 2, 0, 0).unwrap();
        let time_range = TimeRange::new(Some(start), Some(end));
        let manifest = Manifest::new_for_export(
            "greptime".to_string(),
            vec!["public".to_string()],
            false,
            time_range.clone(),
            DataFormat::Parquet,
            None,
        )
        .unwrap();
        let config = ExportConfig {
            catalog: "greptime".to_string(),
            schemas: None,
            schema_only: false,
            format: DataFormat::Parquet,
            force: false,
            time_range,
            chunk_time_window: Some(Duration::from_secs(3600)),
            parallelism: 1,
            snapshot_uri: "file:///tmp/snapshot".to_string(),
            storage_config: ObjectStoreConfig::default(),
        };

        let error = validate_resume_config(&manifest, &config)
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("chunk plan"));
    }

    #[test]
    fn test_validate_resume_config_rejects_format_mismatch() {
        let manifest = Manifest::new_for_export(
            "greptime".to_string(),
            vec!["public".to_string()],
            false,
            TimeRange::unbounded(),
            DataFormat::Parquet,
            None,
        )
        .unwrap();
        let config = ExportConfig {
            catalog: "greptime".to_string(),
            schemas: None,
            schema_only: false,
            format: DataFormat::Csv,
            force: false,
            time_range: TimeRange::unbounded(),
            chunk_time_window: None,
            parallelism: 1,
            snapshot_uri: "file:///tmp/snapshot".to_string(),
            storage_config: ObjectStoreConfig::default(),
        };

        let error = validate_resume_config(&manifest, &config)
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("format"));
    }

    #[test]
    fn test_validate_resume_config_rejects_time_range_mismatch() {
        let start = chrono::Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let end = chrono::Utc.with_ymd_and_hms(2025, 1, 1, 1, 0, 0).unwrap();
        let manifest = Manifest::new_for_export(
            "greptime".to_string(),
            vec!["public".to_string()],
            false,
            TimeRange::new(Some(start), Some(end)),
            DataFormat::Parquet,
            None,
        )
        .unwrap();
        let config = ExportConfig {
            catalog: "greptime".to_string(),
            schemas: None,
            schema_only: false,
            format: DataFormat::Parquet,
            force: false,
            time_range: TimeRange::new(Some(start), Some(start)),
            chunk_time_window: None,
            parallelism: 1,
            snapshot_uri: "file:///tmp/snapshot".to_string(),
            storage_config: ObjectStoreConfig::default(),
        };

        let error = validate_resume_config(&manifest, &config)
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("time_range"));
    }
}
