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
use std::io::{self, Write};
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
    ChunkTimeWindowRequiresBoundsSnafu, DatabaseSnafu, EmptyResultSnafu, IoSnafu,
    ManifestVersionMismatchSnafu, Result, ResumeConfigMismatchSnafu, SchemaOnlyArgsNotAllowedSnafu,
    SchemaOnlyModeMismatchSnafu, SnapshotVerifyFailedSnafu, UnexpectedValueTypeSnafu,
};
use crate::data::export_v2::extractor::SchemaExtractor;
use crate::data::export_v2::manifest::{
    ChunkMeta, ChunkStatus, DataFormat, MANIFEST_FILE, MANIFEST_VERSION, Manifest, TimeRange,
};
use crate::data::export_v2::schema::{DDL_DIR, SCHEMA_DIR, SCHEMAS_FILE};
use crate::data::path::{data_dir_for_schema_chunk, ddl_path_for_schema};
use crate::data::snapshot_storage::{
    OpenDalStorage, SnapshotStorage, validate_snapshot_uri, validate_uri,
};
use crate::data::sql::{escape_sql_identifier, escape_sql_literal};
use crate::database::{DatabaseClient, parse_proxy_opts};

/// Export V2 commands.
#[derive(Debug, Subcommand)]
pub enum ExportV2Command {
    /// Create a new snapshot.
    Create(ExportCreateCommand),
    /// List snapshots under a parent location.
    List(ExportListCommand),
    /// Verify snapshot integrity.
    Verify(ExportVerifyCommand),
    /// Delete a snapshot and all data under it.
    Delete(ExportDeleteCommand),
}

impl ExportV2Command {
    pub async fn build(&self) -> std::result::Result<Box<dyn Tool>, BoxedError> {
        match self {
            ExportV2Command::Create(cmd) => cmd.build().await,
            ExportV2Command::List(cmd) => cmd.build().await,
            ExportV2Command::Verify(cmd) => cmd.build().await,
            ExportV2Command::Delete(cmd) => cmd.build().await,
        }
    }
}

/// List snapshots under a parent location.
#[derive(Debug, Parser)]
pub struct ExportListCommand {
    /// Parent storage location whose direct subdirectories are snapshots.
    #[clap(long)]
    location: String,

    /// Object store configuration for remote storage backends.
    #[clap(flatten)]
    storage: ObjectStoreConfig,
}

impl ExportListCommand {
    pub async fn build(&self) -> std::result::Result<Box<dyn Tool>, BoxedError> {
        validate_uri(&self.location).map_err(BoxedError::new)?;
        let storage = OpenDalStorage::from_parent_uri(&self.location, &self.storage)
            .map_err(BoxedError::new)?;

        Ok(Box::new(ExportList {
            location: self.location.clone(),
            storage,
        }))
    }
}

/// Export list tool implementation.
pub struct ExportList {
    location: String,
    storage: OpenDalStorage,
}

#[async_trait]
impl Tool for ExportList {
    async fn do_work(&self) -> std::result::Result<(), BoxedError> {
        self.run().await.map_err(BoxedError::new)
    }
}

impl ExportList {
    async fn run(&self) -> Result<()> {
        let result = scan_snapshots(&self.storage).await?;

        println!("Scanning: {}", self.location);
        if result.snapshots.is_empty() {
            println!("No snapshots found.");
        } else {
            print_snapshot_list(&result.snapshots, result.unreadable.len());
        }
        print_unreadable_warnings(&result.unreadable);

        Ok(())
    }
}

/// Verify snapshot integrity.
#[derive(Debug, Parser)]
pub struct ExportVerifyCommand {
    /// Snapshot storage location (e.g., s3://bucket/path, file:///tmp/backup).
    #[clap(long)]
    snapshot: String,

    /// Object store configuration for remote storage backends.
    #[clap(flatten)]
    storage: ObjectStoreConfig,
}

impl ExportVerifyCommand {
    pub async fn build(&self) -> std::result::Result<Box<dyn Tool>, BoxedError> {
        validate_uri(&self.snapshot).map_err(BoxedError::new)?;
        let storage =
            OpenDalStorage::from_uri(&self.snapshot, &self.storage).map_err(BoxedError::new)?;

        Ok(Box::new(ExportVerify {
            snapshot: self.snapshot.clone(),
            storage,
        }))
    }
}

/// Export verify tool implementation.
pub struct ExportVerify {
    snapshot: String,
    storage: OpenDalStorage,
}

#[async_trait]
impl Tool for ExportVerify {
    async fn do_work(&self) -> std::result::Result<(), BoxedError> {
        self.run().await.map_err(BoxedError::new)
    }
}

impl ExportVerify {
    async fn run(&self) -> Result<()> {
        let report = verify_snapshot(&self.storage).await?;
        print_verify_report(&self.snapshot, &report);

        if report.has_problems() {
            return SnapshotVerifyFailedSnafu {
                errors: report.error_count(),
                warnings: report.warning_count(),
            }
            .fail();
        }

        Ok(())
    }
}

/// Delete a snapshot and all data under it.
#[derive(Debug, Parser)]
pub struct ExportDeleteCommand {
    /// Snapshot storage location (e.g., s3://bucket/path, file:///tmp/backup).
    #[clap(long)]
    snapshot: String,

    /// Skip interactive confirmation.
    #[clap(long = "no-confirm", alias = "yes")]
    skip_confirmation: bool,

    /// Object store configuration for remote storage backends.
    #[clap(flatten)]
    storage: ObjectStoreConfig,
}

impl ExportDeleteCommand {
    pub async fn build(&self) -> std::result::Result<Box<dyn Tool>, BoxedError> {
        validate_snapshot_uri(&self.snapshot).map_err(BoxedError::new)?;
        let storage =
            OpenDalStorage::from_uri(&self.snapshot, &self.storage).map_err(BoxedError::new)?;

        Ok(Box::new(ExportDelete {
            snapshot: self.snapshot.clone(),
            skip_confirmation: self.skip_confirmation,
            storage,
        }))
    }
}

/// Export delete tool implementation.
pub struct ExportDelete {
    snapshot: String,
    skip_confirmation: bool,
    storage: OpenDalStorage,
}

#[async_trait]
impl Tool for ExportDelete {
    async fn do_work(&self) -> std::result::Result<(), BoxedError> {
        self.run().await.map_err(BoxedError::new)
    }
}

impl ExportDelete {
    async fn run(&self) -> Result<()> {
        self.run_with_confirmation(confirm_delete).await
    }

    async fn run_with_confirmation<F>(&self, confirm: F) -> Result<()>
    where
        F: FnOnce(&str) -> Result<bool>,
    {
        let manifest = self.storage.read_manifest().await?;
        print_delete_summary(&self.snapshot, &manifest);

        if !self.skip_confirmation && !confirm(&self.snapshot)? {
            println!("Deletion cancelled.");
            return Ok(());
        }

        println!("Deleting snapshot...");
        self.storage.delete_snapshot().await?;
        println!("Snapshot deleted successfully.");

        Ok(())
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

    /// Number of export chunks to run concurrently on the client (1..=64).
    #[clap(long, default_value = "1", value_parser = parse_chunk_parallelism)]
    chunk_parallelism: usize,

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
        if self.schema_only {
            let mut invalid_args = Vec::new();
            if self.start_time.is_some() {
                invalid_args.push("--start-time");
            }
            if self.end_time.is_some() {
                invalid_args.push("--end-time");
            }
            if self.chunk_time_window.is_some() {
                invalid_args.push("--chunk-time-window");
            }
            if self.format != DataFormat::Parquet {
                invalid_args.push("--format");
            }
            if self.parallelism != 1 {
                invalid_args.push("--parallelism");
            }
            if self.chunk_parallelism != 1 {
                invalid_args.push("--chunk-parallelism");
            }
            if !invalid_args.is_empty() {
                return SchemaOnlyArgsNotAllowedSnafu {
                    args: invalid_args.join(", "),
                }
                .fail()
                .map_err(BoxedError::new);
            }
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
                chunk_parallelism: self.chunk_parallelism,
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
    chunk_parallelism: usize,
    snapshot_uri: String,
    storage_config: ObjectStoreConfig,
}

fn parse_chunk_parallelism(value: &str) -> std::result::Result<usize, String> {
    let parallelism = value
        .parse::<usize>()
        .map_err(|_| "chunk parallelism must be an integer between 1 and 64".to_string())?;
    if (1..=64).contains(&parallelism) {
        Ok(parallelism)
    } else {
        Err("chunk parallelism must be between 1 and 64".to_string())
    }
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
                    self.config.chunk_parallelism,
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
                self.config.chunk_parallelism,
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

#[derive(Debug)]
struct SnapshotListEntry {
    path: String,
    manifest: Manifest,
}

#[derive(Debug, Default)]
struct SnapshotScanResult {
    snapshots: Vec<SnapshotListEntry>,
    unreadable: Vec<String>,
}

async fn scan_snapshots(storage: &OpenDalStorage) -> Result<SnapshotScanResult> {
    let mut result = SnapshotScanResult::default();
    for dir in storage.list_direct_child_dirs().await? {
        let manifest_path = format!("{}/{}", dir.trim_matches('/'), MANIFEST_FILE);
        let Some(data) = storage.read_file_if_exists(&manifest_path).await? else {
            continue;
        };

        match serde_json::from_slice::<Manifest>(&data) {
            Ok(manifest) => result.snapshots.push(SnapshotListEntry {
                path: format!("{}/", dir.trim_matches('/')),
                manifest,
            }),
            Err(_) => result
                .unreadable
                .push(format!("{}/", dir.trim_matches('/'))),
        }
    }

    result
        .snapshots
        .sort_by_key(|entry| std::cmp::Reverse(entry.manifest.created_at));
    result.unreadable.sort();
    Ok(result)
}

fn print_snapshot_list(snapshots: &[SnapshotListEntry], unreadable_count: usize) {
    if unreadable_count == 0 {
        println!("Found {} snapshots:", snapshots.len());
    } else {
        println!(
            "Found {} snapshots ({} {} skipped: unreadable manifest):",
            snapshots.len(),
            unreadable_count,
            directory_word(unreadable_count)
        );
    }
    println!();
    println!(
        "  {:<24}  {:<36}  {:<19}  {:<9}  {:<7}  {:<6}  Status",
        "Path", "ID", "Created", "Catalog", "Schemas", "Chunks"
    );
    println!(
        "  {:<24}  {:<36}  {:<19}  {:<9}  {:<7}  {:<6}  {:<10}",
        "-".repeat(24),
        "-".repeat(36),
        "-".repeat(19),
        "-".repeat(9),
        "-".repeat(7),
        "-".repeat(6),
        "-".repeat(10)
    );
    for entry in snapshots {
        let manifest = &entry.manifest;
        println!(
            "  {:<24}  {:<36}  {:<19}  {:<9}  {:<7}  {:<6}  {}",
            entry.path,
            manifest.snapshot_id,
            manifest.created_at.format("%Y-%m-%d %H:%M:%S"),
            manifest.catalog,
            manifest.schemas.len(),
            format_list_chunks(manifest),
            snapshot_status(manifest)
        );
    }
}

fn print_unreadable_warnings(unreadable: &[String]) {
    if unreadable.is_empty() {
        return;
    }

    println!();
    println!(
        "Warning: {} {} had corrupt/unreadable manifest.json:",
        unreadable.len(),
        directory_word(unreadable.len())
    );
    for path in unreadable {
        println!("  - {}", path);
    }
}

fn directory_word(count: usize) -> &'static str {
    if count == 1 {
        "directory"
    } else {
        "directories"
    }
}

fn snapshot_status(manifest: &Manifest) -> &'static str {
    if manifest.schema_only {
        "schema-only"
    } else if manifest.is_complete() {
        "complete"
    } else {
        "incomplete"
    }
}

fn format_list_chunks(manifest: &Manifest) -> String {
    let total = manifest.chunks.len();
    if total == 0 {
        return "0".to_string();
    }

    format!(
        "{}/{}",
        manifest.completed_count() + manifest.skipped_count(),
        total
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VerifySeverity {
    Error,
    Warn,
}

impl VerifySeverity {
    fn as_str(self) -> &'static str {
        match self {
            VerifySeverity::Error => "ERROR",
            VerifySeverity::Warn => "WARN",
        }
    }
}

#[derive(Debug)]
struct VerifyProblem {
    severity: VerifySeverity,
    message: String,
}

#[derive(Debug, Default)]
struct VerifyChunkSummary {
    total: usize,
    completed: usize,
    skipped: usize,
    pending: usize,
    in_progress: usize,
    failed: usize,
}

#[derive(Debug)]
struct VerifyReport {
    manifest: Manifest,
    schema_index_exists: bool,
    ddl_file_count: usize,
    chunk_summary: VerifyChunkSummary,
    data_files_total: usize,
    data_files_verified: usize,
    problems: Vec<VerifyProblem>,
}

impl VerifyReport {
    fn error_count(&self) -> usize {
        self.problems
            .iter()
            .filter(|problem| problem.severity == VerifySeverity::Error)
            .count()
    }

    fn warning_count(&self) -> usize {
        self.problems
            .iter()
            .filter(|problem| problem.severity == VerifySeverity::Warn)
            .count()
    }

    fn has_problems(&self) -> bool {
        !self.problems.is_empty()
    }

    fn push_error(&mut self, message: impl Into<String>) {
        self.problems.push(VerifyProblem {
            severity: VerifySeverity::Error,
            message: message.into(),
        });
    }

    fn push_warn(&mut self, message: impl Into<String>) {
        self.problems.push(VerifyProblem {
            severity: VerifySeverity::Warn,
            message: message.into(),
        });
    }
}

async fn verify_snapshot(storage: &OpenDalStorage) -> Result<VerifyReport> {
    let manifest = storage.read_manifest().await?;
    let schema_index_path = format!("{}/{}", SCHEMA_DIR, SCHEMAS_FILE);
    let ddl_prefix = format!("{}/{}/", SCHEMA_DIR, DDL_DIR);
    let schema_index_exists = storage.file_exists(&schema_index_path).await?;
    let ddl_files: HashSet<_> = storage
        .list_files_recursive(&ddl_prefix)
        .await?
        .into_iter()
        .collect();
    let ddl_file_count = ddl_files
        .iter()
        .filter(|path| path.ends_with(".sql"))
        .count();

    let mut report = VerifyReport {
        manifest,
        schema_index_exists,
        ddl_file_count,
        chunk_summary: VerifyChunkSummary::default(),
        data_files_total: 0,
        data_files_verified: 0,
        problems: Vec::new(),
    };

    if report.manifest.version != MANIFEST_VERSION {
        report.push_error(format!(
            "Manifest version mismatch: expected {}, found {}",
            MANIFEST_VERSION, report.manifest.version
        ));
    }

    if !report.schema_index_exists {
        report.push_warn(format!("Missing schema index '{}'", schema_index_path));
    }

    for schema in &report.manifest.schemas {
        let ddl_path = ddl_path_for_schema(schema);
        if !ddl_files.contains(ddl_path.as_str()) {
            report.problems.push(VerifyProblem {
                severity: VerifySeverity::Error,
                message: format!("Schema '{}': missing DDL file '{}'", schema, ddl_path),
            });
        }
    }

    report.chunk_summary = summarize_chunks(&report.manifest);
    if report.manifest.schema_only {
        let chunk_count = report.manifest.chunks.len();
        if chunk_count > 0 {
            report.push_error(format!(
                "Schema-only snapshot should not contain data chunks (found {})",
                chunk_count
            ));
        }
        let mut first_data_file: Option<String> = None;
        storage
            .for_each_file_recursive("data/", |path| {
                let should_update = match &first_data_file {
                    Some(current) => path.as_str() < current.as_str(),
                    None => true,
                };
                if should_update {
                    first_data_file = Some(path);
                }
                Ok(())
            })
            .await?;
        if let Some(path) = first_data_file {
            report.push_error(format!(
                "Schema-only snapshot should not contain data files (found '{}')",
                path
            ));
        }
    } else if report.manifest.chunks.is_empty() {
        report.push_error("Full snapshot should contain at least one data chunk");
    } else {
        verify_chunks_and_data_files(storage, &mut report).await?;
    }

    Ok(report)
}

fn summarize_chunks(manifest: &Manifest) -> VerifyChunkSummary {
    VerifyChunkSummary {
        total: manifest.chunks.len(),
        completed: manifest.completed_count(),
        skipped: manifest.skipped_count(),
        pending: manifest.pending_count(),
        in_progress: manifest.in_progress_count(),
        failed: manifest.failed_count(),
    }
}

/// A data file declared by a completed chunk that is expected to exist in storage.
#[derive(Debug)]
struct ChunkFile {
    chunk_id: u32,
    path: String,
}

/// Expected snapshot contents derived purely from the manifest (no object-store IO).
///
/// Separating planning from scanning makes it obvious which problems come from
/// the manifest alone and which require comparing against actual storage.
#[derive(Debug, Default)]
struct VerifyPlan {
    /// Valid data files declared by completed chunks; each must exist in storage.
    files_to_check: Vec<ChunkFile>,
    /// All syntactically-safe data paths declared by any chunk, regardless of
    /// status. Used as the orphan-detection baseline so a listed-but-invalid
    /// file is not also reported as unexpected.
    claimed_data_files: HashSet<String>,
    /// Total data-file references in completed chunks (valid + invalid).
    data_files_total: usize,
    /// Problems detectable from the manifest alone.
    problems: Vec<VerifyProblem>,
}

/// Data-file scan result. Claimed files are kept only when they are relevant to
/// manifest verification; unexpected files are kept separately for reporting.
#[derive(Debug)]
struct VerifyDataScan {
    existing_claimed_data_files: HashSet<String>,
    unexpected_data_files: Vec<String>,
}

/// Result of reconciling the manifest plan against the storage scan.
#[derive(Debug, Default)]
struct VerifyOutcome {
    data_files_total: usize,
    data_files_verified: usize,
    problems: Vec<VerifyProblem>,
}

async fn verify_chunks_and_data_files(
    storage: &OpenDalStorage,
    report: &mut VerifyReport,
) -> Result<()> {
    let plan = build_verify_plan(&report.manifest);
    let scan = scan_data_files(storage, &plan).await?;
    let outcome = reconcile_plan_with_scan(plan, scan);

    report.data_files_total = outcome.data_files_total;
    report.data_files_verified = outcome.data_files_verified;
    report.problems.extend(outcome.problems);

    Ok(())
}

/// Builds the expected-state plan from the manifest. Pure; performs no IO.
fn build_verify_plan(manifest: &Manifest) -> VerifyPlan {
    let mut plan = VerifyPlan::default();
    let mut seen_chunk_ids = HashSet::new();

    for chunk in &manifest.chunks {
        if !seen_chunk_ids.insert(chunk.id) {
            plan.problems.push(VerifyProblem {
                severity: VerifySeverity::Error,
                message: format!("Chunk {}: duplicate chunk id", chunk.id),
            });
        }
        for file in &chunk.files {
            if let Some(path) = safe_manifest_data_file_path(file) {
                plan.claimed_data_files.insert(path.to_string());
            }
        }

        match chunk.status {
            ChunkStatus::Completed => {
                if chunk.files.is_empty() {
                    plan.problems.push(VerifyProblem {
                        severity: VerifySeverity::Error,
                        message: format!("Chunk {}: completed chunk has no data files", chunk.id),
                    });
                    continue;
                }
                let allowed_prefixes = manifest
                    .schemas
                    .iter()
                    .map(|schema| data_dir_for_schema_chunk(schema, chunk.id))
                    .collect::<Vec<_>>();
                for file in &chunk.files {
                    plan.data_files_total += 1;
                    match valid_manifest_data_file_path(file, &allowed_prefixes) {
                        Some(path) => plan.files_to_check.push(ChunkFile {
                            chunk_id: chunk.id,
                            path: path.to_string(),
                        }),
                        None => plan.problems.push(VerifyProblem {
                            severity: VerifySeverity::Error,
                            message: format!(
                                "Chunk {}: invalid data file path '{}'",
                                chunk.id, file
                            ),
                        }),
                    }
                }
            }
            ChunkStatus::Skipped => {
                if !chunk.files.is_empty() {
                    plan.problems.push(VerifyProblem {
                        severity: VerifySeverity::Error,
                        message: format!(
                            "Chunk {}: skipped chunk should not list data files",
                            chunk.id
                        ),
                    });
                }
            }
            ChunkStatus::Pending => {
                plan.problems.push(VerifyProblem {
                    severity: VerifySeverity::Error,
                    message: format!("Chunk {}: status is 'pending'", chunk.id),
                });
            }
            ChunkStatus::InProgress => {
                plan.problems.push(VerifyProblem {
                    severity: VerifySeverity::Error,
                    message: format!("Chunk {}: status is 'in_progress'", chunk.id),
                });
            }
            ChunkStatus::Failed => {
                let reason = chunk.error.as_deref().unwrap_or("unknown error");
                plan.problems.push(VerifyProblem {
                    severity: VerifySeverity::Error,
                    message: format!("Chunk {}: status is 'failed' (error: {})", chunk.id, reason),
                });
            }
        }
    }

    plan
}

/// Streams data files under `data/` and classifies each path against the plan.
async fn scan_data_files(storage: &OpenDalStorage, plan: &VerifyPlan) -> Result<VerifyDataScan> {
    let mut scan = VerifyDataScan {
        existing_claimed_data_files: HashSet::new(),
        unexpected_data_files: Vec::new(),
    };

    storage
        .for_each_file_recursive("data/", |path| {
            if plan.claimed_data_files.contains(&path) {
                scan.existing_claimed_data_files.insert(path);
            } else {
                scan.unexpected_data_files.push(path);
            }
            Ok(())
        })
        .await?;

    Ok(scan)
}

/// Reconciles the manifest plan against the storage scan. Pure; performs no IO.
///
/// Emits missing-file problems for expected files absent from storage and
/// unexpected-file problems for storage files no chunk claims. Unexpected files
/// are sorted by path so output is deterministic regardless of listing order.
fn reconcile_plan_with_scan(plan: VerifyPlan, mut scan: VerifyDataScan) -> VerifyOutcome {
    let mut problems = plan.problems;
    let mut data_files_verified = 0;

    for file in &plan.files_to_check {
        if scan.existing_claimed_data_files.contains(&file.path) {
            data_files_verified += 1;
        } else {
            problems.push(VerifyProblem {
                severity: VerifySeverity::Error,
                message: format!("Chunk {}: missing file '{}'", file.chunk_id, file.path),
            });
        }
    }

    scan.unexpected_data_files.sort();
    for path in scan.unexpected_data_files {
        problems.push(VerifyProblem {
            severity: VerifySeverity::Error,
            message: format!("Unexpected data file '{}' is not listed in manifest", path),
        });
    }

    VerifyOutcome {
        data_files_total: plan.data_files_total,
        data_files_verified,
        problems,
    }
}

fn valid_manifest_data_file_path<'a>(
    path: &'a str,
    allowed_prefixes: &[String],
) -> Option<&'a str> {
    let normalized = safe_manifest_data_file_path(path)?;

    if !allowed_prefixes
        .iter()
        .any(|prefix| normalized.starts_with(prefix))
    {
        return None;
    }

    Some(normalized)
}

fn safe_manifest_data_file_path(path: &str) -> Option<&str> {
    let normalized = path.trim_start_matches('/');
    if normalized.is_empty() || !normalized.starts_with("data/") {
        return None;
    }

    if normalized
        .split('/')
        .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return None;
    }

    Some(normalized)
}

fn print_verify_report(snapshot: &str, report: &VerifyReport) {
    println!("Verifying snapshot: {}", report.manifest.snapshot_id);
    println!("  Location:     {}", snapshot);
    if report.manifest.version == MANIFEST_VERSION {
        println!("  Manifest:     OK (version {})", report.manifest.version);
    } else {
        println!(
            "  Manifest:     ERROR (version {}, expected {})",
            report.manifest.version, MANIFEST_VERSION
        );
    }
    println!(
        "  Schema files: {}",
        if report.schema_index_exists {
            format!("OK ({})", SCHEMAS_FILE)
        } else {
            format!("WARN (missing {})", SCHEMAS_FILE)
        }
    );
    if report.ddl_file_count > 0 {
        println!("  DDL files:    {} file(s) found", report.ddl_file_count);
    } else {
        println!("  DDL files:    not present");
    }

    let chunks = &report.chunk_summary;
    println!(
        "  Chunks:       {} total ({} completed, {} skipped, {} pending, {} in_progress, {} failed)",
        chunks.total,
        chunks.completed,
        chunks.skipped,
        chunks.pending,
        chunks.in_progress,
        chunks.failed
    );

    if report.manifest.schema_only {
        println!("  Data files:   skipped (schema-only)");
    } else {
        println!(
            "  Data files:   {}/{} files verified",
            report.data_files_verified, report.data_files_total
        );
    }

    if report.problems.is_empty() {
        println!();
        println!("Snapshot is valid.");
        return;
    }

    println!();
    println!("Problems found:");
    for problem in &report.problems {
        println!("  [{}] {}", problem.severity.as_str(), problem.message);
    }
    println!();
    println!(
        "Snapshot has {} error(s), {} warning(s).",
        report.error_count(),
        report.warning_count()
    );
}

fn print_delete_summary(snapshot: &str, manifest: &Manifest) {
    println!("Snapshot: {}", manifest.snapshot_id);
    println!("  Location: {}", snapshot);
    println!(
        "  Created:  {} UTC",
        manifest.created_at.format("%Y-%m-%d %H:%M:%S")
    );
    println!("  Catalog:  {}", manifest.catalog);
    println!("  Schemas:  {}", manifest.schemas.join(", "));
    println!("  Chunks:   {}", format_delete_chunks(manifest));
}

fn format_delete_chunks(manifest: &Manifest) -> String {
    if manifest.schema_only {
        return "0 (schema-only)".to_string();
    }

    let summary = summarize_chunks(manifest);
    if manifest.is_complete() {
        format!("{} (all processed)", summary.total)
    } else {
        format!(
            "{} ({} completed, {} skipped, {} pending, {} in_progress, {} failed)",
            summary.total,
            summary.completed,
            summary.skipped,
            summary.pending,
            summary.in_progress,
            summary.failed
        )
    }
}

fn confirm_delete(snapshot: &str) -> Result<bool> {
    println!();
    println!(
        "Warning: this removes the entire snapshot directory/prefix, not only files listed in manifest."
    );
    println!("This will permanently delete all data under:");
    println!("  {}", display_snapshot_prefix(snapshot));
    print!("Type 'yes' to confirm deletion: ");
    io::stdout().flush().map_err(|error| {
        IoSnafu {
            operation: "flushing delete confirmation prompt",
            error,
        }
        .build()
    })?;

    let mut input = String::new();
    io::stdin().read_line(&mut input).map_err(|error| {
        IoSnafu {
            operation: "reading delete confirmation",
            error,
        }
        .build()
    })?;

    Ok(delete_confirmation_matches(&input))
}

fn delete_confirmation_matches(input: &str) -> bool {
    input.trim() == "yes"
}

fn display_snapshot_prefix(snapshot: &str) -> String {
    if snapshot.ends_with('/') {
        snapshot.to_string()
    } else {
        format!("{}/", snapshot)
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use clap::Parser;
    use tempfile::tempdir;
    use url::Url;

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

    #[tokio::test]
    async fn test_build_rejects_data_export_args_in_schema_only_mode() {
        let cmd = ExportCreateCommand::parse_from([
            "export-v2-create",
            "--addr",
            "127.0.0.1:4000",
            "--to",
            "file:///tmp/export-v2-test",
            "--schema-only",
            "--start-time",
            "2024-01-01T00:00:00Z",
            "--end-time",
            "2024-01-02T00:00:00Z",
            "--chunk-time-window",
            "1h",
            "--format",
            "csv",
            "--parallelism",
            "2",
            "--chunk-parallelism",
            "2",
        ]);

        let error = cmd.build().await.err().unwrap().to_string();

        assert!(error.contains("--schema-only cannot be used with data export arguments"));
        assert!(error.contains("--start-time"));
        assert!(error.contains("--end-time"));
        assert!(error.contains("--chunk-time-window"));
        assert!(error.contains("--format"));
        assert!(error.contains("--parallelism"));
        assert!(error.contains("--chunk-parallelism"));
    }

    #[test]
    fn test_chunk_parallelism_defaults_to_one() {
        let cmd = ExportCreateCommand::parse_from([
            "export-v2-create",
            "--addr",
            "127.0.0.1:4000",
            "--to",
            "file:///tmp/export-v2-test",
        ]);

        assert_eq!(1, cmd.chunk_parallelism);
    }

    #[test]
    fn test_chunk_parallelism_parses_valid_value() {
        let cmd = ExportCreateCommand::parse_from([
            "export-v2-create",
            "--addr",
            "127.0.0.1:4000",
            "--to",
            "file:///tmp/export-v2-test",
            "--chunk-parallelism",
            "64",
        ]);

        assert_eq!(64, cmd.chunk_parallelism);
    }

    #[test]
    fn test_chunk_parallelism_rejects_out_of_range_values() {
        assert!(
            ExportCreateCommand::try_parse_from([
                "export-v2-create",
                "--addr",
                "127.0.0.1:4000",
                "--to",
                "file:///tmp/export-v2-test",
                "--chunk-parallelism",
                "0",
            ])
            .is_err()
        );
        assert!(
            ExportCreateCommand::try_parse_from([
                "export-v2-create",
                "--addr",
                "127.0.0.1:4000",
                "--to",
                "file:///tmp/export-v2-test",
                "--chunk-parallelism",
                "65",
            ])
            .is_err()
        );
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
            chunk_parallelism: 1,
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
            chunk_parallelism: 1,
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
            chunk_parallelism: 1,
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
            chunk_parallelism: 1,
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
            chunk_parallelism: 1,
            snapshot_uri: "file:///tmp/snapshot".to_string(),
            storage_config: ObjectStoreConfig::default(),
        };

        let error = validate_resume_config(&manifest, &config)
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("time_range"));
    }

    #[tokio::test]
    async fn test_scan_snapshots_sorts_and_tracks_unreadable_manifests() {
        let dir = tempdir().unwrap();
        write_test_manifest(
            dir.path(),
            "older",
            test_manifest(
                chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
                false,
                true,
            ),
        );
        write_test_manifest(
            dir.path(),
            "newer",
            test_manifest(
                chrono::Utc.with_ymd_and_hms(2026, 2, 1, 0, 0, 0).unwrap(),
                false,
                true,
            ),
        );

        std::fs::create_dir_all(dir.path().join("empty-dir")).unwrap();
        std::fs::create_dir_all(dir.path().join("not-snapshot")).unwrap();
        std::fs::write(dir.path().join("not-snapshot").join("data.txt"), "x").unwrap();
        std::fs::create_dir_all(dir.path().join("broken")).unwrap();
        std::fs::write(dir.path().join("broken").join(MANIFEST_FILE), "{not-json").unwrap();

        let uri = Url::from_directory_path(dir.path()).unwrap().to_string();
        let storage = OpenDalStorage::from_file_uri(&uri).unwrap();
        let result = scan_snapshots(&storage).await.unwrap();

        assert_eq!(result.snapshots.len(), 2);
        assert_eq!(
            result.snapshots[0].manifest.created_at,
            chrono::Utc.with_ymd_and_hms(2026, 2, 1, 0, 0, 0).unwrap()
        );
        assert_eq!(
            result.snapshots[1].manifest.created_at,
            chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap()
        );
        assert_eq!(result.unreadable, vec!["broken/".to_string()]);
        assert_eq!(result.snapshots[0].path, "newer/");
        assert_eq!(result.snapshots[1].path, "older/");
    }

    #[test]
    fn test_snapshot_list_status_and_chunk_summary() {
        let schema_only = test_manifest(
            chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            true,
            true,
        );
        assert_eq!(snapshot_status(&schema_only), "schema-only");
        assert_eq!(format_list_chunks(&schema_only), "0");

        let complete = test_manifest(
            chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            false,
            true,
        );
        assert_eq!(snapshot_status(&complete), "complete");
        assert_eq!(format_list_chunks(&complete), "2/2");
        assert_eq!(format_delete_chunks(&complete), "2 (all processed)");

        let incomplete = test_manifest(
            chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            false,
            false,
        );
        assert_eq!(snapshot_status(&incomplete), "incomplete");
        assert_eq!(format_list_chunks(&incomplete), "1/2");
        assert_eq!(
            format_delete_chunks(&incomplete),
            "2 (1 completed, 0 skipped, 1 pending, 0 in_progress, 0 failed)"
        );
    }

    #[tokio::test]
    async fn test_delete_build_rejects_bucket_root_uri() {
        let cmd = ExportDeleteCommand::parse_from([
            "export-v2-delete",
            "--snapshot",
            "s3://bucket",
            "--no-confirm",
        ]);

        let error = cmd.build().await.err().unwrap().to_string();
        assert!(error.contains("non-empty path"));
    }

    #[test]
    fn test_delete_skip_confirmation_aliases() {
        let no_confirm = ExportDeleteCommand::parse_from([
            "export-v2-delete",
            "--snapshot",
            "s3://bucket/snapshot",
            "--no-confirm",
        ]);
        assert!(no_confirm.skip_confirmation);

        let yes = ExportDeleteCommand::parse_from([
            "export-v2-delete",
            "--snapshot",
            "s3://bucket/snapshot",
            "--yes",
        ]);
        assert!(yes.skip_confirmation);
    }

    #[tokio::test]
    async fn test_delete_snapshot_with_no_confirm_removes_snapshot_contents() {
        let parent = tempdir().unwrap();
        let snapshot = parent.path().join("snapshot");
        let sibling = parent.path().join("sibling");
        std::fs::create_dir_all(&snapshot).unwrap();
        std::fs::create_dir_all(&sibling).unwrap();
        std::fs::write(sibling.join("keep.txt"), b"keep").unwrap();
        write_root_manifest(
            &snapshot,
            test_manifest(
                chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
                true,
                true,
            ),
        );
        write_snapshot_file(&snapshot, "schema/schemas.json", b"[]");

        let uri = Url::from_directory_path(&snapshot).unwrap().to_string();
        let delete = ExportDelete {
            snapshot: uri,
            skip_confirmation: true,
            storage: file_storage_for_dir(&snapshot),
        };

        delete
            .run_with_confirmation(|_| unreachable!())
            .await
            .unwrap();

        assert!(!snapshot.join(MANIFEST_FILE).exists());
        assert!(!snapshot.join("schema/schemas.json").exists());
        assert!(sibling.join("keep.txt").exists());
    }

    #[tokio::test]
    async fn test_delete_snapshot_requires_manifest() {
        let dir = tempdir().unwrap();
        let uri = Url::from_directory_path(dir.path()).unwrap().to_string();
        let delete = ExportDelete {
            snapshot: uri,
            skip_confirmation: true,
            storage: file_storage_for_dir(dir.path()),
        };

        let error = delete
            .run_with_confirmation(|_| unreachable!())
            .await
            .err()
            .unwrap()
            .to_string();

        assert!(error.contains("Snapshot not found"));
        assert!(dir.path().exists());
    }

    #[tokio::test]
    async fn test_delete_snapshot_cancels_without_exact_confirmation() {
        let dir = tempdir().unwrap();
        write_root_manifest(
            dir.path(),
            test_manifest(
                chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
                true,
                true,
            ),
        );
        write_snapshot_file(dir.path(), "schema/schemas.json", b"[]");
        let uri = Url::from_directory_path(dir.path()).unwrap().to_string();
        let delete = ExportDelete {
            snapshot: uri.clone(),
            skip_confirmation: false,
            storage: file_storage_for_dir(dir.path()),
        };

        delete
            .run_with_confirmation(|snapshot| {
                assert_eq!(snapshot, uri);
                Ok(false)
            })
            .await
            .unwrap();

        assert!(dir.path().join(MANIFEST_FILE).exists());
        assert!(dir.path().join("schema/schemas.json").exists());
    }

    #[test]
    fn test_delete_confirmation_requires_exact_yes() {
        assert!(delete_confirmation_matches("yes"));
        assert!(delete_confirmation_matches(" yes\n"));
        assert!(!delete_confirmation_matches("YES"));
        assert!(!delete_confirmation_matches("y"));
        assert!(!delete_confirmation_matches("yes please"));
    }

    #[test]
    fn test_display_snapshot_prefix_adds_trailing_slash() {
        assert_eq!(
            display_snapshot_prefix("s3://bucket/snapshot"),
            "s3://bucket/snapshot/"
        );
        assert_eq!(
            display_snapshot_prefix("s3://bucket/snapshot/"),
            "s3://bucket/snapshot/"
        );
    }

    #[tokio::test]
    async fn test_verify_snapshot_accepts_valid_full_snapshot() {
        let dir = tempdir().unwrap();
        let manifest = test_manifest(
            chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            false,
            true,
        );
        write_root_manifest(dir.path(), manifest);
        write_snapshot_file(dir.path(), "schema/schemas.json", b"[]");
        write_default_ddl_files(dir.path());
        write_snapshot_file(dir.path(), "data/public/1/file.parquet", b"data");

        let storage = file_storage_for_dir(dir.path());
        let report = verify_snapshot(&storage).await.unwrap();

        assert_eq!(report.error_count(), 0);
        assert_eq!(report.warning_count(), 0);
        assert_eq!(report.data_files_total, 1);
        assert_eq!(report.data_files_verified, 1);
    }

    #[tokio::test]
    async fn test_verify_snapshot_reports_missing_data_file_and_failed_chunk() {
        let dir = tempdir().unwrap();
        let mut manifest = test_manifest(
            chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            false,
            true,
        );
        manifest.chunks[1].mark_failed("copy failed".to_string());
        write_root_manifest(dir.path(), manifest);
        write_snapshot_file(dir.path(), "schema/schemas.json", b"[]");
        write_default_ddl_files(dir.path());

        let storage = file_storage_for_dir(dir.path());
        let report = verify_snapshot(&storage).await.unwrap();

        assert_eq!(report.error_count(), 2);
        assert!(
            report
                .problems
                .iter()
                .any(|problem| problem.message.contains("missing file"))
        );
        assert!(
            report
                .problems
                .iter()
                .any(|problem| problem.message.contains("status is 'failed'"))
        );
    }

    #[tokio::test]
    async fn test_verify_snapshot_reports_missing_schema_index_as_warning() {
        let dir = tempdir().unwrap();
        let manifest = test_manifest(
            chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            false,
            true,
        );
        write_root_manifest(dir.path(), manifest);
        write_default_ddl_files(dir.path());
        write_snapshot_file(dir.path(), "data/public/1/file.parquet", b"data");

        let storage = file_storage_for_dir(dir.path());
        let report = verify_snapshot(&storage).await.unwrap();

        assert_eq!(report.error_count(), 0);
        assert_eq!(report.warning_count(), 1);
        assert!(
            report
                .problems
                .iter()
                .any(|problem| problem.message.contains("Missing schema index"))
        );
    }

    #[tokio::test]
    async fn test_verify_snapshot_rejects_schema_only_snapshot_with_chunks() {
        let dir = tempdir().unwrap();
        let mut manifest = test_manifest(
            chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            true,
            true,
        );
        let mut chunk = ChunkMeta::new(1, TimeRange::unbounded());
        chunk.mark_completed(vec!["data/public/1/file.parquet".to_string()], None);
        manifest.chunks.push(chunk);
        write_root_manifest(dir.path(), manifest);
        write_snapshot_file(dir.path(), "schema/schemas.json", b"[]");
        write_default_ddl_files(dir.path());

        let storage = file_storage_for_dir(dir.path());
        let report = verify_snapshot(&storage).await.unwrap();

        assert_eq!(report.error_count(), 1);
        assert_eq!(report.data_files_total, 0);
        assert!(
            report
                .problems
                .iter()
                .any(|problem| problem.message.contains("should not contain data chunks"))
        );
    }

    #[tokio::test]
    async fn test_verify_snapshot_rejects_schema_only_snapshot_with_data_files() {
        let dir = tempdir().unwrap();
        let manifest = test_manifest(
            chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            true,
            true,
        );
        write_root_manifest(dir.path(), manifest);
        write_snapshot_file(dir.path(), "schema/schemas.json", b"[]");
        write_default_ddl_files(dir.path());
        write_snapshot_file(dir.path(), "data/public/1/file.parquet", b"data");

        let storage = file_storage_for_dir(dir.path());
        let report = verify_snapshot(&storage).await.unwrap();

        assert_eq!(report.error_count(), 1);
        assert_eq!(report.data_files_total, 0);
        assert!(
            report
                .problems
                .iter()
                .any(|problem| problem.message.contains("should not contain data files"))
        );
    }

    #[tokio::test]
    async fn test_verify_snapshot_rejects_full_snapshot_without_chunks() {
        let dir = tempdir().unwrap();
        let mut manifest = test_manifest(
            chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            false,
            true,
        );
        manifest.chunks.clear();
        write_root_manifest(dir.path(), manifest);
        write_snapshot_file(dir.path(), "schema/schemas.json", b"[]");
        write_default_ddl_files(dir.path());

        let storage = file_storage_for_dir(dir.path());
        let report = verify_snapshot(&storage).await.unwrap();

        assert_eq!(report.error_count(), 1);
        assert_eq!(report.data_files_total, 0);
        assert!(
            report
                .problems
                .iter()
                .any(|problem| problem.message.contains("at least one data chunk"))
        );
    }

    #[tokio::test]
    async fn test_verify_snapshot_rejects_skipped_chunk_data_files() {
        let dir = tempdir().unwrap();
        let manifest = test_manifest(
            chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            false,
            true,
        );
        write_root_manifest(dir.path(), manifest);
        write_snapshot_file(dir.path(), "schema/schemas.json", b"[]");
        write_default_ddl_files(dir.path());
        write_snapshot_file(dir.path(), "data/public/1/file.parquet", b"data");
        write_snapshot_file(dir.path(), "data/public/2/file.parquet", b"data");

        let storage = file_storage_for_dir(dir.path());
        let report = verify_snapshot(&storage).await.unwrap();

        assert_eq!(report.error_count(), 1);
        assert!(
            report
                .problems
                .iter()
                .any(|problem| { problem.message.contains("Unexpected data file") })
        );
    }

    #[tokio::test]
    async fn test_verify_snapshot_rejects_duplicate_chunk_ids() {
        let dir = tempdir().unwrap();
        let mut manifest = test_manifest(
            chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            false,
            true,
        );
        let mut duplicate = ChunkMeta::new(1, TimeRange::unbounded());
        duplicate.mark_completed(vec!["data/public/1/file.parquet".to_string()], None);
        manifest.chunks.push(duplicate);
        write_root_manifest(dir.path(), manifest);
        write_snapshot_file(dir.path(), "schema/schemas.json", b"[]");
        write_default_ddl_files(dir.path());
        write_snapshot_file(dir.path(), "data/public/1/file.parquet", b"data");

        let storage = file_storage_for_dir(dir.path());
        let report = verify_snapshot(&storage).await.unwrap();

        assert_eq!(report.error_count(), 1);
        assert!(
            report
                .problems
                .iter()
                .any(|problem| problem.message.contains("duplicate chunk id"))
        );
    }

    #[tokio::test]
    async fn test_verify_snapshot_requires_all_schema_ddl() {
        let dir = tempdir().unwrap();
        let manifest = test_manifest(
            chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            true,
            true,
        );
        write_root_manifest(dir.path(), manifest);
        write_snapshot_file(dir.path(), "schema/schemas.json", b"[]");
        write_snapshot_file(
            dir.path(),
            "schema/ddl/public.sql",
            b"CREATE DATABASE public;",
        );

        let storage = file_storage_for_dir(dir.path());
        let report = verify_snapshot(&storage).await.unwrap();

        assert_eq!(report.error_count(), 1);
        assert!(
            report
                .problems
                .iter()
                .any(|problem| problem.message.contains("analytics"))
        );
    }

    #[tokio::test]
    async fn test_verify_snapshot_reports_missing_ddl_dir() {
        let dir = tempdir().unwrap();
        let manifest = test_manifest(
            chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            false,
            true,
        );
        write_root_manifest(dir.path(), manifest);
        write_snapshot_file(dir.path(), "schema/schemas.json", b"[]");
        write_snapshot_file(dir.path(), "data/public/1/file.parquet", b"data");

        let storage = file_storage_for_dir(dir.path());
        let report = verify_snapshot(&storage).await.unwrap();

        assert_eq!(report.error_count(), 2);
        assert!(
            report
                .problems
                .iter()
                .any(|problem| problem.message.contains("schema/ddl/public.sql"))
        );
        assert!(
            report
                .problems
                .iter()
                .any(|problem| problem.message.contains("schema/ddl/analytics.sql"))
        );
    }

    #[tokio::test]
    async fn test_verify_snapshot_reports_manifest_version_mismatch() {
        let dir = tempdir().unwrap();
        let mut manifest = test_manifest(
            chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            false,
            true,
        );
        manifest.version = MANIFEST_VERSION + 1;
        write_root_manifest(dir.path(), manifest);
        write_snapshot_file(dir.path(), "schema/schemas.json", b"[]");
        write_default_ddl_files(dir.path());
        write_snapshot_file(dir.path(), "data/public/1/file.parquet", b"data");

        let storage = file_storage_for_dir(dir.path());
        let report = verify_snapshot(&storage).await.unwrap();

        assert_eq!(report.error_count(), 1);
        assert!(
            report
                .problems
                .iter()
                .any(|problem| problem.message.contains("Manifest version mismatch"))
        );
    }

    #[tokio::test]
    async fn test_verify_snapshot_rejects_invalid_data_file_paths() {
        let dir = tempdir().unwrap();
        let mut manifest = test_manifest(
            chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            false,
            true,
        );
        manifest.chunks[0].files = vec!["data/public/1/../file.parquet".to_string()];
        write_root_manifest(dir.path(), manifest);
        write_snapshot_file(dir.path(), "schema/schemas.json", b"[]");
        write_default_ddl_files(dir.path());

        let storage = file_storage_for_dir(dir.path());
        let report = verify_snapshot(&storage).await.unwrap();

        assert_eq!(report.error_count(), 1);
        assert!(
            report
                .problems
                .iter()
                .any(|problem| problem.message.contains("invalid data file path"))
        );
        assert_eq!(report.data_files_verified, 0);
    }

    #[tokio::test]
    async fn test_verify_snapshot_accepts_leading_slash_manifest_data_paths() {
        let dir = tempdir().unwrap();
        let mut manifest = test_manifest(
            chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            false,
            true,
        );
        manifest.chunks[0].files = vec!["/data/public/1/file.parquet".to_string()];
        write_root_manifest(dir.path(), manifest);
        write_snapshot_file(dir.path(), "schema/schemas.json", b"[]");
        write_default_ddl_files(dir.path());
        write_snapshot_file(dir.path(), "data/public/1/file.parquet", b"data");

        let storage = file_storage_for_dir(dir.path());
        let report = verify_snapshot(&storage).await.unwrap();

        assert_eq!(report.error_count(), 0);
        assert_eq!(report.data_files_verified, 1);
    }

    #[tokio::test]
    async fn test_verify_snapshot_rejects_unlisted_files_under_completed_chunk_prefix() {
        let dir = tempdir().unwrap();
        let manifest = test_manifest(
            chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            false,
            true,
        );
        write_root_manifest(dir.path(), manifest);
        write_snapshot_file(dir.path(), "schema/schemas.json", b"[]");
        write_default_ddl_files(dir.path());
        write_snapshot_file(dir.path(), "data/public/1/file.parquet", b"data");
        write_snapshot_file(dir.path(), "data/public/1/extra.parquet", b"data");

        let storage = file_storage_for_dir(dir.path());
        let report = verify_snapshot(&storage).await.unwrap();

        assert_eq!(report.error_count(), 1);
        assert!(
            report
                .problems
                .iter()
                .any(|problem| problem.message.contains("Unexpected data file"))
        );
        assert_eq!(report.data_files_verified, 1);
    }

    #[tokio::test]
    async fn test_verify_snapshot_rejects_orphan_data_files_outside_known_chunk_prefixes() {
        let dir = tempdir().unwrap();
        let manifest = test_manifest(
            chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            false,
            true,
        );
        write_root_manifest(dir.path(), manifest);
        write_snapshot_file(dir.path(), "schema/schemas.json", b"[]");
        write_default_ddl_files(dir.path());
        write_snapshot_file(dir.path(), "data/public/1/file.parquet", b"data");
        write_snapshot_file(dir.path(), "data/public/99/file.parquet", b"data");

        let storage = file_storage_for_dir(dir.path());
        let report = verify_snapshot(&storage).await.unwrap();

        assert_eq!(report.error_count(), 1);
        assert!(
            report
                .problems
                .iter()
                .any(|problem| problem.message.contains("Unexpected data file"))
        );
        assert_eq!(report.data_files_verified, 1);
    }

    #[tokio::test]
    async fn test_verify_snapshot_rejects_data_files_under_wrong_chunk_or_schema() {
        let dir = tempdir().unwrap();
        let mut manifest = test_manifest(
            chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            false,
            true,
        );
        manifest.chunks[0].files = vec![
            "data/public/99/file.parquet".to_string(),
            "data/metrics/1/file.parquet".to_string(),
        ];
        write_root_manifest(dir.path(), manifest);
        write_snapshot_file(dir.path(), "schema/schemas.json", b"[]");
        write_default_ddl_files(dir.path());
        write_snapshot_file(dir.path(), "data/public/99/file.parquet", b"data");
        write_snapshot_file(dir.path(), "data/metrics/1/file.parquet", b"data");

        let storage = file_storage_for_dir(dir.path());
        let report = verify_snapshot(&storage).await.unwrap();

        assert_eq!(report.error_count(), 2);
        assert_eq!(report.data_files_verified, 0);
        assert!(
            report
                .problems
                .iter()
                .all(|problem| problem.message.contains("invalid data file path"))
        );
    }

    #[test]
    fn test_build_verify_plan_classifies_chunks_without_io() {
        let mut manifest = test_manifest(
            chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            false,
            true,
        );
        // test_manifest(complete) gives: chunk 1 completed (1 file), chunk 2 skipped.
        let mut failed = ChunkMeta::new(3, TimeRange::unbounded());
        failed.mark_failed("boom".to_string());
        manifest.chunks.push(failed);
        manifest
            .chunks
            .push(ChunkMeta::new(4, TimeRange::unbounded()));

        let plan = build_verify_plan(&manifest);

        assert_eq!(plan.files_to_check.len(), 1);
        assert_eq!(plan.files_to_check[0].chunk_id, 1);
        assert_eq!(plan.files_to_check[0].path, "data/public/1/file.parquet");
        assert_eq!(plan.data_files_total, 1);
        assert!(
            plan.claimed_data_files
                .contains("data/public/1/file.parquet")
        );
        assert_eq!(plan.problems.len(), 2);
        assert!(
            plan.problems
                .iter()
                .any(|problem| problem.message.contains("status is 'failed'"))
        );
        assert!(
            plan.problems
                .iter()
                .any(|problem| problem.message.contains("status is 'pending'"))
        );
    }

    #[tokio::test]
    async fn test_verify_snapshot_produces_deterministic_problem_output() {
        let dir = tempdir().unwrap();
        let manifest = test_manifest(
            chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            false,
            true,
        );
        write_root_manifest(dir.path(), manifest);
        write_snapshot_file(dir.path(), "schema/schemas.json", b"[]");
        write_default_ddl_files(dir.path());
        write_snapshot_file(dir.path(), "data/public/1/file.parquet", b"data");
        // Many orphan files under a known chunk prefix to stress ordering.
        for i in 0..50 {
            write_snapshot_file(
                dir.path(),
                &format!("data/public/1/orphan_{:02}.parquet", i),
                b"x",
            );
        }

        let storage = file_storage_for_dir(dir.path());
        let messages = |report: &VerifyReport| {
            report
                .problems
                .iter()
                .map(|problem| problem.message.clone())
                .collect::<Vec<_>>()
        };
        let first = messages(&verify_snapshot(&storage).await.unwrap());
        let second = messages(&verify_snapshot(&storage).await.unwrap());

        // Output is identical across runs despite HashSet-based scanning.
        assert_eq!(first, second);

        let orphans = first
            .iter()
            .filter(|message| message.contains("Unexpected data file"))
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(orphans.len(), 50);
        let mut sorted = orphans.clone();
        sorted.sort();
        assert_eq!(orphans, sorted);
    }

    fn write_test_manifest(root: &std::path::Path, dir: &str, manifest: Manifest) {
        let snapshot_dir = root.join(dir);
        std::fs::create_dir_all(&snapshot_dir).unwrap();
        std::fs::write(
            snapshot_dir.join(MANIFEST_FILE),
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();
    }

    fn write_root_manifest(root: &std::path::Path, manifest: Manifest) {
        std::fs::write(
            root.join(MANIFEST_FILE),
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();
    }

    fn write_snapshot_file(root: &std::path::Path, relative_path: &str, content: &[u8]) {
        let mut path = root.to_path_buf();
        for segment in relative_path.split('/') {
            path.push(segment);
        }
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
    }

    fn write_default_ddl_files(root: &std::path::Path) {
        write_snapshot_file(root, "schema/ddl/public.sql", b"CREATE DATABASE public;");
        write_snapshot_file(
            root,
            "schema/ddl/analytics.sql",
            b"CREATE DATABASE analytics;",
        );
    }

    fn file_storage_for_dir(root: &std::path::Path) -> OpenDalStorage {
        let uri = Url::from_directory_path(root).unwrap().to_string();
        OpenDalStorage::from_file_uri(&uri).unwrap()
    }

    fn test_manifest(
        created_at: chrono::DateTime<chrono::Utc>,
        schema_only: bool,
        complete: bool,
    ) -> Manifest {
        let mut manifest = Manifest::new_for_export(
            "greptime".to_string(),
            vec!["public".to_string(), "analytics".to_string()],
            schema_only,
            TimeRange::unbounded(),
            DataFormat::Parquet,
            None,
        )
        .unwrap();
        manifest.created_at = created_at;
        manifest.updated_at = created_at;

        if !schema_only {
            manifest.chunks.clear();
            let mut first = ChunkMeta::new(1, TimeRange::unbounded());
            first.mark_completed(vec!["data/public/1/file.parquet".to_string()], None);
            manifest.chunks.push(first);

            if complete {
                manifest
                    .chunks
                    .push(ChunkMeta::skipped(2, TimeRange::unbounded()));
            } else {
                manifest
                    .chunks
                    .push(ChunkMeta::new(2, TimeRange::unbounded()));
            }
        }

        manifest
    }
}
