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

//! Import V2 CLI command.

use std::time::Duration;

use async_trait::async_trait;
use clap::Parser;
use common_error::ext::BoxedError;
use common_telemetry::{error, info};
use snafu::{OptionExt, ResultExt};

use super::ddl_generator::DdlGenerator;
use super::error::{
    ExportSnafu, IncompleteSnapshotSnafu, ManifestVersionMismatchSnafu, Result,
    SchemaNotInSnapshotSnafu,
};
use super::executor::DdlExecutor;
use crate::Tool;
use crate::common::ObjectStoreConfig;
use crate::data::export_v2::data::{build_copy_source, execute_copy_database_from};
use crate::data::export_v2::manifest::{ChunkMeta, ChunkStatus, MANIFEST_VERSION, Manifest};
use crate::data::snapshot_storage::{OpenDalStorage, SnapshotStorage, validate_uri};
use crate::database::{DatabaseClient, parse_proxy_opts};

/// Import from a snapshot.
#[derive(Debug, Parser)]
pub struct ImportV2Command {
    /// Server address to connect (e.g., 127.0.0.1:4000).
    #[clap(long)]
    addr: String,

    /// Source snapshot location (e.g., s3://bucket/path, file:///tmp/backup).
    #[clap(long)]
    from: String,

    /// Target catalog name.
    #[clap(long, default_value = "greptime")]
    catalog: String,

    /// Schema list to import (default: all in snapshot).
    /// Can be specified multiple times or comma-separated.
    #[clap(long, value_delimiter = ',')]
    schemas: Vec<String>,

    /// Verify without importing (dry-run).
    #[clap(long)]
    dry_run: bool,

    /// Use DDL files (schema/ddl/<schema>.sql) instead of JSON schema.
    #[clap(long)]
    use_ddl: bool,

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

impl ImportV2Command {
    pub async fn build(&self) -> std::result::Result<Box<dyn Tool>, BoxedError> {
        // Validate URI format
        validate_uri(&self.from)
            .context(ExportSnafu)
            .map_err(BoxedError::new)?;

        // Parse schemas (empty vec means all schemas)
        let schemas = if self.schemas.is_empty() {
            None
        } else {
            Some(self.schemas.clone())
        };

        // Build storage
        let storage = OpenDalStorage::from_uri(&self.from, &self.storage)
            .context(ExportSnafu)
            .map_err(BoxedError::new)?;

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

        Ok(Box::new(Import {
            catalog: self.catalog.clone(),
            schemas,
            dry_run: self.dry_run,
            use_ddl: self.use_ddl,
            _parallelism: self.parallelism,
            snapshot_uri: self.from.clone(),
            storage_config: self.storage.clone(),
            storage: Box::new(storage),
            database_client,
        }))
    }
}

/// Import tool implementation.
pub struct Import {
    catalog: String,
    schemas: Option<Vec<String>>,
    dry_run: bool,
    use_ddl: bool,
    _parallelism: usize,
    snapshot_uri: String,
    storage_config: ObjectStoreConfig,
    storage: Box<dyn SnapshotStorage>,
    database_client: DatabaseClient,
}

#[async_trait]
impl Tool for Import {
    async fn do_work(&self) -> std::result::Result<(), BoxedError> {
        self.run().await.map_err(BoxedError::new)
    }
}

impl Import {
    async fn run(&self) -> Result<()> {
        // 1. Read manifest
        let manifest = self.storage.read_manifest().await.context(ExportSnafu)?;

        info!(
            "Loading snapshot: {} (version: {}, schema_only: {})",
            manifest.snapshot_id, manifest.version, manifest.schema_only
        );

        // Check version compatibility
        if manifest.version != MANIFEST_VERSION {
            return ManifestVersionMismatchSnafu {
                expected: MANIFEST_VERSION,
                found: manifest.version,
            }
            .fail();
        }

        // 2. Read schema snapshot
        let schema_snapshot = self.storage.read_schema().await.context(ExportSnafu)?;

        info!(
            "Snapshot contains {} schemas, {} tables, {} views",
            schema_snapshot.schemas.len(),
            schema_snapshot.tables.len(),
            schema_snapshot.views.len()
        );

        // 3. Determine schemas to import
        let schemas_to_import = match &self.schemas {
            Some(filter) => {
                // Validate that all specified schemas exist in snapshot
                let mut schemas = Vec::with_capacity(filter.len());
                for schema in filter {
                    let matched_schema = manifest
                        .schemas
                        .iter()
                        .find(|s| s.eq_ignore_ascii_case(schema))
                        .cloned()
                        .context(SchemaNotInSnapshotSnafu {
                            schema: schema.clone(),
                        })?;
                    schemas.push(matched_schema);
                }
                schemas
            }
            None => manifest.schemas.clone(),
        };

        info!("Importing schemas: {:?}", schemas_to_import);

        // 4. Generate DDL statements
        let ddl_statements = if self.use_ddl {
            self.read_ddl_statements(&schemas_to_import).await?
        } else {
            let generator = DdlGenerator::new(&schema_snapshot);
            generator.generate(&schemas_to_import)?
        };

        info!("Generated {} DDL statements", ddl_statements.len());

        // 5. Dry-run mode: print DDL and exit
        if self.dry_run {
            info!("Dry-run mode - DDL statements to execute:");
            println!();
            for (i, stmt) in ddl_statements.iter().enumerate() {
                println!("-- Statement {}", i + 1);
                println!("{};", stmt);
                println!();
            }

            if !manifest.schema_only && !manifest.chunks.is_empty() {
                println!("-- Data import plan:");
                for chunk in &manifest.chunks {
                    println!("-- Chunk {}: {:?}", chunk.id, chunk.status);
                    for schema in &schemas_to_import {
                        if chunk_has_schema_files(chunk, schema) {
                            println!("--   {} -> COPY DATABASE FROM", schema);
                        }
                    }
                }
                println!();
            }
            return Ok(());
        }

        // 6. Execute DDL
        let executor = DdlExecutor::new(&self.database_client);
        let result = executor.execute(&ddl_statements).await?;

        info!(
            "Import completed: {} succeeded, {} failed out of {} total",
            result.succeeded, result.failed, result.total
        );

        if result.has_failures() {
            info!(
                "Warning: {} statements failed. Check logs for details.",
                result.failed
            );
        }

        // 7. Data import for non-schema-only snapshots (M3)
        if !manifest.schema_only && !manifest.chunks.is_empty() {
            self.import_data(&manifest, &schemas_to_import).await?;
        }

        Ok(())
    }

    async fn import_data(&self, manifest: &Manifest, schemas: &[String]) -> Result<()> {
        validate_chunk_statuses(&manifest.chunks)?;

        let total_chunks = manifest
            .chunks
            .iter()
            .filter(|chunk| chunk.status == ChunkStatus::Completed)
            .count();
        info!(
            "Importing data: {} chunks, {} schemas",
            total_chunks,
            schemas.len()
        );

        for (idx, chunk) in manifest.chunks.iter().enumerate() {
            if chunk.status == ChunkStatus::Skipped {
                info!(
                    "[{}/{}] Chunk {}: skipped (no data)",
                    idx + 1,
                    manifest.chunks.len(),
                    chunk.id
                );
                continue;
            }
            info!(
                "[{}/{}] Chunk {} ({:?} ~ {:?})",
                idx + 1,
                manifest.chunks.len(),
                chunk.id,
                chunk.time_range.start,
                chunk.time_range.end
            );
            for schema in schemas {
                if !chunk_has_schema_files(chunk, schema) {
                    info!("  {}: no data, skipped", schema);
                    continue;
                }
                info!("  {}: importing...", schema);
                let source =
                    build_copy_source(&self.snapshot_uri, &self.storage_config, schema, chunk.id)
                        .context(ExportSnafu)?;
                execute_copy_database_from(
                    &self.database_client,
                    &self.catalog,
                    schema,
                    &source,
                    manifest.format,
                )
                .await
                .context(ExportSnafu)?;
                info!("  {}: done", schema);
            }
        }

        Ok(())
    }

    async fn read_ddl_statements(&self, schemas: &[String]) -> Result<Vec<String>> {
        let mut statements = Vec::new();
        for schema in schemas {
            let path = ddl_path_for_schema(schema);
            let content = self.storage.read_text(&path).await.context(ExportSnafu)?;
            statements.extend(parse_ddl_statements(&content));
        }

        Ok(statements)
    }
}

fn ddl_path_for_schema(schema: &str) -> String {
    format!("schema/ddl/{}.sql", schema)
}

fn parse_ddl_statements(content: &str) -> Vec<String> {
    let mut cleaned = String::new();
    for line in content.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("--") || trimmed.is_empty() {
            continue;
        }
        cleaned.push_str(line);
        cleaned.push('\n');
    }

    cleaned
        .split(';')
        .map(|stmt| stmt.trim())
        .filter(|stmt| !stmt.is_empty())
        .map(|stmt| stmt.to_string())
        .collect()
}

fn validate_chunk_statuses(chunks: &[ChunkMeta]) -> Result<()> {
    let invalid_chunks: Vec<_> = chunks
        .iter()
        .filter(|chunk| !matches!(chunk.status, ChunkStatus::Completed | ChunkStatus::Skipped))
        .collect();
    if invalid_chunks.is_empty() {
        return Ok(());
    }

    for chunk in &invalid_chunks {
        error!(
            "Chunk {} has invalid status for import: {:?}",
            chunk.id, chunk.status
        );
    }

    IncompleteSnapshotSnafu {
        chunk_id: invalid_chunks[0].id,
        status: invalid_chunks[0].status,
    }
    .fail()
}

fn chunk_has_schema_files(chunk: &ChunkMeta, schema: &str) -> bool {
    let prefix_with_data = format!("data/{schema}/{}/", chunk.id);
    let prefix_without_data = format!("{schema}/{}/", chunk.id);
    chunk.files.iter().any(|path| {
        let normalized = path.trim_start_matches('/');
        normalized.starts_with(&prefix_with_data) || normalized.starts_with(&prefix_without_data)
    })
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::*;
    use crate::data::export_v2::manifest::{ChunkMeta, ChunkStatus, TimeRange};

    #[test]
    fn test_parse_ddl_statements() {
        let content = r#"
-- Schema: public
CREATE DATABASE public;
CREATE TABLE t (ts TIMESTAMP TIME INDEX, host STRING, PRIMARY KEY (host)) ENGINE=mito;

-- comment
CREATE VIEW v AS SELECT * FROM t;
"#;
        let statements = parse_ddl_statements(content);
        assert_eq!(statements.len(), 3);
        assert!(statements[0].starts_with("CREATE DATABASE public"));
        assert!(statements[1].starts_with("CREATE TABLE t"));
        assert!(statements[2].starts_with("CREATE VIEW v"));
    }

    #[test]
    fn test_validate_chunk_statuses() {
        let time_range = TimeRange::new(
            Some(Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap()),
            Some(Utc.with_ymd_and_hms(2025, 1, 2, 0, 0, 0).unwrap()),
        );
        let mut completed = ChunkMeta::new(1, time_range.clone());
        completed.status = ChunkStatus::Completed;
        let mut skipped = ChunkMeta::new(2, time_range);
        skipped.status = ChunkStatus::Skipped;
        assert!(validate_chunk_statuses(&[completed, skipped]).is_ok());

        let mut failed = ChunkMeta::new(3, TimeRange::unbounded());
        failed.status = ChunkStatus::Failed;
        assert!(validate_chunk_statuses(&[failed]).is_err());
    }

    #[test]
    fn test_chunk_has_schema_files() {
        let mut chunk = ChunkMeta::new(1, TimeRange::unbounded());
        chunk.files = vec![
            "data/public/1/a.parquet".to_string(),
            "data/metrics/1/b.parquet".to_string(),
        ];
        assert!(chunk_has_schema_files(&chunk, "public"));
        assert!(chunk_has_schema_files(&chunk, "metrics"));
        assert!(!chunk_has_schema_files(&chunk, "system"));

        let mut chunk_without_data_prefix = ChunkMeta::new(1, TimeRange::unbounded());
        chunk_without_data_prefix.files = vec!["public/1/a.parquet".to_string()];
        assert!(chunk_has_schema_files(&chunk_without_data_prefix, "public"));

        let mut chunk_with_leading_slash = ChunkMeta::new(1, TimeRange::unbounded());
        chunk_with_leading_slash.files = vec!["/data/public/1/a.parquet".to_string()];
        assert!(chunk_has_schema_files(&chunk_with_leading_slash, "public"));
    }
}
