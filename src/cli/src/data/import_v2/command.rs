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

use std::collections::HashSet;
use std::time::Duration;

use async_trait::async_trait;
use clap::Parser;
use common_error::ext::BoxedError;
use common_telemetry::info;
use snafu::ResultExt;

use crate::Tool;
use crate::common::ObjectStoreConfig;
use crate::data::export_v2::data::{build_copy_source, execute_copy_database_from};
use crate::data::export_v2::manifest::{ChunkMeta, ChunkStatus, DataFormat, MANIFEST_VERSION};
use crate::data::import_v2::error::{
    ChunkImportFailedSnafu, IncompleteSnapshotSnafu, ManifestVersionMismatchSnafu,
    MissingChunkDataSnafu, Result, SchemaNotInSnapshotSnafu, SnapshotStorageSnafu,
};
use crate::data::import_v2::executor::{DdlExecutor, DdlStatement};
use crate::data::path::{data_dir_for_schema_chunk, ddl_path_for_schema};
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
            .context(SnapshotStorageSnafu)
            .map_err(BoxedError::new)?;

        // Parse schemas (empty vec means all schemas)
        let schemas = if self.schemas.is_empty() {
            None
        } else {
            Some(self.schemas.clone())
        };

        // Build storage
        let storage = OpenDalStorage::from_uri(&self.from, &self.storage)
            .context(SnapshotStorageSnafu)
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
        let manifest = self
            .storage
            .read_manifest()
            .await
            .context(SnapshotStorageSnafu)?;

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

        info!("Snapshot contains {} schema(s)", manifest.schemas.len());

        // 2. Determine schemas to import
        let schemas_to_import = match &self.schemas {
            Some(filter) => canonicalize_schema_filter(filter, &manifest.schemas)?,
            None => manifest.schemas.clone(),
        };

        info!("Importing schemas: {:?}", schemas_to_import);

        // 3. Read DDL statements
        let ddl_statements = self.read_ddl_statements(&schemas_to_import).await?;

        info!("Generated {} DDL statements", ddl_statements.len());

        let data_prefixes = if !manifest.schema_only && !manifest.chunks.is_empty() {
            Some(
                validate_data_snapshot(self.storage.as_ref(), &manifest.chunks, &schemas_to_import)
                    .await?,
            )
        } else {
            None
        };

        // 4. Dry-run mode: print DDL and exit
        if self.dry_run {
            info!("Dry-run mode - DDL statements to execute:");
            println!();
            for (i, stmt) in ddl_statements.iter().enumerate() {
                println!("-- Statement {}", i + 1);
                println!("{};", stmt.sql);
                println!();
            }
            if !manifest.schema_only && !manifest.chunks.is_empty() {
                for line in format_data_import_plan(&manifest.chunks, &schemas_to_import) {
                    println!("{line}");
                }
                println!();
            }
            return Ok(());
        }

        // 5. Execute DDL
        let executor = DdlExecutor::new(&self.database_client);
        executor.execute_strict(&ddl_statements).await?;

        if !manifest.schema_only && !manifest.chunks.is_empty() {
            self.import_data(
                &manifest.chunks,
                &schemas_to_import,
                manifest.format,
                data_prefixes.expect("validated full snapshot must provide data prefixes"),
            )
            .await?;
        }

        info!(
            "Import completed: {} DDL statements executed",
            ddl_statements.len()
        );

        Ok(())
    }

    async fn read_ddl_statements(&self, schemas: &[String]) -> Result<Vec<DdlStatement>> {
        let mut statements = Vec::new();
        for schema in schemas {
            let path = ddl_path_for_schema(schema);
            let content = self
                .storage
                .read_text(&path)
                .await
                .context(SnapshotStorageSnafu)?;
            statements.extend(
                parse_ddl_statements(&content)
                    .into_iter()
                    .map(|sql| ddl_statement_for_schema(schema, sql)),
            );
        }

        Ok(statements)
    }

    async fn import_data(
        &self,
        chunks: &[ChunkMeta],
        schemas: &[String],
        format: DataFormat,
        actual_prefixes: HashSet<String>,
    ) -> Result<()> {
        let total_chunks = chunks
            .iter()
            .filter(|chunk| chunk.status == ChunkStatus::Completed)
            .count();
        info!(
            "Importing data: {} chunks, {} schemas",
            total_chunks,
            schemas.len()
        );

        for (idx, chunk) in chunks.iter().enumerate() {
            if chunk.status == ChunkStatus::Skipped {
                info!(
                    "[{}/{}] Chunk {}: skipped (no data)",
                    idx + 1,
                    chunks.len(),
                    chunk.id
                );
                continue;
            }

            info!(
                "[{}/{}] Chunk {} ({:?} ~ {:?})",
                idx + 1,
                chunks.len(),
                chunk.id,
                chunk.time_range.start,
                chunk.time_range.end
            );

            for schema in schemas {
                if !validate_chunk_schema_files(chunk, schema, &actual_prefixes)? {
                    info!("  {}: no data, skipped", schema);
                    continue;
                }

                info!("  {}: importing...", schema);
                let source =
                    build_copy_source(&self.snapshot_uri, &self.storage_config, schema, chunk.id)
                        .context(ChunkImportFailedSnafu {
                        chunk_id: chunk.id,
                        schema: schema.clone(),
                    })?;

                execute_copy_database_from(
                    &self.database_client,
                    &self.catalog,
                    schema,
                    &source,
                    format,
                )
                .await
                .context(ChunkImportFailedSnafu {
                    chunk_id: chunk.id,
                    schema: schema.clone(),
                })?;

                info!("  {}: done", schema);
            }
        }

        Ok(())
    }
}

fn parse_ddl_statements(content: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut chars = content.chars().peekable();
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut in_line_comment = false;
    let mut in_block_comment = false;

    while let Some(ch) = chars.next() {
        if in_line_comment {
            if ch == '\n' {
                in_line_comment = false;
                current.push('\n');
            }
            continue;
        }

        if in_block_comment {
            if ch == '*' && chars.peek() == Some(&'/') {
                chars.next();
                in_block_comment = false;
            }
            continue;
        }

        if in_single_quote {
            current.push(ch);
            if ch == '\'' {
                if chars.peek() == Some(&'\'') {
                    current.push(chars.next().expect("peeked quote must exist"));
                } else {
                    in_single_quote = false;
                }
            }
            continue;
        }

        if in_double_quote {
            current.push(ch);
            if ch == '"' {
                if chars.peek() == Some(&'"') {
                    current.push(chars.next().expect("peeked quote must exist"));
                } else {
                    in_double_quote = false;
                }
            }
            continue;
        }

        match ch {
            '-' if chars.peek() == Some(&'-') => {
                chars.next();
                in_line_comment = true;
            }
            '/' if chars.peek() == Some(&'*') => {
                chars.next();
                in_block_comment = true;
            }
            '\'' => {
                in_single_quote = true;
                current.push(ch);
            }
            '"' => {
                in_double_quote = true;
                current.push(ch);
            }
            ';' => {
                let statement = current.trim();
                if !statement.is_empty() {
                    statements.push(statement.to_string());
                }
                current.clear();
            }
            _ => current.push(ch),
        }
    }

    let statement = current.trim();
    if !statement.is_empty() {
        statements.push(statement.to_string());
    }

    statements
}

fn ddl_statement_for_schema(schema: &str, sql: String) -> DdlStatement {
    if is_schema_scoped_statement(&sql) {
        DdlStatement::with_execution_schema(sql, schema.to_string())
    } else {
        DdlStatement::new(sql)
    }
}

fn is_schema_scoped_statement(sql: &str) -> bool {
    let trimmed = sql.trim_start();
    if !starts_with_keyword(trimmed, "CREATE") {
        return false;
    }

    let Some(rest) = trimmed.get("CREATE".len()..) else {
        return false;
    };
    let mut rest = rest.trim_start();
    if starts_with_keyword(rest, "OR") {
        let Some(next) = rest.get("OR".len()..) else {
            return false;
        };
        rest = next.trim_start();
        if !starts_with_keyword(rest, "REPLACE") {
            return false;
        }
        let Some(next) = rest.get("REPLACE".len()..) else {
            return false;
        };
        rest = next.trim_start();
    }

    if starts_with_keyword(rest, "EXTERNAL") {
        let Some(next) = rest.get("EXTERNAL".len()..) else {
            return false;
        };
        rest = next.trim_start();
    }

    starts_with_keyword(rest, "TABLE") || starts_with_keyword(rest, "VIEW")
}

fn starts_with_keyword(input: &str, keyword: &str) -> bool {
    input
        .get(0..keyword.len())
        .map(|s| s.eq_ignore_ascii_case(keyword))
        .unwrap_or(false)
        && input
            .as_bytes()
            .get(keyword.len())
            .map(|b| !b.is_ascii_alphanumeric() && *b != b'_')
            .unwrap_or(true)
}

fn canonicalize_schema_filter(
    filter: &[String],
    manifest_schemas: &[String],
) -> Result<Vec<String>> {
    let mut canonicalized = Vec::new();
    let mut seen = HashSet::new();

    for schema in filter {
        let canonical = manifest_schemas
            .iter()
            .find(|candidate| candidate.eq_ignore_ascii_case(schema))
            .cloned()
            .ok_or_else(|| {
                SchemaNotInSnapshotSnafu {
                    schema: schema.clone(),
                }
                .build()
            })?;

        if seen.insert(canonical.to_ascii_lowercase()) {
            canonicalized.push(canonical);
        }
    }

    Ok(canonicalized)
}

fn validate_chunk_statuses(chunks: &[ChunkMeta]) -> Result<()> {
    let invalid_chunk = chunks
        .iter()
        .find(|chunk| !matches!(chunk.status, ChunkStatus::Completed | ChunkStatus::Skipped));

    if let Some(chunk) = invalid_chunk {
        return IncompleteSnapshotSnafu {
            chunk_id: chunk.id,
            status: chunk.status,
        }
        .fail();
    }

    Ok(())
}

fn chunk_has_schema_files(chunk: &ChunkMeta, schema: &str) -> bool {
    let prefix = data_dir_for_schema_chunk(schema, chunk.id);
    chunk.files.iter().any(|path| {
        let normalized = path.trim_start_matches('/');
        normalized.starts_with(&prefix)
    })
}

fn format_data_import_plan(chunks: &[ChunkMeta], schemas: &[String]) -> Vec<String> {
    let mut lines = vec!["-- Data import plan:".to_string()];
    for chunk in chunks {
        lines.push(format!("-- Chunk {}: {:?}", chunk.id, chunk.status));
        for schema in schemas {
            if chunk_has_schema_files(chunk, schema) {
                lines.push(format!("--   {} -> COPY DATABASE FROM", schema));
            }
        }
    }
    lines
}

async fn validate_data_snapshot(
    storage: &dyn SnapshotStorage,
    chunks: &[ChunkMeta],
    schemas: &[String],
) -> Result<HashSet<String>> {
    validate_chunk_statuses(chunks)?;
    let actual_prefixes = collect_chunk_data_prefixes(storage).await?;

    for chunk in chunks {
        if chunk.status == ChunkStatus::Skipped {
            continue;
        }
        for schema in schemas {
            validate_chunk_schema_files(chunk, schema, &actual_prefixes)?;
        }
    }

    Ok(actual_prefixes)
}

async fn collect_chunk_data_prefixes(storage: &dyn SnapshotStorage) -> Result<HashSet<String>> {
    let files = storage
        .list_files_recursive("data/")
        .await
        .context(SnapshotStorageSnafu)?;
    let mut prefixes = HashSet::new();

    for path in files {
        let normalized = path.trim_start_matches('/');
        let mut parts = normalized.splitn(4, '/');
        let Some(root) = parts.next() else {
            continue;
        };
        let Some(schema) = parts.next() else {
            continue;
        };
        let Some(chunk_id) = parts.next() else {
            continue;
        };
        if root != "data" {
            continue;
        }
        prefixes.insert(format!("data/{schema}/{chunk_id}/"));
    }

    Ok(prefixes)
}

fn validate_chunk_schema_files(
    chunk: &ChunkMeta,
    schema: &str,
    actual_prefixes: &HashSet<String>,
) -> Result<bool> {
    if !chunk_has_schema_files(chunk, schema) {
        return Ok(false);
    }

    let prefix = data_dir_for_schema_chunk(schema, chunk.id);
    if !actual_prefixes.contains(&prefix) {
        return MissingChunkDataSnafu {
            chunk_id: chunk.id,
            schema: schema.to_string(),
            path: prefix,
        }
        .fail();
    }

    Ok(true)
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use async_trait::async_trait;

    use super::*;
    use crate::data::export_v2::manifest::{ChunkMeta, ChunkStatus, Manifest, TimeRange};
    use crate::data::export_v2::schema::SchemaSnapshot;
    use crate::data::snapshot_storage::SnapshotStorage;

    struct StubStorage {
        manifest: Manifest,
        files_by_prefix: HashMap<String, Vec<String>>,
    }

    #[async_trait]
    impl SnapshotStorage for StubStorage {
        async fn exists(&self) -> crate::data::export_v2::error::Result<bool> {
            Ok(true)
        }

        async fn read_manifest(&self) -> crate::data::export_v2::error::Result<Manifest> {
            Ok(self.manifest.clone())
        }

        async fn write_manifest(
            &self,
            _manifest: &Manifest,
        ) -> crate::data::export_v2::error::Result<()> {
            unimplemented!("not needed in import_v2::command tests")
        }

        async fn read_text(&self, _path: &str) -> crate::data::export_v2::error::Result<String> {
            unimplemented!("not needed in import_v2::command tests")
        }

        async fn write_text(
            &self,
            _path: &str,
            _content: &str,
        ) -> crate::data::export_v2::error::Result<()> {
            unimplemented!("not needed in import_v2::command tests")
        }

        async fn write_schema(
            &self,
            _snapshot: &SchemaSnapshot,
        ) -> crate::data::export_v2::error::Result<()> {
            unimplemented!("not needed in import_v2::command tests")
        }

        async fn create_dir_all(&self, _path: &str) -> crate::data::export_v2::error::Result<()> {
            unimplemented!("not needed in import_v2::command tests")
        }

        async fn list_files_recursive(
            &self,
            prefix: &str,
        ) -> crate::data::export_v2::error::Result<Vec<String>> {
            Ok(self
                .files_by_prefix
                .iter()
                .filter(|(candidate, _)| candidate.starts_with(prefix))
                .flat_map(|(_, files)| files.clone())
                .collect())
        }

        async fn delete_snapshot(&self) -> crate::data::export_v2::error::Result<()> {
            unimplemented!("not needed in import_v2::command tests")
        }
    }

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
    fn test_parse_ddl_statements_preserves_semicolons_in_string_literals() {
        let content = r#"
CREATE TABLE t (
    host STRING DEFAULT 'a;b'
);
CREATE VIEW v AS SELECT ';' AS marker;
"#;

        let statements = parse_ddl_statements(content);

        assert_eq!(statements.len(), 2);
        assert!(statements[0].contains("'a;b'"));
        assert!(statements[1].contains("';' AS marker"));
    }

    #[test]
    fn test_parse_ddl_statements_handles_comments_without_splitting() {
        let content = r#"
-- leading comment
CREATE TABLE t (ts TIMESTAMP TIME INDEX); /* block; comment */
CREATE VIEW v AS SELECT 1;
"#;

        let statements = parse_ddl_statements(content);

        assert_eq!(statements.len(), 2);
        assert!(statements[0].starts_with("CREATE TABLE t"));
        assert!(statements[1].starts_with("CREATE VIEW v"));
    }

    #[test]
    fn test_canonicalize_schema_filter_uses_manifest_casing() {
        let filter = vec!["TEST_DB".to_string(), "PUBLIC".to_string()];
        let manifest_schemas = vec!["test_db".to_string(), "public".to_string()];

        let canonicalized = canonicalize_schema_filter(&filter, &manifest_schemas).unwrap();

        assert_eq!(canonicalized, vec!["test_db", "public"]);
    }

    #[test]
    fn test_canonicalize_schema_filter_dedupes_case_insensitive_matches() {
        let filter = vec![
            "TEST_DB".to_string(),
            "test_db".to_string(),
            "PUBLIC".to_string(),
            "public".to_string(),
        ];
        let manifest_schemas = vec!["test_db".to_string(), "public".to_string()];

        let canonicalized = canonicalize_schema_filter(&filter, &manifest_schemas).unwrap();

        assert_eq!(canonicalized, vec!["test_db", "public"]);
    }

    #[test]
    fn test_canonicalize_schema_filter_rejects_missing_schema() {
        let filter = vec!["missing".to_string()];
        let manifest_schemas = vec!["test_db".to_string()];

        let error = canonicalize_schema_filter(&filter, &manifest_schemas)
            .expect_err("missing schema should fail")
            .to_string();

        assert!(error.contains("missing"));
    }

    #[test]
    fn test_ddl_statement_for_schema_create_table_uses_execution_schema() {
        let stmt = ddl_statement_for_schema(
            "test_db",
            "CREATE TABLE metrics (ts TIMESTAMP TIME INDEX) ENGINE=mito".to_string(),
        );
        assert_eq!(stmt.execution_schema.as_deref(), Some("test_db"));
    }

    #[test]
    fn test_ddl_statement_for_schema_create_view_uses_execution_schema() {
        let stmt = ddl_statement_for_schema(
            "test_db",
            "CREATE VIEW metrics_view AS SELECT * FROM metrics".to_string(),
        );
        assert_eq!(stmt.execution_schema.as_deref(), Some("test_db"));
    }

    #[test]
    fn test_ddl_statement_for_schema_create_or_replace_view_uses_execution_schema() {
        let stmt = ddl_statement_for_schema(
            "test_db",
            "CREATE OR REPLACE VIEW metrics_view AS SELECT * FROM metrics".to_string(),
        );
        assert_eq!(stmt.execution_schema.as_deref(), Some("test_db"));
    }

    #[test]
    fn test_ddl_statement_for_schema_create_external_table_uses_execution_schema() {
        let stmt = ddl_statement_for_schema(
            "test_db",
            "CREATE EXTERNAL TABLE IF NOT EXISTS ext_metrics (ts TIMESTAMP TIME INDEX) ENGINE=file"
                .to_string(),
        );
        assert_eq!(stmt.execution_schema.as_deref(), Some("test_db"));
    }

    #[test]
    fn test_ddl_statement_for_schema_create_database_uses_public_context() {
        let stmt = ddl_statement_for_schema("test_db", "CREATE DATABASE test_db".to_string());
        assert_eq!(stmt.execution_schema, None);
    }

    #[test]
    fn test_starts_with_keyword_requires_word_boundary() {
        assert!(starts_with_keyword("CREATE TABLE t", "CREATE"));
        assert!(!starts_with_keyword("CREATED TABLE t", "CREATE"));
        assert!(!starts_with_keyword("TABLESPACE foo", "TABLE"));
    }

    #[test]
    fn test_validate_chunk_statuses_rejects_failed_chunk() {
        let mut failed = ChunkMeta::new(3, TimeRange::unbounded());
        failed.status = ChunkStatus::Failed;

        let error = validate_chunk_statuses(&[failed]).expect_err("failed chunk should error");
        assert!(error.to_string().contains("Incomplete snapshot"));
    }

    #[test]
    fn test_validate_chunk_statuses_accepts_completed_and_skipped_chunks() {
        let mut completed = ChunkMeta::new(1, TimeRange::unbounded());
        completed.status = ChunkStatus::Completed;
        let skipped = ChunkMeta::skipped(2, TimeRange::unbounded());

        assert!(validate_chunk_statuses(&[completed, skipped]).is_ok());
    }

    #[test]
    fn test_chunk_has_schema_files_matches_encoded_schema_prefix() {
        let mut chunk = ChunkMeta::new(7, TimeRange::unbounded());
        chunk.files = vec![
            "data/public/7/a.parquet".to_string(),
            "data/%E6%B5%8B%E8%AF%95/7/b.parquet".to_string(),
        ];

        assert!(chunk_has_schema_files(&chunk, "public"));
        assert!(chunk_has_schema_files(&chunk, "测试"));
        assert!(!chunk_has_schema_files(&chunk, "metrics"));
    }

    #[test]
    fn test_format_data_import_plan_includes_matching_schemas_only() {
        let mut completed = ChunkMeta::new(1, TimeRange::unbounded());
        completed.status = ChunkStatus::Completed;
        completed.files = vec![
            "data/public/1/a.parquet".to_string(),
            "data/%E6%B5%8B%E8%AF%95/1/b.parquet".to_string(),
        ];
        let skipped = ChunkMeta::skipped(2, TimeRange::unbounded());

        let lines = format_data_import_plan(
            &[completed, skipped],
            &[
                "public".to_string(),
                "测试".to_string(),
                "metrics".to_string(),
            ],
        );

        assert_eq!(lines[0], "-- Data import plan:");
        assert!(lines.contains(&"-- Chunk 1: Completed".to_string()));
        assert!(lines.contains(&"--   public -> COPY DATABASE FROM".to_string()));
        assert!(lines.contains(&"--   测试 -> COPY DATABASE FROM".to_string()));
        assert!(!lines.contains(&"--   metrics -> COPY DATABASE FROM".to_string()));
        assert!(lines.contains(&"-- Chunk 2: Skipped".to_string()));
    }

    #[tokio::test]
    async fn test_collect_chunk_data_prefixes_indexes_present_prefixes() {
        let storage = StubStorage {
            manifest: Manifest::new_schema_only("greptime".to_string(), vec!["public".to_string()]),
            files_by_prefix: HashMap::from([
                (
                    "data/public/7/".to_string(),
                    vec!["data/public/7/a.parquet".to_string()],
                ),
                (
                    "data/%E6%B5%8B%E8%AF%95/9/".to_string(),
                    vec!["data/%E6%B5%8B%E8%AF%95/9/b.parquet".to_string()],
                ),
            ]),
        };

        let prefixes = collect_chunk_data_prefixes(&storage).await.unwrap();

        assert!(prefixes.contains("data/public/7/"));
        assert!(prefixes.contains("data/%E6%B5%8B%E8%AF%95/9/"));
    }

    #[test]
    fn test_validate_chunk_schema_files_accepts_present_prefix() {
        let mut chunk = ChunkMeta::new(7, TimeRange::unbounded());
        chunk.files = vec!["data/public/7/a.parquet".to_string()];
        let actual_prefixes = HashSet::from(["data/public/7/".to_string()]);

        assert!(validate_chunk_schema_files(&chunk, "public", &actual_prefixes).unwrap());
    }

    #[test]
    fn test_validate_chunk_schema_files_rejects_missing_prefix() {
        let mut chunk = ChunkMeta::new(7, TimeRange::unbounded());
        chunk.files = vec!["data/public/7/a.parquet".to_string()];

        let error = validate_chunk_schema_files(&chunk, "public", &HashSet::new())
            .expect_err("missing chunk prefix should fail")
            .to_string();
        assert!(error.contains("marked completed but no files were found"));
    }

    #[test]
    fn test_validate_chunk_schema_files_skips_absent_schema() {
        let mut chunk = ChunkMeta::new(7, TimeRange::unbounded());
        chunk.files = vec!["data/public/7/a.parquet".to_string()];

        assert!(!validate_chunk_schema_files(&chunk, "metrics", &HashSet::new()).unwrap());
    }

    #[tokio::test]
    async fn test_validate_data_snapshot_rejects_failed_chunk_before_dry_run() {
        let mut failed = ChunkMeta::new(3, TimeRange::unbounded());
        failed.status = ChunkStatus::Failed;

        let storage = StubStorage {
            manifest: Manifest::new_schema_only("greptime".to_string(), vec!["public".to_string()]),
            files_by_prefix: HashMap::new(),
        };

        let error = validate_data_snapshot(&storage, &[failed], &["public".to_string()])
            .await
            .expect_err("failed chunk should reject dry-run validation")
            .to_string();
        assert!(error.contains("Incomplete snapshot"));
    }

    #[tokio::test]
    async fn test_validate_data_snapshot_rejects_missing_chunk_prefix_before_dry_run() {
        let mut completed = ChunkMeta::new(7, TimeRange::unbounded());
        completed.status = ChunkStatus::Completed;
        completed.files = vec!["data/public/7/a.parquet".to_string()];

        let storage = StubStorage {
            manifest: Manifest::new_schema_only("greptime".to_string(), vec!["public".to_string()]),
            files_by_prefix: HashMap::new(),
        };

        let error = validate_data_snapshot(&storage, &[completed], &["public".to_string()])
            .await
            .expect_err("missing chunk prefix should reject dry-run validation")
            .to_string();
        assert!(error.contains("marked completed but no files were found"));
    }
}
