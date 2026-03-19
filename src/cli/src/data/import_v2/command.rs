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
use crate::data::export_v2::manifest::MANIFEST_VERSION;
use crate::data::import_v2::error::{
    ManifestVersionMismatchSnafu, Result, SchemaNotInSnapshotSnafu, SnapshotStorageSnafu,
};
use crate::data::import_v2::executor::{DdlExecutor, DdlStatement};
use crate::data::path::ddl_path_for_schema;
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
            schemas,
            dry_run: self.dry_run,
            _parallelism: self.parallelism,
            storage: Box::new(storage),
            database_client,
        }))
    }
}

/// Import tool implementation.
pub struct Import {
    schemas: Option<Vec<String>>,
    dry_run: bool,
    _parallelism: usize,
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

        // 4. Dry-run mode: print DDL and exit
        if self.dry_run {
            info!("Dry-run mode - DDL statements to execute:");
            println!();
            for (i, stmt) in ddl_statements.iter().enumerate() {
                println!("-- Statement {}", i + 1);
                println!("{};", stmt.sql);
                println!();
            }
            return Ok(());
        }

        // 5. Execute DDL
        let executor = DdlExecutor::new(&self.database_client);
        executor.execute_strict(&ddl_statements).await?;

        info!(
            "Import completed: {} DDL statements executed",
            ddl_statements.len()
        );

        // 6. Data import would happen here for non-schema-only snapshots (M2/M3)
        if !manifest.schema_only && !manifest.chunks.is_empty() {
            info!(
                "Data import not yet implemented (M3). {} chunks pending.",
                manifest.chunks.len()
            );
        }

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

#[cfg(test)]
mod tests {
    use super::*;

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
}
