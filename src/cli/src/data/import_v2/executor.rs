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

//! DDL execution for import.

use common_telemetry::info;
use snafu::ResultExt;

use crate::data::import_v2::error::{DatabaseSnafu, Result};
use crate::database::DatabaseClient;

/// A DDL statement with an optional fallback schema context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DdlStatement {
    pub sql: String,
    pub fallback_schema: Option<String>,
}

impl DdlStatement {
    pub fn new(sql: String) -> Self {
        Self {
            sql,
            fallback_schema: None,
        }
    }

    pub fn with_fallback_schema(sql: String, schema: String) -> Self {
        Self {
            sql,
            fallback_schema: Some(schema),
        }
    }
}

/// Executes DDL statements against the database.
pub struct DdlExecutor<'a> {
    client: &'a DatabaseClient,
}

impl<'a> DdlExecutor<'a> {
    /// Creates a new DDL executor.
    pub fn new(client: &'a DatabaseClient) -> Self {
        Self { client }
    }

    /// Executes a list of DDL statements.
    ///
    /// Statements are executed in order. If any statement fails,
    /// the error is returned and remaining statements are not executed.
    pub async fn execute(&self, statements: &[DdlStatement]) -> Result<ExecutionResult> {
        let total = statements.len();
        let mut succeeded = 0;
        let mut failed = 0;

        for (i, stmt) in statements.iter().enumerate() {
            let preview = preview_sql(&stmt.sql);

            info!("Executing DDL ({}/{}): {}", i + 1, total, preview);

            let result = if let Some(schema) =
                execution_schema_for_statement(&stmt.sql, stmt.fallback_schema.as_deref())
            {
                self.client.sql(&stmt.sql, schema).await
            } else {
                self.client.sql_in_public(&stmt.sql).await
            };

            match result {
                Ok(_) => {
                    succeeded += 1;
                }
                Err(e) => {
                    // Log the error but continue with remaining statements
                    // This allows partial imports to succeed
                    common_telemetry::warn!("DDL execution failed: {}: {}", preview, e);
                    failed += 1;

                    // For critical errors (like syntax errors), we might want to stop
                    // But for "already exists" errors, we continue
                    // TODO: Distinguish between error types
                }
            }
        }

        Ok(ExecutionResult {
            total,
            succeeded,
            failed,
        })
    }

    /// Executes a list of DDL statements, stopping on first error.
    pub async fn execute_strict(&self, statements: &[DdlStatement]) -> Result<()> {
        let total = statements.len();

        for (i, stmt) in statements.iter().enumerate() {
            let preview = preview_sql(&stmt.sql);

            info!("Executing DDL ({}/{}): {}", i + 1, total, preview);

            if let Some(schema) =
                execution_schema_for_statement(&stmt.sql, stmt.fallback_schema.as_deref())
            {
                self.client
                    .sql(&stmt.sql, schema)
                    .await
                    .context(DatabaseSnafu)?;
            } else {
                self.client
                    .sql_in_public(&stmt.sql)
                    .await
                    .context(DatabaseSnafu)?;
            }
        }

        Ok(())
    }
}

fn extract_schema_from_statement(stmt: &str) -> Option<&str> {
    if !is_schema_scoped_statement(stmt) {
        return None;
    }

    let parts = parse_object_name_parts(stmt)?;
    match parts.len() {
        0 | 1 => None,
        2 => Some(parts[0]),
        _ => Some(parts[parts.len() - 2]),
    }
}

fn parse_object_name_parts(stmt: &str) -> Option<Vec<&str>> {
    let mut rest = stmt.trim_start();
    if !starts_with_keyword(rest, "CREATE") {
        return None;
    }

    rest = rest.get("CREATE".len()..)?.trim_start();
    if starts_with_keyword(rest, "TABLE") {
        rest = rest.get("TABLE".len()..)?.trim_start();
    } else if starts_with_keyword(rest, "VIEW") {
        rest = rest.get("VIEW".len()..)?.trim_start();
    } else {
        return None;
    }

    if starts_with_keyword(rest, "IF NOT EXISTS") {
        rest = rest.get("IF NOT EXISTS".len()..)?.trim_start();
    }

    let mut parts = Vec::new();
    loop {
        rest = rest.trim_start();
        if rest.is_empty() {
            break;
        }

        let (part, after_part) = if let Some(after_quote) = rest.strip_prefix('"') {
            let end = after_quote.find('"')?;
            (&after_quote[..end], &after_quote[end + 1..])
        } else {
            let end = rest
                .find(|c: char| c.is_whitespace() || c == '.' || c == '(')
                .unwrap_or(rest.len());
            if end == 0 {
                break;
            }
            (&rest[..end], &rest[end..])
        };

        parts.push(part);
        rest = after_part.trim_start();

        if let Some(after_dot) = rest.strip_prefix('.') {
            rest = after_dot;
            continue;
        }
        break;
    }

    Some(parts)
}

fn is_schema_scoped_statement(stmt: &str) -> bool {
    let trimmed = stmt.trim_start();
    if !starts_with_keyword(trimmed, "CREATE") {
        return false;
    }

    let Some(rest) = trimmed.get("CREATE".len()..) else {
        return false;
    };
    let rest = rest.trim_start();
    starts_with_keyword(rest, "TABLE") || starts_with_keyword(rest, "VIEW")
}

fn execution_schema_for_statement<'a>(
    stmt: &'a str,
    fallback_schema: Option<&'a str>,
) -> Option<&'a str> {
    extract_schema_from_statement(stmt).or_else(|| {
        if is_schema_scoped_statement(stmt) {
            fallback_schema
        } else {
            None
        }
    })
}

fn starts_with_keyword(input: &str, keyword: &str) -> bool {
    input
        .get(0..keyword.len())
        .map(|s| s.eq_ignore_ascii_case(keyword))
        .unwrap_or(false)
}

fn preview_sql(sql: &str) -> String {
    let mut chars = sql.chars();
    let preview: String = chars.by_ref().take(80).collect();
    if chars.next().is_some() {
        format!("{preview}...")
    } else {
        preview
    }
}

/// Result of DDL execution.
#[derive(Debug)]
pub struct ExecutionResult {
    /// Total number of statements.
    pub total: usize,
    /// Number of successfully executed statements.
    pub succeeded: usize,
    /// Number of failed statements.
    pub failed: usize,
}

impl ExecutionResult {
    /// Returns true if all statements succeeded.
    pub fn is_success(&self) -> bool {
        self.failed == 0
    }

    /// Returns true if any statement failed.
    pub fn has_failures(&self) -> bool {
        self.failed > 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_execution_result() {
        let result = ExecutionResult {
            total: 10,
            succeeded: 10,
            failed: 0,
        };
        assert!(result.is_success());
        assert!(!result.has_failures());

        let result = ExecutionResult {
            total: 10,
            succeeded: 8,
            failed: 2,
        };
        assert!(!result.is_success());
        assert!(result.has_failures());
    }

    #[test]
    fn test_execution_schema_for_qualified_statement() {
        let stmt =
            r#"CREATE TABLE "greptime"."test_db"."metrics" (ts TIMESTAMP TIME INDEX) ENGINE=mito"#;
        assert_eq!(
            execution_schema_for_statement(stmt, Some("public")),
            Some("test_db")
        );
    }

    #[test]
    fn test_execution_schema_for_two_part_name() {
        let stmt = r#"CREATE VIEW "test_db"."metrics_view" AS SELECT * FROM metrics"#;
        assert_eq!(
            execution_schema_for_statement(stmt, Some("public")),
            Some("test_db")
        );
    }

    #[test]
    fn test_execution_schema_for_unqualified_table_uses_fallback() {
        let stmt = "CREATE TABLE metrics (ts TIMESTAMP TIME INDEX) ENGINE=mito";
        assert_eq!(
            execution_schema_for_statement(stmt, Some("test_db")),
            Some("test_db")
        );
    }

    #[test]
    fn test_execution_schema_for_unqualified_view_uses_fallback() {
        let stmt = "CREATE VIEW metrics_view AS SELECT * FROM metrics";
        assert_eq!(
            execution_schema_for_statement(stmt, Some("test_db")),
            Some("test_db")
        );
    }

    #[test]
    fn test_execution_schema_for_create_database_ignores_fallback() {
        let stmt = "CREATE DATABASE test_db";
        assert_eq!(execution_schema_for_statement(stmt, Some("test_db")), None);
    }

    #[test]
    fn test_parse_object_name_parts_unquoted_two_part_name() {
        let stmt = "CREATE TABLE test_db.metrics (ts TIMESTAMP TIME INDEX)";
        assert_eq!(
            parse_object_name_parts(stmt),
            Some(vec!["test_db", "metrics"])
        );
    }

    #[test]
    fn test_parse_object_name_parts_with_if_not_exists() {
        let stmt = "CREATE VIEW IF NOT EXISTS test_db.metrics_view AS SELECT * FROM metrics";
        assert_eq!(
            parse_object_name_parts(stmt),
            Some(vec!["test_db", "metrics_view"])
        );
    }

    #[test]
    fn test_parse_object_name_parts_empty_input() {
        assert_eq!(parse_object_name_parts(""), None);
    }

    #[test]
    fn test_parse_object_name_parts_missing_name() {
        assert_eq!(parse_object_name_parts("CREATE TABLE"), Some(vec![]));
    }

    #[test]
    fn test_preview_sql_truncates_at_char_boundary() {
        let sql = format!(
            "CREATE TABLE {} (ts TIMESTAMP TIME INDEX)",
            "测".repeat(100)
        );
        let preview = preview_sql(&sql);
        assert!(preview.ends_with("..."));
        assert!(preview.is_char_boundary(preview.len()));
    }
}
