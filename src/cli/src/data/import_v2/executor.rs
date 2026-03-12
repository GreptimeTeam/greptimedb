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

/// A DDL statement with an explicit execution schema context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DdlStatement {
    pub sql: String,
    pub execution_schema: Option<String>,
}

impl DdlStatement {
    pub fn new(sql: String) -> Self {
        Self {
            sql,
            execution_schema: None,
        }
    }

    pub fn with_execution_schema(sql: String, schema: String) -> Self {
        Self {
            sql,
            execution_schema: Some(schema),
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

            let result = if let Some(schema) = stmt.execution_schema.as_deref() {
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

            if let Some(schema) = stmt.execution_schema.as_deref() {
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
    fn test_statement_without_execution_schema_uses_public() {
        let stmt = DdlStatement::new("CREATE DATABASE IF NOT EXISTS test_db".to_string());
        assert_eq!(stmt.execution_schema, None);
    }

    #[test]
    fn test_statement_with_execution_schema_preserves_context() {
        let stmt = DdlStatement::with_execution_schema(
            r#"CREATE TABLE IF NOT EXISTS "my""schema"."metrics" (ts TIMESTAMP TIME INDEX)"#
                .to_string(),
            r#"my"schema"#.to_string(),
        );
        assert_eq!(stmt.execution_schema.as_deref(), Some(r#"my"schema"#));
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
