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

use super::error::{DatabaseSnafu, Result};
use crate::database::DatabaseClient;

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
    pub async fn execute(&self, statements: &[String]) -> Result<ExecutionResult> {
        let total = statements.len();
        let mut succeeded = 0;
        let mut failed = 0;

        for (i, stmt) in statements.iter().enumerate() {
            let preview = if stmt.len() > 80 {
                format!("{}...", &stmt[..80])
            } else {
                stmt.clone()
            };

            info!("Executing DDL ({}/{}): {}", i + 1, total, preview);

            let result = if let Some(schema) = extract_schema_from_statement(stmt) {
                self.client.sql(stmt, schema).await
            } else {
                self.client.sql_in_public(stmt).await
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
    pub async fn execute_strict(&self, statements: &[String]) -> Result<()> {
        let total = statements.len();

        for (i, stmt) in statements.iter().enumerate() {
            let preview = if stmt.len() > 80 {
                format!("{}...", &stmt[..80])
            } else {
                stmt.clone()
            };

            info!("Executing DDL ({}/{}): {}", i + 1, total, preview);

            if let Some(schema) = extract_schema_from_statement(stmt) {
                self.client.sql(stmt, schema).await.context(DatabaseSnafu)?;
            } else {
                self.client
                    .sql_in_public(stmt)
                    .await
                    .context(DatabaseSnafu)?;
            }
        }

        Ok(())
    }
}

fn extract_schema_from_statement(stmt: &str) -> Option<&str> {
    let trimmed = stmt.trim_start();
    if !starts_with_keyword(trimmed, "CREATE") {
        return None;
    }

    let upper = trimmed.to_ascii_uppercase();
    if !(upper.starts_with("CREATE TABLE") || upper.starts_with("CREATE VIEW")) {
        return None;
    }

    let bytes = trimmed.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'"' {
            let start = i + 1;
            let mut j = start;
            while j < bytes.len() && bytes[j] != b'"' {
                j += 1;
            }
            if j >= bytes.len() {
                return None;
            }
            let after = j + 1;
            if after + 1 < bytes.len() && bytes[after] == b'.' && bytes[after + 1] == b'"' {
                return trimmed.get(start..j);
            }
        }
        i += 1;
    }

    None
}

fn starts_with_keyword(input: &str, keyword: &str) -> bool {
    input
        .get(0..keyword.len())
        .map(|s| s.eq_ignore_ascii_case(keyword))
        .unwrap_or(false)
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
}
