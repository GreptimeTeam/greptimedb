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

use std::collections::HashMap;
use std::fs::{OpenOptions, create_dir_all};
use std::io::Write;
use std::path::PathBuf;

use snafu::ResultExt;

use crate::error::{self, Result};
use crate::utils::get_gt_fuzz_dump_buffer_max_bytes;

/// Session writer for table-scoped SQL trace files.
#[derive(Debug)]
pub struct SqlDumpSession {
    /// Session directory path.
    pub run_dir: PathBuf,
    /// Max in-memory buffer size before auto flush.
    pub max_buffer_bytes: usize,
    buffered_bytes: usize,
    entries_by_table: HashMap<String, Vec<String>>,
}

impl SqlDumpSession {
    /// Creates SQL dump session with default buffer limit.
    pub fn new(run_dir: PathBuf) -> Result<Self> {
        Self::new_with_buffer_limit(run_dir, get_gt_fuzz_dump_buffer_max_bytes())
    }

    /// Creates SQL dump session with custom buffer limit.
    pub fn new_with_buffer_limit(run_dir: PathBuf, max_buffer_bytes: usize) -> Result<Self> {
        create_dir_all(&run_dir).context(error::CreateFileSnafu {
            path: run_dir.to_string_lossy().to_string(),
        })?;

        Ok(Self {
            run_dir,
            max_buffer_bytes,
            buffered_bytes: 0,
            entries_by_table: HashMap::new(),
        })
    }

    /// Appends one SQL statement for a logical table.
    pub fn append_sql(&mut self, table: &str, sql: &str, comment: Option<&str>) -> Result<()> {
        let entry = format_sql_entry(sql, comment);
        self.push_entry(table, entry)?;
        Ok(())
    }

    /// Broadcasts one comment event to all table trace files.
    pub fn broadcast_event<I, T>(&mut self, tables: I, event: &str, sql: &str) -> Result<()>
    where
        I: IntoIterator<Item = T>,
        T: AsRef<str>,
    {
        let entry = format_sql_entry(sql, Some(event));
        for table in tables {
            self.push_entry(table.as_ref(), entry.clone())?;
        }
        Ok(())
    }

    /// Flushes all staged SQL traces to table-scoped files.
    pub fn flush_all(&mut self) -> Result<()> {
        self.flush_buffered_entries()
    }

    fn push_entry(&mut self, table: &str, entry: String) -> Result<()> {
        self.buffered_bytes += entry.len();
        self.entries_by_table
            .entry(table.to_string())
            .or_default()
            .push(entry);

        if self.buffered_bytes >= self.max_buffer_bytes {
            self.flush_buffered_entries()?;
        }
        Ok(())
    }

    fn flush_buffered_entries(&mut self) -> Result<()> {
        if self.entries_by_table.is_empty() {
            return Ok(());
        }

        for (table, entries) in &self.entries_by_table {
            let path = self
                .run_dir
                .join(format!("{}.trace.sql", sanitize_file_name(table)));
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .context(error::CreateFileSnafu {
                    path: path.to_string_lossy().to_string(),
                })?;

            for entry in entries {
                file.write_all(entry.as_bytes())
                    .context(error::WriteFileSnafu {
                        path: path.to_string_lossy().to_string(),
                    })?;
                file.write_all(b"\n").context(error::WriteFileSnafu {
                    path: path.to_string_lossy().to_string(),
                })?;
            }
        }

        self.entries_by_table.clear();
        self.buffered_bytes = 0;
        Ok(())
    }
}

fn format_sql_entry(sql: &str, comment: Option<&str>) -> String {
    let normalized_sql = normalize_sql(sql);
    if let Some(comment) = comment {
        format!("{}\n{normalized_sql}", format_comment(comment))
    } else {
        normalized_sql
    }
}

fn format_comment(comment: &str) -> String {
    comment
        .lines()
        .map(|line| format!("-- {line}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn normalize_sql(sql: &str) -> String {
    let trimmed = sql.trim_end();
    if trimmed.ends_with(';') {
        trimmed.to_string()
    } else {
        format!("{trimmed};")
    }
}

fn sanitize_file_name(raw: &str) -> String {
    raw.chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::SqlDumpSession;

    #[test]
    fn test_append_sql_writes_table_trace_file() {
        let run_dir = std::env::temp_dir().join(format!(
            "tests-fuzz-sql-dump-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
        ));

        let mut session = SqlDumpSession::new_with_buffer_limit(run_dir.clone(), 1024).unwrap();
        session
            .append_sql(
                "metric-a",
                "INSERT INTO t VALUES(1)",
                Some("kind=insert elapsed_ms=10"),
            )
            .unwrap();
        session.flush_all().unwrap();

        let content = std::fs::read_to_string(run_dir.join("metric-a.trace.sql")).unwrap();
        assert!(content.contains("-- kind=insert elapsed_ms=10"));
        assert!(content.contains("INSERT INTO t VALUES(1);"));
    }

    #[test]
    fn test_broadcast_event_writes_to_all_tables() {
        let run_dir = std::env::temp_dir().join(format!(
            "tests-fuzz-sql-broadcast-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
        ));

        let mut session = SqlDumpSession::new_with_buffer_limit(run_dir.clone(), 1024).unwrap();
        session
            .broadcast_event(
                ["metric-a", "metric-b"],
                "repartition action_idx=3",
                "ALTER TABLE t REPARTITION",
            )
            .unwrap();
        session.flush_all().unwrap();

        let content_a = std::fs::read_to_string(run_dir.join("metric-a.trace.sql")).unwrap();
        let content_b = std::fs::read_to_string(run_dir.join("metric-b.trace.sql")).unwrap();
        assert!(content_a.contains("-- repartition action_idx=3"));
        assert!(content_a.contains("ALTER TABLE t REPARTITION;"));
        assert!(content_b.contains("-- repartition action_idx=3"));
        assert!(content_b.contains("ALTER TABLE t REPARTITION;"));
    }

    #[test]
    fn test_multiline_comment_is_prefixed_per_line() {
        let run_dir = std::env::temp_dir().join(format!(
            "tests-fuzz-sql-dump-comment-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
        ));

        let mut session = SqlDumpSession::new_with_buffer_limit(run_dir.clone(), 1024).unwrap();
        session
            .append_sql(
                "metric-a",
                "INSERT INTO t VALUES(1)",
                Some("kind=insert\nstarted_at_ms=1 elapsed_ms=2"),
            )
            .unwrap();
        session.flush_all().unwrap();

        let content = std::fs::read_to_string(run_dir.join("metric-a.trace.sql")).unwrap();
        assert!(content.contains("-- kind=insert\n-- started_at_ms=1 elapsed_ms=2"));
    }

    #[test]
    fn test_auto_flush_on_buffer_limit() {
        let run_dir = std::env::temp_dir().join(format!(
            "tests-fuzz-sql-dump-limit-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
        ));

        let mut session = SqlDumpSession::new_with_buffer_limit(run_dir.clone(), 1).unwrap();
        session
            .append_sql("metric-a", "INSERT INTO t VALUES(1)", None)
            .unwrap();

        assert!(run_dir.join("metric-a.trace.sql").exists());
        assert_eq!(session.buffered_bytes, 0);
    }
}
