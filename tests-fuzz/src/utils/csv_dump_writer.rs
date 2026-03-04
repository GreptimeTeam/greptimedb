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

use std::collections::HashSet;
use std::fs::{File, OpenOptions, create_dir_all};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use snafu::ResultExt;

use crate::error::{self, Result};
use crate::translator::csv::CsvRecords;
use crate::utils::{
    get_gt_fuzz_dump_buffer_max_bytes, get_gt_fuzz_dump_dir, get_gt_fuzz_dump_suffix,
};

/// Metadata for one CSV dump session.
#[derive(Debug, Clone)]
pub struct CsvDumpMetadata {
    /// Fuzz target name.
    pub target: String,
    /// Seed used by current fuzz input.
    pub seed: u64,
    /// Repartition action count.
    pub actions: usize,
    /// Initial partition count.
    pub partitions: usize,
    /// Logical table count.
    pub tables: usize,
    /// Session start time in unix milliseconds.
    pub started_at_unix_ms: i64,
}

impl CsvDumpMetadata {
    /// Builds dump metadata with current timestamp.
    pub fn new(
        target: impl Into<String>,
        seed: u64,
        actions: usize,
        partitions: usize,
        tables: usize,
    ) -> Self {
        Self {
            target: target.into(),
            seed,
            actions,
            partitions,
            tables,
            started_at_unix_ms: current_unix_millis(),
        }
    }
}

/// Session writer for staged CSV dump records.
#[derive(Debug)]
pub struct CsvDumpSession {
    /// Session metadata.
    pub metadata: CsvDumpMetadata,
    /// Session directory path.
    pub run_dir: PathBuf,
    /// Max in-memory buffer size before auto flush.
    pub max_buffer_bytes: usize,
    records: Vec<CsvRecords>,
    buffered_bytes: usize,
    written_tables: HashSet<String>,
}

impl CsvDumpSession {
    /// Creates session directory and writes seed metadata file.
    pub fn new(metadata: CsvDumpMetadata) -> Result<Self> {
        Self::new_with_buffer_limit(metadata, get_gt_fuzz_dump_buffer_max_bytes())
    }

    /// Creates session with a custom in-memory buffer limit.
    pub fn new_with_buffer_limit(
        metadata: CsvDumpMetadata,
        max_buffer_bytes: usize,
    ) -> Result<Self> {
        let run_dir = build_run_dir(&metadata);
        create_dir_all(&run_dir).context(error::CreateFileSnafu {
            path: run_dir.to_string_lossy().to_string(),
        })?;
        write_seed_meta(&run_dir, &metadata)?;

        Ok(Self {
            metadata,
            run_dir,
            max_buffer_bytes,
            records: Vec::new(),
            buffered_bytes: 0,
            written_tables: HashSet::new(),
        })
    }

    /// Appends one table CSV records batch.
    pub fn append(&mut self, records: CsvRecords) -> Result<()> {
        self.buffered_bytes += estimate_csv_records_size(&records);
        self.records.push(records);
        if self.buffered_bytes >= self.max_buffer_bytes {
            self.flush_buffered_records()?;
        }
        Ok(())
    }

    /// Flushes all appended batches to CSV files.
    pub fn flush_all(&mut self) -> Result<()> {
        self.flush_buffered_records()
    }

    fn flush_buffered_records(&mut self) -> Result<()> {
        for batch in &self.records {
            write_batch_csv(&self.run_dir, batch, &mut self.written_tables)?;
        }
        self.records.clear();
        self.buffered_bytes = 0;
        Ok(())
    }
}

fn write_seed_meta(run_dir: &Path, metadata: &CsvDumpMetadata) -> Result<()> {
    let path = run_dir.join("seed.meta");
    let mut file = File::create(&path).context(error::CreateFileSnafu {
        path: path.to_string_lossy().to_string(),
    })?;

    let content = format!(
        "target={}\nseed={}\nactions={}\npartitions={}\ntables={}\nstarted_at_unix_ms={}\n",
        metadata.target,
        metadata.seed,
        metadata.actions,
        metadata.partitions,
        metadata.tables,
        metadata.started_at_unix_ms,
    );
    file.write_all(content.as_bytes())
        .context(error::WriteFileSnafu {
            path: path.to_string_lossy().to_string(),
        })
}

fn write_batch_csv(
    run_dir: &Path,
    batch: &CsvRecords,
    written_tables: &mut HashSet<String>,
) -> Result<()> {
    let file_name = format!("{}.table-data.csv", sanitize_file_name(&batch.table_name));
    let path = run_dir.join(file_name);
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .context(error::CreateFileSnafu {
            path: path.to_string_lossy().to_string(),
        })?;

    if written_tables.insert(batch.table_name.clone()) {
        file.write_all(join_line(&batch.headers).as_bytes())
            .context(error::WriteFileSnafu {
                path: path.to_string_lossy().to_string(),
            })?;
        file.write_all(b"\n").context(error::WriteFileSnafu {
            path: path.to_string_lossy().to_string(),
        })?;
    }

    for record in &batch.records {
        file.write_all(join_line(&record.values).as_bytes())
            .context(error::WriteFileSnafu {
                path: path.to_string_lossy().to_string(),
            })?;
        file.write_all(b"\n").context(error::WriteFileSnafu {
            path: path.to_string_lossy().to_string(),
        })?;
    }

    Ok(())
}

fn estimate_csv_records_size(records: &CsvRecords) -> usize {
    let headers = records.headers.iter().map(String::len).sum::<usize>();
    let rows = records
        .records
        .iter()
        .flat_map(|record| record.values.iter())
        .map(String::len)
        .sum::<usize>();
    headers + rows
}

fn join_line(cells: &[String]) -> String {
    cells
        .iter()
        .map(|cell| escape_csv_cell(cell))
        .collect::<Vec<_>>()
        .join(",")
}

fn escape_csv_cell(value: &str) -> String {
    if value.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
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

fn build_run_dir(metadata: &CsvDumpMetadata) -> PathBuf {
    let base = PathBuf::from(get_gt_fuzz_dump_dir());
    let suffix = get_gt_fuzz_dump_suffix();
    let name = format!(
        "{}_seed_{}_actions_{}_ts_{}{}",
        metadata.target, metadata.seed, metadata.actions, metadata.started_at_unix_ms, suffix
    );
    base.join(name)
}

fn current_unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::{CsvDumpMetadata, CsvDumpSession};
    use crate::translator::csv::{CsvRecord, CsvRecords};

    #[test]
    fn test_create_session_and_flush() {
        let mut session = CsvDumpSession::new_with_buffer_limit(
            CsvDumpMetadata::new("fuzz_case", 1, 2, 3, 4),
            1024,
        )
        .unwrap();
        session
            .append(CsvRecords {
                table_name: "metric-a".to_string(),
                headers: vec!["host".to_string(), "value".to_string()],
                records: vec![CsvRecord {
                    values: vec!["web-1".to_string(), "10".to_string()],
                }],
            })
            .unwrap();
        session.flush_all().unwrap();

        assert!(session.run_dir.exists());
        assert!(session.run_dir.join("seed.meta").exists());
        assert!(session.run_dir.join("metric-a.table-data.csv").exists());
    }

    #[test]
    fn test_auto_flush_on_buffer_limit() {
        let mut session =
            CsvDumpSession::new_with_buffer_limit(CsvDumpMetadata::new("fuzz_case", 5, 2, 3, 4), 1)
                .unwrap();
        session
            .append(CsvRecords {
                table_name: "metric-b".to_string(),
                headers: vec!["host".to_string()],
                records: vec![CsvRecord {
                    values: vec!["web-2".to_string()],
                }],
            })
            .unwrap();

        assert!(session.run_dir.join("metric-b.table-data.csv").exists());
        assert_eq!(session.buffered_bytes, 0);
    }
}
