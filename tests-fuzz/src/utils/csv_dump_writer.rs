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

use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions, create_dir_all, remove_dir_all};
use std::io::Write;
use std::path::{Path, PathBuf};

use common_telemetry::{info, warn};
use common_time::util::current_time_millis;
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
            started_at_unix_ms: current_time_millis(),
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
    full_headers_by_table: HashMap<String, Vec<String>>,
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
        info!(
            "Create csv dump session, target: {}, run_dir: {}, max_buffer_bytes: {}",
            metadata.target,
            run_dir.display(),
            max_buffer_bytes
        );

        Ok(Self {
            metadata,
            run_dir,
            max_buffer_bytes,
            records: Vec::new(),
            buffered_bytes: 0,
            written_tables: HashSet::new(),
            full_headers_by_table: HashMap::new(),
        })
    }

    /// Appends one table CSV records batch with full table headers.
    pub fn append(&mut self, records: CsvRecords, full_headers: Vec<String>) -> Result<()> {
        self.full_headers_by_table
            .entry(records.table_name.clone())
            .or_insert(full_headers);
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

    /// Removes session directory after successful validation.
    pub fn cleanup_on_success(&self) -> std::io::Result<()> {
        match remove_dir_all(&self.run_dir) {
            Ok(_) => {
                info!(
                    "Cleanup csv dump directory on success: {}",
                    self.run_dir.display()
                );
                Ok(())
            }
            Err(err) => {
                warn!(
                    "Cleanup csv dump directory failed: {}, error: {:?}",
                    self.run_dir.display(),
                    err
                );
                Err(err)
            }
        }
    }

    fn flush_buffered_records(&mut self) -> Result<()> {
        if self.records.is_empty() {
            return Ok(());
        }
        for batch in &self.records {
            write_batch_csv(
                &self.run_dir,
                batch,
                &mut self.written_tables,
                &self.full_headers_by_table,
            )?;
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
    full_headers_by_table: &HashMap<String, Vec<String>>,
) -> Result<()> {
    let output_headers = full_headers_by_table
        .get(&batch.table_name)
        .cloned()
        .unwrap_or_else(|| batch.headers.clone());
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
        file.write_all(join_line(&output_headers).as_bytes())
            .context(error::WriteFileSnafu {
                path: path.to_string_lossy().to_string(),
            })?;
        file.write_all(b"\n").context(error::WriteFileSnafu {
            path: path.to_string_lossy().to_string(),
        })?;
    }

    let header_index = batch
        .headers
        .iter()
        .enumerate()
        .map(|(idx, header)| (header.as_str(), idx))
        .collect::<HashMap<_, _>>();

    for record in &batch.records {
        let aligned_values = output_headers
            .iter()
            .map(|header| {
                header_index
                    .get(header.as_str())
                    .and_then(|idx| record.values.get(*idx))
                    .cloned()
                    .unwrap_or_default()
            })
            .collect::<Vec<_>>();
        file.write_all(join_line(&aligned_values).as_bytes())
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
            .append(
                CsvRecords {
                    table_name: "metric-a".to_string(),
                    headers: vec!["host".to_string(), "value".to_string()],
                    records: vec![CsvRecord {
                        values: vec!["web-1".to_string(), "10".to_string()],
                    }],
                },
                vec!["host".to_string(), "value".to_string()],
            )
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
            .append(
                CsvRecords {
                    table_name: "metric-b".to_string(),
                    headers: vec!["host".to_string()],
                    records: vec![CsvRecord {
                        values: vec!["web-2".to_string()],
                    }],
                },
                vec!["host".to_string()],
            )
            .unwrap();

        assert!(session.run_dir.join("metric-b.table-data.csv").exists());
        assert_eq!(session.buffered_bytes, 0);
    }

    #[test]
    fn test_flush_with_partial_headers_uses_full_headers() {
        let mut session = CsvDumpSession::new_with_buffer_limit(
            CsvDumpMetadata::new("fuzz_case", 7, 2, 3, 4),
            1024,
        )
        .unwrap();
        session
            .append(
                CsvRecords {
                    table_name: "metric-c".to_string(),
                    headers: vec!["host".to_string(), "value".to_string()],
                    records: vec![CsvRecord {
                        values: vec!["web-3".to_string(), "12".to_string()],
                    }],
                },
                vec!["host".to_string(), "idc".to_string(), "value".to_string()],
            )
            .unwrap();
        session.flush_all().unwrap();

        let file =
            std::fs::read_to_string(session.run_dir.join("metric-c.table-data.csv")).unwrap();
        let mut lines = file.lines();
        assert_eq!(lines.next().unwrap(), "host,idc,value");
        assert_eq!(lines.next().unwrap(), "web-3,,12");
    }
}
