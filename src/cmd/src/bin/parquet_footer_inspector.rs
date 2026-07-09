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

#![allow(clippy::print_stdout)]

use std::collections::BTreeSet;
use std::fs::{self, File};
use std::path::{Path, PathBuf};

use clap::Parser;
use parquet::file::reader::{FileReader, SerializedFileReader};
use serde::Serialize;

#[derive(Debug, Parser)]
#[command(about = "Inspect local Parquet/SST footer metadata as JSON")]
struct Args {
    #[arg(long)]
    root: PathBuf,
    #[arg(long, default_value = "greptime_value")]
    column: String,
    #[arg(long)]
    include_metadata_files: bool,
}

#[derive(Debug, Serialize)]
struct Report {
    root: String,
    summary: Summary,
    files: Vec<FileReport>,
}

#[derive(Debug, Default, Serialize)]
struct Summary {
    column: String,
    file_count: usize,
    files_with_column: usize,
    total_file_size: u64,
    total_rows: i64,
    column_compressed_size: i64,
    column_uncompressed_size: i64,
    unique_encodings: Vec<String>,
}

#[derive(Debug, Serialize)]
struct FileReport {
    path: String,
    relative_path: String,
    file_size: u64,
    num_rows: i64,
    num_row_groups: usize,
    columns: Vec<ColumnChunkReport>,
}

#[derive(Debug, Serialize)]
struct ColumnChunkReport {
    row_group_index: usize,
    column_index: usize,
    column_path: String,
    encodings: Vec<String>,
    compression: String,
    compressed_size: i64,
    uncompressed_size: i64,
    num_values: i64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let files = collect_parquet_files(&args.root, args.include_metadata_files)?;
    let reports = files
        .iter()
        .map(|path| inspect_file(&args.root, path, &args.column))
        .collect::<Result<Vec<_>, _>>()?;
    let summary = summarize(&args.column, &reports);
    println!(
        "{}",
        serde_json::to_string_pretty(&Report {
            root: args.root.display().to_string(),
            summary,
            files: reports,
        })?
    );
    Ok(())
}

fn collect_parquet_files(
    root: &Path,
    include_metadata_files: bool,
) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    let mut files = Vec::new();
    collect_parquet_files_inner(root, root, include_metadata_files, &mut files)?;
    files.sort();
    Ok(files)
}

fn collect_parquet_files_inner(
    root: &Path,
    path: &Path,
    include_metadata_files: bool,
    files: &mut Vec<PathBuf>,
) -> Result<(), Box<dyn std::error::Error>> {
    if path.is_dir() {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            collect_parquet_files_inner(root, &entry.path(), include_metadata_files, files)?;
        }
    } else if is_data_parquet(root, path, include_metadata_files) {
        files.push(path.to_path_buf());
    }
    Ok(())
}

fn is_data_parquet(root: &Path, path: &Path, include_metadata_files: bool) -> bool {
    let is_parquet = path.extension().is_some_and(|ext| ext == "parquet");
    let relative_path = path.strip_prefix(root).unwrap_or(path);
    is_parquet && (include_metadata_files || !has_metadata_segment(relative_path))
}

fn has_metadata_segment(path: &Path) -> bool {
    path.components()
        .any(|component| component.as_os_str() == "metadata")
}

fn inspect_file(
    root: &Path,
    path: &Path,
    column: &str,
) -> Result<FileReport, Box<dyn std::error::Error>> {
    let file_size = fs::metadata(path)?.len();
    let reader = SerializedFileReader::new(File::open(path)?)?;
    let metadata = reader.metadata().file_metadata();
    let row_groups = reader.metadata().row_groups();
    let mut columns = Vec::new();
    for (row_group_index, row_group) in row_groups.iter().enumerate() {
        for (column_index, chunk) in row_group.columns().iter().enumerate() {
            let column_path = chunk.column_path().string();
            if column_path == column {
                columns.push(ColumnChunkReport {
                    row_group_index,
                    column_index,
                    column_path,
                    encodings: chunk
                        .encodings()
                        .map(|encoding| format!("{encoding:?}"))
                        .collect(),
                    compression: format!("{:?}", chunk.compression()),
                    compressed_size: chunk.compressed_size(),
                    uncompressed_size: chunk.uncompressed_size(),
                    num_values: chunk.num_values(),
                });
            }
        }
    }
    Ok(FileReport {
        path: path.display().to_string(),
        relative_path: path
            .strip_prefix(root)
            .unwrap_or(path)
            .display()
            .to_string(),
        file_size,
        num_rows: metadata.num_rows(),
        num_row_groups: row_groups.len(),
        columns,
    })
}

fn summarize(column: &str, files: &[FileReport]) -> Summary {
    let mut encodings = BTreeSet::new();
    let mut summary = Summary {
        column: column.to_string(),
        file_count: files.len(),
        ..Default::default()
    };
    for file in files {
        summary.total_file_size += file.file_size;
        summary.total_rows += file.num_rows;
        if !file.columns.is_empty() {
            summary.files_with_column += 1;
        }
        for column in &file.columns {
            summary.column_compressed_size += column.compressed_size;
            summary.column_uncompressed_size += column.uncompressed_size;
            encodings.extend(column.encodings.iter().cloned());
        }
    }
    summary.unique_encodings = encodings.into_iter().collect();
    summary
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filters_metadata_segment_by_default() {
        let root = Path::new("/tmp/metadata/root");
        assert!(is_data_parquet(
            root,
            Path::new("/tmp/metadata/root/data/a.parquet"),
            false
        ));
        assert!(!is_data_parquet(
            root,
            Path::new("/tmp/metadata/root/region/metadata/a.parquet"),
            false
        ));
        assert!(is_data_parquet(
            root,
            Path::new("/tmp/metadata/root/region/metadata/a.parquet"),
            true
        ));
        assert!(!is_data_parquet(
            root,
            Path::new("/tmp/metadata/root/region/data/a.sst"),
            false
        ));
    }

    #[test]
    fn summarizes_column_chunks() {
        let files = vec![
            FileReport {
                path: "a.parquet".to_string(),
                relative_path: "a.parquet".to_string(),
                file_size: 100,
                num_rows: 10,
                num_row_groups: 1,
                columns: vec![ColumnChunkReport {
                    row_group_index: 0,
                    column_index: 1,
                    column_path: "greptime_value".to_string(),
                    encodings: vec!["PLAIN".to_string(), "RLE".to_string()],
                    compression: "ZSTD".to_string(),
                    compressed_size: 30,
                    uncompressed_size: 80,
                    num_values: 10,
                }],
            },
            FileReport {
                path: "b.parquet".to_string(),
                relative_path: "b.parquet".to_string(),
                file_size: 50,
                num_rows: 5,
                num_row_groups: 1,
                columns: vec![ColumnChunkReport {
                    row_group_index: 0,
                    column_index: 1,
                    column_path: "greptime_value".to_string(),
                    encodings: vec!["BYTE_STREAM_SPLIT".to_string(), "RLE".to_string()],
                    compression: "ZSTD".to_string(),
                    compressed_size: 10,
                    uncompressed_size: 20,
                    num_values: 5,
                }],
            },
        ];

        let summary = summarize("greptime_value", &files);
        assert_eq!(summary.file_count, 2);
        assert_eq!(summary.files_with_column, 2);
        assert_eq!(summary.total_file_size, 150);
        assert_eq!(summary.total_rows, 15);
        assert_eq!(summary.column_compressed_size, 40);
        assert_eq!(summary.column_uncompressed_size, 100);
        assert_eq!(
            summary.unique_encodings,
            vec!["BYTE_STREAM_SPLIT", "PLAIN", "RLE"]
        );
    }
}
