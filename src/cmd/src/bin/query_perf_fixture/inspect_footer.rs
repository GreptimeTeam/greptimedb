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

use std::collections::BTreeSet;
use std::fs::{self, File};
use std::path::{Path, PathBuf};

use clap::Args as ClapArgs;
use parquet::file::reader::{FileReader, SerializedFileReader};
use serde::Serialize;

#[derive(Debug, ClapArgs)]
pub(super) struct InspectFooterArgs {
    #[arg(long)]
    root: PathBuf,
    #[arg(long, default_value = "greptime_value")]
    column: String,
    #[arg(long)]
    include_metadata_files: bool,
}

#[derive(Debug, Serialize)]
struct FooterReport {
    root: String,
    summary: FooterSummary,
    files: Vec<FooterFileReport>,
}
#[derive(Debug, Default, Serialize)]
struct FooterSummary {
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
struct FooterFileReport {
    path: String,
    relative_path: String,
    file_size: u64,
    num_rows: i64,
    num_row_groups: usize,
    columns: Vec<FooterColumnChunkReport>,
}
#[derive(Debug, Serialize)]
struct FooterColumnChunkReport {
    row_group_index: usize,
    column_index: usize,
    column_path: String,
    encodings: Vec<String>,
    compression: String,
    compressed_size: i64,
    uncompressed_size: i64,
    num_values: i64,
}

pub(super) fn run_inspect_footer(
    args: InspectFooterArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    let files = collect_parquet_files(&args.root, args.include_metadata_files)?;
    let reports = files
        .iter()
        .map(|p| inspect_file(&args.root, p, &args.column))
        .collect::<Result<Vec<_>, _>>()?;
    println!(
        "{}",
        serde_json::to_string_pretty(&FooterReport {
            root: args.root.display().to_string(),
            summary: summarize_footer(&args.column, &reports),
            files: reports
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
            collect_parquet_files_inner(root, &entry?.path(), include_metadata_files, files)?;
        }
    } else if path.extension().is_some_and(|ext| ext == "parquet")
        && (include_metadata_files
            || !path
                .strip_prefix(root)
                .unwrap_or(path)
                .components()
                .any(|c| c.as_os_str() == "metadata"))
    {
        files.push(path.to_path_buf());
    }
    Ok(())
}
fn inspect_file(
    root: &Path,
    path: &Path,
    column: &str,
) -> Result<FooterFileReport, Box<dyn std::error::Error>> {
    let file_size = fs::metadata(path)?.len();
    let reader = SerializedFileReader::new(File::open(path)?)?;
    let metadata = reader.metadata().file_metadata();
    let row_groups = reader.metadata().row_groups();
    let mut columns = Vec::new();
    for (row_group_index, rg) in row_groups.iter().enumerate() {
        for (column_index, chunk) in rg.columns().iter().enumerate() {
            let column_path = chunk.column_path().string();
            if column_path == column {
                columns.push(FooterColumnChunkReport {
                    row_group_index,
                    column_index,
                    column_path,
                    encodings: chunk.encodings().map(|e| format!("{e:?}")).collect(),
                    compression: format!("{:?}", chunk.compression()),
                    compressed_size: chunk.compressed_size(),
                    uncompressed_size: chunk.uncompressed_size(),
                    num_values: chunk.num_values(),
                });
            }
        }
    }
    Ok(FooterFileReport {
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
fn summarize_footer(column: &str, files: &[FooterFileReport]) -> FooterSummary {
    let mut encodings = BTreeSet::new();
    let mut s = FooterSummary {
        column: column.to_string(),
        file_count: files.len(),
        ..Default::default()
    };
    for file in files {
        s.total_file_size += file.file_size;
        s.total_rows += file.num_rows;
        if !file.columns.is_empty() {
            s.files_with_column += 1;
        }
        for c in &file.columns {
            s.column_compressed_size += c.compressed_size;
            s.column_uncompressed_size += c.uncompressed_size;
            encodings.extend(c.encodings.iter().cloned());
        }
    }
    s.unique_encodings = encodings.into_iter().collect();
    s
}
