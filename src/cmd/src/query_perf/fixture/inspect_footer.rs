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

use parquet::file::reader::{FileReader, SerializedFileReader};

use crate::query_perf::error::{Error, Result};
use crate::query_perf::fixture::{
    FooterColumnChunkObservation, FooterFileObservation, FooterObservation, FooterRequest,
    FooterSummary, validate_footer_request,
};

pub fn inspect_footer(request: FooterRequest) -> Result<FooterObservation> {
    validate_footer_request(&request)?;
    let files = collect_parquet_files(&request.root, request.include_metadata_files)?;
    let reports = files
        .iter()
        .map(|p| inspect_file(&request.root, p, &request.column))
        .collect::<Result<Vec<_>>>()?;
    Ok(FooterObservation {
        root: request.root.display().to_string(),
        summary: summarize_footer(&request.column, &reports),
        files: reports,
    })
}
fn collect_parquet_files(root: &Path, include_metadata_files: bool) -> Result<Vec<PathBuf>> {
    if !root.exists() {
        return Err(Error::new(format!(
            "footer root does not exist: {}",
            root.display()
        )));
    }
    if !root.is_dir() {
        return Err(Error::new(format!(
            "footer root is not a directory: {}",
            root.display()
        )));
    }
    let mut files = Vec::new();
    collect_parquet_files_inner(root, root, include_metadata_files, &mut files)?;
    files.sort();
    if files.is_empty() {
        return Err(Error::new(format!(
            "footer root contains no parquet files: {}",
            root.display()
        )));
    }
    Ok(files)
}
fn collect_parquet_files_inner(
    root: &Path,
    path: &Path,
    include_metadata_files: bool,
    files: &mut Vec<PathBuf>,
) -> Result<()> {
    if path.is_dir() {
        for entry in fs::read_dir(path)
            .map_err(|err| Error::new(format!("failed to read {}: {err}", path.display())))?
        {
            let entry = entry
                .map_err(|err| Error::new(format!("failed to read directory entry: {err}")))?;
            collect_parquet_files_inner(root, &entry.path(), include_metadata_files, files)?;
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
fn inspect_file(root: &Path, path: &Path, column: &str) -> Result<FooterFileObservation> {
    let file_size = fs::metadata(path)
        .map_err(|err| Error::new(format!("failed to read {} metadata: {err}", path.display())))?
        .len();
    let reader = SerializedFileReader::new(
        File::open(path)
            .map_err(|err| Error::new(format!("failed to open {}: {err}", path.display())))?,
    )
    .map_err(|err| {
        Error::new(format!(
            "failed to read parquet footer {}: {err}",
            path.display()
        ))
    })?;
    let metadata = reader.metadata().file_metadata();
    let row_groups = reader.metadata().row_groups();
    let mut columns = Vec::new();
    for (row_group_index, rg) in row_groups.iter().enumerate() {
        for (column_index, chunk) in rg.columns().iter().enumerate() {
            let column_path = chunk.column_path().string();
            if column_path == column {
                columns.push(FooterColumnChunkObservation {
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
    Ok(FooterFileObservation {
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
fn summarize_footer(column: &str, files: &[FooterFileObservation]) -> FooterSummary {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn footer_root_must_exist_be_a_directory_and_contain_parquet() {
        let root = tempfile::tempdir().unwrap_or_else(|err| panic!("temporary directory: {err}"));
        assert!(collect_parquet_files(&root.path().join("missing"), false).is_err());
        let file = root.path().join("not-a-directory");
        fs::write(&file, "x").unwrap_or_else(|err| panic!("temporary file: {err}"));
        assert!(collect_parquet_files(&file, false).is_err());
        assert!(collect_parquet_files(root.path(), false).is_err());
    }
}
